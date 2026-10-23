/*
 * embed_test.c - hcore inside an application's own event loop, the way htm8
 * runs it (one loop per worker thread, every thread listening on one port):
 *
 *   - hc_reactor_new_on_base(): the test owns the event_base and runs it
 *   - hc_listen_options.reuse_port: a second server binds the same port
 *   - hc_server_pause_accept / resume_accept
 *   - hc_server_drain(): idle keep-alive connections close at once; a
 *     response not yet started carries "Connection: close"; one already under
 *     way completes and then closes; connection count reaches 0; resume ends
 *     the drain
 *
 * The client is a thread with plain blocking sockets, so the test sees exactly
 * what goes over the wire. The server side runs in the loop and is driven by
 * request paths; a polling timer moves the test through its phases.
 */
#include "hc.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <event2/event.h>

#define PORT 9310

static int passed, failed;

#define CHECK(c) do {                                                      \
        if (c) { passed++; }                                               \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } \
    } while (0)

static struct event_base *base;
static hc_reactor        *R;
static hc_server         *S;

/* ======================================================================== */
/* Server: behaviour chosen by path                                         */
/* ======================================================================== */

typedef struct test_handler {
    hc_server_handler    base;
    hc_response_channel *rc;
    hc_data_channel     *out;
    hc_timer            *timer;
    char                 path[64];
} test_handler;

#define TH(self) ((test_handler *)(void *)(self))

static hc_timer *resume_timer;

static void
resume_cb(void *arg)
{
    (void)arg;
    hc_server_resume_accept(S);
}

static void
later_cb(void *arg)
{
    test_handler *t = arg;

    if (strcmp(t->path, "/drain-then-respond") == 0) {
        hc_response_send_simple(t->rc, 200, "text/plain", "drained", 0);
    } else if (strcmp(t->path, "/stream-then-drain") == 0 && t->out) {
        hc_data_write(t->out, "-second", 7);
        hc_data_end(t->out, NULL);
        t->out = NULL;
    }
}

static void
th_handle_request(hc_server_handler *self, hc_server_exchange *ex,
                  const hc_request_head *head, const hc_entity *entity,
                  hc_response_channel *rc)
{
    test_handler *t = TH(self);

    (void)ex;
    (void)entity;
    t->rc = rc;
    snprintf(t->path, sizeof(t->path), "%s", head->target);

    if (strcmp(t->path, "/pause") == 0) {
        /* stop accepting for 300 ms */
        hc_server_pause_accept(S);
        resume_timer = hc_timer_new(R, resume_cb, NULL);
        hc_timer_start(resume_timer, 300);
        hc_response_send_simple(rc, 200, "text/plain", "paused", 0);
    } else if (strcmp(t->path, "/drain-then-respond") == 0) {
        /* drain first, answer 100 ms later: the response hasn't started */
        hc_server_drain(S);
        t->timer = hc_timer_new(R, later_cb, t);
        hc_timer_start(t->timer, 100);
    } else if (strcmp(t->path, "/stream-then-drain") == 0) {
        /* the response is under way when the drain starts */
        hc_headers      *h = hc_headers_new();
        hc_response_head rh = { 200, 11, h };
        hc_entity        e  = { -1, "text/plain" };

        t->out = hc_response_send(rc, &rh, &e);
        hc_headers_free(h);
        hc_data_write(t->out, "first", 5);
        hc_server_drain(S);
        t->timer = hc_timer_new(R, later_cb, t);
        hc_timer_start(t->timer, 100);
    } else {
        hc_response_send_simple(rc, 200, "text/plain", "ok", 0);
    }
}

static void th_consume(hc_server_handler *s, const void *d, size_t n) { (void)s; (void)d; (void)n; }
static void th_stream_end(hc_server_handler *s, const hc_headers *t) { (void)s; (void)t; }

static void
th_release(hc_server_handler *self)
{
    test_handler *t = TH(self);

    if (t->timer) {
        hc_timer_free(t->timer);
    }

    free(t);
}

static const hc_server_handler_vtbl th_vtbl = {
    .handle_request = th_handle_request,
    .consume        = th_consume,
    .stream_end     = th_stream_end,
    .release        = th_release,
};

static hc_server_handler *
th_factory(void *arg)
{
    test_handler *t = calloc(1, sizeof(*t));

    (void)arg;
    t->base.vtbl = &th_vtbl;
    return &t->base;
}

/* ======================================================================== */
/* Client thread: plain sockets                                             */
/* ======================================================================== */

static double
now_s(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

static int
connect_to(void)
{
    struct sockaddr_in sin = { .sin_family = AF_INET, .sin_port = htons(PORT) };
    struct timeval     tv  = { 3, 0 };
    int                fd  = socket(AF_INET, SOCK_STREAM, 0);

    inet_pton(AF_INET, "127.0.0.1", &sin.sin_addr);
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    if (connect(fd, (struct sockaddr *)&sin, sizeof(sin)) != 0) {
        close(fd);
        return -1;
    }

    return fd;
}

static void
send_get(int fd, const char *path)
{
    char req[256];
    int  n = snprintf(req, sizeof(req), "GET %s HTTP/1.1\r\nHost: test\r\n\r\n", path);

    (void)!write(fd, req, (size_t)n);
}

/* One response: headers, then a Content-Length body or a chunked body. */
static int
read_response(int fd, char *buf, size_t cap)
{
    size_t len = 0;

    for (;;) {
        char   *hdr_end;
        ssize_t n;

        buf[len] = '\0';

        if ((hdr_end = strstr(buf, "\r\n\r\n")) != NULL) {
            const char *cl   = strcasestr(buf, "Content-Length:");
            size_t      head = (size_t)(hdr_end + 4 - buf);

            if (cl && cl < hdr_end && len >= head + (size_t)atoi(cl + 15)) {
                return (int)len;
            }

            if (strcasestr(buf, "Transfer-Encoding: chunked") && strstr(hdr_end, "\r\n0\r\n\r\n")) {
                return (int)len;
            }
        }

        if (len + 1 >= cap || (n = read(fd, buf + len, cap - len - 1)) <= 0) {
            return -1;
        }

        len += (size_t)n;
    }
}

/* 1 if the peer closes within the socket's receive timeout. */
static int
sees_eof(int fd)
{
    char c;

    return read(fd, &c, 1) == 0;
}

static volatile struct {   /* shared with the client thread */
    int    plain_ok, plain_close;
    int    close_ok;
    double close_waited;
    int    pause_ok;
    double pause_waited;
    int    idle_closed;
    int    drained_ok, drained_close, drained_eof;
    int    stream_ok, stream_close, stream_eof;
    int    phase;               /* client -> loop: 1 = drain done, 2 = all done */
} C;

static void *
client_main(void *arg)
{
    char   buf[8192];
    int    idle, fd;
    double t0;

    (void)arg;

    /* the client asks to close: response, then the server closes at once */
    fd = connect_to();
    t0 = now_s();
    (void)!write(fd, "GET /plain HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n", 54);
    C.close_ok     = read_response(fd, buf, sizeof(buf)) > 0 && strcasestr(buf, "Connection: close") && sees_eof(fd);
    C.close_waited = now_s() - t0;
    close(fd);

    /* keep-alive request; the connection then sits idle */
    idle = connect_to();
    send_get(idle, "/plain");
    C.plain_ok    = read_response(idle, buf, sizeof(buf)) > 0 && strstr(buf, "\r\n\r\nok");
    C.plain_close = strcasestr(buf, "Connection: close") != NULL;

    /* pause accepting for 300 ms; a new connection waits until resumed */
    fd = connect_to();
    send_get(fd, "/pause");
    read_response(fd, buf, sizeof(buf));
    close(fd);

    t0 = now_s();
    fd = connect_to();          /* the kernel completes it; accept() waits */
    send_get(fd, "/plain");
    C.pause_ok     = read_response(fd, buf, sizeof(buf)) > 0 && strstr(buf, "\r\n\r\nok");
    C.pause_waited = now_s() - t0;
    close(fd);

    /* drain while a response is still to come */
    fd = connect_to();
    send_get(fd, "/drain-then-respond");
    C.drained_ok    = read_response(fd, buf, sizeof(buf)) > 0 && strstr(buf, "drained");
    C.drained_close = strcasestr(buf, "Connection: close") != NULL;
    C.drained_eof   = sees_eof(fd);
    close(fd);

    C.idle_closed = sees_eof(idle);    /* the idle keep-alive connection */
    close(idle);

    C.phase = 1;                /* the loop waits for 0 connections, resumes */

    while (C.phase != 3) {
        usleep(10000);
    }

    /* drain while a response is under way */
    fd = connect_to();
    send_get(fd, "/stream-then-drain");
    C.stream_ok    = read_response(fd, buf, sizeof(buf)) > 0 && strstr(buf, "first") && strstr(buf, "-second");
    C.stream_close = strcasestr(buf, "Connection: close") != NULL;
    C.stream_eof   = sees_eof(fd);
    close(fd);

    C.phase = 4;
    return NULL;
}

/* ======================================================================== */
/* The loop's side                                                          */
/* ======================================================================== */

static int64_t   deadline;
static hc_timer *poll_timer;

static void
poll_cb(void *arg)
{
    (void)arg;

    if (hc_reactor_now_ms(R) > deadline) {
        fprintf(stderr, "  FAIL: timed out in phase %d\n", C.phase);
        failed++;
        event_base_loopbreak(base);
        return;
    }

    if (C.phase == 1 && hc_server_connection_count(S) == 0) {
        CHECK(hc_server_resume_accept(S) == 0);    /* ends the drain */
        C.phase = 3;
    } else if (C.phase == 4 && hc_server_connection_count(S) == 0) {
        event_base_loopbreak(base);
        return;
    }

    hc_timer_start(poll_timer, 10);
}

int
main(void)
{
    hc_listen_options reuse = { .backlog = 16, .reuse_port = 1, .defer_accept = 1 };
    hc_server        *twin, *plain;
    pthread_t         th;

    /* The application's loop, not hcore's. */
    base = event_base_new();
    R    = hc_reactor_new_on_base(base);
    CHECK(R != NULL);
    CHECK(hc_reactor_new_on_base(NULL) == NULL);

    S = hc_server_new(R, th_factory, NULL);
    CHECK(hc_server_listen_opts(S, "127.0.0.1", PORT, &reuse) == 0);

    /* SO_REUSEPORT: a second server (another thread, in htm8) on the same
     * port; without the option the bind fails. */
    twin  = hc_server_new(R, th_factory, NULL);
    plain = hc_server_new(R, th_factory, NULL);
    CHECK(hc_server_listen_opts(twin, "127.0.0.1", PORT, &reuse) == 0);
    CHECK(hc_server_listen(plain, "127.0.0.1", PORT) != 0);
    hc_server_unlisten(twin);
    hc_server_unlisten(twin);                   /* twice is harmless */
    CHECK(hc_server_pause_accept(twin) == -1);  /* not listening */
    hc_server_free(twin);
    hc_server_free(plain);
    CHECK(hc_server_connection_count(S) == 0);

    deadline   = hc_reactor_now_ms(R) + 15000;
    poll_timer = hc_timer_new(R, poll_cb, NULL);
    hc_timer_start(poll_timer, 10);

    pthread_create(&th, NULL, client_main, NULL);
    event_base_dispatch(base);                  /* the application runs it */
    C.phase = (C.phase == 4) ? 4 : 3;           /* release a waiting client */
    pthread_join(th, NULL);

    CHECK(C.close_ok);                          /* "Connection: close" honoured */
    CHECK(C.close_waited < 1.0);                /* ... at once, not on a timeout */
    CHECK(C.plain_ok && !C.plain_close);
    CHECK(C.pause_ok);
    CHECK(C.pause_waited >= 0.25);              /* held while paused */
    CHECK(C.drained_ok && C.drained_close);     /* not started: "Connection: close" */
    CHECK(C.drained_eof);                       /* ... and closed after it */
    CHECK(C.idle_closed);                       /* idle keep-alive closed at once */
    CHECK(C.stream_ok);                         /* under way: completes ... */
    CHECK(!C.stream_close);                     /* (headers already sent) */
    CHECK(C.stream_eof);                        /* ... then closes */
    CHECK(hc_server_connection_count(S) == 0);

    printf("  pause held a new connection for %.0f ms\n", C.pause_waited * 1000);
    printf("  \"Connection: close\" closed after %.0f ms\n", C.close_waited * 1000);

    hc_timer_free(poll_timer);
    hc_timer_free(resume_timer);
    hc_server_free(S);
    hc_reactor_free(R);                         /* leaves the base alone ... */
    CHECK(event_base_get_method(base) != NULL); /* ... which still works */
    event_base_free(base);

    printf("embed_test: %d checks, %d failed\n", passed + failed, failed);
    return failed != 0;
}
