/*
 * timeout_test.c - connection timeouts and the request body limit, on both
 * sides of the engine.
 *
 * Server (hc_server_set_timeouts 300 ms / 300 ms, hc_server_set_max_body
 * 1000 bytes), driven by raw sockets:
 *   - a client that connects and sends nothing is closed
 *   - a client that stops in the middle of a request head is closed
 *   - an idle keep-alive connection is closed after its response
 *   - a slow handler (800 ms) is not cut off by the read timeout
 *   - a client that stops reading a large response is closed (write
 *     timeout) and the handler is told
 *   - a body within the limit is served
 *   - Content-Length over the limit: 413 without the handler, no
 *     "100 Continue", connection closed
 *   - a chunked body that grows over the limit: 413, the handler gets
 *     ->failed("request body too large"), connection closed
 * The same server over HTTP/2 (curl, when it has HTTP/2 and the libevhtp
 * build does too):
 *   - a slow stream (800 ms) completes: no read timeout while it is busy
 *   - Content-Length over the limit: 413; the connection stays usable
 *   - a body without Content-Length that grows over the limit: 413
 *
 * Client (requester timeouts 300 ms / 300 ms):
 *   - an origin that never answers: "upstream read timeout"
 *   - a slow origin within the timeout: fine
 *   - hc_client_set_timeouts() raising the read timeout for one exchange
 *   - an upload slower than the read timeout: fine (counted from the end
 *     of the request)
 *   - an origin that never reads a large upload: "upstream write timeout"
 *   - a pooled connection idle for longer than the read timeout stays
 *     open and is reused
 *
 * Usage: timeout_test <tests/tls dir>
 */
#define _GNU_SOURCE     /* strcasestr, popen */
#include "hc.h"
#include "hc_pool.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <event2/event.h>
#include <event2/thread.h>

#define PORT_H1     9350    /* plain HTTP/1.1 server                */
#define PORT_TLS    9351    /* TLS (HTTP/2 when available)          */
#define PORT_ORIGIN 9352    /* origin for the client cases          */
#define PORT_SINK   9353    /* accepts nothing, reads nothing       */

/* 300 ms each; HC_TEST_SCALE=N multiplies them (and the test's
 * tolerances), for slow runs such as valgrind. */
static int K        = 1;
static int READ_MS  = 300;
static int WRITE_MS = 300;
static int SLACK    = 700;
#define MAX_BODY 1000

static int passed, failed;

#define CHECK(c) do {                                                      \
        if (c) { passed++; }                                               \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } \
    } while (0)

static struct event_base *base;
static hc_reactor        *R;

/* What the server handlers saw (written on the loop thread). */
static volatile int handled_upload;     /* /upload handle_request calls */
static volatile int upload_length;      /* its Content-Length (-1: none) */
static char         last_failure[128];
static volatile int failures;

static int64_t
now_ms(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void
sleep_ms(int ms)
{
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };

    nanosleep(&ts, NULL);
}

/* ======================================================================== */
/* Server handler                                                           */
/* ======================================================================== */

typedef struct th {
    hc_server_handler    base;
    hc_response_channel *rc;
    char                 path[64];
    size_t               got;
    size_t               sent, total;
    hc_timer            *timer;
} th;

#define TH(self) ((th *)(void *)(self))

static void
reply(th *t, int status, const char *body)
{
    hc_response_send_simple(t->rc, status, "text/plain", body, 0);
}

static void
slow_cb(void *arg)
{
    reply(arg, 200, "slow done");
}

static void
h_request(hc_server_handler *self, hc_server_exchange *ex, const hc_request_head *head,
          const hc_entity *entity, hc_response_channel *rc)
{
    th *t = TH(self);

    (void)ex;
    (void)entity;
    t->rc = rc;
    snprintf(t->path, sizeof(t->path), "%s", head->target);

    if (strcmp(t->path, "/hello") == 0) {
        reply(t, 200, "hello");
    } else if (strncmp(t->path, "/slow/", 6) == 0) {
        t->timer = hc_timer_new(R, slow_cb, t);
        hc_timer_start(t->timer, atoi(t->path + 6));
    } else if (strcmp(t->path, "/stall") == 0) {
        /* never answers */
    } else if (strncmp(t->path, "/big/", 5) == 0) {
        hc_headers      *h  = hc_headers_new();
        hc_response_head rh = { 200, 11, h };
        hc_entity        e  = { -1, "application/octet-stream" };

        t->total = (size_t)atol(t->path + 5);
        hc_response_send(rc, &rh, &e);
        hc_headers_free(h);
    } else if (strcmp(t->path, "/upload") == 0) {
        handled_upload++;           /* answered at stream_end */
        upload_length = entity ? (int)entity->content_length : 0;
    } else {
        reply(t, 404, "?");
    }
}

static void
h_consume(hc_server_handler *self, const void *data, size_t len)
{
    (void)data;
    TH(self)->got += len;
}

static void
h_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    th  *t = TH(self);
    char msg[64];

    (void)trailers;

    if (strcmp(t->path, "/upload") == 0) {
        snprintf(msg, sizeof(msg), "n=%zu", t->got);
        reply(t, 200, msg);
    }
}

static void
h_produce(hc_server_handler *self, hc_data_channel *ch)
{
    th  *t = TH(self);
    char buf[16384];

    memset(buf, 'x', sizeof(buf));

    while (t->sent < t->total) {
        size_t room = hc_data_capacity(ch);
        size_t n    = t->total - t->sent;

        if (room == 0) {
            return;
        }

        n = n < room ? n : room;
        n = n < sizeof(buf) ? n : sizeof(buf);

        if (hc_data_write(ch, buf, n) != (long)n) {
            return;
        }

        t->sent += n;
    }

    hc_data_end(ch, NULL);
}

static void
h_failed(hc_server_handler *self, const char *reason)
{
    (void)self;
    snprintf(last_failure, sizeof(last_failure), "%s", reason);
    failures++;
}

static void
h_release(hc_server_handler *self)
{
    th *t = TH(self);

    if (t->timer) {
        hc_timer_free(t->timer);
    }

    free(t);
}

static const hc_server_handler_vtbl vt = {
    .handle_request = h_request,
    .consume        = h_consume,
    .stream_end     = h_stream_end,
    .produce        = h_produce,
    .failed         = h_failed,
    .release        = h_release,
};

static hc_server_handler *
factory(void *arg)
{
    th *t = calloc(1, sizeof(*t));

    (void)arg;
    t->base.vtbl = &vt;
    return &t->base;
}

/* ======================================================================== */
/* Raw sockets                                                              */
/* ======================================================================== */

static int
connect_to(int port)
{
    struct sockaddr_in sa;
    int                fd = socket(AF_INET, SOCK_STREAM, 0);

    memset(&sa, 0, sizeof(sa));
    sa.sin_family      = AF_INET;
    sa.sin_port        = htons((uint16_t)port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) {
        close(fd);
        return -1;
    }

    return fd;
}

static void
send_str(int fd, const char *s)
{
    size_t len = strlen(s);

    while (len > 0) {
        ssize_t n = send(fd, s, len, MSG_NOSIGNAL);

        if (n <= 0) {
            return;
        }

        s   += n;
        len -= (size_t)n;
    }
}

/* Read until the server closes (or limit_ms passes). Returns the bytes
 * read (the first cap-1 kept in out), and sets *closed / *ms (time until
 * the close, from the call). */
static size_t
read_until_close(int fd, char *out, size_t cap, int limit_ms, int *closed, int64_t *ms)
{
    int64_t start = now_ms();
    size_t  total = 0, kept = 0;
    char    buf[65536];

    *closed = 0;

    for (;;) {
        int64_t       left = limit_ms - (now_ms() - start);
        struct pollfd p    = { fd, POLLIN, 0 };
        ssize_t       n;

        if (left <= 0 || poll(&p, 1, (int)left) <= 0) {
            break;
        }

        n = recv(fd, buf, sizeof(buf), 0);

        if (n <= 0) {
            *closed = 1;    /* EOF or reset */
            break;
        }

        if (kept < cap - 1) {
            size_t k = (size_t)n < cap - 1 - kept ? (size_t)n : cap - 1 - kept;

            memcpy(out + kept, buf, k);
            kept += k;
        }

        total += (size_t)n;
    }

    out[kept] = '\0';
    *ms       = now_ms() - start;

    return total;
}

/* Read one response that ends with body text `end` (or until limit_ms). */
static int
read_response(int fd, char *out, size_t cap, const char *end, int limit_ms)
{
    int64_t start = now_ms();
    size_t  kept  = 0;

    out[0] = '\0';

    while (now_ms() - start < limit_ms && strstr(out, end) == NULL) {
        struct pollfd p = { fd, POLLIN, 0 };
        ssize_t       n;

        if (poll(&p, 1, 50) <= 0) {
            continue;
        }

        n = recv(fd, out + kept, cap - 1 - kept, 0);

        if (n <= 0) {
            break;
        }

        kept      += (size_t)n;
        out[kept]  = '\0';
    }

    return strstr(out, end) != NULL;
}

/* ======================================================================== */
/* Server cases                                                             */
/* ======================================================================== */

static void
server_cases(void)
{
    char    out[8192];
    int     fd, closed;
    int64_t ms;
    size_t  n;

    puts("server, HTTP/1.1:");

    /* silent client */
    fd = connect_to(PORT_H1);
    read_until_close(fd, out, sizeof(out), 3000 * K, &closed, &ms);
    CHECK(closed && ms >= READ_MS - 50 && ms < READ_MS + SLACK);
    printf("  silent client closed after %lld ms\n", (long long)ms);
    close(fd);

    /* stops in the middle of the request head */
    fd = connect_to(PORT_H1);
    send_str(fd, "GET /hello HTTP/1.1\r\nHost: x\r\n");
    read_until_close(fd, out, sizeof(out), 3000 * K, &closed, &ms);
    CHECK(closed && out[0] == '\0' && ms < READ_MS + SLACK);
    printf("  half a request head: closed after %lld ms\n", (long long)ms);
    close(fd);

    /* idle keep-alive */
    fd = connect_to(PORT_H1);
    send_str(fd, "GET /hello HTTP/1.1\r\nHost: x\r\n\r\n");
    CHECK(read_response(fd, out, sizeof(out), "hello", 2000 * K));
    read_until_close(fd, out, sizeof(out), 3000 * K, &closed, &ms);
    CHECK(closed && ms >= READ_MS - 50 && ms < READ_MS + SLACK);
    printf("  idle keep-alive closed %lld ms after the response\n", (long long)ms);
    close(fd);

    /* slow handler: longer than the read timeout */
    fd = connect_to(PORT_H1);
    snprintf(out, sizeof(out), "GET /slow/%d HTTP/1.1\r\nHost: x\r\n\r\n", 800 * K);
    send_str(fd, out);
    CHECK(read_response(fd, out, sizeof(out), "slow done", 3000 * K));
    CHECK(strncmp(out, "HTTP/1.1 200", 12) == 0);
    printf("  slow handler (800 ms): %s\n", strstr(out, "slow done") ? "answered" : "cut off");
    close(fd);

    /* a client that stops reading a large response */
    failures = 0;
    fd       = connect_to(PORT_H1);
    send_str(fd, "GET /big/268435456 HTTP/1.1\r\nHost: x\r\n\r\n");
    sleep_ms(WRITE_MS * 5);
    n = read_until_close(fd, out, sizeof(out), 5000, &closed, &ms);
    CHECK(closed && n < 268435456u);
    sleep_ms(100 * K);
    CHECK(failures == 1 && strcmp(last_failure, "client write timeout") == 0);
    printf("  client not reading: closed after %zu of 268435456 bytes; handler: %s\n", n,
           last_failure);
    close(fd);

    /* within the limit */
    {
        char req[2048], body[MAX_BODY + 1];

        memset(body, 'b', MAX_BODY);
        body[MAX_BODY] = '\0';
        snprintf(req, sizeof(req), "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n%s",
                 MAX_BODY, body);
        fd = connect_to(PORT_H1);
        send_str(fd, req);
        CHECK(read_response(fd, out, sizeof(out), "n=1000", 2000 * K));
        CHECK(strncmp(out, "HTTP/1.1 200", 12) == 0);
        printf("  body of %d bytes (the limit): %s\n", MAX_BODY,
               strncmp(out, "HTTP/1.1 200", 12) == 0 ? "200" : out);
        close(fd);
    }

    /* Content-Length over the limit, with Expect: 100-continue */
    handled_upload = 0;
    fd             = connect_to(PORT_H1);
    send_str(fd, "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: 2000\r\n"
                 "Expect: 100-continue\r\n\r\n");
    read_until_close(fd, out, sizeof(out), 3000 * K, &closed, &ms);
    CHECK(strncmp(out, "HTTP/1.1 413", 12) == 0);
    CHECK(strstr(out, "100 Continue") == NULL);
    CHECK(strcasestr(out, "connection: close") != NULL);
    CHECK(closed && ms < SLACK + 300);
    CHECK(handled_upload == 0);
    printf("  Content-Length 2000: %.12s, closed, handler not run\n", out);
    close(fd);

    /* chunked, growing over the limit */
    {
        char req[4096], body[1501];

        memset(body, 'c', 1500);
        body[1500] = '\0';
        snprintf(req, sizeof(req), "POST /upload HTTP/1.1\r\nHost: x\r\n"
                 "Transfer-Encoding: chunked\r\n\r\n5dc\r\n%s\r\n", body);
        handled_upload = 0;
        failures       = 0;
        fd             = connect_to(PORT_H1);
        send_str(fd, req);
        read_until_close(fd, out, sizeof(out), 3000 * K, &closed, &ms);
        CHECK(strncmp(out, "HTTP/1.1 413", 12) == 0);
        CHECK(closed && ms < SLACK + 300);
        sleep_ms(100 * K);
        CHECK(handled_upload == 1);
        CHECK(failures == 1 && strcmp(last_failure, "request body too large") == 0);
        printf("  chunked body of 1500: %.12s, closed; handler: %s\n", out, last_failure);
        close(fd);
    }
}

/* ======================================================================== */
/* HTTP/2 cases (curl)                                                      */
/* ======================================================================== */

static int
curl(const char *args, char *out, size_t cap)
{
    char   cmd[1024];
    FILE  *p;
    size_t n;

    snprintf(cmd, sizeof(cmd), "curl -sS -k --max-time 30 %s 2>&1", args);
    p      = popen(cmd, "r");
    n      = fread(out, 1, cap - 1, p);
    out[n] = '\0';

    return WEXITSTATUS(pclose(p));
}

static void
h2_cases(const char *tmpdir)
{
    char out[8192], args[1024], file[128];
    FILE *f;

    puts("server, HTTP/2:");

    snprintf(args, sizeof(args), "--http2 -w ' %%{http_version}' https://127.0.0.1:%d/slow/%d",
             PORT_TLS, 800 * K);
    CHECK(curl(args, out, sizeof(out)) == 0);
    CHECK(strcmp(out, "slow done 2") == 0);
    printf("  slow stream (800 ms): %s\n", out);

    snprintf(file, sizeof(file), "%s/body", tmpdir);
    f = fopen(file, "wb");
    for (int i = 0; i < 2000; i++) {
        fputc('d', f);
    }
    fclose(f);

    /* Content-Length over the limit, then another request on the same
     * connection */
    handled_upload = 0;
    snprintf(args, sizeof(args), "--http2 -w ' %%{http_code} %%{num_connects}\\n' "
             "--data-binary @%s https://127.0.0.1:%d/upload "
             "--next -k --http2 -w ' %%{http_code} %%{num_connects}' https://127.0.0.1:%d/hello",
             file, PORT_TLS, PORT_TLS);
    CHECK(curl(args, out, sizeof(out)) == 0);
    CHECK(strstr(out, " 413 1\n") != NULL && strstr(out, "hello 200 0") != NULL);
    CHECK(handled_upload == 0);
    printf("  Content-Length 2000: %s\n", strstr(out, " 413 1\n") ? "413, connection reused" : out);

    /* no Content-Length: an upload from stdin, whose length curl cannot
     * know (curl 8.15 adds Content-Length to a chunked --data-binary on
     * HTTP/2, which would test the case above again) */
    failures       = 0;
    handled_upload = 0;
    upload_length  = 0;
    last_failure[0] = '\0';
    snprintf(args, sizeof(args), "--http2 -w ' %%{http_code}' -X POST -T - "
             "https://127.0.0.1:%d/upload < %s", PORT_TLS, file);
    CHECK(curl(args, out, sizeof(out)) == 0);
    CHECK(strstr(out, " 413") != NULL);
    sleep_ms(100 * K);
    CHECK(handled_upload == 1 && upload_length == -1);   /* really no length */
    CHECK(failures == 1 && strcmp(last_failure, "request body too large") == 0);
    printf("  2000 bytes, no Content-Length: %s; handler: %s\n",
           strstr(out, " 413") ? "413" : out, last_failure);
}

/* ======================================================================== */
/* Client cases                                                             */
/* ======================================================================== */

typedef struct ch {
    hc_client_handler base;
    const char       *path;
    size_t            upload;       /* body bytes to send, slowly        */
    int               slow_upload;  /* in pieces, 150 ms apart           */
    int               waiting;      /* for the timer                     */
    int64_t           read_ms;      /* hc_client_set_timeouts(); 0: no call */
    size_t            sent;
    hc_timer         *timer;
    hc_data_channel  *ch;
    int               status;
    char              body[256];
    size_t            blen;
    int               ok;
    char              reason[128];
} ch_t;

#define CH(self) ((ch_t *)(void *)(self))

static void
c_produce_request(hc_client_handler *self, hc_request_channel *rc)
{
    ch_t            *c = CH(self);
    hc_headers      *h = hc_headers_new();
    hc_request_head  head = { c->upload ? "POST" : "GET", c->path, "127.0.0.1", 11, h };
    hc_entity        e    = { (int64_t)c->upload, "application/octet-stream" };

    if (c->read_ms) {
        hc_client_set_timeouts(hc_request_channel_exchange(rc), c->read_ms, -1);
    }

    c->ch = hc_request_send(rc, &head, c->upload ? &e : NULL);
    hc_headers_free(h);
}

static void
c_send_some(ch_t *c)
{
    char   buf[16384];
    size_t piece = c->slow_upload ? c->upload / 5 + 1 : sizeof(buf);

    memset(buf, 'u', sizeof(buf));

    while (c->sent < c->upload) {
        size_t room = hc_data_capacity(c->ch);
        size_t n    = c->upload - c->sent;

        if (room == 0) {
            return;
        }

        n = n < room ? n : room;
        n = n < piece ? n : piece;
        n = n < sizeof(buf) ? n : sizeof(buf);

        if (hc_data_write(c->ch, buf, n) != (long)n) {
            return;
        }

        c->sent += n;

        if (c->slow_upload && c->sent < c->upload) {
            c->waiting = 1;
            hc_timer_start(c->timer, 150 * K);
            return;
        }
    }

    hc_data_end(c->ch, NULL);
}

static void
c_timer(void *arg)
{
    ch_t *c = arg;

    c->waiting = 0;
    c_send_some(c);
}

static void
c_produce(hc_client_handler *self, hc_data_channel *dc)
{
    ch_t *c = CH(self);

    c->ch = dc;

    if (!c->slow_upload || !c->waiting) {
        c_send_some(c);
    }
}

static void
c_consume_response(hc_client_handler *self, const hc_response_head *head, const hc_entity *e)
{
    (void)e;
    CH(self)->status = head->status;
}

static void
c_consume(hc_client_handler *self, const void *data, size_t len)
{
    ch_t  *c = CH(self);
    size_t k = len < sizeof(c->body) - 1 - c->blen ? len : sizeof(c->body) - 1 - c->blen;

    memcpy(c->body + c->blen, data, k);
    c->blen          += k;
    c->body[c->blen]  = '\0';
}

static void
c_stream_end(hc_client_handler *self, const hc_headers *t)
{
    (void)t;
    CH(self)->ok = 1;
}

static void
c_failed(hc_client_handler *self, const char *reason)
{
    snprintf(CH(self)->reason, sizeof(CH(self)->reason), "%s", reason);
}

static void
c_release(hc_client_handler *self)
{
    (void)self;
    hc_reactor_stop(R);
}

static const hc_client_handler_vtbl cvt = {
    .produce_request  = c_produce_request,
    .consume_response = c_consume_response,
    .consume          = c_consume,
    .stream_end       = c_stream_end,
    .produce          = c_produce,
    .failed           = c_failed,
    .release          = c_release,
};

static void
idle_done(void *arg)
{
    (void)arg;
    hc_reactor_stop(R);
}

/* Runs one exchange to the end; returns its duration in ms. */
static int64_t
run_exchange(hc_requester *rq, int port, ch_t *c)
{
    hc_host target;
    int64_t start;

    c->base.vtbl = &cvt;
    c->timer     = hc_timer_new(R, c_timer, c);
    hc_host_init(&target, "http", "127.0.0.1", port);
    start = now_ms();

    if (hc_requester_execute(rq, &target, 2000 * K, &c->base) != 0) {
        snprintf(c->reason, sizeof(c->reason), "execute failed");
    } else {
        hc_reactor_run(R);
    }

    hc_timer_free(c->timer);
    return now_ms() - start;
}

static void
client_cases(hc_server *origin)
{
    hc_requester_config cfg = { 0 };
    hc_requester       *rq;
    int64_t             ms;
    int                 sink;

    puts("client:");

    cfg.read_timeout_ms  = READ_MS;
    cfg.write_timeout_ms = WRITE_MS;
    rq                   = hc_requester_new(R, &cfg);

    /* origin that never answers */
    {
        ch_t c = { .path = "/stall" };

        ms = run_exchange(rq, PORT_ORIGIN, &c);
        CHECK(!c.ok && strcmp(c.reason, "upstream read timeout") == 0);
        CHECK(ms >= READ_MS - 50 && ms < READ_MS + SLACK);
        printf("  origin never answers: \"%s\" after %lld ms\n", c.reason, (long long)ms);
    }

    /* slow origin within the timeout */
    {
        char path[32];
        ch_t c = { .path = path };

        snprintf(path, sizeof(path), "/slow/%d", 100 * K);

        run_exchange(rq, PORT_ORIGIN, &c);
        CHECK(c.ok && c.status == 200 && strcmp(c.body, "slow done") == 0);
        printf("  origin answers in 100 ms: %s\n", c.ok ? "ok" : c.reason);
    }

    /* per-exchange read timeout */
    {
        char path[32];
        ch_t c = { .path = path, .read_ms = 2000 * K };

        snprintf(path, sizeof(path), "/slow/%d", 800 * K);

        run_exchange(rq, PORT_ORIGIN, &c);
        CHECK(c.ok && strcmp(c.body, "slow done") == 0);
        printf("  hc_client_set_timeouts(2000): 800 ms answer %s\n", c.ok ? "ok" : c.reason);
    }

    /* upload slower than the read timeout (5 pieces, 150 ms apart) */
    {
        ch_t c = { .path = "/upload", .upload = 900, .slow_upload = 1 };

        ms = run_exchange(rq, PORT_ORIGIN, &c);
        CHECK(c.ok && strcmp(c.body, "n=900") == 0);
        CHECK(ms > READ_MS);
        printf("  slow upload (%lld ms): %s\n", (long long)ms, c.ok ? c.body : c.reason);
    }

    /* pooled connection idle for longer than the read timeout */
    {
        ch_t c1 = { .path = "/hello" }, c2 = { .path = "/hello" };

        hc_timer *idle = hc_timer_new(R, idle_done, NULL);
        size_t    conns;

        run_exchange(rq, PORT_ORIGIN, &c1);
        CHECK(c1.ok);
        conns = hc_server_connection_count(origin);
        hc_timer_start(idle, READ_MS * 3);  /* the loop runs meanwhile */
        hc_reactor_run(R);
        hc_timer_free(idle);
        CHECK(hc_server_connection_count(origin) == conns);
        CHECK(hc_pool_get_total_stats(hc_requester_pool(rq)).available >= 1);
        run_exchange(rq, PORT_ORIGIN, &c2);
        CHECK(c2.ok && strcmp(c2.body, "hello") == 0);
        CHECK(hc_server_connection_count(origin) == conns);
        printf("  pooled connection idle %d ms, then reused: %s\n", READ_MS * 3,
               c2.ok && hc_server_connection_count(origin) == conns ? "ok" : "no");
    }

    /* origin that never reads: large upload */
    {
        struct sockaddr_in sa;
        int                one = 1, small = 4096;
        ch_t               c = { .path = "/upload", .upload = 64u * 1024 * 1024 };

        sink = socket(AF_INET, SOCK_STREAM, 0);
        setsockopt(sink, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        setsockopt(sink, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
        memset(&sa, 0, sizeof(sa));
        sa.sin_family      = AF_INET;
        sa.sin_port        = htons(PORT_SINK);
        sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        CHECK(bind(sink, (struct sockaddr *)&sa, sizeof(sa)) == 0 && listen(sink, 4) == 0);

        ms = run_exchange(rq, PORT_SINK, &c);
        CHECK(!c.ok && strcmp(c.reason, "upstream write timeout") == 0);
        CHECK(c.sent < c.upload);
        printf("  origin never reads: \"%s\" after %zu bytes, %lld ms\n", c.reason, c.sent,
               (long long)ms);
        close(sink);
    }

    hc_requester_free(rq);
}

static void *
loop_main(void *arg)
{
    event_base_dispatch(arg);
    return NULL;
}

static void
stop_cb(evutil_socket_t fd, short what, void *arg)
{
    (void)fd;
    (void)what;
    event_base_loopbreak(arg);
}

int
main(int argc, char **argv)
{
    char       cert[512], key[512], tmpdir[64];
    pthread_t  th_loop;
    hc_server *s_h1, *s_tls, *origin;
    int        h2;

    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc < 2) {
        fprintf(stderr, "usage: %s <tests/tls dir>\n", argv[0]);
        return 2;
    }

    if (getenv("HC_TEST_SCALE") && atoi(getenv("HC_TEST_SCALE")) > 1) {
        int k = atoi(getenv("HC_TEST_SCALE"));

        K         = k;
        READ_MS  *= k;
        WRITE_MS *= k;
        SLACK    *= k;
    }

    snprintf(cert, sizeof(cert), "%s/localhost.pem", argv[1]);
    snprintf(key, sizeof(key), "%s/localhost.key", argv[1]);
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/hc_to_%d", getpid());
    mkdir(tmpdir, 0700);

    evthread_use_pthreads();
    base   = event_base_new();
    R      = hc_reactor_new_on_base(base);
    s_h1   = hc_server_new(R, factory, NULL);
    s_tls  = hc_server_new(R, factory, NULL);
    origin = hc_server_new(R, factory, NULL);

    hc_server_set_timeouts(s_h1, READ_MS, WRITE_MS);
    hc_server_set_max_body(s_h1, MAX_BODY);
    hc_server_set_timeouts(s_tls, READ_MS, WRITE_MS);
    hc_server_set_max_body(s_tls, MAX_BODY);

    if (hc_server_listen(s_h1, "127.0.0.1", PORT_H1) != 0 ||
        hc_server_enable_tls(s_tls, cert, key) != 0 ||
        hc_server_listen(s_tls, "127.0.0.1", PORT_TLS) != 0 ||
        hc_server_listen(origin, "127.0.0.1", PORT_ORIGIN) != 0) {
        fprintf(stderr, "setup failed\n");
        return 1;
    }

    /* Server cases: the loop runs on a thread, the test talks to it. */
    pthread_create(&th_loop, NULL, loop_main, base);

    server_cases();

    /* HTTP/2 when both curl and libevhtp have it */
    {
        char out[256], args[256];

        snprintf(args, sizeof(args), "--http2 -w ' %%{http_version}' https://127.0.0.1:%d/hello",
                 PORT_TLS);
        h2 = curl(args, out, sizeof(out)) == 0 && strcmp(out, "hello 2") == 0;

        if (!h2) {
            printf("  (HTTP/2 probe: %s)\n", out);
        }
    }

    if (h2) {
        h2_cases(tmpdir);
    } else {
        puts("server, HTTP/2: skipped (no HTTP/2 in curl or libevhtp)");
    }

    event_base_once(base, -1, EV_TIMEOUT, stop_cb, base, NULL);
    pthread_join(th_loop, NULL);

    /* Client cases: the loop runs here, one exchange at a time. */
    client_cases(origin);

    hc_server_free(s_h1);
    hc_server_free(s_tls);
    hc_server_free(origin);
    hc_reactor_free(R);
    event_base_free(base);

    {
        char cmd[128];

        snprintf(cmd, sizeof(cmd), "rm -rf %s", tmpdir);
        if (system(cmd) != 0) {
            /* leftover temp files are harmless */
        }
    }

    printf("timeout_test: %d checks, %d failed\n", passed + failed, failed);
    return failed ? 1 : 0;
}
