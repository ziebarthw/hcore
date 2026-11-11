/*
 * client_execute_test.c - hc_client_execute(): one new connection for one
 * exchange, against an in-process server. The request goes out before the
 * connection is up, so these check that the response still reaches the
 * handler intact:
 *
 *   - a GET with no body
 *   - a POST with Content-Length, and a chunked POST (50000 bytes each,
 *     written from ->produce())
 *   - a "103 Early Hints" ahead of the final response, delivered to
 *     ->consume_information()
 *   - every exchange released exactly once
 *
 * Usage: client_execute_test
 */
#define _GNU_SOURCE
#include "hc.h"
#include "hc_support.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <event2/event.h>
#include <event2/thread.h>

#define PORT   9395
#define UPLOAD 50000

static int passed, failed;

#define CHECK(c) do {                                                      \
        if (c) { passed++; }                                               \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } \
    } while (0)

static hc_reactor *R;

static void
sleep_ms(int ms)
{
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };

    nanosleep(&ts, NULL);
}

static char
pattern(size_t i)
{
    return (char)('a' + (i * 7) % 26);
}

static unsigned long
checksum(const char *p, size_t len)
{
    unsigned long sum = 0;

    for (size_t i = 0; i < len; i++) {
        sum = sum * 31 + (unsigned char)p[i];
    }

    return sum;
}

/* ------------------------------------------------------------------------ */
/* The server                                                               */
/* ------------------------------------------------------------------------ */

static void
app_request(void *arg, hc_basic_request *r)
{
    hc_response_channel *rc     = hc_basic_request_channel(r);
    const char          *target = hc_basic_request_target(r);
    char                 msg[128];
    size_t               len;
    const char          *body   = hc_basic_request_body(r, &len);

    (void)arg;

    if (strcmp(target, "/early") == 0) {
        hc_headers      *h    = hc_headers_new();
        hc_response_head info = { 103, 11, h };

        hc_headers_add(h, "Link", "</style.css>; rel=preload");
        hc_response_send_information(rc, &info);
        hc_headers_free(h);
    }

    snprintf(msg, sizeof(msg), "%s %s len=%zu sum=%lu", hc_basic_request_method(r), target,
             len, checksum(body, len));
    hc_response_send_simple(rc, 200, "text/plain", msg, 0);
}

static hc_basic_server_config CONFIG = { .request = app_request };

/* ------------------------------------------------------------------------ */
/* The client                                                               */
/* ------------------------------------------------------------------------ */

typedef struct client {
    hc_client_handler base;
    const char       *method;
    const char       *target;
    int64_t           body_len;   /* -2: no body; -1: chunked */
    size_t            sent;
    int               ended;
    hc_data_channel  *ch;
    int               status;
    int               info_status;
    char              info_link[64];
    char              got[256];
    size_t            got_len;
    int               failures;
    volatile int      released;
} client;

static void
cl_produce_request(hc_client_handler *self, hc_request_channel *rc)
{
    client         *c  = (client *)self;
    hc_headers     *h  = hc_headers_new();
    hc_request_head rh = { c->method, c->target, "127.0.0.1", 11, h };
    hc_entity       e  = { c->body_len, "application/octet-stream" };

    c->ch = hc_request_send(rc, &rh, c->body_len == -2 ? NULL : &e);
    hc_headers_free(h);
}

static void
cl_produce(hc_client_handler *self, hc_data_channel *ch)
{
    client *c = (client *)self;
    char    buf[4096];

    while (!c->ended && c->sent < UPLOAD && hc_data_capacity(ch) > 0) {
        size_t n = UPLOAD - c->sent < sizeof(buf) ? UPLOAD - c->sent : sizeof(buf);

        for (size_t i = 0; i < n; i++) {
            buf[i] = pattern(c->sent + i);
        }

        if (hc_data_write(ch, buf, n) != (long)n) {
            return;
        }

        c->sent += n;
    }

    if (!c->ended && c->sent >= UPLOAD) {
        c->ended = 1;
        hc_data_end(ch, NULL);
    }
}

static void
cl_consume_response(hc_client_handler *self, const hc_response_head *head,
                    const hc_entity *entity)
{
    (void)entity;
    ((client *)self)->status = head->status;
}

static void
cl_consume(hc_client_handler *self, const void *data, size_t len)
{
    client *c = (client *)self;

    if (c->got_len + len < sizeof(c->got)) {
        memcpy(c->got + c->got_len, data, len);
        c->got_len += len;
        c->got[c->got_len] = '\0';
    }
}

static void
cl_stream_end(hc_client_handler *self, const hc_headers *trailers)
{
    (void)self;
    (void)trailers;
}

static void
cl_failed(hc_client_handler *self, const char *reason)
{
    ((client *)self)->failures++;
    fprintf(stderr, "  client failed: %s\n", reason);
}

static void
cl_release(hc_client_handler *self)
{
    ((client *)self)->released++;
}

static void
cl_consume_information(hc_client_handler *self, const hc_response_head *head)
{
    client     *c    = (client *)self;
    const char *link = hc_headers_get(head->headers, "Link");

    c->info_status = head->status;
    snprintf(c->info_link, sizeof(c->info_link), "%s", link ? link : "");
}

static const hc_client_handler_vtbl CLIENT_VTBL = {
    cl_produce_request, cl_consume_response, cl_consume, cl_stream_end,
    cl_produce, cl_failed, cl_release, cl_consume_information
};

static void
start_cb(evutil_socket_t fd, short what, void *arg)
{
    client *c = arg;
    hc_host h;

    (void)fd;
    (void)what;
    hc_host_init(&h, "http", "127.0.0.1", PORT);

    if (hc_client_execute(R, &h, &c->base) == NULL) {
        c->failures++;
        c->released = 1;
    }
}

static void
run(struct event_base *base, client *c, const char *method, const char *target,
    int64_t body_len)
{
    memset(c, 0, sizeof(*c));
    c->base.vtbl = &CLIENT_VTBL;
    c->method    = method;
    c->target    = target;
    c->body_len  = body_len;

    event_base_once(base, -1, EV_TIMEOUT, start_cb, c, NULL);

    for (int i = 0; i < 200 && !c->released; i++) {
        sleep_ms(50);
    }

    sleep_ms(50);   /* a second release would show here */
}

/* ------------------------------------------------------------------------ */

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
main(void)
{
    struct event_base *base;
    hc_server         *s;
    pthread_t          th;
    client             c;
    char               want[128], body[UPLOAD];

    setvbuf(stdout, NULL, _IONBF, 0);

    for (size_t i = 0; i < UPLOAD; i++) {
        body[i] = pattern(i);
    }

    evthread_use_pthreads();
    base = event_base_new();
    R    = hc_reactor_new_on_base(base);
    s    = hc_server_new(R, hc_basic_server_factory, &CONFIG);

    if (hc_server_listen(s, "127.0.0.1", PORT) != 0) {
        fprintf(stderr, "setup failed\n");
        return 1;
    }

    pthread_create(&th, NULL, loop_main, base);

    run(base, &c, "GET", "/hello", -2);
    CHECK(c.released == 1 && c.failures == 0);
    CHECK(c.status == 200 && strcmp(c.got, "GET /hello len=0 sum=0") == 0);
    printf("GET: %d %s\n", c.status, c.got);

    snprintf(want, sizeof(want), "POST /echo len=%d sum=%lu", UPLOAD, checksum(body, UPLOAD));
    run(base, &c, "POST", "/echo", UPLOAD);
    CHECK(c.released == 1 && c.failures == 0);
    CHECK(c.status == 200 && strcmp(c.got, want) == 0);
    printf("POST, Content-Length %d: %s\n", UPLOAD, strcmp(c.got, want) == 0 ? "whole body" : c.got);

    run(base, &c, "POST", "/echo", -1);
    CHECK(c.released == 1 && c.failures == 0);
    CHECK(c.status == 200 && strcmp(c.got, want) == 0);
    printf("POST, chunked: %s\n", strcmp(c.got, want) == 0 ? "whole body" : c.got);

    run(base, &c, "GET", "/early", -2);
    CHECK(c.released == 1 && c.failures == 0);
    CHECK(c.info_status == 103 && strcmp(c.info_link, "</style.css>; rel=preload") == 0);
    CHECK(c.status == 200 && strcmp(c.got, "GET /early len=0 sum=0") == 0);
    printf("103 Early Hints, then: %d %s (interim %d, Link %s)\n", c.status, c.got,
           c.info_status, c.info_link);

    event_base_once(base, -1, EV_TIMEOUT, stop_cb, base, NULL);
    pthread_join(th, NULL);

    hc_server_free(s);
    hc_reactor_free(R);
    event_base_free(base);

    printf("client_execute_test: %d checks, %d failed\n", passed + failed, failed);
    return failed ? 1 : 0;
}
