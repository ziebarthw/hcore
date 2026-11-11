/*
 * transform_test.c - body transformations (hc_transform.h) on real
 * exchanges, with small test codecs:
 *
 *   upper    upper-cases
 *   x4       writes every byte 4 times (expands: exercises the pending
 *            buffer); shrink is its inverse
 *   hold     holds everything back until finish (a compressor without
 *            flushing); flush() lets it go
 *   fail     fails once 10000 bytes have gone through
 *
 * Server (a basic-server application wrapped by
 * hc_server_transform_handler_new()), checked with curl over HTTP/1.1, and
 * over HTTP/2 when curl and libevhtp have it:
 *   - a request body decoded (upper) before the application sees it, with
 *     Content-Length removed and the choose function's header added
 *   - a request refused by the choose function: 415, the application never
 *     called
 *   - a 100000-byte response expanded x4, sent with unknown length and
 *     written only while the wrapper reports capacity; and the same to a
 *     slow reader (curl --limit-rate): the wrapper said "no capacity" and
 *     produce() came back
 *   - hold: nothing until the end, then all of it
 *   - hold on a never-ending event stream: without flush_each_write the
 *     client sees nothing; with it, the first event at once
 *   - fail: the exchange aborted, the application's ->failed() gets "body
 *     transformation failed", ->release() once
 *   - a bodiless response (204): the choose function is called, with no
 *     entity, and its transformation freed unused
 *   - every transformation's state freed exactly once
 *
 * Client (a pooled requester, the handler wrapped by
 * hc_client_transform_handler_new()):
 *   - a request body expanded x4 on the way out (chunked), checked by the
 *     origin; a header added to every request, bodiless ones too, and seen
 *     by choose_response
 *   - a response x4 from the server decoded (shrink) on the way in: both
 *     wrappers on one exchange, the original bytes back
 *   - a response refused by choose_response: ->failed("body transformation
 *     refused"), ->release() once
 *
 * Usage: transform_test <tests/tls dir>
 */
#define _GNU_SOURCE
#include "hc.h"
#include "hc_support.h"
#include "hc_transform.h"

#include <ctype.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/wait.h>
#include <unistd.h>

#include <event2/event.h>
#include <event2/thread.h>

#define PORT       9396
#define PORT_TLS   9397
#define PORT_FLUSH 9398
#define BIG        100000
#define FAIL_AT    10000

static int passed, failed;

#define CHECK(c) do {                                                      \
        if (c) { passed++; }                                               \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } \
    } while (0)

static hc_reactor  *R;
static volatile int states_made, states_freed;

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
checksum(const void *p, size_t len)
{
    unsigned long sum = 0;

    for (size_t i = 0; i < len; i++) {
        sum = sum * 31 + ((const unsigned char *)p)[i];
    }

    return sum;
}

/* ------------------------------------------------------------------------ */
/* Test codecs                                                              */
/* ------------------------------------------------------------------------ */

typedef struct codec_state {
    size_t   seen;      /* fail: input so far; shrink: position */
    hc_bytes held;      /* hold */
} codec_state;

static void *
state_new(void)
{
    states_made++;
    return calloc(1, sizeof(codec_state));
}

static void
state_free(void *state)
{
    codec_state *s = state;

    states_freed++;
    hc_bytes_free(&s->held);
    free(s);
}

static int
finish_nothing(void *state, hc_bytes *out)
{
    (void)state;
    (void)out;
    return 0;
}

static int
upper_update(void *state, const void *in, size_t len, hc_bytes *out)
{
    unsigned char *p = hc_bytes_reserve(out, len);

    (void)state;

    if (p == NULL) {
        return -1;
    }

    for (size_t i = 0; i < len; i++) {
        p[i] = (unsigned char)toupper(((const unsigned char *)in)[i]);
    }

    hc_bytes_commit(out, len);
    return 0;
}

static int
x4_update(void *state, const void *in, size_t len, hc_bytes *out)
{
    unsigned char *p = hc_bytes_reserve(out, len * 4);

    (void)state;

    if (p == NULL) {
        return -1;
    }

    for (size_t i = 0; i < len; i++) {
        memset(p + i * 4, ((const unsigned char *)in)[i], 4);
    }

    hc_bytes_commit(out, len * 4);
    return 0;
}

static int
shrink_update(void *state, const void *in, size_t len, hc_bytes *out)
{
    codec_state *s = state;

    for (size_t i = 0; i < len; i++, s->seen++) {
        if (s->seen % 4 == 0 && hc_bytes_append(out, (const char *)in + i, 1) != 0) {
            return -1;
        }
    }

    return 0;
}

static int
shrink_finish(void *state, hc_bytes *out)
{
    (void)out;
    return ((codec_state *)state)->seen % 4 == 0 ? 0 : -1;    /* truncated */
}

static int
hold_update(void *state, const void *in, size_t len, hc_bytes *out)
{
    (void)out;
    return hc_bytes_append(&((codec_state *)state)->held, in, len);
}

static int
hold_flush(void *state, hc_bytes *out)
{
    codec_state *s = state;
    int          rv = hc_bytes_append(out, s->held.data, s->held.len);

    s->held.len = 0;
    return rv;
}

static int
fail_update(void *state, const void *in, size_t len, hc_bytes *out)
{
    codec_state *s = state;

    if ((s->seen += len) > FAIL_AT) {
        return -1;
    }

    return hc_bytes_append(out, in, len);
}

static const hc_transform_ops UPPER  = { upper_update, finish_nothing, NULL, state_free };
static const hc_transform_ops X4     = { x4_update, finish_nothing, NULL, state_free };
static const hc_transform_ops SHRINK = { shrink_update, shrink_finish, NULL, state_free };
static const hc_transform_ops HOLD   = { hold_update, hold_flush, hold_flush, state_free };
static const hc_transform_ops FAILS  = { fail_update, finish_nothing, NULL, state_free };

static void
use(hc_transform *t, const hc_transform_ops *ops)
{
    t->ops   = ops;
    t->state = state_new();
}

/* ------------------------------------------------------------------------ */
/* Server: choose functions                                                 */
/* ------------------------------------------------------------------------ */

static volatile int bodiless_chosen;

static int
srv_choose_request(void *arg, const hc_request_head *request, hc_headers *headers,
                   const hc_entity *entity, hc_transform *t)
{
    const char *codec = hc_headers_get(headers, "X-Codec");

    (void)arg;
    (void)request;

    if (codec && strcmp(codec, "refuse") == 0) {
        return 415;
    }

    if (codec && strcmp(codec, "upper") == 0 && entity) {
        use(t, &UPPER);
        hc_headers_remove(headers, "X-Codec");
        hc_headers_add(headers, "X-Decoded", "upper");
    }

    return 0;
}

static int
srv_choose_response(void *arg, const hc_request_head *request, int *status,
                    hc_headers *headers, const hc_entity *entity, hc_transform *t)
{
    const char *target = request->target;

    (void)arg;
    (void)status;

    if (strncmp(target, "/x4", 3) == 0) {
        use(t, &X4);
        hc_headers_add(headers, "X-Encoded", "x4");
    } else if (strncmp(target, "/hold", 5) == 0 || strncmp(target, "/stream", 7) == 0) {
        use(t, &HOLD);
    } else if (strcmp(target, "/fail") == 0) {
        use(t, &FAILS);
    } else if (strcmp(target, "/nobody") == 0) {
        use(t, &UPPER);         /* no body: must be freed unused */
        bodiless_chosen += entity == NULL;
    }

    return 0;
}

static const hc_server_transform_config TCFG = {
    srv_choose_request, srv_choose_response, NULL, 16 * 1024, 0
};

static const hc_server_transform_config TCFG_FLUSH = {
    srv_choose_request, srv_choose_response, NULL, 0, 1
};

/* ------------------------------------------------------------------------ */
/* Server: the application                                                  */
/* ------------------------------------------------------------------------ */

static volatile int delivered, released, failures, cap_zero, produce_calls;
static char         last_failure[128];

typedef struct app_req {
    size_t    sent;
    size_t    total;
    int       events;
    hc_timer *timer;
    hc_data_channel *ch;
} app_req;

static void
event_cb(void *arg)
{
    app_req *a = arg;
    char     ev[64];

    if (a->ch) {
        snprintf(ev, sizeof(ev), "data: event %d\n\n", ++a->events);
        hc_data_write(a->ch, ev, strlen(ev));
        hc_timer_start(a->timer, 50);
    }
}

static void
app_request(void *arg, hc_basic_request *r)
{
    app_req             *a      = calloc(1, sizeof(*a));
    hc_response_channel *rc     = hc_basic_request_channel(r);
    const char          *target = hc_basic_request_target(r);
    const hc_headers    *h      = hc_basic_request_headers(r);
    size_t               len;
    const char          *body   = hc_basic_request_body(r, &len);
    char                 msg[256];

    (void)arg;
    delivered++;
    hc_basic_request_set_data(r, a);

    if (strncmp(target, "/echo", 5) == 0) {
        snprintf(msg, sizeof(msg), "decoded=%s cl=%s len=%zu sum=%lu",
                 hc_headers_get(h, "X-Decoded") ? hc_headers_get(h, "X-Decoded") : "-",
                 hc_headers_get(h, "Content-Length") ? "yes" : "no", len, checksum(body, len));
        hc_response_send_simple(rc, 200, "text/plain", msg, 0);
    } else if (strcmp(target, "/nobody") == 0) {
        hc_response_send_simple(rc, 204, NULL, NULL, 0);
    } else if (strncmp(target, "/stream", 7) == 0) {
        hc_response_head rh = { 200, 11, NULL };
        hc_entity        e  = { -1, "text/event-stream" };

        a->ch    = hc_response_send(rc, &rh, &e);
        a->timer = hc_timer_new(R, event_cb, a);
        hc_timer_start(a->timer, 0);
    } else {
        hc_response_head rh = { 200, 11, NULL };
        hc_entity        e  = { BIG, "text/plain" };   /* the wrapper makes it -1 */

        a->total = BIG;
        hc_response_send(rc, &rh, &e);                  /* body from ->produce() */
    }
}

static void
app_produce(void *arg, hc_basic_request *r, hc_data_channel *ch)
{
    app_req *a = hc_basic_request_data(r);
    char     buf[4096];

    (void)arg;

    if (a->total == 0 || a->sent >= a->total) {
        return;
    }

    produce_calls++;

    for (;;) {
        size_t cap = hc_data_capacity(ch);
        size_t n   = a->total - a->sent;

        if (cap == 0) {
            cap_zero++;
            return;
        }

        n = n < sizeof(buf) ? n : sizeof(buf);
        n = n < cap ? n : cap;

        for (size_t i = 0; i < n; i++) {
            buf[i] = pattern(a->sent + i);
        }

        if (hc_data_write(ch, buf, n) != (long)n) {
            return;
        }

        if ((a->sent += n) >= a->total) {
            hc_data_end(ch, NULL);
            return;
        }
    }
}

static void
app_failed(void *arg, hc_basic_request *r, const char *reason)
{
    (void)arg;
    (void)r;
    failures++;
    snprintf(last_failure, sizeof(last_failure), "%s", reason);
}

static void
app_release(void *arg, hc_basic_request *r)
{
    app_req *a = hc_basic_request_data(r);

    (void)arg;
    released++;

    if (a->timer) {
        hc_timer_free(a->timer);
    }

    free(a);
}

static hc_basic_server_config CONFIG = {
    .request = app_request,
    .produce = app_produce,
    .failed  = app_failed,
    .release = app_release,
};

static hc_server_handler *
factory(void *cfg)
{
    hc_server_handler *inner = hc_basic_server_factory(&CONFIG);
    hc_server_handler *w     = inner ? hc_server_transform_handler_new(inner, cfg) : NULL;

    if (w == NULL && inner) {
        inner->vtbl->release(inner);
    }

    return w;
}

/* ------------------------------------------------------------------------ */
/* Client                                                                   */
/* ------------------------------------------------------------------------ */

static volatile int saw_client_header;

static int
cli_choose_request(void *arg, const hc_request_head *request, hc_headers *headers,
                   const hc_entity *entity, hc_transform *t)
{
    (void)arg;
    hc_headers_add(headers, "X-Client-Wrapped", "yes");     /* every request */

    if (entity && strcmp(request->target, "/echo-x4") == 0) {
        use(t, &X4);
    }

    return 0;
}

static int
cli_choose_response(void *arg, const hc_request_head *request, int status,
                    hc_headers *headers, const hc_entity *entity, hc_transform *t)
{
    const char *enc = hc_headers_get(headers, "X-Encoded");

    (void)arg;
    (void)status;
    (void)entity;
    saw_client_header = request && hc_headers_get(request->headers, "X-Client-Wrapped") != NULL;

    if (request && strcmp(request->target, "/hold-refused") == 0) {
        return 502;
    }

    if (enc && strcmp(enc, "x4") == 0) {
        use(t, &SHRINK);
        hc_headers_remove(headers, "X-Encoded");
    }

    return 0;
}

static const hc_client_transform_config CCFG = {
    cli_choose_request, cli_choose_response, NULL, 0, 0
};

typedef struct client {
    hc_client_handler base;
    const char       *target;
    int               upload;     /* bytes to POST (chunked), 0: GET */
    size_t            sent;
    int               status;
    hc_bytes          got;
    int               failures;
    char              reason[64];
    volatile int      released;
} client;

static void
cl_produce_request(hc_client_handler *self, hc_request_channel *rc)
{
    client         *c  = (client *)self;
    hc_request_head rh = { c->upload ? "POST" : "GET", c->target, "127.0.0.1", 11, NULL };
    hc_entity       e  = { -1, "text/plain" };

    hc_request_send(rc, &rh, c->upload ? &e : NULL);
}

static void
cl_produce(hc_client_handler *self, hc_data_channel *ch)
{
    client *c = (client *)self;
    char    buf[3000];

    while (c->sent < (size_t)c->upload && hc_data_capacity(ch) > 0) {
        size_t n = (size_t)c->upload - c->sent < sizeof(buf) ? (size_t)c->upload - c->sent
                                                            : sizeof(buf);

        for (size_t i = 0; i < n; i++) {
            buf[i] = pattern(c->sent + i);
        }

        if (hc_data_write(ch, buf, n) != (long)n) {
            return;
        }

        if ((c->sent += n) == (size_t)c->upload) {
            hc_data_end(ch, NULL);
        }
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
    hc_bytes_append(&((client *)self)->got, data, len);
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
    client *c = (client *)self;

    c->failures++;
    snprintf(c->reason, sizeof(c->reason), "%s", reason);
}

static void
cl_release(hc_client_handler *self)
{
    ((client *)self)->released++;
}

static const hc_client_handler_vtbl CLIENT_VTBL = {
    cl_produce_request, cl_consume_response, cl_consume, cl_stream_end,
    cl_produce, cl_failed, cl_release, NULL
};

static hc_requester *RQ;

static void
start_client(evutil_socket_t fd, short what, void *arg)
{
    client            *c = arg;
    hc_client_handler *w = hc_client_transform_handler_new(&c->base, &CCFG);
    hc_host            h;

    (void)fd;
    (void)what;
    hc_host_init(&h, "http", "127.0.0.1", PORT);

    if (RQ == NULL) {
        RQ = hc_requester_new(R, NULL);
    }

    if (w == NULL || hc_requester_execute(RQ, &h, 20000, w) != 0) {
        c->released = 1;
    }
}

static void
run_client(struct event_base *base, client *c, const char *target, int upload)
{
    hc_bytes_free(&c->got);
    memset(c, 0, sizeof(*c));
    c->base.vtbl = &CLIENT_VTBL;
    c->target    = target;
    c->upload    = upload;

    event_base_once(base, -1, EV_TIMEOUT, start_client, c, NULL);

    for (int i = 0; i < 300 && !c->released; i++) {
        sleep_ms(50);
    }

    sleep_ms(50);
}

/* ------------------------------------------------------------------------ */

static int
curl(const char *args, char *out, size_t cap)
{
    char   cmd[1024];
    FILE  *p;
    size_t n;

    snprintf(cmd, sizeof(cmd), "curl -sS -k --max-time 20 %s 2>/dev/null", args);
    p      = popen(cmd, "r");
    n      = fread(out, 1, cap - 1, p);
    out[n] = '\0';

    return WEXITSTATUS(pclose(p));
}

static int
is_pattern(const char *p, size_t len, size_t n, int repeat)
{
    if (len != n * (size_t)repeat) {
        return 0;
    }

    for (size_t i = 0; i < len; i++) {
        if (p[i] != pattern(i / (size_t)repeat)) {
            return 0;
        }
    }

    return 1;
}

static void
run_cases(const char *proto, const char *scheme, int port, int slow)
{
    static char out[BIG * 4 + 4096];
    char        args[512], want[256], file[64];
    int         before, rc;
    FILE       *f;

    printf("%s:\n", proto);

    /* request body decoded before the application sees it */
    snprintf(file, sizeof(file), "/tmp/hc_tt_%d", (int)getpid());
    f = fopen(file, "wb");
    for (size_t i = 0; i < 50000; i++) {
        fputc(pattern(i), f);
    }
    fclose(f);
    for (size_t i = 0; i < 50000; i++) {
        out[i] = (char)toupper((unsigned char)pattern(i));
    }
    snprintf(want, sizeof(want), "decoded=upper cl=no len=50000 sum=%lu", checksum(out, 50000));
    snprintf(args, sizeof(args), "%s -H 'X-Codec: upper' --data-binary @%s %s://127.0.0.1:%d/echo",
             proto, file, scheme, port);
    CHECK(curl(args, out, sizeof(out)) == 0 && strcmp(out, want) == 0);
    printf("  request body decoded: %s\n", strcmp(out, want) == 0 ? "upper-cased, no Content-Length" : out);

    before = delivered;
    snprintf(args, sizeof(args), "%s -o /dev/null -w '%%{http_code}' -H 'X-Codec: refuse' "
             "--data-binary @%s %s://127.0.0.1:%d/echo", proto, file, scheme, port);
    curl(args, out, sizeof(out));
    sleep_ms(100);
    CHECK(strcmp(out, "415") == 0 && delivered == before);
    printf("  request refused: %s, application %s\n", out, delivered == before ? "not called" : "CALLED");
    unlink(file);

    /* expanded response */
    cap_zero = produce_calls = 0;
    snprintf(args, sizeof(args), "%s -i %s://127.0.0.1:%d/x4", proto, scheme, port);
    CHECK(curl(args, out, sizeof(out)) == 0);
    {
        char  *body = strstr(out, "\r\n\r\n");
        size_t blen = body ? strlen(body + 4) : 0;

        CHECK(strcasestr(out, "\nx-encoded: x4") != NULL);
        CHECK(strcasestr(out, "\ncontent-length:") == NULL);
        CHECK(body && is_pattern(body + 4, blen, BIG, 4));
        printf("  %d bytes x4: %zu bytes, length unknown, %s\n", BIG, blen,
               body && is_pattern(body + 4, blen, BIG, 4) ? "intact" : "WRONG");
    }

    if (slow) {
        cap_zero = produce_calls = 0;
        snprintf(args, sizeof(args), "%s --limit-rate 400k %s://127.0.0.1:%d/x4", proto, scheme, port);
        CHECK(curl(args, out, sizeof(out)) == 0 && is_pattern(out, strlen(out), BIG, 4));
        CHECK(cap_zero > 0 && produce_calls > cap_zero);
        printf("  slow reader: intact; told \"no capacity\" %d times, produce() %d times\n",
               cap_zero, produce_calls);
    }

    /* a codec holding everything back */
    snprintf(args, sizeof(args), "%s %s://127.0.0.1:%d/hold", proto, scheme, port);
    CHECK(curl(args, out, sizeof(out)) == 0 && is_pattern(out, strlen(out), BIG, 1));
    printf("  hold until finish: %s\n", is_pattern(out, strlen(out), BIG, 1) ? "intact" : "WRONG");

    /* failing codec */
    failures = 0;
    before   = released;
    snprintf(args, sizeof(args), "%s %s://127.0.0.1:%d/fail", proto, scheme, port);
    rc = curl(args, out, sizeof(out));
    sleep_ms(200);
    CHECK(rc != 0);
    CHECK(failures == 1 && strcmp(last_failure, "body transformation failed") == 0);
    CHECK(released == before + 1);
    printf("  failing codec: curl exit %d, failed(\"%s\"), released %d\n", rc, last_failure,
           released - before);

    /* bodiless response */
    bodiless_chosen = 0;
    snprintf(args, sizeof(args), "%s -o /dev/null -w '%%{http_code}' %s://127.0.0.1:%d/nobody",
             proto, scheme, port);
    CHECK(curl(args, out, sizeof(out)) == 0 && strcmp(out, "204") == 0 && bodiless_chosen == 1);
    printf("  204: choose called without an entity: %s\n", bodiless_chosen == 1 ? "yes" : "no");

    sleep_ms(100);
    CHECK(released == delivered);
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
    char               cert[512], key[512], out[4096], args[256];
    struct event_base *base;
    hc_server         *s, *s_tls, *s_flush;
    pthread_t          th;
    client             c;
    char              *want;

    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc < 2) {
        fprintf(stderr, "usage: %s <tests/tls dir>\n", argv[0]);
        return 2;
    }

    snprintf(cert, sizeof(cert), "%s/localhost.pem", argv[1]);
    snprintf(key, sizeof(key), "%s/localhost.key", argv[1]);

    {   /* hc_bytes */
        hc_bytes b = { NULL, 0, 0 };

        CHECK(hc_bytes_append(&b, "hello", 5) == 0 && b.len == 5);
        CHECK(hc_bytes_reserve(&b, 100000) != NULL && b.cap >= 100005);
        hc_bytes_commit(&b, 3);
        hc_bytes_consume(&b, 2);
        CHECK(b.len == 6 && memcmp(b.data, "llo", 3) == 0);
        hc_bytes_consume(&b, 99);
        CHECK(b.len == 0);
        hc_bytes_free(&b);
        CHECK(b.data == NULL && b.cap == 0);
        CHECK(hc_server_transform_handler_new(NULL, &TCFG) == NULL);
        puts("hc_bytes: ok");
    }

    evthread_use_pthreads();
    base    = event_base_new();
    R       = hc_reactor_new_on_base(base);
    s       = hc_server_new(R, factory, (void *)&TCFG);
    s_tls   = hc_server_new(R, factory, (void *)&TCFG);
    s_flush = hc_server_new(R, factory, (void *)&TCFG_FLUSH);

    if (hc_server_listen(s, "127.0.0.1", PORT) != 0 ||
        hc_server_enable_tls(s_tls, cert, key) != 0 ||
        hc_server_listen(s_tls, "127.0.0.1", PORT_TLS) != 0 ||
        hc_server_listen(s_flush, "127.0.0.1", PORT_FLUSH) != 0) {
        fprintf(stderr, "setup failed\n");
        return 1;
    }

    pthread_create(&th, NULL, loop_main, base);

    run_cases("--http1.1", "http", PORT, 1);

    snprintf(args, sizeof(args), "--http2 -o /dev/null -w '%%{http_version}' https://127.0.0.1:%d/nobody",
             PORT_TLS);
    if (curl(args, out, sizeof(out)) == 0 && strcmp(out, "2") == 0) {
        run_cases("--http2", "https", PORT_TLS, 0);
    } else {
        puts("--http2: skipped (no HTTP/2 in curl or libevhtp)");
    }

    puts("flush_each_write:");
    snprintf(args, sizeof(args), "-N --max-time 0.4 http://127.0.0.1:%d/stream", PORT);
    curl(args, out, sizeof(out));
    CHECK(out[0] == '\0');
    printf("  without: %s\n", out[0] ? "events arrived" : "nothing (all held back)");
    snprintf(args, sizeof(args), "-N --max-time 0.4 http://127.0.0.1:%d/stream", PORT_FLUSH);
    curl(args, out, sizeof(out));
    CHECK(strncmp(out, "data: event 1\n\n", 15) == 0);
    printf("  with: %s\n", strncmp(out, "data: event 1\n\n", 15) == 0 ? "events as written" : out);
    sleep_ms(300);  /* the server notices the clients left */

    puts("client:");
    memset(&c, 0, sizeof(c));
    run_client(base, &c, "/echo-x4", 30000);
    want = malloc(30000 * 4);
    for (size_t i = 0; i < 30000 * 4; i++) {
        want[i] = pattern(i / 4);
    }
    snprintf(args, sizeof(args), "decoded=- cl=no len=120000 sum=%lu", checksum(want, 120000));
    free(want);
    CHECK(c.status == 200 && c.got.len == strlen(args) && memcmp(c.got.data, args, c.got.len) == 0);
    CHECK(c.released == 1 && c.failures == 0);
    printf("  request body x4 on the way out: %.*s\n", (int)c.got.len, (char *)c.got.data);

    run_client(base, &c, "/x4", 0);
    CHECK(c.status == 200 && is_pattern((char *)c.got.data, c.got.len, BIG, 1));
    CHECK(c.released == 1 && c.failures == 0 && saw_client_header);
    printf("  response x4 decoded on the way in: %s; header on a GET seen by choose_response: %s\n",
           is_pattern((char *)c.got.data, c.got.len, BIG, 1) ? "original bytes" : "WRONG",
           saw_client_header ? "yes" : "no");

    run_client(base, &c, "/hold-refused", 0);
    CHECK(c.released == 1 && c.failures == 1 && strcmp(c.reason, "body transformation refused") == 0);
    printf("  response refused: failed(\"%s\"), released %d\n", c.reason, c.released);
    hc_bytes_free(&c.got);

    sleep_ms(200);
    event_base_once(base, -1, EV_TIMEOUT, stop_cb, base, NULL);
    pthread_join(th, NULL);

    if (RQ) {
        hc_requester_free(RQ);
    }
    hc_server_free(s);
    hc_server_free(s_tls);
    hc_server_free(s_flush);
    hc_reactor_free(R);
    event_base_free(base);

    CHECK(released == delivered);
    CHECK(states_made > 0 && states_made == states_freed);
    printf("transformations: %d made, %d freed\n", states_made, states_freed);
    printf("transform_test: %d checks, %d failed\n", passed + failed, failed);
    return failed ? 1 : 0;
}
