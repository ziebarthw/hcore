/*
 * channel_test.c - application channels (hc_channel.h) on real exchanges:
 *
 *   - server: the application answers through a wrapper response channel
 *     that adds a header and upper-cases the body on its way to the
 *     engine's channel; checked with curl over HTTP/1.1, and over HTTP/2
 *     when curl and libevhtp have it:
 *       hc_response_send_simple() with a body, and without one (204)
 *       hc_response_send() with a 300000-byte body of unknown length,
 *       written only while the wrapper reports capacity, so the engine's
 *       flow control reaches the application through the wrapper
 *       hc_response_send_information() on a wrapper without that function
 *   - client (pooled requester): a request sent through a wrapper request channel (header
 *     added, 100000-byte chunked body upper-cased); the origin echoes what
 *     arrived; hc_request_channel_exchange() reaches the real exchange
 *   - hc_*_channel_new() refuses incomplete ops; _free() ignores NULL and
 *     the engine's own channels
 *
 * Usage: channel_test <tests/tls dir>
 */
#define _GNU_SOURCE
#include "hc.h"
#include "hc_channel.h"
#include "hc_support.h"

#include <ctype.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/wait.h>

#include <event2/event.h>
#include <event2/thread.h>

#define PORT      9390
#define PORT_TLS  9391
#define BIG       300000
#define UPLOAD    100000

static int passed, failed;

#define CHECK(c) do {                                                      \
        if (c) { passed++; }                                               \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } \
    } while (0)

static hc_reactor  *R;
static hc_requester *RQ;
static volatile int delivered, released, info_rv, empty_rv, produce_calls;

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

/* ------------------------------------------------------------------------ */
/* The wrapper: one per exchange, for either side                           */
/* ------------------------------------------------------------------------ */

typedef struct upcase {
    hc_response_channel *real_rc;     /* server side: what is wrapped    */
    hc_request_channel  *real_qc;     /* client side: what is wrapped    */
    hc_data_channel     *real_body;   /* the engine's body channel       */
    hc_response_channel *rc;          /* given to the application        */
    hc_request_channel  *qc;
    hc_data_channel     *body;
} upcase;

static long
up_write(void *arg, const void *data, size_t len)
{
    upcase *u   = arg;
    char   *tmp = malloc(len ? len : 1);
    long    n;

    for (size_t i = 0; i < len; i++) {
        tmp[i] = (char)toupper(((const unsigned char *)data)[i]);
    }

    n = hc_data_write(u->real_body, tmp, len);
    free(tmp);
    return n;
}

static int
up_end(void *arg, const hc_headers *trailers)
{
    return hc_data_end(((upcase *)arg)->real_body, trailers);
}

static size_t
up_capacity(void *arg)
{
    return hc_data_capacity(((upcase *)arg)->real_body);
}

static void
up_request_output(void *arg)
{
    hc_data_request_output(((upcase *)arg)->real_body);
}

static const hc_data_channel_ops UP_DATA = {
    up_write, up_end, up_capacity, up_request_output
};

static hc_headers *
marked(const hc_headers *h)
{
    hc_headers *c = h ? hc_headers_copy(h) : hc_headers_new();

    hc_headers_add(c, "X-Wrapped", "upcase");
    return c;
}

static int
up_send_response(void *arg, const hc_response_head *head, const hc_entity *entity,
                 hc_data_channel **body)
{
    upcase          *u  = arg;
    hc_response_head h2 = *head;
    hc_headers      *h  = marked(head->headers);
    int              rv;

    h2.headers = h;
    rv         = hc_response_submit(u->real_rc, &h2, entity, &u->real_body);
    hc_headers_free(h);

    *body = u->real_body ? u->body : NULL;
    return rv;
}

/* no send_information: hc_response_send_information() must say -1 */
static const hc_response_channel_ops UP_RESPONSE = { up_send_response, NULL };

static int
up_send_request(void *arg, const hc_request_head *head, const hc_entity *entity,
                hc_data_channel **body)
{
    upcase         *u  = arg;
    hc_request_head h2 = *head;
    hc_headers     *h  = marked(head->headers);
    int             rv;

    h2.headers = h;
    rv         = hc_request_submit(u->real_qc, &h2, entity, &u->real_body);
    hc_headers_free(h);

    *body = u->real_body ? u->body : NULL;
    return rv;
}

static hc_client_exchange *
up_exchange(void *arg)
{
    return hc_request_channel_exchange(((upcase *)arg)->real_qc);
}

static const hc_request_channel_ops UP_REQUEST = { up_send_request, up_exchange };

static upcase *
upcase_new(hc_response_channel *real_rc, hc_request_channel *real_qc)
{
    upcase *u = calloc(1, sizeof(*u));

    u->real_rc = real_rc;
    u->real_qc = real_qc;
    u->body    = hc_data_channel_new(&UP_DATA, u);

    if (real_rc) {
        u->rc = hc_response_channel_new(&UP_RESPONSE, u);
    } else {
        u->qc = hc_request_channel_new(&UP_REQUEST, u);
    }

    return u;
}

static void
upcase_free(upcase *u)
{
    if (u) {
        hc_data_channel_free(u->body);
        hc_response_channel_free(u->rc);
        hc_request_channel_free(u->qc);
        free(u);
    }
}

/* ------------------------------------------------------------------------ */
/* The server application                                                   */
/* ------------------------------------------------------------------------ */

typedef struct app_req {
    upcase *u;
    size_t  sent;       /* /big */
} app_req;

static void
app_request(void *arg, hc_basic_request *r)
{
    app_req    *a      = calloc(1, sizeof(*a));
    const char *target = hc_basic_request_target(r);

    (void)arg;
    delivered++;
    hc_basic_request_set_data(r, a);

    if (strcmp(target, "/echo-raw") == 0) {
        /* unwrapped: what the client's wrapper sent, as it arrived */
        size_t      len;
        const char *body = hc_basic_request_body(r, &len);
        const char *mark = hc_headers_get(hc_basic_request_headers(r), "X-Wrapped");
        char       *msg  = malloc(len + 64);

        snprintf(msg, len + 64, "%s|%s", mark ? mark : "-", body);
        hc_response_send_simple(hc_basic_request_channel(r), 200, "text/plain", msg, 0);
        free(msg);
        return;
    }

    a->u = upcase_new(hc_basic_request_channel(r), NULL);

    if (strcmp(target, "/simple") == 0) {
        hc_response_send_simple(a->u->rc, 200, "text/plain", "hello, channels", 0);
    } else if (strcmp(target, "/empty") == 0) {
        empty_rv = hc_response_send_simple(a->u->rc, 204, NULL, NULL, 0);
    } else if (strcmp(target, "/info") == 0) {
        hc_response_head info = { 103, 11, NULL };

        info_rv = hc_response_send_information(a->u->rc, &info);
        hc_response_send_simple(a->u->rc, 200, "text/plain", "after info", 0);
    } else if (strcmp(target, "/big") == 0) {
        hc_response_head rh = { 200, 11, NULL };
        hc_entity        e  = { -1, "text/plain" };

        hc_response_send(a->u->rc, &rh, &e);    /* body from ->produce() */
    } else {
        hc_response_send_simple(a->u->rc, 404, "text/plain", "?", 0);
    }
}

/* The engine calls with its own channel; the application writes to the
 * wrapper's, as long as the wrapper says there is room. */
static void
app_produce(void *arg, hc_basic_request *r, hc_data_channel *ch)
{
    app_req *a = hc_basic_request_data(r);
    char     buf[4096];

    (void)arg;
    (void)ch;

    if (a->u == NULL || a->sent >= BIG) {
        return;
    }

    produce_calls++;

    while (a->sent < BIG && hc_data_capacity(a->u->body) > 0) {
        size_t n = BIG - a->sent < sizeof(buf) ? BIG - a->sent : sizeof(buf);

        for (size_t i = 0; i < n; i++) {
            buf[i] = pattern(a->sent + i);
        }

        if (hc_data_write(a->u->body, buf, n) != (long)n) {
            return;
        }

        a->sent += n;
    }

    if (a->sent >= BIG) {
        hc_data_end(a->u->body, NULL);
    }
}

static void
app_release(void *arg, hc_basic_request *r)
{
    app_req *a = hc_basic_request_data(r);

    (void)arg;
    released++;
    upcase_free(a->u);      /* after the exchange: nothing calls it now */
    free(a);
}

static hc_basic_server_config CONFIG = {
    .request = app_request,
    .produce = app_produce,
    .release = app_release,
};

/* ------------------------------------------------------------------------ */
/* The client: a request through a wrapper request channel                 */
/* ------------------------------------------------------------------------ */

typedef struct client {
    hc_client_handler base;
    upcase           *u;
    size_t            sent;
    char             *got;
    size_t            got_len;
    int               status;
    int               same_exchange;
    volatile int      done;
} client;

static client C;

static void
cl_produce_request(hc_client_handler *self, hc_request_channel *rc)
{
    client         *c  = (client *)self;
    hc_headers     *h  = hc_headers_new();
    hc_request_head rh = { "POST", "/echo-raw", "127.0.0.1", 11, h };
    hc_entity       e  = { -1, "text/plain" };      /* chunked */

    c->u             = upcase_new(NULL, rc);
    c->same_exchange = hc_request_channel_exchange(c->u->qc) != NULL &&
                       hc_request_channel_exchange(c->u->qc) == hc_request_channel_exchange(rc);
    hc_request_send(c->u->qc, &rh, &e);
    hc_headers_free(h);
}

static void
cl_produce(hc_client_handler *self, hc_data_channel *ch)
{
    client *c = (client *)self;
    char    buf[3000];

    (void)ch;

    while (c->sent < UPLOAD && hc_data_capacity(c->u->body) > 0) {
        size_t n = UPLOAD - c->sent < sizeof(buf) ? UPLOAD - c->sent : sizeof(buf);

        for (size_t i = 0; i < n; i++) {
            buf[i] = pattern(c->sent + i);
        }

        if (hc_data_write(c->u->body, buf, n) != (long)n) {
            return;
        }

        c->sent += n;
    }

    if (c->sent >= UPLOAD) {
        hc_data_end(c->u->body, NULL);
        c->sent++;      /* ended */
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

    c->got = realloc(c->got, c->got_len + len + 1);
    memcpy(c->got + c->got_len, data, len);
    c->got_len += len;
    c->got[c->got_len] = '\0';
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
    (void)self;
    fprintf(stderr, "  client failed: %s\n", reason);
}

static void
cl_release(hc_client_handler *self)
{
    client *c = (client *)self;

    upcase_free(c->u);
    c->u    = NULL;
    c->done = 1;
}

static const hc_client_handler_vtbl CLIENT_VTBL = {
    cl_produce_request, cl_consume_response, cl_consume, cl_stream_end,
    cl_produce, cl_failed, cl_release, NULL
};

static void
start_client(evutil_socket_t fd, short what, void *arg)
{
    hc_host h;

    (void)fd;
    (void)what;
    (void)arg;
    hc_host_init(&h, "http", "127.0.0.1", PORT);
    C.base.vtbl = &CLIENT_VTBL;
    RQ          = hc_requester_new(R, NULL);

    if (RQ == NULL || hc_requester_execute(RQ, &h, 20000, &C.base) != 0) {
        C.done = 1;
    }
}

/* ------------------------------------------------------------------------ */

static int
curl(const char *args, char *out, size_t cap)
{
    char   cmd[1024];
    FILE  *p;
    size_t n;

    snprintf(cmd, sizeof(cmd), "curl -sS -k --max-time 20 %s 2>&1", args);
    p      = popen(cmd, "r");
    n      = fread(out, 1, cap - 1, p);
    out[n] = '\0';

    return WEXITSTATUS(pclose(p));
}

static void
run_cases(const char *proto, const char *scheme, int port)
{
    static char out[BIG + 4096];
    char        args[512];
    int         ok;

    printf("%s:\n", proto);

    snprintf(args, sizeof(args), "%s -i %s://127.0.0.1:%d/simple", proto, scheme, port);
    CHECK(curl(args, out, sizeof(out)) == 0);
    CHECK(strcasestr(out, "\nx-wrapped: upcase") != NULL);
    CHECK(strstr(out, "\r\n\r\nHELLO, CHANNELS") != NULL);
    CHECK(strcasestr(out, "\ncontent-length: 15") != NULL);   /* length kept */
    printf("  send_simple through a wrapper: %s\n",
           strstr(out, "HELLO, CHANNELS") ? "header added, body upper-cased" : out);

    empty_rv = 99;
    snprintf(args, sizeof(args), "%s -i %s://127.0.0.1:%d/empty", proto, scheme, port);
    CHECK(curl(args, out, sizeof(out)) == 0);
    CHECK(strncmp(out, "HTTP/", 5) == 0 && strstr(out, " 204") != NULL);
    CHECK(strcasestr(out, "\nx-wrapped: upcase") != NULL);
    CHECK(empty_rv == 0);
    printf("  send_simple without a body: 204, returned %d\n", empty_rv);

    info_rv = 99;
    snprintf(args, sizeof(args), "%s %s://127.0.0.1:%d/info", proto, scheme, port);
    CHECK(curl(args, out, sizeof(out)) == 0 && strcmp(out, "AFTER INFO") == 0);
    CHECK(info_rv == -1);
    printf("  send_information on a wrapper without it: %d\n", info_rv);

    produce_calls = 0;
    snprintf(args, sizeof(args), "%s %s://127.0.0.1:%d/big", proto, scheme, port);
    CHECK(curl(args, out, sizeof(out)) == 0);
    ok = strlen(out) == BIG;
    for (size_t i = 0; ok && i < BIG; i++) {
        ok = out[i] == (char)toupper((unsigned char)pattern(i));
    }
    CHECK(ok);
    CHECK(produce_calls > 1);   /* the wrapper's capacity ran out and came back */
    printf("  %d-byte body, length unknown: %s, in %d produce() calls\n", BIG,
           ok ? "intact and upper-cased" : "WRONG", produce_calls);

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

static void
unit_cases(void)
{
    static const hc_data_channel_ops     no_end  = { up_write, NULL, up_capacity, NULL };
    static const hc_response_channel_ops no_send = { NULL, NULL };
    static const hc_request_channel_ops  no_qs   = { NULL, up_exchange };
    static const hc_request_channel_ops  no_exch = { up_send_request, NULL };
    hc_request_channel *qc;

    puts("unit:");
    CHECK(hc_data_channel_new(NULL, NULL) == NULL);
    CHECK(hc_data_channel_new(&no_end, NULL) == NULL);
    CHECK(hc_response_channel_new(&no_send, NULL) == NULL);
    CHECK(hc_request_channel_new(&no_qs, NULL) == NULL);

    qc = hc_request_channel_new(&no_exch, NULL);
    CHECK(qc != NULL && hc_request_channel_exchange(qc) == NULL);
    hc_request_channel_free(qc);

    hc_data_channel_free(NULL);
    hc_response_channel_free(NULL);
    hc_request_channel_free(NULL);

    CHECK(hc_response_submit(NULL, NULL, NULL, NULL) == -1);
    CHECK(hc_request_submit(NULL, NULL, NULL, NULL) == -1);
    printf("  incomplete ops refused, NULLs harmless\n");
}

int
main(int argc, char **argv)
{
    char               cert[512], key[512], out[256], args[256];
    struct event_base *base;
    hc_server         *s, *s_tls;
    pthread_t          th;
    int                ok;

    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc < 2) {
        fprintf(stderr, "usage: %s <tests/tls dir>\n", argv[0]);
        return 2;
    }

    snprintf(cert, sizeof(cert), "%s/localhost.pem", argv[1]);
    snprintf(key, sizeof(key), "%s/localhost.key", argv[1]);

    unit_cases();

    evthread_use_pthreads();
    base  = event_base_new();
    R     = hc_reactor_new_on_base(base);
    s     = hc_server_new(R, hc_basic_server_factory, &CONFIG);
    s_tls = hc_server_new(R, hc_basic_server_factory, &CONFIG);

    if (hc_server_listen(s, "127.0.0.1", PORT) != 0 ||
        hc_server_enable_tls(s_tls, cert, key) != 0 ||
        hc_server_listen(s_tls, "127.0.0.1", PORT_TLS) != 0) {
        fprintf(stderr, "setup failed\n");
        return 1;
    }

    pthread_create(&th, NULL, loop_main, base);

    run_cases("--http1.1", "http", PORT);

    snprintf(args, sizeof(args), "--http2 -o /dev/null -w '%%{http_version}' https://127.0.0.1:%d/simple",
             PORT_TLS);
    if (curl(args, out, sizeof(out)) == 0 && strcmp(out, "2") == 0) {
        run_cases("--http2", "https", PORT_TLS);
    } else {
        puts("--http2: skipped (no HTTP/2 in curl or libevhtp)");
    }

    puts("client:");
    event_base_once(base, -1, EV_TIMEOUT, start_client, NULL, NULL);
    for (int i = 0; i < 200 && !C.done; i++) {
        sleep_ms(50);
    }
    CHECK(C.done);
    CHECK(C.status == 200);
    CHECK(C.same_exchange);
    ok = C.got_len == strlen("upcase|") + UPLOAD && strncmp(C.got, "upcase|", 7) == 0;
    for (size_t i = 0; ok && i < UPLOAD; i++) {
        ok = C.got[7 + i] == (char)toupper((unsigned char)pattern(i));
    }
    CHECK(ok);
    printf("  %d-byte chunked request through a wrapper: %s; exchange reachable: %s\n", UPLOAD,
           ok ? "header added, body upper-cased" : "WRONG", C.same_exchange ? "yes" : "no");
    free(C.got);

    event_base_once(base, -1, EV_TIMEOUT, stop_cb, base, NULL);
    pthread_join(th, NULL);

    if (RQ) {
        hc_requester_free(RQ);
    }
    hc_server_free(s);
    hc_server_free(s_tls);
    hc_reactor_free(R);
    event_base_free(base);

    CHECK(released == delivered);
    printf("channel_test: %d checks, %d failed\n", passed + failed, failed);
    return failed ? 1 : 0;
}
