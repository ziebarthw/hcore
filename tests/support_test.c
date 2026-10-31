/*
 * support_test.c - in-process integration tests for hc_support.h (and the
 * server/client paths under it): an HTTP and an HTTPS test server and a
 * requester share one reactor, and a list of cases runs one after another,
 * each started from the previous one's released() callback.
 *
 * Usage: support_test <tests/tls dir>
 */
#include "hc.h"
#include "hc_pool.h"
#include "hc_support.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define HTTP_PORT  9301
#define HTTPS_PORT 9302
#define ROUTER_PORT 9303

static hc_reactor   *R;
static hc_requester *RQ;
static int           failures, checks;

#define CHECK(cond)                                                          \
    do {                                                                     \
        checks++;                                                            \
        if (!(cond)) {                                                       \
            failures++;                                                      \
            fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        }                                                                    \
    } while (0)

/* ======================================================================== */
/* Test server: /echo streams the body back, /hints sends 103 first,        */
/* anything else answers "hello"                                            */
/* ======================================================================== */

typedef struct srv_handler {
    hc_server_handler base;
    hc_data_channel  *out;
} srv_handler;

static void
srv_handle_request(hc_server_handler *self, hc_server_exchange *ex,
                   const hc_request_head *head, const hc_entity *entity,
                   hc_response_channel *rc)
{
    srv_handler     *s = (srv_handler *)(void *)self;
    hc_headers      *h = hc_headers_new();
    hc_response_head r = { 200, 11, h };
    hc_entity        chunked = { -1, "application/octet-stream" };

    (void)ex;

    if (strncmp(head->target, "/hints", 6) == 0) {
        hc_headers_add(h, "Link", "</a.css>; rel=preload");
        r.status = 103;
        CHECK(hc_response_send_information(rc, &r) == 0);
        r.status = 100;
        CHECK(hc_response_send_information(rc, &r) == -1);  /* engine's job */
        hc_response_send_simple(rc, 200, "text/plain", "hinted", 0);
    } else if (strncmp(head->target, "/echo", 5) == 0 && entity) {
        s->out = hc_response_send(rc, &r, &chunked);
    } else {
        hc_response_send_simple(rc, 200, "text/plain", "hello", 0);
    }

    hc_headers_free(h);
}

static void
srv_consume(hc_server_handler *self, const void *data, size_t len)
{
    srv_handler *s = (srv_handler *)(void *)self;

    if (s->out) {
        hc_data_write(s->out, data, len);
    }
}

static void
srv_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    srv_handler *s = (srv_handler *)(void *)self;

    (void)trailers;

    if (s->out) {
        hc_data_end(s->out, NULL);
    }
}

static void
srv_release(hc_server_handler *self)
{
    free(self);
}

static const hc_server_handler_vtbl srv_vtbl = {
    .handle_request = srv_handle_request,
    .consume        = srv_consume,
    .stream_end     = srv_stream_end,
    .release        = srv_release,
};

static hc_server_handler *
srv_factory(void *arg)
{
    srv_handler *s = calloc(1, sizeof(*s));

    (void)arg;
    s->base.vtbl = &srv_vtbl;

    return &s->base;
}

/* ======================================================================== */
/* Router test server: each route answers with its own tag                  */
/* ======================================================================== */

typedef struct tag_handler {
    hc_server_handler base;
    const char       *tag;
} tag_handler;

/* What hc_server_exchange_remote/local_address() reported, per request. */
static struct {
    int  calls, ok;
    char remote[64], local[64];
} A;

static void
sockaddr_str(const struct sockaddr_in *sin, char *buf, size_t len)
{
    char ip[INET_ADDRSTRLEN];

    inet_ntop(AF_INET, &sin->sin_addr, ip, sizeof(ip));
    snprintf(buf, len, "%s:%u", ip, (unsigned)ntohs(sin->sin_port));
}

static void
record_addresses(hc_server_exchange *ex)
{
    struct sockaddr_storage remote, local;
    struct sockaddr_in      small;
    socklen_t               rlen = sizeof(remote), llen = sizeof(local), slen = 4;
    int                     ok;

    A.calls++;
    ok = hc_server_exchange_remote_address(ex, (struct sockaddr *)&remote, &rlen) == 0 &&
         hc_server_exchange_local_address(ex, (struct sockaddr *)&local, &llen) == 0 &&
         remote.ss_family == AF_INET && rlen == sizeof(struct sockaddr_in) &&
         local.ss_family == AF_INET && llen == sizeof(struct sockaddr_in);

    /* like getpeername(): a short buffer gets a prefix, *len the full size */
    ok = ok && hc_server_exchange_remote_address(ex, (struct sockaddr *)&small, &slen) == 0 &&
         slen == sizeof(struct sockaddr_in);
    /* asking again uses the cached local address */
    llen = sizeof(local);
    ok = ok && hc_server_exchange_local_address(ex, (struct sockaddr *)&local, &llen) == 0;
    ok = ok && hc_server_exchange_remote_address(ex, NULL, &rlen) == -1;

    if (ok) {
        A.ok++;
        sockaddr_str((struct sockaddr_in *)&remote, A.remote, sizeof(A.remote));
        sockaddr_str((struct sockaddr_in *)&local, A.local, sizeof(A.local));
    }
}

static void
tag_handle_request(hc_server_handler *self, hc_server_exchange *ex,
                   const hc_request_head *head, const hc_entity *entity,
                   hc_response_channel *rc)
{
    record_addresses(ex);
    (void)head;
    (void)entity;
    hc_response_send_simple(rc, 200, "text/plain", ((tag_handler *)(void *)self)->tag, 0);
}

static void
tag_consume(hc_server_handler *self, const void *data, size_t len)
{
    (void)self;
    (void)data;
    (void)len;
}

static void
tag_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    (void)self;
    (void)trailers;
}

static void
tag_release(hc_server_handler *self)
{
    free(self);
}

static const hc_server_handler_vtbl tag_vtbl = {
    .handle_request = tag_handle_request,
    .consume        = tag_consume,
    .stream_end     = tag_stream_end,
    .release        = tag_release,
};

static hc_server_handler *
tag_factory(void *arg)
{
    tag_handler *t = calloc(1, sizeof(*t));

    t->base.vtbl = &tag_vtbl;
    t->tag       = arg;

    return &t->base;
}

static struct {
    int  requests, responses, completed, kept_alive;
    int  last_status;
    char last_target[128];
    char remote[64];
} L;

static void
l_request(void *arg, const char *remote, const hc_request_head *head)
{
    (void)arg;
    L.requests++;
    snprintf(L.last_target, sizeof(L.last_target), "%s", head->target);
    snprintf(L.remote, sizeof(L.remote), "%s", remote);
}

static void
l_response(void *arg, const char *remote, const hc_response_head *head)
{
    (void)arg;
    (void)remote;
    L.responses++;
    L.last_status = head->status;
}

static void
l_complete(void *arg, const char *remote, int keep_alive)
{
    (void)arg;
    (void)remote;
    L.completed++;
    L.kept_alive += keep_alive;
}

/* The requester's stream listener (Java: Http1StreamListener on the
 * requester), reset at the start of every case. */
static struct {
    int  requests, responses, interim, completed, kept_alive;
    int  last_status, interim_status;
    char last_method[16];
    char last_target[128];
    char remote[64];
    int  order_ok;              /* request head before every response head */
} CL;

static void
cl_reset(void)
{
    memset(&CL, 0, sizeof(CL));
    CL.order_ok = 1;
}

static void
cl_request(void *arg, const char *remote, const hc_request_head *head)
{
    (void)arg;
    CL.requests++;
    snprintf(CL.last_method, sizeof(CL.last_method), "%s", head->method);
    snprintf(CL.last_target, sizeof(CL.last_target), "%s", head->target);
    snprintf(CL.remote, sizeof(CL.remote), "%s", remote);
}

static void
cl_response(void *arg, const char *remote, const hc_response_head *head)
{
    (void)arg;
    CL.order_ok = CL.order_ok && CL.requests > 0 && strcmp(remote, CL.remote) == 0;

    if (head->status < 200) {
        CL.interim++;
        CL.interim_status = head->status;
    } else {
        CL.responses++;
        CL.last_status = head->status;
    }
}

static void
cl_complete(void *arg, const char *remote, int keep_alive)
{
    (void)arg;
    CL.order_ok = CL.order_ok && CL.responses > 0 && strcmp(remote, CL.remote) == 0;
    CL.completed++;
    CL.kept_alive += keep_alive;
}

/* requests, final responses, interim responses, completions, kept alive */
static int
cl_counts(int rq, int rs, int in, int co, int ka)
{
    int ok = CL.order_ok && CL.requests == rq && CL.responses == rs && CL.interim == in &&
             CL.completed == co && CL.kept_alive == ka;

    if (!ok) {
        fprintf(stderr, "    requester listener: %d %d %d %d %d (order %d), expected %d %d %d %d %d\n",
                CL.requests, CL.responses, CL.interim, CL.completed, CL.kept_alive,
                CL.order_ok, rq, rs, in, co, ka);
    }

    return ok;
}

static int
cl_remote_is(int port)
{
    char want[32];

    snprintf(want, sizeof(want), "127.0.0.1:%d", port);

    return strcmp(CL.remote, want) == 0;
}

/* ======================================================================== */
/* Cases                                                                    */
/* ======================================================================== */

typedef struct outcome {
    int    completed, failed, released, information;
    int    status, info_status;
    char   reason[256];
    char  *body;
    size_t body_len;
    char   info_link[64];
} outcome;

static outcome   O;
static int       step;
static hc_timer *last_timer;
static void      next_case(void);
static void      next_route_case(void);
static int       route_step;      /* > 0 while the routing cases run */

static void
next_case_cb(void *arg)
{
    (void)arg;
    next_case();
}

static void
cb_completed(void *arg, const hc_message *m)
{
    size_t      len = 0;
    const char *d   = m->body ? hc_buffer_entity_consumer_data(m->body, &len) : "";

    (void)arg;
    O.completed++;
    O.status   = m->head->status;
    O.body     = malloc(len + 1);
    memcpy(O.body, d, len + 1);
    O.body_len = len;
}

static void
cb_failed(void *arg, const char *reason)
{
    (void)arg;
    O.failed++;
    snprintf(O.reason, sizeof(O.reason), "%s", reason);
}

static void
cb_information(void *arg, const hc_response_head *head)
{
    const char *link = hc_headers_get(head->headers, "link");

    (void)arg;
    O.information++;
    O.info_status = head->status;
    snprintf(O.info_link, sizeof(O.info_link), "%s", link ? link : "");
}

static void
cb_released(void *arg)
{
    (void)arg;
    O.released++;
    /* Start the next case from the loop, not from inside this callback. The
     * timer is freed by next_case() when it fires. */
    last_timer = hc_timer_new(R, next_case_cb, NULL);
    hc_timer_start(last_timer, 0);
}

static const hc_result_callback CB = { cb_completed, cb_failed, cb_information, cb_released, NULL };

static unsigned char *big;
#define BIG_LEN (3u * 1024 * 1024 + 7)

static void
run(const char *method, const char *uri, hc_entity_producer *entity, size_t max_body,
    const char *expect_header)
{
    hc_request_producer *req = hc_request_producer_new(method, uri, entity);
    hc_entity_consumer  *body = hc_buffer_entity_consumer_new(max_body);

    free(O.body);
    memset(&O, 0, sizeof(O));
    cl_reset();

    if (req == NULL) {
        CHECK(!"request producer");
        return;
    }

    if (expect_header) {
        hc_request_producer_add_header(req, "Expect", expect_header);
    }

    CHECK(hc_requester_execute_request(RQ, req, body, 10000, &CB) == 0);
}

static hc_pool_stats
route_stats(const char *scheme, uint16_t port)
{
    hc_host h;

    hc_host_init(&h, scheme, scheme[4] == 's' ? "localhost" : "127.0.0.1", port);

    return hc_pool_get_route_stats(hc_requester_pool(RQ), &h);
}

/* Each case checks the previous case's outcome, then starts its own. */
static void
next_case(void)
{
    char url[128];

    /* free the timer that fired us (created in cb_released) */
    if (last_timer) {
        hc_timer_free(last_timer);
        last_timer = NULL;
    }

    if (route_step > 0) {
        next_route_case();
        return;
    }

    switch (step++) {
        case 0:
            puts("string producer POST, buffer consumer");
            snprintf(url, sizeof(url), "http://127.0.0.1:%d/echo", HTTP_PORT);
            run("POST", url, hc_string_entity_producer_new("stuff", NULL), 0, NULL);
            break;

        case 1:
            CHECK(O.completed == 1 && O.failed == 0 && O.released == 1);
            CHECK(O.status == 200 && O.body_len == 5 && strcmp(O.body, "stuff") == 0);
            /* requester listener: request head, response head, completion */
            CHECK(cl_counts(1, 1, 0, 1, 1));
            CHECK(strcmp(CL.last_method, "POST") == 0 && strcmp(CL.last_target, "/echo") == 0);
            CHECK(CL.last_status == 200 && cl_remote_is(HTTP_PORT));
            puts("bytes producer 3MB round trip (chunked response)");
            snprintf(url, sizeof(url), "http://127.0.0.1:%d/echo", HTTP_PORT);
            run("PUT", url, hc_bytes_entity_producer_new(big, BIG_LEN, NULL), 0, NULL);
            break;

        case 2:
            CHECK(O.completed == 1 && O.body_len == BIG_LEN && memcmp(O.body, big, BIG_LEN) == 0);
            CHECK(route_stats("http", HTTP_PORT).available == 1);   /* kept alive */
            puts("GET without a body");
            snprintf(url, sizeof(url), "http://127.0.0.1:%d/plain?x=1", HTTP_PORT);
            run("GET", url, NULL, 0, NULL);
            break;

        case 3:
            CHECK(O.completed == 1 && strcmp(O.body, "hello") == 0);
            puts("consumer limit: 1000-byte buffer, 3MB response -> failed, connection dropped");
            snprintf(url, sizeof(url), "http://127.0.0.1:%d/echo", HTTP_PORT);
            run("POST", url, hc_bytes_entity_producer_new(big, BIG_LEN, NULL), 1000, NULL);
            break;

        case 4:
            CHECK(O.completed == 0 && O.failed == 1 && O.released == 1);
            CHECK(strstr(O.reason, "rejected") != NULL);
            CHECK(route_stats("http", HTTP_PORT).available == 0);   /* not reused */
            CHECK(cl_counts(1, 1, 0, 0, 0));    /* head seen, never completed */
            puts("server sendInformation(103) -> client information callback");
            snprintf(url, sizeof(url), "http://127.0.0.1:%d/hints", HTTP_PORT);
            run("GET", url, NULL, 0, NULL);
            break;

        case 5:
            CHECK(O.information == 1 && O.info_status == 103);
            CHECK(strcmp(O.info_link, "</a.css>; rel=preload") == 0);
            CHECK(O.completed == 1 && strcmp(O.body, "hinted") == 0);
            CHECK(cl_counts(1, 1, 1, 1, 1) && CL.interim_status == 103);
            puts("Expect: 100-continue -> 100 from the engine, before any response head");
            snprintf(url, sizeof(url), "http://127.0.0.1:%d/echo", HTTP_PORT);
            run("POST", url, hc_bytes_entity_producer_new(big, BIG_LEN, NULL), 0, "100-continue");
            break;

        case 6:
            CHECK(O.information == 1 && O.info_status == 100);
            CHECK(cl_counts(1, 1, 1, 1, 1) && CL.interim_status == 100);
            CHECK(O.completed == 1 && O.body_len == BIG_LEN && memcmp(O.body, big, BIG_LEN) == 0);
            puts("https: 3MB round trip with verification against the test CA");
            snprintf(url, sizeof(url), "https://localhost:%d/echo", HTTPS_PORT);
            run("POST", url, hc_bytes_entity_producer_new(big, BIG_LEN, NULL), 0, NULL);
            break;

        case 7:
            CHECK(O.completed == 1 && O.body_len == BIG_LEN && memcmp(O.body, big, BIG_LEN) == 0);
            CHECK(route_stats("https", HTTPS_PORT).available == 1);
            CHECK(cl_counts(1, 1, 0, 1, 1) && cl_remote_is(HTTPS_PORT));
            puts("https again: reuses the pooled TLS connection");
            snprintf(url, sizeof(url), "https://localhost:%d/hints", HTTPS_PORT);
            run("GET", url, NULL, 0, NULL);
            break;

        case 8:
            CHECK(O.completed == 1 && O.information == 1 && strcmp(O.body, "hinted") == 0);
            CHECK(route_stats("https", HTTPS_PORT).available == 1);
            puts("https by IP: certificate is for localhost -> verification fails");
            snprintf(url, sizeof(url), "https://127.0.0.1:%d/x", HTTPS_PORT);
            run("GET", url, NULL, 0, NULL);
            break;

        case 9:
            CHECK(O.completed == 0 && O.failed == 1 && O.released == 1);
            CHECK(strstr(O.reason, "IP address mismatch") != NULL);
            CHECK(cl_counts(0, 0, 0, 0, 0));    /* nothing was sent */
            puts("http to a closed port -> failed, released once");
            run("GET", "http://127.0.0.1:9/x", NULL, 0, NULL);
            break;

        case 10:
            CHECK(O.completed == 0 && O.failed == 1 && O.released == 1);
            CHECK(strstr(O.reason, "Connection refused") != NULL);
            CHECK(cl_counts(0, 0, 0, 0, 0));
            puts("bad URI is rejected up front");
            CHECK(hc_request_producer_new("GET", "ftp://x/", hc_string_entity_producer_new("x", NULL)) == NULL);
            CHECK(hc_request_producer_new("GET", "no-scheme", NULL) == NULL);
            puts("routing: exact, prefix, suffix, longest wins, tie -> '*' suffix pattern, 404");
            next_route_case();
            break;
    }
}

/* Routing: one request per entry, checked as each completes. */
static const struct {
    const char *path;
    int         status;
    const char *body;
} ROUTES[] = {
    { "/echo",          200, "exact" },     /* exact beats the "/e*" wildcard     */
    { "/echo?x=1",      200, "exact" },     /* query is not part of the path      */
    { "/echoes",        200, "e-prefix" },  /* "/e*"                              */
    { "/api/x",         200, "api" },       /* "/api*"                            */
    { "/api/v2/x",      200, "v2" },        /* "/api/v2*" is longer than "/api*"  */
    { "/f.txt",         200, "txt" },       /* "*.txt"                            */
    { "/api/x.txt",     200, "api" },       /* tie "/api*" vs "*.txt": '*'-ending */
    { "/nothing/here",  404, "Resource not found" },
};

static void
route_completed(void *arg, const hc_message *m)
{
    size_t      len;
    const char *body = hc_buffer_entity_consumer_data(m->body, &len);
    int         i    = route_step - 1;

    (void)arg;
    CHECK(m->head->status == ROUTES[i].status);
    CHECK(strcmp(body, ROUTES[i].body) == 0);

    if (m->head->status != ROUTES[i].status || strcmp(body, ROUTES[i].body) != 0) {
        fprintf(stderr, "    route %s -> %d \"%s\"\n", ROUTES[i].path, m->head->status, body);
    }
}

static void
route_failed(void *arg, const char *reason)
{
    (void)arg;
    fprintf(stderr, "    route %s failed: %s\n", ROUTES[route_step - 1].path, reason);
    CHECK(!"route request failed");
}

static void
route_released(void *arg)
{
    (void)arg;
    last_timer = hc_timer_new(R, next_case_cb, NULL);
    hc_timer_start(last_timer, 0);
}

static void
next_route_case(void)
{
    static const hc_result_callback cb = { route_completed, route_failed, NULL, route_released, NULL };
    char                            url[128];
    hc_request_producer            *req;

    if (route_step == (int)(sizeof(ROUTES) / sizeof(ROUTES[0]))) {
        int n = route_step;

        /* listener: one request head, response head and normal completion
         * per request, all on one kept-alive connection */
        CHECK(L.requests == n && L.responses == n && L.completed == n);
        CHECK(L.kept_alive == n);
        CHECK(L.last_status == 404 && strcmp(L.last_target, "/nothing/here") == 0);
        CHECK(strncmp(L.remote, "127.0.0.1:", 10) == 0);

        /* requester listener: the same exchanges from the client's side */
        CHECK(cl_counts(n, n, 0, n, n) && cl_remote_is(ROUTER_PORT));
        CHECK(strcmp(CL.last_method, "GET") == 0 && strcmp(CL.last_target, "/nothing/here") == 0);
        CHECK(CL.last_status == 404);

        /* exchange addresses: every handler saw both; the remote one is the
         * client the listener reported, the local one this server */
        {
            char local_expected[32];

            snprintf(local_expected, sizeof(local_expected), "127.0.0.1:%d", ROUTER_PORT);
            CHECK(A.calls == n - 1 && A.ok == A.calls);   /* the 404 has no tag handler */
            CHECK(strcmp(A.remote, L.remote) == 0);
            CHECK(strcmp(A.local, local_expected) == 0);
            printf("  exchange addresses: remote %s, local %s\n", A.remote, A.local);
        }
        step = -1;                          /* done */
        hc_reactor_stop(R);
        return;
    }

    if (route_step == 0) {
        cl_reset();
    }

    snprintf(url, sizeof(url), "http://127.0.0.1:%d%s", ROUTER_PORT, ROUTES[route_step++].path);
    req = hc_request_producer_new("GET", url, NULL);
    CHECK(req && hc_requester_execute_request(RQ, req, hc_buffer_entity_consumer_new(0),
                                              10000, &cb) == 0);
}

static void
first_case(void *arg)
{
    (void)arg;
    next_case();
}

int
main(int argc, char **argv)
{
    char                cert[512], key[512];
    hc_requester_config cfg = { 0 };
    hc_server          *http, *https, *router;
    hc_server_listener  lis = { l_request, l_response, l_complete, NULL };
    hc_requester_listener clis = { cl_request, cl_response, cl_complete, NULL };
    hc_timer           *start;

    if (argc < 2) {
        fprintf(stderr, "usage: %s <tests/tls dir>\n", argv[0]);
        return 2;
    }

    snprintf(cert, sizeof(cert), "%s/localhost.pem", argv[1]);
    snprintf(key, sizeof(key), "%s/localhost.key", argv[1]);
    cfg.tls_ca_file = cert;

    big = malloc(BIG_LEN);

    for (size_t i = 0; i < BIG_LEN; i++) {
        big[i] = (unsigned char)(i * 131 + (i >> 9));
    }

    R     = hc_reactor_new();
    RQ    = hc_requester_new(R, &cfg);
    http  = hc_server_new(R, srv_factory, NULL);
    https = hc_server_new(R, srv_factory, NULL);
    router = hc_server_new(R, NULL, NULL);          /* unmatched: 404 */
    hc_server_set_listener(router, &lis);
    hc_requester_set_listener(RQ, &clis);

    if (!RQ || hc_server_listen(http, "127.0.0.1", HTTP_PORT) != 0 ||
        hc_server_enable_tls(https, cert, key) != 0 ||
        hc_server_listen(https, "127.0.0.1", HTTPS_PORT) != 0 ||
        hc_server_register(router, "/echo", tag_factory, "exact") != 0 ||
        hc_server_register(router, "/e*", tag_factory, "e-prefix") != 0 ||
        hc_server_register(router, "/api*", tag_factory, "api") != 0 ||
        hc_server_register(router, "/api/v2*", tag_factory, "v2") != 0 ||
        hc_server_register(router, "*.txt", tag_factory, "txt") != 0 ||
        hc_server_listen(router, "127.0.0.1", ROUTER_PORT) != 0) {
        fprintf(stderr, "setup failed\n");
        return 1;
    }

    start = hc_timer_new(R, first_case, NULL);
    hc_timer_start(start, 0);
    hc_reactor_run(R);

    hc_timer_free(start);
    free(O.body);
    hc_requester_free(RQ);
    hc_server_free(http);
    hc_server_free(https);
    hc_server_free(router);
    hc_reactor_free(R);
    free(big);

    printf("%d checks, %d failed\n", checks, failures);

    return failures || step != -1 ? 1 : 0;
}
