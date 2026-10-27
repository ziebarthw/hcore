/*
 * processor_test.c - interceptors and HTTP processors (hc_processor.h) on
 * both sides of real exchanges: an hcore client (requester with a
 * processor) talks to an hcore server (with its own processor) on one
 * reactor.
 *
 *   server   request interceptors run in order (first/last), change headers
 *            the handler sees, and share the context with the handler and
 *            the response interceptors; one refuses a request (403, the
 *            handler never runs); response interceptors add Date/Server,
 *            change the status, and one refuses (connection dropped)
 *   client   standard interceptors (Host from the target, User-Agent,
 *            default headers, Date on requests with a body), a custom one
 *            with the context carried to the response side; a refused
 *            request (nothing sent) and a refused response
 *   both     one processor shared by several owners, unreferenced early
 *            (valgrind: nothing leaks)
 */
#include "hc.h"
#include "hc_processor.h"
#include "hc_support.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PORT 9350

static int passed, failed;

#define CHECK(c) do {                                                      \
        if (c) { passed++; }                                               \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } \
    } while (0)

static hc_reactor   *R;
static hc_requester *RQ;
static int           handler_runs;

/* ======================================================================== */
/* Server interceptors                                                      */
/* ======================================================================== */

static int
srv_order_cb(void *arg, hc_http_request *req, const hc_entity *e, hc_context *ctx)
{
    (void)e;
    (void)ctx;
    hc_headers_add(req->headers, "X-Order", arg);     /* "first", "second" */
    return 0;
}

static int
srv_tag_cb(void *arg, hc_http_request *req, const hc_entity *e, hc_context *ctx)
{
    static int n;
    char      *rid = malloc(16);

    (void)arg;
    (void)e;
    snprintf(rid, 16, "rid-%d", ++n);
    hc_context_set_attribute(ctx, "rid", rid, free);    /* for handler + response */
    hc_headers_remove(req->headers, "X-Secret");        /* the handler never sees it */
    CHECK(hc_context_is_server(ctx) && hc_context_target(ctx) == NULL);
    CHECK(hc_context_remote(ctx) && strncmp(hc_context_remote(ctx), "127.0.0.1:", 10) == 0);
    return 0;
}

static int
srv_refuse_cb(void *arg, hc_http_request *req, const hc_entity *e, hc_context *ctx)
{
    (void)arg;
    (void)e;
    (void)ctx;
    return strcmp(req->target, "/forbidden") == 0 ? 403 : 0;
}

static int
srv_resp_cb(void *arg, hc_http_response *resp, const hc_entity *e, hc_context *ctx)
{
    const char *rid = hc_context_get_attribute(ctx, "rid");

    (void)arg;
    (void)e;

    if (rid) {
        hc_headers_add(resp->headers, "X-Rid", rid);
    }

    if (resp->status == 299) {
        resp->status = 200;                             /* rewritten */
        hc_headers_add(resp->headers, "X-Status-Was", "299");
    }

    return hc_headers_get(resp->headers, "X-Drop") ? 500 : 0;
}

/* ======================================================================== */
/* Server handler: tells what it saw                                         */
/* ======================================================================== */

typedef struct sh {
    hc_server_handler    base;
    hc_server_exchange  *ex;
    hc_response_channel *rc;
    char                 target[512];
    char                 seen[1024];
} sh;

static void
sh_request(hc_server_handler *self, hc_server_exchange *ex, const hc_request_head *head,
           const hc_entity *entity, hc_response_channel *rc)
{
    sh         *t = (sh *)(void *)self;
    const char *k, *v;
    char        order[64] = "";
    hc_context *ctx = hc_server_exchange_context(ex);
    const char *rid = hc_context_get_attribute(ctx, "rid");

    (void)entity;
    handler_runs++;
    t->ex = ex;
    t->rc = rc;
    snprintf(t->target, sizeof(t->target), "%s", head->target);

    for (size_t i = 0; hc_headers_at(head->headers, i, &k, &v) == 0; i++) {
        if (strcasecmp(k, "x-order") == 0) {
            strncat(order, v, sizeof(order) - strlen(order) - 2);
            strcat(order, ",");
        }
    }

    snprintf(t->seen, sizeof(t->seen),
             "host=%s ua=%s accept=%s date=%s x-client=%s secret=%s order=%s rid=%s target=%s",
             head->authority ? head->authority : "-",
             hc_headers_get(head->headers, "user-agent") ? hc_headers_get(head->headers, "user-agent") : "-",
             hc_headers_get(head->headers, "accept") ? hc_headers_get(head->headers, "accept") : "-",
             hc_headers_get(head->headers, "date") ? "yes" : "no",
             hc_headers_get(head->headers, "x-client") ? hc_headers_get(head->headers, "x-client") : "-",
             hc_headers_get(head->headers, "x-secret") ? "seen" : "hidden",
             order, rid ? rid : "-", t->target);
}

static void sh_consume(hc_server_handler *s, const void *d, size_t n) { (void)s; (void)d; (void)n; }

static void
sh_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    sh              *t = (sh *)(void *)self;
    hc_headers      *h = hc_headers_new();
    hc_response_head rh = { 200, 11, h };
    hc_entity        e  = { (int64_t)strlen(t->seen), "text/plain" };
    hc_data_channel *ch;

    (void)trailers;

    if (strcmp(t->target, "/status299") == 0) {
        rh.status = 299;
    }
    if (strcmp(t->target, "/drop") == 0) {
        hc_headers_add(h, "X-Drop", "1");
    }
    if (strcmp(t->target, "/bad") == 0) {
        hc_headers_add(h, "X-Bad", "1");
    }

    if ((ch = hc_response_send(t->rc, &rh, &e)) != NULL) {
        hc_data_write(ch, t->seen, strlen(t->seen));
        hc_data_end(ch, NULL);
    }

    hc_headers_free(h);
}

static void sh_release(hc_server_handler *self) { free(self); }

static const hc_server_handler_vtbl sh_vtbl = {
    .handle_request = sh_request,
    .consume        = sh_consume,
    .stream_end     = sh_stream_end,
    .release        = sh_release,
};

static hc_server_handler *
sh_factory(void *arg)
{
    sh *t = calloc(1, sizeof(*t));

    (void)arg;
    t->base.vtbl = &sh_vtbl;
    return &t->base;
}

/* ======================================================================== */
/* Client interceptors                                                      */
/* ======================================================================== */

static int
cli_req_cb(void *arg, hc_http_request *req, const hc_entity *e, hc_context *ctx)
{
    (void)arg;
    (void)e;
    CHECK(!hc_context_is_server(ctx) && hc_context_target(ctx) != NULL);
    CHECK(hc_context_target(ctx) && hc_context_target(ctx)->port == PORT);
    hc_context_set_attribute(ctx, "sent", (void *)"yes", NULL);
    hc_headers_add(req->headers, "X-Client", "on");
    hc_headers_add(req->headers, "X-Secret", "s3cret");
    return strstr(req->target, "client-refuse") ? 403 : 0;
}

static int
cli_resp_cb(void *arg, hc_http_response *resp, const hc_entity *e, hc_context *ctx)
{
    (void)arg;
    (void)e;

    /* the request side's context is still there */
    if (hc_context_get_attribute(ctx, "sent")) {
        hc_headers_add(resp->headers, "X-Client-Seen", "yes");
    }

    return hc_headers_get(resp->headers, "X-Bad") ? 502 : 0;
}

/* ======================================================================== */
/* Running requests                                                         */
/* ======================================================================== */

typedef struct outcome {
    int    completed, failed_n;
    int    status;
    char   reason[128];
    char   body[1024];
    char   rid[32], server[64], date[64], client_seen[8], status_was[8];
} outcome;

static outcome O;

static void
copy_hdr(char *dst, size_t n, const hc_headers *h, const char *name)
{
    const char *v = hc_headers_get(h, name);

    snprintf(dst, n, "%s", v ? v : "");
}

static void
on_completed(void *arg, const hc_message *m)
{
    size_t len;

    (void)arg;
    O.completed++;
    O.status = m->head->status;
    snprintf(O.body, sizeof(O.body), "%s", hc_buffer_entity_consumer_data(m->body, &len));
    copy_hdr(O.rid, sizeof(O.rid), m->head->headers, "x-rid");
    copy_hdr(O.server, sizeof(O.server), m->head->headers, "server");
    copy_hdr(O.date, sizeof(O.date), m->head->headers, "date");
    copy_hdr(O.client_seen, sizeof(O.client_seen), m->head->headers, "x-client-seen");
    copy_hdr(O.status_was, sizeof(O.status_was), m->head->headers, "x-status-was");
}

static void
on_failed(void *arg, const char *reason)
{
    (void)arg;
    O.failed_n++;
    snprintf(O.reason, sizeof(O.reason), "%s", reason);
}

static void
on_released(void *arg)
{
    (void)arg;
    hc_reactor_stop(R);
}

static void
run(const char *method, const char *path, const char *body)
{
    char                uri[512];
    hc_result_callback  cb = { on_completed, on_failed, NULL, on_released, NULL };
    hc_request_producer *req;

    memset(&O, 0, sizeof(O));
    snprintf(uri, sizeof(uri), "http://127.0.0.1:%d%s", PORT, path);
    req = hc_request_producer_new(method, uri, body ? hc_string_entity_producer_new(body, NULL) : NULL);
    CHECK(hc_requester_execute_request(RQ, req, hc_buffer_entity_consumer_new(0), 5000, &cb) == 0);
    hc_reactor_run(R);
}

int
main(void)
{
    hc_processor_builder *b;
    hc_processor         *srv_proc, *cli_proc;
    hc_requester_config   cfg = { 0 };
    hc_server            *S;
    hc_headers           *defaults = hc_headers_new();
    int                   runs;

    R = hc_reactor_new();

    /* server: "second" added last, "first" added first: they run first, second */
    b = hc_processor_builder_new();
    CHECK(hc_processor_builder_add_request_last(b, srv_order_cb, "second", NULL) == 0);
    CHECK(hc_processor_builder_add_request_first(b, srv_order_cb, "first", NULL) == 0);
    hc_processor_builder_add_request_last(b, srv_tag_cb, NULL, NULL);
    hc_processor_builder_add_request_last(b, srv_refuse_cb, NULL, NULL);
    hc_processor_builder_add_response_date(b);
    hc_processor_builder_add_response_server(b, "test-server");
    hc_processor_builder_add_response_last(b, srv_resp_cb, NULL, NULL);
    srv_proc = hc_processor_builder_build(b);
    CHECK(srv_proc != NULL);

    S = hc_server_new(R, sh_factory, NULL);
    hc_server_set_processor(S, srv_proc);
    hc_processor_unref(srv_proc);                       /* the server keeps it */
    CHECK(hc_server_listen(S, "127.0.0.1", PORT) == 0);

    /* client: standard interceptors + a custom one */
    hc_headers_add(defaults, "Accept", "text/plain");
    b = hc_processor_builder_new();
    hc_processor_builder_add_target_host(b);
    hc_processor_builder_add_user_agent(b, "test-agent/1.0");
    hc_processor_builder_add_default_headers(b, defaults);
    hc_processor_builder_add_request_date(b);
    hc_processor_builder_add_request_last(b, cli_req_cb, NULL, NULL);
    hc_processor_builder_add_response_last(b, cli_resp_cb, NULL, NULL);
    cli_proc = hc_processor_builder_build(b);
    hc_headers_free(defaults);                          /* copied */
    cfg.processor = cli_proc;
    RQ = hc_requester_new(R, &cfg);
    hc_processor_unref(cli_proc);                       /* the requester keeps it */
    CHECK(RQ != NULL);

    puts("GET: both processors");
    run("GET", "/hello", NULL);
    printf("  server handler saw: %s\n", O.body);
    CHECK(O.completed == 1 && O.status == 200);
    CHECK(strstr(O.body, "host=127.0.0.1:9350 ") != NULL);
    CHECK(strstr(O.body, "ua=test-agent/1.0 ") != NULL);
    CHECK(strstr(O.body, "accept=text/plain ") != NULL);
    CHECK(strstr(O.body, "date=no ") != NULL);          /* no body: no Date */
    CHECK(strstr(O.body, "x-client=on ") != NULL);
    CHECK(strstr(O.body, "secret=hidden ") != NULL);    /* removed server-side */
    CHECK(strstr(O.body, "order=first,second,") != NULL);
    CHECK(strstr(O.body, "rid=rid-1") != NULL);         /* context: interceptor -> handler */
    CHECK(strcmp(O.rid, "rid-1") == 0);                 /* context: -> response interceptor */
    CHECK(strcmp(O.server, "test-server") == 0);
    CHECK(strlen(O.date) == 29 && strstr(O.date, " GMT") != NULL);
    CHECK(strcmp(O.client_seen, "yes") == 0);           /* client context, request -> response */
    printf("  response: Server: %s, Date: %s, X-Rid: %s\n", O.server, O.date, O.rid);
    CHECK(strstr(O.body, " target=/hello") != NULL);   /* no query: the path itself */

    puts("request-target with a query reaches the handler as path?query");
    run("GET", "/q?x=1&y=2", NULL);
    CHECK(O.completed == 1 && O.status == 200);
    CHECK(strstr(O.body, " target=/q?x=1&y=2") != NULL);
    {
        /* longer than the 256-byte stack scratch: built on the heap */
        char path[400], want[420];

        memset(path, 0, sizeof(path));
        strcpy(path, "/long?v=");
        memset(path + 8, 'z', 300);
        snprintf(want, sizeof(want), " target=%s", path);
        run("GET", path, NULL);
        CHECK(O.completed == 1 && O.status == 200);
        CHECK(strstr(O.body, want) != NULL);
    }

    puts("POST: Date on a request with a body");
    run("POST", "/post", "data");
    CHECK(O.completed == 1 && strstr(O.body, "date=yes ") != NULL);

    puts("server response interceptor changes the status");
    run("GET", "/status299", NULL);
    CHECK(O.completed == 1 && O.status == 200 && strcmp(O.status_was, "299") == 0);

    puts("server request interceptor refuses: 403, the handler never runs");
    runs = handler_runs;
    run("GET", "/forbidden", NULL);
    CHECK(O.completed == 1 && O.status == 403 && strcmp(O.body, "Request refused") == 0);
    CHECK(handler_runs == runs);
    CHECK(strcmp(O.server, "test-server") == 0);         /* response interceptors ran */

    puts("server response interceptor refuses: the connection is dropped");
    run("GET", "/drop", NULL);
    CHECK(O.completed == 0 && O.failed_n == 1);
    printf("  client: %s\n", O.reason);

    puts("client request interceptor refuses: nothing sent");
    runs = handler_runs;
    run("GET", "/client-refuse", NULL);
    CHECK(O.failed_n == 1 && strcmp(O.reason, "request refused by interceptor") == 0);
    CHECK(handler_runs == runs);

    puts("client response interceptor refuses");
    run("GET", "/bad", NULL);
    CHECK(O.failed_n == 1 && strcmp(O.reason, "response refused by interceptor") == 0);

    puts("afterwards the client and server still work");
    run("GET", "/hello", NULL);
    CHECK(O.completed == 1 && O.status == 200);

    puts("defaults and a shared processor");
    {
        hc_processor *c = hc_processor_client_default(NULL);
        hc_processor *s = hc_processor_server_default("x");
        hc_headers   *h = hc_headers_new();
        hc_context   *cc;
        hc_host       t;
        hc_http_request  rq = { "GET", "/", NULL, 11, h };
        hc_http_response rs = { 200, 11, NULL };

        hc_host_init(&t, "http", "example.org", 8080);
        cc = hc_context_new(0, &t, NULL);
        CHECK(hc_processor_process_request(c, &rq, NULL, cc) == 0);
        CHECK(strcmp(hc_headers_get(h, "host"), "example.org:8080") == 0);
        CHECK(strcmp(hc_headers_get(h, "user-agent"), "hcore") == 0);
        rs.headers = h;
        CHECK(hc_processor_process_response(s, &rs, NULL, cc) == 0);
        CHECK(strcmp(hc_headers_get(h, "server"), "x") == 0 && hc_headers_get(h, "date"));

        /* several owners; the last unref frees it */
        hc_processor_ref(s);
        hc_processor_ref(s);
        hc_processor_unref(s);
        hc_processor_unref(s);
        hc_processor_unref(s);
        hc_processor_unref(c);
        hc_context_free(cc);
        hc_headers_free(h);
    }

    hc_requester_free(RQ);
    hc_server_free(S);
    hc_reactor_free(R);

    printf("processor_test: %d checks, %d failed\n", passed + failed, failed);
    return failed ? 1 : 0;
}
