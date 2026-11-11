/*
 * decorator_test.c - handler decorator lists on a server and a requester:
 *
 *   - list operations: first, last, before, after, replace, remove;
 *     duplicate and unknown names refused
 *   - order on a server: the first in the list sees the request first and
 *     the response last (checked with tracing decorators, over curl)
 *   - a decorator answering early (401) without passing the request on:
 *     the decorators inside it get ->release() only, the application is
 *     never called
 *   - a decorator returning its inner handler unchanged: no effect
 *   - a decorator failing (NULL): 503, what it was given is released
 *   - a transform handler as a decorator (hc_server_transform_decorator)
 *   - routes (hc_server_register) are decorated too
 *   - order on a requester: the first in the list is closest to the
 *     connection, so it sees the request last and the response first
 *   - a client decorator failing: the exchange fails, the handler gets
 *     ->failed("cannot start exchange: decorator failed") and ->release()
 *   - every handler released exactly once
 *
 * Usage: decorator_test
 */
#define _GNU_SOURCE
#include "hc.h"
#include "hc_channel.h"
#include "hc_support.h"
#include "hc_transform.h"

#include <ctype.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/wait.h>

#include <event2/event.h>
#include <event2/thread.h>

#define PORT 9399

static int passed, failed;

#define CHECK(c) do {                                                      \
        if (c) { passed++; }                                               \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } \
    } while (0)

static hc_reactor  *R;
static char         trace[512];
static volatile int made, released, fail_next;

static void
sleep_ms(int ms)
{
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };

    nanosleep(&ts, NULL);
}

static void
note(const char *s)
{
    strncat(trace, s, sizeof(trace) - strlen(trace) - 1);
}

/* ------------------------------------------------------------------------ */
/* A tracing server decorator: ">name" on the request, "<name" on the      */
/* response head                                                            */
/* ------------------------------------------------------------------------ */

typedef struct strace {
    hc_server_handler    base;
    hc_server_handler   *inner;
    const char          *name;
    hc_response_channel *real;
    hc_response_channel *rc;
} strace;

static int
st_send(void *arg, const hc_response_head *head, const hc_entity *entity, hc_data_channel **body)
{
    strace *t = arg;

    note("<");
    note(t->name);
    return hc_response_submit(t->real, head, entity, body);
}

static const hc_response_channel_ops ST_RC = { st_send, NULL };

static void
st_handle_request(hc_server_handler *self, hc_server_exchange *ex, const hc_request_head *head,
                  const hc_entity *entity, hc_response_channel *rc)
{
    strace *t = (strace *)self;

    note(">");
    note(t->name);
    t->real = rc;
    t->rc   = hc_response_channel_new(&ST_RC, t);
    t->inner->vtbl->handle_request(t->inner, ex, head, entity, t->rc);
}

static void
st_consume(hc_server_handler *self, const void *data, size_t len)
{
    strace *t = (strace *)self;

    t->inner->vtbl->consume(t->inner, data, len);
}

static void
st_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    strace *t = (strace *)self;

    t->inner->vtbl->stream_end(t->inner, trailers);
}

static void
st_produce(hc_server_handler *self, hc_data_channel *ch)
{
    strace *t = (strace *)self;

    if (t->inner->vtbl->produce) {
        t->inner->vtbl->produce(t->inner, ch);
    }
}

static void
st_failed(hc_server_handler *self, const char *reason)
{
    strace *t = (strace *)self;

    t->inner->vtbl->failed(t->inner, reason);
}

static void
st_release(hc_server_handler *self)
{
    strace *t = (strace *)self;

    released++;
    t->inner->vtbl->release(t->inner);
    hc_response_channel_free(t->rc);
    free(t);
}

static const hc_server_handler_vtbl ST_VTBL = {
    st_handle_request, st_consume, st_stream_end, st_produce, st_failed, st_release
};

static hc_server_handler *
st_decorate(void *arg, hc_server_handler *inner)
{
    strace *t = calloc(1, sizeof(*t));

    made++;
    t->base.vtbl = &ST_VTBL;
    t->inner     = inner;
    t->name      = arg;
    return &t->base;
}

/* ------------------------------------------------------------------------ */
/* An authorising decorator: 401 without "X-Token: ok"                       */
/* ------------------------------------------------------------------------ */

typedef struct auth {
    hc_server_handler  base;
    hc_server_handler *inner;
    int                passed;
} auth;

static void
au_handle_request(hc_server_handler *self, hc_server_exchange *ex, const hc_request_head *head,
                  const hc_entity *entity, hc_response_channel *rc)
{
    auth       *a   = (auth *)self;
    const char *tok = hc_headers_get(head->headers, "X-Token");

    if (tok == NULL || strcmp(tok, "ok") != 0) {
        note("!auth");
        hc_response_send_simple(rc, 401, "text/plain", "no\n", 0);
        return;     /* never passed on */
    }

    a->passed = 1;
    a->inner->vtbl->handle_request(a->inner, ex, head, entity, rc);
}

static void
au_consume(hc_server_handler *self, const void *data, size_t len)
{
    auth *a = (auth *)self;

    if (a->passed) {
        a->inner->vtbl->consume(a->inner, data, len);
    }
}

static void
au_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    auth *a = (auth *)self;

    if (a->passed) {
        a->inner->vtbl->stream_end(a->inner, trailers);
    }
}

static void
au_produce(hc_server_handler *self, hc_data_channel *ch)
{
    auth *a = (auth *)self;

    if (a->passed && a->inner->vtbl->produce) {
        a->inner->vtbl->produce(a->inner, ch);
    }
}

static void
au_failed(hc_server_handler *self, const char *reason)
{
    auth *a = (auth *)self;

    if (a->passed) {
        a->inner->vtbl->failed(a->inner, reason);
    }
}

static void
au_release(hc_server_handler *self)
{
    auth *a = (auth *)self;

    released++;
    a->inner->vtbl->release(a->inner);
    free(a);
}

static const hc_server_handler_vtbl AU_VTBL = {
    au_handle_request, au_consume, au_stream_end, au_produce, au_failed, au_release
};

static hc_server_handler *
au_decorate(void *arg, hc_server_handler *inner)
{
    auth *a = calloc(1, sizeof(*a));

    (void)arg;
    made++;
    a->base.vtbl = &AU_VTBL;
    a->inner     = inner;
    return &a->base;
}

static hc_server_handler *
skip_decorate(void *arg, hc_server_handler *inner)
{
    (void)arg;
    return inner;
}

static hc_server_handler *
failing_decorate(void *arg, hc_server_handler *inner)
{
    (void)arg;
    return fail_next ? NULL : inner;
}

/* ------------------------------------------------------------------------ */
/* Transform: upper-case the response of /upper                             */
/* ------------------------------------------------------------------------ */

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
upper_finish(void *state, hc_bytes *out)
{
    (void)state;
    (void)out;
    return 0;
}

static const hc_transform_ops UPPER = { upper_update, upper_finish, NULL, NULL };

static int
choose_upper(void *arg, const hc_request_head *request, int *status, hc_headers *headers,
             const hc_entity *entity, hc_transform *t)
{
    (void)arg;
    (void)status;
    (void)headers;

    if (entity && strncmp(request->target, "/upper", 6) == 0) {
        t->ops = &UPPER;
    }

    return 0;
}

static const hc_server_transform_config UPPER_CFG = { NULL, choose_upper, NULL, 0, 0 };

/* ------------------------------------------------------------------------ */
/* The application                                                          */
/* ------------------------------------------------------------------------ */

static volatile int delivered, app_released;

static void
app_request(void *arg, hc_basic_request *r)
{
    char msg[128];

    delivered++;
    snprintf(msg, sizeof(msg), "%s %s", (const char *)arg, hc_basic_request_target(r));
    hc_response_send_simple(hc_basic_request_channel(r), 200, "text/plain", msg, 0);
}

static void
app_release(void *arg, hc_basic_request *r)
{
    (void)arg;
    (void)r;
    app_released++;
}

static hc_basic_server_config APP   = { .request = app_request, .release = app_release, .arg = "app" };
static hc_basic_server_config ROUTE = { .request = app_request, .release = app_release, .arg = "route" };

/* ------------------------------------------------------------------------ */
/* Client side: a tracing decorator, and a client                           */
/* ------------------------------------------------------------------------ */

typedef struct ctrace {
    hc_client_handler   base;
    hc_client_handler  *inner;
    const char         *name;
    hc_request_channel *real;
    hc_request_channel *rc;
} ctrace;

static int
ct_send(void *arg, const hc_request_head *head, const hc_entity *entity, hc_data_channel **body)
{
    ctrace *t = arg;

    note(">");
    note(t->name);
    return hc_request_submit(t->real, head, entity, body);
}

static hc_client_exchange *
ct_exchange(void *arg)
{
    return hc_request_channel_exchange(((ctrace *)arg)->real);
}

static const hc_request_channel_ops CT_RC = { ct_send, ct_exchange };

static void
ct_produce_request(hc_client_handler *self, hc_request_channel *rc)
{
    ctrace *t = (ctrace *)self;

    t->real = rc;
    t->rc   = hc_request_channel_new(&CT_RC, t);
    t->inner->vtbl->produce_request(t->inner, t->rc);
}

static void
ct_consume_response(hc_client_handler *self, const hc_response_head *head, const hc_entity *entity)
{
    ctrace *t = (ctrace *)self;

    note("<");
    note(t->name);
    t->inner->vtbl->consume_response(t->inner, head, entity);
}

static void
ct_consume(hc_client_handler *self, const void *data, size_t len)
{
    ctrace *t = (ctrace *)self;

    t->inner->vtbl->consume(t->inner, data, len);
}

static void
ct_stream_end(hc_client_handler *self, const hc_headers *trailers)
{
    ctrace *t = (ctrace *)self;

    t->inner->vtbl->stream_end(t->inner, trailers);
}

static void
ct_produce(hc_client_handler *self, hc_data_channel *ch)
{
    ctrace *t = (ctrace *)self;

    if (t->inner->vtbl->produce) {
        t->inner->vtbl->produce(t->inner, ch);
    }
}

static void
ct_failed(hc_client_handler *self, const char *reason)
{
    ctrace *t = (ctrace *)self;

    t->inner->vtbl->failed(t->inner, reason);
}

static void
ct_release(hc_client_handler *self)
{
    ctrace *t = (ctrace *)self;

    released++;
    t->inner->vtbl->release(t->inner);
    hc_request_channel_free(t->rc);
    free(t);
}

static const hc_client_handler_vtbl CT_VTBL = {
    ct_produce_request, ct_consume_response, ct_consume, ct_stream_end,
    ct_produce, ct_failed, ct_release, NULL
};

static hc_client_handler *
ct_decorate(void *arg, hc_client_handler *inner)
{
    ctrace *t = calloc(1, sizeof(*t));

    made++;
    t->base.vtbl = &CT_VTBL;
    t->inner     = inner;
    t->name      = arg;
    return &t->base;
}

static hc_client_handler *
cl_failing_decorate(void *arg, hc_client_handler *inner)
{
    (void)arg;
    return fail_next ? NULL : inner;
}

typedef struct client {
    hc_client_handler base;
    int               status;
    char              got[128];
    size_t            got_len;
    char              reason[64];
    int               failures;
    volatile int      released;
} client;

static void
cl_produce_request(hc_client_handler *self, hc_request_channel *rc)
{
    hc_headers     *h  = hc_headers_new();
    hc_request_head rh = { "GET", "/client", "127.0.0.1", 11, h };

    (void)self;
    hc_headers_add(h, "X-Token", "ok");     /* past the server's auth decorator */
    note(">app");
    hc_request_send(rc, &rh, NULL);
    hc_headers_free(h);
}

static void
cl_consume_response(hc_client_handler *self, const hc_response_head *head, const hc_entity *entity)
{
    (void)entity;
    note("<app");
    ((client *)self)->status = head->status;
}

static void
cl_consume(hc_client_handler *self, const void *data, size_t len)
{
    client *c = (client *)self;

    if (c->got_len + len < sizeof(c->got)) {
        memcpy(c->got + c->got_len, data, len);
        c->got_len += len;
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
    client *c = (client *)self;

    c->failures++;
    snprintf(c->reason, sizeof(c->reason), "%s", reason);
}

static void
cl_release(hc_client_handler *self)
{
    ((client *)self)->released++;
}

static const hc_client_handler_vtbl CL_VTBL = {
    cl_produce_request, cl_consume_response, cl_consume, cl_stream_end,
    NULL, cl_failed, cl_release, NULL
};

static hc_requester *RQ;

static void
start_client(evutil_socket_t fd, short what, void *arg)
{
    client *c = arg;
    hc_host h;

    (void)fd;
    (void)what;
    hc_host_init(&h, "http", "127.0.0.1", PORT);

    if (hc_requester_execute(RQ, &h, 10000, &c->base) != 0) {
        c->released = 1;
    }
}

static void
run_client(struct event_base *base, client *c)
{
    memset(c, 0, sizeof(*c));
    c->base.vtbl = &CL_VTBL;
    trace[0]     = '\0';
    event_base_once(base, -1, EV_TIMEOUT, start_client, c, NULL);

    for (int i = 0; i < 200 && !c->released; i++) {
        sleep_ms(50);
    }

    sleep_ms(50);
}

/* ------------------------------------------------------------------------ */

static int
curl(const char *args, char *out, size_t cap)
{
    char   cmd[512];
    FILE  *p;
    size_t n;

    snprintf(cmd, sizeof(cmd), "curl -sS --max-time 10 %s 2>&1", args);
    trace[0] = '\0';
    p        = popen(cmd, "r");
    n        = fread(out, 1, cap - 1, p);
    out[n]   = '\0';

    return WEXITSTATUS(pclose(p));
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
main(void)
{
    struct event_base *base;
    hc_server         *s;
    pthread_t          th;
    char               out[512], args[256];
    client             c;
    int                before;

    setvbuf(stdout, NULL, _IONBF, 0);
    evthread_use_pthreads();
    base = event_base_new();
    R    = hc_reactor_new_on_base(base);
    s    = hc_server_new(R, hc_basic_server_factory, &APP);
    RQ   = hc_requester_new(R, NULL);

    puts("list operations:");
    CHECK(hc_server_add_decorator_last(s, "b", st_decorate, "b") == 0);
    CHECK(hc_server_add_decorator_first(s, "a", st_decorate, "a") == 0);
    CHECK(hc_server_add_decorator_after(s, "b", "d", st_decorate, "d") == 0);
    CHECK(hc_server_add_decorator_before(s, "d", "c", st_decorate, "c") == 0);
    CHECK(hc_server_add_decorator_last(s, "b", st_decorate, "x") == -1);       /* duplicate */
    CHECK(hc_server_add_decorator_after(s, "nope", "e", st_decorate, "e") == -1);
    CHECK(hc_server_add_decorator_before(s, "nope", "e", st_decorate, "e") == -1);
    CHECK(hc_server_add_decorator_last(s, NULL, st_decorate, "e") == -1);
    CHECK(hc_server_add_decorator_last(s, "e", NULL, NULL) == -1);
    CHECK(hc_server_remove_decorator(s, "nope") == -1);
    CHECK(hc_server_replace_decorator(s, "nope", st_decorate, "x") == -1);
    CHECK(hc_server_add_decorator_last(s, "gone", st_decorate, "gone") == 0);
    CHECK(hc_server_remove_decorator(s, "gone") == 0);
    CHECK(hc_server_replace_decorator(s, "d", st_decorate, "D") == 0);
    /* a b c D, then the rest */
    CHECK(hc_server_add_decorator_after(s, "a", "auth", au_decorate, NULL) == 0);
    CHECK(hc_server_add_decorator_last(s, "skip", skip_decorate, NULL) == 0);
    CHECK(hc_server_add_decorator_first(s, "fail", failing_decorate, NULL) == 0);
    CHECK(hc_server_add_decorator_last(s, "upper", hc_server_transform_decorator,
                                       (void *)&UPPER_CFG) == 0);
    CHECK(hc_server_register(s, "/route*", hc_basic_server_factory, &ROUTE) == 0);
    puts("  first/last/before/after/replace/remove: ok; duplicates and unknown names refused");

    CHECK(hc_requester_add_decorator_last(RQ, "c2", ct_decorate, "c2") == 0);
    CHECK(hc_requester_add_decorator_first(RQ, "c1", ct_decorate, "c1") == 0);
    CHECK(hc_requester_add_decorator_first(RQ, "cfail", cl_failing_decorate, NULL) == 0);
    CHECK(hc_requester_add_decorator_first(RQ, "c1", ct_decorate, "c1") == -1);

    if (hc_server_listen(s, "127.0.0.1", PORT) != 0) {
        fprintf(stderr, "setup failed\n");
        return 1;
    }

    pthread_create(&th, NULL, loop_main, base);

    puts("server:");
    snprintf(args, sizeof(args), "-H 'X-Token: ok' http://127.0.0.1:%d/hello", PORT);
    CHECK(curl(args, out, sizeof(out)) == 0 && strcmp(out, "app /hello") == 0);
    CHECK(strcmp(trace, ">a>b>c>D<D<c<b<a") == 0);
    printf("  order: %s (first in the list: request first, response last)\n", trace);

    before = app_released;
    snprintf(args, sizeof(args), "-w ' %%{http_code}' http://127.0.0.1:%d/hello", PORT);
    curl(args, out, sizeof(out));
    sleep_ms(100);
    CHECK(strcmp(out, "no\n 401") == 0);
    CHECK(strcmp(trace, ">a!auth<a") == 0);
    CHECK(app_released == before);  /* the basic application never had it */
    printf("  401 from a decorator: %s; inner decorators released, application not called\n", trace);

    snprintf(args, sizeof(args), "-H 'X-Token: ok' http://127.0.0.1:%d/route/x", PORT);
    CHECK(curl(args, out, sizeof(out)) == 0 && strcmp(out, "route /route/x") == 0);
    CHECK(strcmp(trace, ">a>b>c>D<D<c<b<a") == 0);
    puts("  a registered route: decorated the same");

    snprintf(args, sizeof(args), "-H 'X-Token: ok' http://127.0.0.1:%d/upper", PORT);
    CHECK(curl(args, out, sizeof(out)) == 0 && strcmp(out, "APP /UPPER") == 0);
    printf("  transform handler as a decorator: %s\n", out);

    fail_next = 1;
    snprintf(args, sizeof(args), "-H 'X-Token: ok' -o /dev/null -w '%%{http_code}' "
             "http://127.0.0.1:%d/hello", PORT);
    curl(args, out, sizeof(out));
    fail_next = 0;
    CHECK(strcmp(out, "503") == 0 && trace[0] == '\0');
    printf("  failing decorator: %s, nothing ran\n", out);

    puts("client:");
    run_client(base, &c);
    CHECK(c.status == 200 && c.released == 1 && c.failures == 0);
    CHECK(strncmp(c.got, "app /client", 11) == 0);
    /* trace mixes in the server's own decorators; keep the client's parts */
    {
        char  mine[128] = "";
        char *p         = trace;

        while ((p = strpbrk(p, "<>")) != NULL) {
            size_t n = strcspn(p + 1, "<>!");

            if ((n == 2 && p[1] == 'c') || (n == 3 && strncmp(p + 1, "app", 3) == 0)) {
                strncat(mine, p, n + 1);
            }

            p += n + 1;
        }

        CHECK(strcmp(mine, ">app>c2>c1<c1<c2<app") == 0);
        printf("  order: %s (first in the list: request last, response first)\n", mine);
    }

    fail_next = 1;
    run_client(base, &c);
    fail_next = 0;
    CHECK(c.released == 1 && c.failures == 1 &&
          strcmp(c.reason, "cannot start exchange: decorator failed") == 0);
    printf("  failing decorator: failed(\"%s\"), released %d\n", c.reason, c.released);

    sleep_ms(100);
    event_base_once(base, -1, EV_TIMEOUT, stop_cb, base, NULL);
    pthread_join(th, NULL);

    hc_requester_free(RQ);
    hc_server_free(s);
    hc_reactor_free(R);
    event_base_free(base);

    CHECK(made == released);
    CHECK(app_released == delivered);
    printf("decorators made %d, released %d\n", made, released);
    printf("decorator_test: %d checks, %d failed\n", passed + failed, failed);
    return failed ? 1 : 0;
}
