/*
 * hc_processor.c - interceptors, the HTTP processor and the exchange
 * context (see hc_processor.h).
 */
#include "hc_processor.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

/* ======================================================================== */
/* Context                                                                  */
/* ======================================================================== */

typedef struct ctx_attr {
    struct ctx_attr *next;
    char            *name;
    void            *value;
    void           (*free_fn)(void *);
} ctx_attr;

struct hc_context {
    int       is_server;
    int       version;      /* 0: not known yet */
    int       is_tls;
    int       has_target;
    hc_host   target;
    char     *remote;
    ctx_attr *attrs;
};

static char *
dup_str(const char *s)
{
    size_t n;
    char  *d;

    if (s == NULL) {
        return NULL;
    }

    n = strlen(s) + 1;

    if ((d = malloc(n)) != NULL) {
        memcpy(d, s, n);
    }

    return d;
}

hc_context *
hc_context_new(int is_server, const hc_host *target, const char *remote)
{
    hc_context *ctx = calloc(1, sizeof(*ctx));

    if (ctx == NULL) {
        return NULL;
    }

    ctx->is_server = is_server != 0;

    if (target) {
        ctx->target     = *target;
        ctx->has_target = 1;
        ctx->is_tls     = hc_host_is_tls(target);
    }

    if (remote && (ctx->remote = dup_str(remote)) == NULL) {
        free(ctx);
        return NULL;
    }

    return ctx;
}

static void
attr_free(ctx_attr *a)
{
    if (a->free_fn) {
        a->free_fn(a->value);
    }

    free(a->name);
    free(a);
}

void
hc_context_free(hc_context *ctx)
{
    if (ctx == NULL) {
        return;
    }

    while (ctx->attrs) {
        ctx_attr *a = ctx->attrs;

        ctx->attrs = a->next;
        attr_free(a);
    }

    free(ctx->remote);
    free(ctx);
}

void
hc_context_set_protocol(hc_context *ctx, int version, int is_tls)
{
    if (ctx) {
        ctx->version = version;
        ctx->is_tls  = is_tls != 0;
    }
}

static ctx_attr **
attr_find(const hc_context *ctx, const char *name)
{
    ctx_attr **pp = (ctx_attr **)&ctx->attrs;

    for (; *pp; pp = &(*pp)->next) {
        if (strcmp((*pp)->name, name) == 0) {
            return pp;
        }
    }

    return NULL;
}

int
hc_context_set_attribute(hc_context *ctx, const char *name, void *value,
                         void (*free_fn)(void *))
{
    ctx_attr **pp;
    ctx_attr  *a;

    if (ctx == NULL || name == NULL) {
        if (free_fn) {
            free_fn(value);
        }
        return -1;
    }

    if ((pp = attr_find(ctx, name)) != NULL) {
        a = *pp;

        if (a->free_fn && a->value != value) {
            a->free_fn(a->value);
        }

        a->value   = value;
        a->free_fn = free_fn;
        return 0;
    }

    if ((a = calloc(1, sizeof(*a))) == NULL || (a->name = dup_str(name)) == NULL) {
        free(a);
        if (free_fn) {
            free_fn(value);
        }
        return -1;
    }

    a->value   = value;
    a->free_fn = free_fn;
    a->next    = ctx->attrs;
    ctx->attrs = a;

    return 0;
}

void *
hc_context_get_attribute(const hc_context *ctx, const char *name)
{
    ctx_attr **pp = ctx && name ? attr_find(ctx, name) : NULL;

    return pp ? (*pp)->value : NULL;
}

void
hc_context_remove_attribute(hc_context *ctx, const char *name)
{
    ctx_attr **pp = ctx && name ? attr_find(ctx, name) : NULL;

    if (pp) {
        ctx_attr *a = *pp;

        *pp = a->next;
        attr_free(a);
    }
}

int
hc_context_is_server(const hc_context *ctx)
{
    return ctx ? ctx->is_server : 0;
}

int
hc_context_protocol_version(const hc_context *ctx)
{
    return ctx ? ctx->version : 0;
}

int
hc_context_is_tls(const hc_context *ctx)
{
    return ctx ? ctx->is_tls : 0;
}

const hc_host *
hc_context_target(const hc_context *ctx)
{
    return ctx && ctx->has_target ? &ctx->target : NULL;
}

const char *
hc_context_remote(const hc_context *ctx)
{
    return ctx ? ctx->remote : NULL;
}

/* ======================================================================== */
/* Builder and processor                                                    */
/* ======================================================================== */

typedef struct icpt {
    union {
        hc_request_interceptor_fn  req;
        hc_response_interceptor_fn resp;
    } fn;
    void  *arg;
    void (*free_arg)(void *);
} icpt;

typedef struct icpt_list {
    icpt  *v;
    size_t n, cap;
} icpt_list;

struct hc_processor_builder {
    icpt_list req, resp;
    int       failed;       /* an add failed: build() returns NULL */
};

struct hc_processor {
    int       refs;         /* atomic */
    icpt_list req, resp;
};

static int
list_add(icpt_list *l, icpt it, int first)
{
    if (l->n == l->cap) {
        size_t ncap = l->cap ? l->cap * 2 : 8;
        icpt  *nv   = realloc(l->v, ncap * sizeof(*nv));

        if (nv == NULL) {
            return -1;
        }

        l->v   = nv;
        l->cap = ncap;
    }

    if (first) {
        memmove(l->v + 1, l->v, l->n * sizeof(*l->v));
        l->v[0] = it;
    } else {
        l->v[l->n] = it;
    }

    l->n++;
    return 0;
}

static void
list_free(icpt_list *l)
{
    for (size_t i = 0; i < l->n; i++) {
        if (l->v[i].free_arg) {
            l->v[i].free_arg(l->v[i].arg);
        }
    }

    free(l->v);
    l->v = NULL;
    l->n = l->cap = 0;
}

hc_processor_builder *
hc_processor_builder_new(void)
{
    return calloc(1, sizeof(hc_processor_builder));
}

void
hc_processor_builder_free(hc_processor_builder *b)
{
    if (b) {
        list_free(&b->req);
        list_free(&b->resp);
        free(b);
    }
}

static int
builder_add(hc_processor_builder *b, int response, icpt it, int first)
{
    if (b == NULL || it.fn.req == NULL ||
        list_add(response ? &b->resp : &b->req, it, first) != 0) {
        if (it.free_arg) {
            it.free_arg(it.arg);
        }
        if (b) {
            b->failed = 1;
        }
        return -1;
    }

    return 0;
}

int
hc_processor_builder_add_request_first(hc_processor_builder *b, hc_request_interceptor_fn fn,
                                       void *arg, void (*free_arg)(void *))
{
    icpt it = { .fn.req = fn, .arg = arg, .free_arg = free_arg };

    return builder_add(b, 0, it, 1);
}

int
hc_processor_builder_add_request_last(hc_processor_builder *b, hc_request_interceptor_fn fn,
                                      void *arg, void (*free_arg)(void *))
{
    icpt it = { .fn.req = fn, .arg = arg, .free_arg = free_arg };

    return builder_add(b, 0, it, 0);
}

int
hc_processor_builder_add_response_first(hc_processor_builder *b, hc_response_interceptor_fn fn,
                                        void *arg, void (*free_arg)(void *))
{
    icpt it = { .fn.resp = fn, .arg = arg, .free_arg = free_arg };

    return builder_add(b, 1, it, 1);
}

int
hc_processor_builder_add_response_last(hc_processor_builder *b, hc_response_interceptor_fn fn,
                                       void *arg, void (*free_arg)(void *))
{
    icpt it = { .fn.resp = fn, .arg = arg, .free_arg = free_arg };

    return builder_add(b, 1, it, 0);
}

hc_processor *
hc_processor_builder_build(hc_processor_builder *b)
{
    hc_processor *p;

    if (b == NULL) {
        return NULL;
    }

    if (b->failed || (p = calloc(1, sizeof(*p))) == NULL) {
        hc_processor_builder_free(b);
        return NULL;
    }

    p->refs = 1;
    p->req  = b->req;
    p->resp = b->resp;
    free(b);

    return p;
}

hc_processor *
hc_processor_ref(hc_processor *p)
{
    if (p) {
        __atomic_add_fetch(&p->refs, 1, __ATOMIC_RELAXED);
    }

    return p;
}

void
hc_processor_unref(hc_processor *p)
{
    if (p && __atomic_sub_fetch(&p->refs, 1, __ATOMIC_ACQ_REL) == 0) {
        list_free(&p->req);
        list_free(&p->resp);
        free(p);
    }
}

/* Interceptor results: 0 go on; 400..599 as given; anything else 500. */
static int
verdict(int rv)
{
    return rv == 0 ? 0 : (rv >= 400 && rv <= 599) ? rv : 500;
}

int
hc_processor_process_request(const hc_processor *p, hc_http_request *request,
                             const hc_entity *entity, hc_context *ctx)
{
    for (size_t i = 0; p && i < p->req.n; i++) {
        int rv = verdict(p->req.v[i].fn.req(p->req.v[i].arg, request, entity, ctx));

        if (rv) {
            return rv;
        }
    }

    return 0;
}

int
hc_processor_process_response(const hc_processor *p, hc_http_response *response,
                              const hc_entity *entity, hc_context *ctx)
{
    for (size_t i = 0; p && i < p->resp.n; i++) {
        int rv = verdict(p->resp.v[i].fn.resp(p->resp.v[i].arg, response, entity, ctx));

        if (rv) {
            return rv;
        }
    }

    return 0;
}

/* ======================================================================== */
/* Standard interceptors                                                    */
/* ======================================================================== */

/* IMF-fixdate (RFC 9110 5.6.7), independent of the locale. */
static void
http_date(char out[64])
{
    static const char *const days[]   = { "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
    static const char *const months[] = { "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
    time_t    now = time(NULL);
    struct tm tm;

    gmtime_r(&now, &tm);
    snprintf(out, 64, "%s, %02d %s %04d %02d:%02d:%02d GMT",
             days[tm.tm_wday], tm.tm_mday, months[tm.tm_mon], tm.tm_year + 1900,
             tm.tm_hour, tm.tm_min, tm.tm_sec);
}

static int
add_if_absent(hc_headers *h, const char *name, const char *value)
{
    if (hc_headers_get(h, name) == NULL && hc_headers_add(h, name, value) != 0) {
        return 500;
    }

    return 0;
}

static int
target_host_cb(void *arg, hc_http_request *req, const hc_entity *entity, hc_context *ctx)
{
    const hc_host *t = hc_context_target(ctx);
    char           authority[300];

    (void)arg;
    (void)entity;

    if (req->authority || t == NULL || hc_headers_get(req->headers, "host")) {
        return 0;
    }

    hc_host_authority(t, authority, sizeof(authority));
    return add_if_absent(req->headers, "Host", authority);
}

static int
user_agent_cb(void *arg, hc_http_request *req, const hc_entity *entity, hc_context *ctx)
{
    (void)entity;
    (void)ctx;
    return add_if_absent(req->headers, "User-Agent", arg);
}

static int
request_date_cb(void *arg, hc_http_request *req, const hc_entity *entity, hc_context *ctx)
{
    char date[64];

    (void)arg;
    (void)ctx;

    if (entity == NULL) {
        return 0;
    }

    http_date(date);
    return add_if_absent(req->headers, "Date", date);
}

static int
default_headers_cb(void *arg, hc_http_request *req, const hc_entity *entity, hc_context *ctx)
{
    const hc_headers *defaults = arg;
    const char       *k, *v;

    (void)entity;
    (void)ctx;

    for (size_t i = 0; hc_headers_at(defaults, i, &k, &v) == 0; i++) {
        if (add_if_absent(req->headers, k, v) != 0) {
            return 500;
        }
    }

    return 0;
}

static int
response_date_cb(void *arg, hc_http_response *resp, const hc_entity *entity, hc_context *ctx)
{
    char date[64];

    (void)arg;
    (void)entity;
    (void)ctx;

    if (resp->status < 200) {
        return 0;
    }

    http_date(date);
    return add_if_absent(resp->headers, "Date", date);
}

static int
response_server_cb(void *arg, hc_http_response *resp, const hc_entity *entity, hc_context *ctx)
{
    (void)entity;
    (void)ctx;
    return add_if_absent(resp->headers, "Server", arg);
}

static void
free_headers(void *h)
{
    hc_headers_free(h);
}

int
hc_processor_builder_add_target_host(hc_processor_builder *b)
{
    return hc_processor_builder_add_request_last(b, target_host_cb, NULL, NULL);
}

int
hc_processor_builder_add_user_agent(hc_processor_builder *b, const char *agent)
{
    char *a = dup_str(agent ? agent : "hcore");

    if (a == NULL) {
        if (b) {
            b->failed = 1;
        }
        return -1;
    }

    return hc_processor_builder_add_request_last(b, user_agent_cb, a, free);
}

int
hc_processor_builder_add_request_date(hc_processor_builder *b)
{
    return hc_processor_builder_add_request_last(b, request_date_cb, NULL, NULL);
}

int
hc_processor_builder_add_default_headers(hc_processor_builder *b, const hc_headers *headers)
{
    hc_headers *copy = hc_headers_copy(headers);

    if (copy == NULL) {
        if (b) {
            b->failed = 1;
        }
        return -1;
    }

    return hc_processor_builder_add_request_last(b, default_headers_cb, copy, free_headers);
}

int
hc_processor_builder_add_response_date(hc_processor_builder *b)
{
    return hc_processor_builder_add_response_last(b, response_date_cb, NULL, NULL);
}

int
hc_processor_builder_add_response_server(hc_processor_builder *b, const char *name)
{
    char *n = dup_str(name ? name : "hcore");

    if (n == NULL) {
        if (b) {
            b->failed = 1;
        }
        return -1;
    }

    return hc_processor_builder_add_response_last(b, response_server_cb, n, free);
}

hc_processor *
hc_processor_client_default(const char *agent)
{
    hc_processor_builder *b = hc_processor_builder_new();

    hc_processor_builder_add_target_host(b);
    hc_processor_builder_add_user_agent(b, agent);

    return hc_processor_builder_build(b);
}

hc_processor *
hc_processor_server_default(const char *name)
{
    hc_processor_builder *b = hc_processor_builder_new();

    hc_processor_builder_add_response_date(b);
    hc_processor_builder_add_response_server(b, name);

    return hc_processor_builder_build(b);
}
