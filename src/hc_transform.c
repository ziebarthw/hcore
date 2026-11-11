/*
 * hc_transform.c - body transformations: hc_bytes, and the server and
 * client transform handlers (hc_transform.h).
 *
 * Engine-independent: hc.h and hc_channel.h only. A transform handler is a
 * wrapper in the shape of exec_handler in hc_evhtp.c: it stands in for the
 * application's handler, forwards every call, and puts itself between the
 * handler and the engine where a body is transformed:
 *
 *   incoming   ->consume() / ->stream_end() run the transformation and
 *              pass its output on
 *   outgoing   the wrapped handler gets a response/request channel of the
 *              wrapper's (hc_channel.h); when it sends a head with a body to
 *              transform, it gets a data channel of the wrapper's too, whose
 *              writes are transformed into a pending buffer and sent to the
 *              engine's channel as it has room
 */
#include "hc_transform.h"
#include "hc_channel.h"

#include <stdlib.h>
#include <string.h>

#define HC_TRANSFORM_HIGH_WATER (64 * 1024)

/* ======================================================================== */
/* hc_bytes                                                                 */
/* ======================================================================== */

void *
hc_bytes_reserve(hc_bytes *b, size_t n)
{
    if (b->cap - b->len < n) {
        size_t         cap = b->cap ? b->cap : 256;
        unsigned char *p;

        while (cap - b->len < n) {
            if (cap > ((size_t)-1) / 2) {
                return NULL;
            }

            cap *= 2;
        }

        if ((p = realloc(b->data, cap)) == NULL) {
            return NULL;
        }

        b->data = p;
        b->cap  = cap;
    }

    return b->data + b->len;
}

void
hc_bytes_commit(hc_bytes *b, size_t n)
{
    b->len += n <= b->cap - b->len ? n : b->cap - b->len;
}

int
hc_bytes_append(hc_bytes *b, const void *p, size_t n)
{
    void *dst;

    if (n == 0) {
        return 0;
    }

    if ((dst = hc_bytes_reserve(b, n)) == NULL) {
        return -1;
    }

    memcpy(dst, p, n);
    b->len += n;
    return 0;
}

void
hc_bytes_consume(hc_bytes *b, size_t n)
{
    if (n >= b->len) {
        b->len = 0;
    } else {
        memmove(b->data, b->data + n, b->len - n);
        b->len -= n;
    }
}

void
hc_bytes_free(hc_bytes *b)
{
    free(b->data);
    b->data = NULL;
    b->len  = 0;
    b->cap  = 0;
}

/* ======================================================================== */
/* Shared parts                                                             */
/* ======================================================================== */

static const char REFUSED[] = "body transformation refused";
static const char FAILED[]  = "body transformation failed";

static void
transform_drop(hc_transform *t)
{
    if (t->ops) {
        if (t->ops->free) {
            t->ops->free(t->state);
        }

        t->ops   = NULL;
        t->state = NULL;
    }
}

/* A copy of a request head, kept for the choose_response functions. */
typedef struct head_copy {
    hc_request_head head;
    char           *strings;    /* method, target, authority back to back */
    hc_headers     *headers;
} head_copy;

static int
head_copy_set(head_copy *c, const hc_request_head *h, hc_headers *headers)
{
    size_t m = strlen(h->method ? h->method : "") + 1;
    size_t t = strlen(h->target ? h->target : "") + 1;
    size_t a = h->authority ? strlen(h->authority) + 1 : 0;

    if ((c->strings = malloc(m + t + a)) == NULL) {
        return -1;
    }

    memcpy(c->strings, h->method ? h->method : "", m);
    memcpy(c->strings + m, h->target ? h->target : "", t);

    if (a) {
        memcpy(c->strings + m + t, h->authority, a);
    }

    c->headers        = headers;
    c->head.method    = c->strings;
    c->head.target    = c->strings + m;
    c->head.authority = a ? c->strings + m + t : NULL;
    c->head.version   = h->version;
    c->head.headers   = headers;
    return 0;
}

static void
head_copy_free(head_copy *c)
{
    free(c->strings);
    hc_headers_free(c->headers);
    memset(c, 0, sizeof(*c));
}

/* The status a choose function returned: 0, or one that refuses. */
static int
refusal(int rv)
{
    return rv == 0 ? 0 : (rv >= 400 && rv <= 599 ? rv : 500);
}

/* --- incoming bodies ----------------------------------------------------- */

typedef struct in_body {
    hc_transform t;
    hc_bytes     out;
} in_body;

/* Run data through the transformation (or finish it: data NULL) and hand
 * the output to deliver. -1 when the transformation fails. */
static int
in_run(in_body *in, const void *data, size_t len,
       void (*deliver)(void *arg, const void *p, size_t n), void *arg)
{
    int rv = data ? in->t.ops->update(in->t.state, data, len, &in->out)
                  : in->t.ops->finish(in->t.state, &in->out);

    if (rv != 0) {
        return -1;
    }

    if (in->out.len > 0) {
        deliver(arg, in->out.data, in->out.len);
        in->out.len = 0;
    }

    return 0;
}

static void
in_free(in_body *in)
{
    if (in) {
        transform_drop(&in->t);
        hc_bytes_free(&in->out);
        free(in);
    }
}

/* --- outgoing bodies ----------------------------------------------------- */

typedef struct out_body {
    hc_transform     t;
    hc_data_channel *real;      /* the engine's (or the next wrapper's)  */
    hc_data_channel *ch;        /* the wrapped handler writes to this    */
    hc_bytes         pending;   /* transformed, not yet taken by real    */
    hc_headers      *trailers;
    size_t           high_water;
    int              flush_each_write;
    int              ended;     /* the wrapped handler called end        */
    int              sent_end;  /* hc_data_end(real) done                */
    int              broken;
    void           (*fail)(void *owner, const char *reason);
    void            *owner;
} out_body;

static void
out_break(out_body *o, const char *reason)
{
    o->broken = 1;

    if (reason) {
        o->fail(o->owner, reason);
    }
}

/* Send what the real channel can take; end it once all is sent. */
static void
out_pump(out_body *o)
{
    while (!o->broken && o->pending.len > 0) {
        size_t cap = hc_data_capacity(o->real);   /* 0 arms a produce() */
        size_t n   = o->pending.len < cap ? o->pending.len : cap;
        long   w;

        if (n == 0) {
            return;
        }

        if ((w = hc_data_write(o->real, o->pending.data, n)) < 0) {
            out_break(o, NULL);     /* the exchange is failing anyway */
            return;
        }

        hc_bytes_consume(&o->pending, (size_t)w);
    }

    if (!o->broken && o->ended && !o->sent_end && o->pending.len == 0) {
        o->sent_end = 1;

        if (hc_data_end(o->real, o->trailers) != 0) {
            out_break(o, NULL);
        }
    }
}

static long
out_write(void *arg, const void *data, size_t len)
{
    out_body *o = arg;

    if (o->broken || o->ended) {
        return -1;
    }

    if (len == 0) {
        return 0;
    }

    if (o->t.ops->update(o->t.state, data, len, &o->pending) != 0 ||
        (o->flush_each_write && o->t.ops->flush &&
         o->t.ops->flush(o->t.state, &o->pending) != 0)) {
        out_break(o, FAILED);
        return -1;
    }

    out_pump(o);
    return o->broken ? -1 : (long)len;
}

static int
out_end(void *arg, const hc_headers *trailers)
{
    out_body *o = arg;

    if (o->broken || o->ended) {
        return -1;
    }

    o->ended = 1;

    if (o->t.ops->finish(o->t.state, &o->pending) != 0) {
        out_break(o, FAILED);
        return -1;
    }

    if (trailers && hc_headers_count(trailers) > 0 &&
        (o->trailers = hc_headers_copy(trailers)) == NULL) {
        out_break(o, FAILED);
        return -1;
    }

    out_pump(o);
    return o->broken ? -1 : 0;
}

static size_t
out_capacity(void *arg)
{
    out_body *o = arg;

    if (o->broken || o->ended || o->pending.len >= o->high_water) {
        return 0;   /* at the mark: the real channel is full, its drain
                     * calls the wrapper's ->produce() */
    }

    return o->high_water - o->pending.len;
}

static void
out_request_output(void *arg)
{
    out_body *o = arg;

    if (!o->broken && !o->ended) {
        hc_data_request_output(o->real);    /* -> the wrapper's ->produce() */
    }
}

static const hc_data_channel_ops OUT_OPS = {
    out_write, out_end, out_capacity, out_request_output
};

/* The real channel has room: send pending bytes first. Returns 1 when the
 * wrapped handler may now produce more. */
static int
out_produce(out_body *o)
{
    out_pump(o);
    return !o->broken && !o->ended && o->pending.len < o->high_water;
}

static out_body *
out_new(hc_transform *t, hc_data_channel *real, size_t high_water, int flush_each_write,
        void (*fail)(void *owner, const char *reason), void *owner)
{
    out_body *o = calloc(1, sizeof(*o));

    if (o == NULL || (o->ch = hc_data_channel_new(&OUT_OPS, o)) == NULL) {
        free(o);
        return NULL;
    }

    o->t                = *t;
    o->real             = real;
    o->high_water       = high_water ? high_water : HC_TRANSFORM_HIGH_WATER;
    o->flush_each_write = flush_each_write;
    o->fail             = fail;
    o->owner            = owner;
    t->ops              = NULL;     /* o owns it now */
    return o;
}

static void
out_free(out_body *o)
{
    if (o) {
        transform_drop(&o->t);
        hc_bytes_free(&o->pending);
        hc_headers_free(o->trailers);
        hc_data_channel_free(o->ch);
        free(o);
    }
}

/* The entity the other side sees: of unknown length, without
 * Content-Length, when its body is transformed. */
static void
entity_for(const hc_entity *entity, const hc_transform *t, hc_headers *headers,
           hc_entity *copy)
{
    *copy = *entity;

    if (t->ops) {
        copy->content_length = -1;
        hc_headers_remove(headers, "Content-Length");
    }
}

/* ======================================================================== */
/* Server                                                                   */
/* ======================================================================== */

typedef struct srv_wrap {
    hc_server_handler                 base;
    hc_server_handler                *inner;
    const hc_server_transform_config *cfg;
    hc_server_exchange               *ex;
    hc_response_channel              *real_rc;
    hc_response_channel              *rc;       /* the wrapped handler's */
    head_copy                         req;
    in_body                          *in;
    out_body                         *out;
    const char                       *reason;   /* ours, for ->failed() */
    int                               refused;  /* answered by us       */
    int                               started;  /* inner has the request */
    int                               failed_sent;
} srv_wrap;

static void
srv_break(void *owner, const char *reason)
{
    srv_wrap *w = owner;

    if (w->reason == NULL) {
        w->reason = reason;
        hc_server_abort(w->ex);     /* deferred: ->failed(), ->release() */
    }
}

static int
srv_send(void *arg, const hc_response_head *head, const hc_entity *entity,
         hc_data_channel **body)
{
    srv_wrap        *w  = arg;
    hc_transform     t  = { NULL, NULL };
    hc_response_head h2 = *head;
    hc_entity        e2;
    hc_headers      *h  = hc_headers_copy(head->headers);
    hc_data_channel *real_body;
    int              rv;

    *body = NULL;

    if (w->reason || h == NULL) {
        hc_headers_free(h);
        return -1;
    }

    if (w->cfg->choose_response &&
        refusal(w->cfg->choose_response(w->cfg->arg, &w->req.head, &h2.status, h, entity, &t))) {
        transform_drop(&t);
        hc_headers_free(h);
        srv_break(w, REFUSED);
        return -1;
    }

    if (entity == NULL) {
        transform_drop(&t);
    } else {
        entity_for(entity, &t, h, &e2);
    }

    h2.headers = h;
    rv         = hc_response_submit(w->real_rc, &h2, entity ? &e2 : NULL, &real_body);
    hc_headers_free(h);

    if (rv != 0 || real_body == NULL || t.ops == NULL) {
        transform_drop(&t);
        *body = rv == 0 ? real_body : NULL;
        return rv;
    }

    if ((w->out = out_new(&t, real_body, w->cfg->high_water, w->cfg->flush_each_write,
                          srv_break, w)) == NULL) {
        transform_drop(&t);
        srv_break(w, FAILED);
        return -1;
    }

    *body = w->out->ch;
    return 0;
}

static int
srv_send_information(void *arg, const hc_response_head *head)
{
    return hc_response_send_information(((srv_wrap *)arg)->real_rc, head);
}

static const hc_response_channel_ops SRV_RC_OPS = { srv_send, srv_send_information };

static void
srv_handle_request(hc_server_handler *self, hc_server_exchange *ex,
                   const hc_request_head *head, const hc_entity *entity,
                   hc_response_channel *rc)
{
    srv_wrap       *w  = (srv_wrap *)self;
    hc_transform    t  = { NULL, NULL };
    hc_request_head h2 = *head;
    hc_entity       e2;
    hc_headers     *h  = hc_headers_copy(head->headers);
    int             status = 0;

    w->ex      = ex;
    w->real_rc = rc;

    if (h == NULL) {
        srv_break(w, FAILED);
        return;
    }

    if (w->cfg->choose_request) {
        status = refusal(w->cfg->choose_request(w->cfg->arg, head, h, entity, &t));
    }

    if (status) {
        transform_drop(&t);
        hc_headers_free(h);
        w->refused = 1;
        hc_response_send_simple(rc, status, NULL, NULL, 0);
        return;
    }

    if (entity == NULL) {
        transform_drop(&t);
    } else {
        entity_for(entity, &t, h, &e2);
    }

    if (head_copy_set(&w->req, head, h) != 0 ||
        (w->rc = hc_response_channel_new(&SRV_RC_OPS, w)) == NULL ||
        (t.ops && (w->in = calloc(1, sizeof(*w->in))) == NULL)) {
        if (w->req.headers == NULL) {
            hc_headers_free(h);
        }

        transform_drop(&t);
        srv_break(w, FAILED);
        return;
    }

    if (w->in) {
        w->in->t = t;
    }

    h2.headers = h;
    w->started = 1;
    w->inner->vtbl->handle_request(w->inner, ex, &h2, entity ? &e2 : NULL, w->rc);
}

static void
srv_deliver(void *arg, const void *p, size_t n)
{
    srv_wrap *w = arg;

    w->inner->vtbl->consume(w->inner, p, n);
}

static void
srv_consume(hc_server_handler *self, const void *data, size_t len)
{
    srv_wrap *w = (srv_wrap *)self;

    if (!w->started || w->reason) {
        return;
    }

    if (w->in == NULL) {
        w->inner->vtbl->consume(w->inner, data, len);
    } else if (in_run(w->in, data, len, srv_deliver, w) != 0) {
        srv_break(w, FAILED);
    }
}

static void
srv_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    srv_wrap *w = (srv_wrap *)self;

    if (!w->started || w->reason) {
        return;
    }

    if (w->in && in_run(w->in, NULL, 0, srv_deliver, w) != 0) {
        srv_break(w, FAILED);
        return;
    }

    w->inner->vtbl->stream_end(w->inner, trailers);
}

static void
srv_produce(hc_server_handler *self, hc_data_channel *ch)
{
    srv_wrap *w = (srv_wrap *)self;

    if (!w->started) {
        return;
    }

    if (w->out == NULL) {
        if (w->inner->vtbl->produce) {
            w->inner->vtbl->produce(w->inner, ch);
        }
    } else if (out_produce(w->out) && w->inner->vtbl->produce) {
        w->inner->vtbl->produce(w->inner, w->out->ch);
    }
}

static void
srv_failed(hc_server_handler *self, const char *reason)
{
    srv_wrap *w = (srv_wrap *)self;

    if (w->started && !w->failed_sent) {
        w->failed_sent = 1;
        w->inner->vtbl->failed(w->inner, w->reason ? w->reason : reason);
    }
}

static void
srv_release(hc_server_handler *self)
{
    srv_wrap *w = (srv_wrap *)self;

    if (w->reason && w->started && !w->failed_sent) {
        w->failed_sent = 1;
        w->inner->vtbl->failed(w->inner, w->reason);
    }

    w->inner->vtbl->release(w->inner);  /* last use of our channels */
    in_free(w->in);
    out_free(w->out);
    hc_response_channel_free(w->rc);
    head_copy_free(&w->req);
    free(w);
}

static const hc_server_handler_vtbl SRV_VTBL = {
    srv_handle_request, srv_consume, srv_stream_end, srv_produce, srv_failed, srv_release
};

hc_server_handler *
hc_server_transform_handler_new(hc_server_handler *inner, const hc_server_transform_config *cfg)
{
    srv_wrap *w;

    if (inner == NULL || cfg == NULL || (w = calloc(1, sizeof(*w))) == NULL) {
        return NULL;
    }

    w->base.vtbl = &SRV_VTBL;
    w->inner     = inner;
    w->cfg       = cfg;
    return &w->base;
}

/* ======================================================================== */
/* Client                                                                   */
/* ======================================================================== */

typedef struct cli_wrap {
    hc_client_handler                 base;
    hc_client_handler                *inner;
    const hc_client_transform_config *cfg;
    hc_request_channel               *real_rc;
    hc_request_channel               *rc;       /* the wrapped handler's */
    head_copy                         req;      /* as sent */
    in_body                          *in;
    out_body                         *out;
    const char                       *reason;
    int                               started;  /* inner was asked for the request */
    int                               failed_sent;
} cli_wrap;

static void
cli_break(void *owner, const char *reason)
{
    cli_wrap *w = owner;

    if (w->reason == NULL) {
        w->reason = reason;
        hc_client_cancel(hc_request_channel_exchange(w->real_rc));
    }
}

static int
cli_send(void *arg, const hc_request_head *head, const hc_entity *entity,
         hc_data_channel **body)
{
    cli_wrap        *w  = arg;
    hc_transform     t  = { NULL, NULL };
    hc_request_head  h2 = *head;
    hc_entity        e2;
    hc_headers      *h  = hc_headers_copy(head->headers);
    hc_data_channel *real_body;
    int              rv;

    *body = NULL;

    if (w->reason || h == NULL || w->req.strings) {
        hc_headers_free(h);
        return -1;
    }

    if (w->cfg->choose_request &&
        refusal(w->cfg->choose_request(w->cfg->arg, head, h, entity, &t))) {
        transform_drop(&t);
        hc_headers_free(h);
        cli_break(w, REFUSED);
        return -1;
    }

    if (entity == NULL) {
        transform_drop(&t);
    } else {
        entity_for(entity, &t, h, &e2);
    }

    if (head_copy_set(&w->req, head, h) != 0) {
        transform_drop(&t);
        hc_headers_free(h);
        cli_break(w, FAILED);
        return -1;
    }

    h2.headers = h;     /* kept in w->req for choose_response */
    rv         = hc_request_submit(w->real_rc, &h2, entity ? &e2 : NULL, &real_body);

    if (rv != 0 || real_body == NULL || t.ops == NULL) {
        transform_drop(&t);
        *body = rv == 0 ? real_body : NULL;
        return rv;
    }

    if ((w->out = out_new(&t, real_body, w->cfg->high_water, w->cfg->flush_each_write,
                          cli_break, w)) == NULL) {
        transform_drop(&t);
        cli_break(w, FAILED);
        return -1;
    }

    *body = w->out->ch;
    return 0;
}

static hc_client_exchange *
cli_exchange(void *arg)
{
    return hc_request_channel_exchange(((cli_wrap *)arg)->real_rc);
}

static const hc_request_channel_ops CLI_RC_OPS = { cli_send, cli_exchange };

static void
cli_produce_request(hc_client_handler *self, hc_request_channel *rc)
{
    cli_wrap *w = (cli_wrap *)self;

    w->real_rc = rc;

    if ((w->rc = hc_request_channel_new(&CLI_RC_OPS, w)) == NULL) {
        cli_break(w, FAILED);
        return;
    }

    w->started = 1;
    w->inner->vtbl->produce_request(w->inner, w->rc);
}

static void
cli_consume_response(hc_client_handler *self, const hc_response_head *head,
                     const hc_entity *entity)
{
    cli_wrap        *w  = (cli_wrap *)self;
    hc_transform     t  = { NULL, NULL };
    hc_response_head h2 = *head;
    hc_entity        e2;
    hc_headers      *h;

    if (w->reason) {
        return;
    }

    if ((h = hc_headers_copy(head->headers)) == NULL) {
        cli_break(w, FAILED);
        return;
    }

    if (w->cfg->choose_response &&
        refusal(w->cfg->choose_response(w->cfg->arg, w->req.strings ? &w->req.head : NULL,
                                        head->status, h, entity, &t))) {
        transform_drop(&t);
        hc_headers_free(h);
        cli_break(w, REFUSED);
        return;
    }

    if (entity == NULL) {
        transform_drop(&t);
    } else {
        entity_for(entity, &t, h, &e2);
    }

    if (t.ops) {
        if ((w->in = calloc(1, sizeof(*w->in))) == NULL) {
            transform_drop(&t);
            hc_headers_free(h);
            cli_break(w, FAILED);
            return;
        }

        w->in->t = t;
    }

    h2.headers = h;
    w->inner->vtbl->consume_response(w->inner, &h2, entity ? &e2 : NULL);
    hc_headers_free(h);
}

static void
cli_deliver(void *arg, const void *p, size_t n)
{
    cli_wrap *w = arg;

    w->inner->vtbl->consume(w->inner, p, n);
}

static void
cli_consume(hc_client_handler *self, const void *data, size_t len)
{
    cli_wrap *w = (cli_wrap *)self;

    if (w->reason) {
        return;
    }

    if (w->in == NULL) {
        w->inner->vtbl->consume(w->inner, data, len);
    } else if (in_run(w->in, data, len, cli_deliver, w) != 0) {
        cli_break(w, FAILED);
    }
}

static void
cli_stream_end(hc_client_handler *self, const hc_headers *trailers)
{
    cli_wrap *w = (cli_wrap *)self;

    if (w->reason) {
        return;
    }

    if (w->in && in_run(w->in, NULL, 0, cli_deliver, w) != 0) {
        cli_break(w, FAILED);
        return;
    }

    w->inner->vtbl->stream_end(w->inner, trailers);
}

static void
cli_produce(hc_client_handler *self, hc_data_channel *ch)
{
    cli_wrap *w = (cli_wrap *)self;

    if (w->out == NULL) {
        if (w->inner->vtbl->produce) {
            w->inner->vtbl->produce(w->inner, ch);
        }
    } else if (out_produce(w->out) && w->inner->vtbl->produce) {
        w->inner->vtbl->produce(w->inner, w->out->ch);
    }
}

static void
cli_failed(hc_client_handler *self, const char *reason)
{
    cli_wrap *w = (cli_wrap *)self;

    if (w->started && !w->failed_sent) {
        w->failed_sent = 1;
        w->inner->vtbl->failed(w->inner, w->reason ? w->reason : reason);
    }
}

static void
cli_release(hc_client_handler *self)
{
    cli_wrap *w = (cli_wrap *)self;

    if (w->reason && w->started && !w->failed_sent) {
        w->failed_sent = 1;
        w->inner->vtbl->failed(w->inner, w->reason);
    }

    w->inner->vtbl->release(w->inner);
    in_free(w->in);
    out_free(w->out);
    hc_request_channel_free(w->rc);
    head_copy_free(&w->req);
    free(w);
}

static void
cli_consume_information(hc_client_handler *self, const hc_response_head *head)
{
    cli_wrap *w = (cli_wrap *)self;

    if (!w->reason && w->inner->vtbl->consume_information) {
        w->inner->vtbl->consume_information(w->inner, head);
    }
}

static const hc_client_handler_vtbl CLI_VTBL = {
    cli_produce_request, cli_consume_response, cli_consume, cli_stream_end,
    cli_produce, cli_failed, cli_release, cli_consume_information
};

hc_client_handler *
hc_client_transform_handler_new(hc_client_handler *inner, const hc_client_transform_config *cfg)
{
    cli_wrap *w;

    if (inner == NULL || cfg == NULL || (w = calloc(1, sizeof(*w))) == NULL) {
        return NULL;
    }

    w->base.vtbl = &CLI_VTBL;
    w->inner     = inner;
    w->cfg       = cfg;
    return &w->base;
}
