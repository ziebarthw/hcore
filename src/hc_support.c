/*
 * hc_support.c - producers, consumers and the basic client exchange
 * handler (see hc_support.h). Uses only the public hc.h API.
 */
#include "hc_support.h"

#include <stdlib.h>
#include <string.h>

static char *
dup_str(const char *s)
{
    size_t n;
    char  *p;

    if (s == NULL) {
        return NULL;
    }

    n = strlen(s) + 1;
    p = malloc(n);

    return p ? memcpy(p, s, n) : NULL;
}

/* ======================================================================== */
/* Bytes / string entity producer                                           */
/* ======================================================================== */

typedef struct bytes_producer {
    hc_entity_producer base;            /* must be first */
    unsigned char     *data;
    size_t             len;
    size_t             off;
    char              *content_type;
} bytes_producer;

static void
bytes_produce(hc_entity_producer *self, hc_data_channel *ch)
{
    bytes_producer *b = (bytes_producer *)(void *)self;
    size_t          room;

    while (b->off < b->len && (room = hc_data_capacity(ch)) > 0) {
        size_t n = b->len - b->off < room ? b->len - b->off : room;

        if (hc_data_write(ch, b->data + b->off, n) < 0) {
            return;
        }

        b->off += n;
    }

    if (b->off == b->len) {
        hc_data_end(ch, NULL);
    }
}

static void
bytes_release(hc_entity_producer *self)
{
    bytes_producer *b = (bytes_producer *)(void *)self;

    free(b->data);
    free(b->content_type);
    free(b);
}

static const hc_entity_producer_vtbl bytes_vtbl = {
    .produce = bytes_produce,
    .release = bytes_release,
};

hc_entity_producer *
hc_bytes_entity_producer_new(const void *data, size_t len, const char *content_type)
{
    bytes_producer *b = calloc(1, sizeof(*b));

    if (b == NULL) {
        return NULL;
    }

    b->data         = malloc(len ? len : 1);
    b->len          = len;
    b->content_type = dup_str(content_type);

    if (b->data == NULL || (content_type && b->content_type == NULL)) {
        free(b->data);
        free(b->content_type);
        free(b);
        return NULL;
    }

    if (len) {
        memcpy(b->data, data, len);
    }

    b->base.vtbl                   = &bytes_vtbl;
    b->base.details.content_length = (int64_t)len;
    b->base.details.content_type   = b->content_type;

    return &b->base;
}

hc_entity_producer *
hc_string_entity_producer_new(const char *text, const char *content_type)
{
    return hc_bytes_entity_producer_new(text, strlen(text),
                                        content_type ? content_type : "text/plain; charset=UTF-8");
}

/* ======================================================================== */
/* Request producer                                                         */
/* ======================================================================== */

struct hc_request_producer {
    char               *method;
    hc_host             target;
    char                path[2048];
    hc_headers         *headers;
    hc_entity_producer *entity;
};

hc_request_producer *
hc_request_producer_new(const char *method, const char *uri, hc_entity_producer *entity)
{
    hc_request_producer *p = calloc(1, sizeof(*p));

    if (p == NULL || hc_uri_parse(uri, &p->target, p->path, sizeof(p->path)) != 0 ||
        (p->method = dup_str(method)) == NULL || (p->headers = hc_headers_new()) == NULL) {
        if (p) {
            free(p->method);
            free(p);
        }

        if (entity) {
            entity->vtbl->release(entity);
        }

        return NULL;
    }

    p->entity = entity;

    return p;
}

int
hc_request_producer_add_header(hc_request_producer *p, const char *name, const char *value)
{
    return hc_headers_add(p->headers, name, value);
}

const hc_host *
hc_request_producer_target(const hc_request_producer *p)
{
    return &p->target;
}

void
hc_request_producer_free(hc_request_producer *p)
{
    if (p) {
        if (p->entity) {
            p->entity->vtbl->release(p->entity);
        }

        hc_headers_free(p->headers);
        free(p->method);
        free(p);
    }
}

/* ======================================================================== */
/* Buffer entity consumer                                                   */
/* ======================================================================== */

typedef struct buffer_consumer {
    hc_entity_consumer base;            /* must be first */
    char              *data;
    size_t             len;
    size_t             cap;
    size_t             max;
} buffer_consumer;

static int
buffer_consume(hc_entity_consumer *self, const void *data, size_t len)
{
    buffer_consumer *b = (buffer_consumer *)(void *)self;

    if (len > b->max - b->len) {
        return -1;              /* over the limit */
    }

    if (b->len + len + 1 > b->cap) {
        size_t ncap = b->cap ? b->cap : 4096;
        char  *nd;

        while (ncap < b->len + len + 1) {
            ncap *= 2;
        }

        if ((nd = realloc(b->data, ncap)) == NULL) {
            return -1;
        }

        b->data = nd;
        b->cap  = ncap;
    }

    memcpy(b->data + b->len, data, len);
    b->len          += len;
    b->data[b->len]  = '\0';

    return 0;
}

static void
buffer_release(hc_entity_consumer *self)
{
    buffer_consumer *b = (buffer_consumer *)(void *)self;

    free(b->data);
    free(b);
}

static const hc_entity_consumer_vtbl buffer_vtbl = {
    .consume = buffer_consume,
    .release = buffer_release,
};

hc_entity_consumer *
hc_buffer_entity_consumer_new(size_t max_bytes)
{
    buffer_consumer *b = calloc(1, sizeof(*b));

    if (b == NULL) {
        return NULL;
    }

    b->base.vtbl = &buffer_vtbl;
    b->max       = max_bytes ? max_bytes : 16u * 1024 * 1024;

    return &b->base;
}

const char *
hc_buffer_entity_consumer_data(const hc_entity_consumer *c, size_t *len)
{
    const buffer_consumer *b = (const buffer_consumer *)(const void *)c;

    if (len) {
        *len = b->len;
    }

    return b->data ? b->data : "";
}

/* ======================================================================== */
/* Basic client exchange handler                                            */
/* ======================================================================== */

typedef struct basic_handler {
    hc_client_handler    base;          /* must be first */
    hc_request_producer *req;
    hc_entity_consumer  *body;
    hc_result_callback   cb;
    hc_client_exchange  *cx;
    int                  status;
    int                  version;
    hc_headers          *resp_headers;
    int                  delivered;     /* completed/failed called */
    const char          *abort_reason;
} basic_handler;

#define BASIC(self) ((basic_handler *)(void *)(self))

static void
basic_fail(basic_handler *b, const char *reason)
{
    if (b->delivered) {
        return;
    }

    b->delivered = 1;

    if (b->req->entity && b->req->entity->vtbl->failed) {
        b->req->entity->vtbl->failed(b->req->entity, reason);
    }

    if (b->body && b->body->vtbl->failed) {
        b->body->vtbl->failed(b->body, reason);
    }

    if (b->cb.failed) {
        b->cb.failed(b->cb.arg, reason);
    }
}

static void
basic_produce_request(hc_client_handler *self, hc_request_channel *rc)
{
    basic_handler       *b = BASIC(self);
    hc_request_producer *p = b->req;
    hc_request_head      head;
    char                 authority[300];

    hc_host_authority(&p->target, authority, sizeof(authority));
    b->cx = hc_request_channel_exchange(rc);

    head.method    = p->method;
    head.target    = p->path;
    head.authority = authority;
    head.version   = 11;
    head.headers   = p->headers;

    /* The body channel (if any) comes back through ->produce(). */
    hc_request_send(rc, &head, p->entity ? &p->entity->details : NULL);
}

static void
basic_produce(hc_client_handler *self, hc_data_channel *ch)
{
    basic_handler *b = BASIC(self);

    if (b->req->entity) {
        b->req->entity->vtbl->produce(b->req->entity, ch);
    }
}

static void
basic_consume_information(hc_client_handler *self, const hc_response_head *head)
{
    basic_handler *b = BASIC(self);

    if (b->cb.information) {
        b->cb.information(b->cb.arg, head);
    }
}

static void
basic_consume_response(hc_client_handler *self, const hc_response_head *head,
                       const hc_entity *entity)
{
    basic_handler *b = BASIC(self);
    const char    *k, *v;

    b->status       = head->status;
    b->version      = head->version;
    b->resp_headers = hc_headers_new();

    for (size_t i = 0; b->resp_headers && hc_headers_at(head->headers, i, &k, &v) == 0; i++) {
        hc_headers_add(b->resp_headers, k, v);
    }

    if (entity && b->body && b->body->vtbl->stream_start) {
        b->body->vtbl->stream_start(b->body, entity);
    }
}

static void
basic_consume(hc_client_handler *self, const void *data, size_t len)
{
    basic_handler *b = BASIC(self);

    if (b->body == NULL || b->abort_reason) {
        return;                 /* discarding */
    }

    if (b->body->vtbl->consume(b->body, data, len) != 0) {
        b->abort_reason = "response body rejected by the consumer";
        hc_client_cancel(b->cx);
    }
}

static void
basic_stream_end(hc_client_handler *self, const hc_headers *trailers)
{
    basic_handler   *b = BASIC(self);
    hc_response_head head;
    hc_message       msg;

    if (b->abort_reason || b->delivered) {
        return;
    }

    if (b->body && b->body->vtbl->stream_end) {
        b->body->vtbl->stream_end(b->body, trailers);
    }

    head.status  = b->status;
    head.version = b->version;
    head.headers = b->resp_headers;
    msg.head     = &head;
    msg.body     = b->body;
    b->delivered = 1;

    if (b->cb.completed) {
        b->cb.completed(b->cb.arg, &msg);
    }
}

static void
basic_failed(hc_client_handler *self, const char *reason)
{
    basic_handler *b = BASIC(self);

    basic_fail(b, b->abort_reason ? b->abort_reason : reason);
}

static void
basic_release(hc_client_handler *self)
{
    basic_handler     *b  = BASIC(self);
    hc_result_callback cb = b->cb;

    basic_fail(b, "exchange ended without a response");     /* no-op normally */

    hc_request_producer_free(b->req);

    if (b->body) {
        b->body->vtbl->release(b->body);
    }

    hc_headers_free(b->resp_headers);
    free(b);

    if (cb.released) {
        cb.released(cb.arg);
    }
}

static const hc_client_handler_vtbl basic_vtbl = {
    .produce_request     = basic_produce_request,
    .consume_response    = basic_consume_response,
    .consume             = basic_consume,
    .stream_end          = basic_stream_end,
    .produce             = basic_produce,
    .failed              = basic_failed,
    .release             = basic_release,
    .consume_information = basic_consume_information,
};

hc_client_handler *
hc_basic_client_handler_new(hc_request_producer *request, hc_entity_consumer *body,
                            const hc_result_callback *cb)
{
    basic_handler *b = calloc(1, sizeof(*b));

    if (b == NULL) {
        return NULL;
    }

    b->base.vtbl = &basic_vtbl;
    b->req       = request;
    b->body      = body;
    b->cb        = *cb;

    return &b->base;
}

int
hc_requester_execute_request(hc_requester *rq, hc_request_producer *request,
                             hc_entity_consumer *body, int64_t timeout_ms,
                             const hc_result_callback *cb)
{
    hc_client_handler *h = hc_basic_client_handler_new(request, body, cb);

    if (h == NULL) {
        return -1;
    }

    if (hc_requester_execute(rq, &request->target, timeout_ms, h) != 0) {
        free(h);                /* nothing was taken over */
        return -1;
    }

    return 0;
}

/* ======================================================================== */
/* Basic server exchange handler                                            */
/* ======================================================================== */

#define BASIC_SERVER_MAX_BODY (1024u * 1024u)

struct hc_basic_request {
    hc_server_handler             base;     /* must be first */
    const hc_basic_server_config *cfg;
    hc_server_exchange           *ex;
    hc_response_channel          *rc;
    char                         *method;
    char                         *target;
    char                         *authority;
    int                           version;
    hc_headers                   *headers;
    char                         *body;     /* NUL-terminated */
    size_t                        len;
    size_t                        cap;
    size_t                        max;
    int                           rejected; /* over the limit: 413 sent  */
    int                           delivered;/* ->request() was called    */
    void                         *data;
};

#define BREQ(self) ((hc_basic_request *)(void *)(self))

static void
breq_reject(hc_basic_request *r)
{
    if (!r->rejected) {
        r->rejected = 1;
        /* -1 when a response has started already: nothing to add then */
        hc_response_send_simple(r->rc, 413, "text/plain", "Request body too large\n", 1);
    }
}

static void
breq_handle_request(hc_server_handler *self, hc_server_exchange *ex,
                    const hc_request_head *head, const hc_entity *entity,
                    hc_response_channel *rc)
{
    hc_basic_request *r = BREQ(self);

    r->ex        = ex;
    r->rc        = rc;
    r->version   = head->version;
    r->method    = dup_str(head->method);
    r->target    = dup_str(head->target);
    r->authority = dup_str(head->authority);
    r->headers   = hc_headers_copy(head->headers);
    r->body      = malloc(1);

    if (!r->method || !r->target || (head->authority && !r->authority) ||
        !r->headers || !r->body) {
        r->rejected = 1;
        hc_response_send_simple(rc, 500, "text/plain", "out of memory\n", 1);
        return;
    }

    r->body[0] = '\0';

    if (entity && entity->content_length > 0 && (uint64_t)entity->content_length > r->max) {
        breq_reject(r);     /* never read into memory */
    }
}

static void
breq_consume(hc_server_handler *self, const void *data, size_t len)
{
    hc_basic_request *r = BREQ(self);

    if (r->rejected || len == 0) {
        return;
    }

    if (len > r->max - r->len) {
        breq_reject(r);
        return;
    }

    if (r->len + len + 1 > r->cap) {
        size_t cap = r->cap ? r->cap : 4096;
        char  *p;

        while (cap < r->len + len + 1) {
            cap *= 2;
        }

        if ((p = realloc(r->body, cap)) == NULL) {
            r->rejected = 1;
            hc_response_send_simple(r->rc, 500, "text/plain", "out of memory\n", 1);
            return;
        }

        r->body = p;
        r->cap  = cap;
    }

    memcpy(r->body + r->len, data, len);
    r->len            += len;
    r->body[r->len]    = '\0';
}

static void
breq_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    hc_basic_request *r = BREQ(self);

    (void)trailers;

    if (!r->rejected && !r->delivered) {
        r->delivered = 1;
        r->cfg->request(r->cfg->arg, r);
    }
}

static void
breq_produce(hc_server_handler *self, hc_data_channel *ch)
{
    hc_basic_request *r = BREQ(self);

    if (r->delivered && r->cfg->produce) {
        r->cfg->produce(r->cfg->arg, r, ch);
    }
}

static void
breq_failed(hc_server_handler *self, const char *reason)
{
    hc_basic_request *r = BREQ(self);

    if (r->delivered && r->cfg->failed) {
        r->cfg->failed(r->cfg->arg, r, reason);
    }
}

static void
breq_release(hc_server_handler *self)
{
    hc_basic_request *r = BREQ(self);

    if (r->delivered && r->cfg->release) {
        r->cfg->release(r->cfg->arg, r);
    }

    free(r->method);
    free(r->target);
    free(r->authority);
    hc_headers_free(r->headers);
    free(r->body);
    free(r);
}

static const hc_server_handler_vtbl breq_vtbl = {
    .handle_request = breq_handle_request,
    .consume        = breq_consume,
    .stream_end     = breq_stream_end,
    .produce        = breq_produce,
    .failed         = breq_failed,
    .release        = breq_release,
};

hc_server_handler *
hc_basic_server_factory(void *config)
{
    const hc_basic_server_config *cfg = config;
    hc_basic_request             *r;

    if (cfg == NULL || cfg->request == NULL || (r = calloc(1, sizeof(*r))) == NULL) {
        return NULL;
    }

    r->base.vtbl = &breq_vtbl;
    r->cfg       = cfg;
    r->max       = cfg->max_body ? cfg->max_body : BASIC_SERVER_MAX_BODY;

    return &r->base;
}

const char *
hc_basic_request_method(const hc_basic_request *r)
{
    return r->method;
}

const char *
hc_basic_request_target(const hc_basic_request *r)
{
    return r->target;
}

const char *
hc_basic_request_authority(const hc_basic_request *r)
{
    return r->authority;
}

int
hc_basic_request_version(const hc_basic_request *r)
{
    return r->version;
}

const hc_headers *
hc_basic_request_headers(const hc_basic_request *r)
{
    return r->headers;
}

const char *
hc_basic_request_body(const hc_basic_request *r, size_t *len)
{
    if (len) {
        *len = r->len;
    }

    return r->body ? r->body : "";
}

hc_server_exchange *
hc_basic_request_exchange(hc_basic_request *r)
{
    return r->ex;
}

hc_response_channel *
hc_basic_request_channel(hc_basic_request *r)
{
    return r->rc;
}

void
hc_basic_request_set_data(hc_basic_request *r, void *data)
{
    r->data = data;
}

void *
hc_basic_request_data(const hc_basic_request *r)
{
    return r->data;
}
