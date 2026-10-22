/*
 * reverse_proxy.c - a C port of httpcore5's AsyncReverseProxyExample.
 *
 * Usage: reverse_proxy [--cafile PEM] [--insecure] [--quiet]
 *                      <[http|https://]hostname[:port]> [listener port]
 *
 * The application is written only against hc.h: it supplies an
 * "incoming" server exchange handler (client -> proxy) and an "outgoing"
 * client exchange handler (proxy -> origin) that share one
 * exchange_state, exactly like the Java example's IncomingExchangeHandler,
 * OutgoingExchangeHandler and ProxyExchangeState.
 *
 * Bodies are streamed in both directions with bounded buffering: a side
 * whose relay buffer is full is suspended (Java: CapacityChannel) and
 * resumed from the other side's produce() once the buffer drains.
 *
 * Origin connections come from a pool, as in the Java example: the incoming
 * handler leases an endpoint (hc_requester_connect, Java:
 * requester.connect), executes the outgoing handler on it, and the outgoing
 * handler hands it back with release_and_reuse() at stream end or
 * release_and_discard() on failure.
 */
#include "hc.h"
#include "hc_pool.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#define INIT_BUFFER_SIZE 4096
#define MAX_BUFFERED     (64 * 1024)  /* suspend the source above this */
#define RESUME_BELOW     (16 * 1024)

#define CONNECT_TIMEOUT_MS 30000      /* Java: Timeout.ofSeconds(30) */

static int           quiet;
static hc_reactor   *reactor;
static hc_requester *requester;
static hc_host     target;              /* Java: HttpHost targetHost */
static char        target_authority[300];

static void
log_line_at(const char *func, int line, const char *fmt, ...)
{
    char      date[64];
    time_t    now = time(NULL);
    struct tm tm;
    va_list   ap;

    if (quiet) {
        return;
    }

    gmtime_r(&now, &tm);
    strftime(date, sizeof(date), "%a, %d %b %Y %H:%M:%S GMT", &tm);
    printf("%s | %s:%d | ", date, func, line);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    putchar('\n');
    fflush(stdout);
}

/* Every log_line(...) call records where it was made. */
#define log_line(...) log_line_at(__func__, __LINE__, __VA_ARGS__)

/* ------------------------------------------------------------------------ */
/* ProxyBuffer: a growable FIFO of bytes                                    */
/* ------------------------------------------------------------------------ */

typedef struct proxy_buffer {
    unsigned char *data;
    size_t         off;
    size_t         len;
    size_t         cap;
} proxy_buffer;

static int
pbuf_append(proxy_buffer *b, const void *src, size_t n)
{
    if (b->off + b->len + n > b->cap) {
        if (b->off) {           /* compact first */
            memmove(b->data, b->data + b->off, b->len);
            b->off = 0;
        }

        if (b->len + n > b->cap) {
            size_t         ncap = b->cap ? b->cap : INIT_BUFFER_SIZE;
            unsigned char *nd;

            while (ncap < b->len + n) {
                ncap *= 2;
            }

            if ((nd = realloc(b->data, ncap)) == NULL) {
                return -1;
            }

            b->data = nd;
            b->cap  = ncap;
        }
    }

    memcpy(b->data + b->off + b->len, src, n);
    b->len += n;

    return 0;
}

/* Write as much buffered data as the channel accepts. Returns bytes written. */
static size_t
pbuf_write_to(proxy_buffer *b, hc_data_channel *ch)
{
    size_t room = hc_data_capacity(ch);
    size_t n    = b->len < room ? b->len : room;

    if (n == 0 || hc_data_write(ch, b->data + b->off, n) < 0) {
        return 0;
    }

    b->off += n;
    b->len -= n;

    if (b->len == 0) {
        b->off = 0;
    }

    return n;
}

static void
pbuf_free(proxy_buffer *b)
{
    free(b->data);
    memset(b, 0, sizeof(*b));
}

/* ------------------------------------------------------------------------ */
/* ProxyExchangeState                                                       */
/* ------------------------------------------------------------------------ */

static const char *const HOP_BY_HOP[] = {
    "host", "content-length", "transfer-encoding", "connection", "keep-alive",
    "proxy-authenticate", "te", "trailer", "upgrade",
    "expect",                   /* 100-continue was already answered by us */
    NULL
};

static int
is_hop_by_hop(const char *name)
{
    for (int i = 0; HOP_BY_HOP[i]; i++) {
        if (strcasecmp(name, HOP_BY_HOP[i]) == 0) {
            return 1;
        }
    }

    return 0;
}

static hc_headers *
copy_end_to_end_headers(const hc_headers *src)
{
    hc_headers *dst = hc_headers_new();
    const char *k, *v;

    for (size_t i = 0; dst && hc_headers_at(src, i, &k, &v) == 0; i++) {
        if (!is_hop_by_hop(k)) {
            hc_headers_add(dst, k, v);
        }
    }

    return dst;
}

typedef struct exchange_state {
    char                 id[16];
    int                  refs;     /* incoming + outgoing handler        */

    /* client -> proxy -> origin */
    char                *method;
    char                *target;
    int                  version;
    hc_headers          *request_headers;
    int                  has_request_entity;
    hc_entity            request_entity;
    hc_server_exchange  *server_ex;        /* NULL after incoming release */
    hc_data_channel     *request_data;     /* upstream request body       */
    proxy_buffer         inbuf;
    int                  input_end;
    int                  input_suspended;

    /* origin -> proxy -> client */
    hc_response_channel *response_channel; /* NULL after incoming release */
    hc_data_channel     *response_data;
    hc_client_exchange  *client_ex;        /* NULL after outgoing release */
    hc_connect_op       *connect_op;       /* lease/connect in progress   */
    hc_endpoint         *endpoint;         /* leased origin connection    */
    proxy_buffer         outbuf;
    int                  output_end;
    int                  output_suspended;
    int                  response_sent;

    int                  downstream_gone;
    int                  upstream_gone;
} exchange_state;

static void
state_unref(exchange_state *s)
{
    if (--s->refs > 0) {
        return;
    }

    free(s->method);
    free(s->target);
    hc_headers_free(s->request_headers);
    pbuf_free(&s->inbuf);
    pbuf_free(&s->outbuf);
    free(s);
}

static char *
dup_str(const char *s)
{
    size_t n = strlen(s) + 1;
    char  *p = malloc(n);

    return p ? memcpy(p, s, n) : NULL;
}

/* Move inbuf -> upstream; end upstream body when everything is through. */
static void
flush_request_side(exchange_state *s)
{
    if (s->request_data == NULL) {
        return;
    }

    if (s->inbuf.len) {
        size_t n = pbuf_write_to(&s->inbuf, s->request_data);

        if (n) {
            log_line("[proxy->origin] %s %zu bytes sent", s->id, n);
        }
    }

    if (s->inbuf.len == 0 && s->input_end) {
        log_line("[proxy->origin] %s end of input", s->id);
        hc_data_end(s->request_data, NULL);
        s->request_data = NULL;
    }

    if (s->input_suspended && s->inbuf.len < RESUME_BELOW && s->server_ex) {
        s->input_suspended = 0;
        log_line("[client->proxy] %s input resumed", s->id);
        hc_server_resume_input(s->server_ex);
    }
}

/* Move outbuf -> client; end client body when everything is through. */
static void
flush_response_side(exchange_state *s)
{
    if (s->response_data == NULL) {
        return;
    }

    if (s->outbuf.len) {
        size_t n = pbuf_write_to(&s->outbuf, s->response_data);

        if (n) {
            log_line("[client<-proxy] %s %zu bytes sent", s->id, n);
        }
    }

    if (s->outbuf.len == 0 && s->output_end) {
        log_line("[client<-proxy] %s end of output", s->id);
        hc_data_end(s->response_data, NULL);
        s->response_data = NULL;
    }

    if (s->output_suspended && s->outbuf.len < RESUME_BELOW && s->client_ex) {
        s->output_suspended = 0;
        log_line("[proxy<-origin] %s input resumed", s->id);
        hc_client_resume_input(s->client_ex);
    }
}

/* ------------------------------------------------------------------------ */
/* OutgoingExchangeHandler (proxy -> origin): an hc_client_handler          */
/* ------------------------------------------------------------------------ */

typedef struct outgoing_handler {
    hc_client_handler base;    /* must be first: the "interface" */
    exchange_state   *state;
} outgoing_handler;

#define OUTGOING(self) ((outgoing_handler *)(void *)(self))

static void
out_produce_request(hc_client_handler *self, hc_request_channel *rc)
{
    exchange_state *s = OUTGOING(self)->state;
    hc_headers     *h = copy_end_to_end_headers(s->request_headers);
    hc_request_head head;

    head.method    = s->method;
    head.target    = s->target;
    head.authority = target_authority;
    head.version   = 11;
    head.headers   = h;

    log_line("[proxy->origin] %s %s %s", s->id, s->method, s->target);

    s->request_data = hc_request_send(rc, &head,
                                      s->has_request_entity ? &s->request_entity : NULL);
    hc_headers_free(h);
}

static void
out_produce(hc_client_handler *self, hc_data_channel *ch)
{
    exchange_state *s = OUTGOING(self)->state;

    (void)ch;
    flush_request_side(s);
}

static void
out_consume_response(hc_client_handler *self, const hc_response_head *head,
                     const hc_entity *entity)
{
    exchange_state  *s = OUTGOING(self)->state;
    hc_headers      *h;
    hc_response_head out;

    log_line("[proxy<-origin] %s status %d", s->id, head->status);

    if (s->response_channel == NULL) {
        return;                 /* client already gone */
    }

    h           = copy_end_to_end_headers(head->headers);
    out.status  = head->status;
    out.version = 11;
    out.headers = h;

    log_line("[client<-proxy] %s status %d", s->id, head->status);

    s->response_data = hc_response_send(s->response_channel, &out, entity);
    s->response_sent = 1;
    hc_headers_free(h);
}

/* Java's example ignores consumeInformation(); relaying is what a proxy
 * should do. "100 Continue" is the exception: the engine already answered
 * the client's "Expect: 100-continue" itself (and Expect is not forwarded). */
static void
out_consume_information(hc_client_handler *self, const hc_response_head *head)
{
    exchange_state  *s = OUTGOING(self)->state;
    hc_response_head out;

    log_line("[proxy<-origin] %s interim %d", s->id, head->status);

    if (head->status == 100 || s->response_channel == NULL) {
        return;
    }

    out.status  = head->status;
    out.version = 11;
    out.headers = copy_end_to_end_headers(head->headers);

    if (hc_response_send_information(s->response_channel, &out) == 0) {
        log_line("[client<-proxy] %s interim %d", s->id, head->status);
    }

    hc_headers_free((hc_headers *)out.headers);
}

static void
out_consume(hc_client_handler *self, const void *data, size_t len)
{
    exchange_state *s = OUTGOING(self)->state;

    log_line("[proxy<-origin] %s %zu bytes received", s->id, len);

    if (s->downstream_gone) {
        return;                 /* nobody to deliver to */
    }

    /* Write straight through when nothing is queued, buffer the rest. */
    if (s->response_data && s->outbuf.len == 0) {
        size_t room = hc_data_capacity(s->response_data);
        size_t n    = len < room ? len : room;

        if (n && hc_data_write(s->response_data, data, n) == (long)n) {
            log_line("[client<-proxy] %s %zu bytes sent", s->id, n);
            data = (const unsigned char *)data + n;
            len -= n;
        }
    }

    if (len) {
        pbuf_append(&s->outbuf, data, len);
        hc_data_request_output(s->response_data);
    }

    if (s->outbuf.len >= MAX_BUFFERED && !s->output_suspended && s->client_ex) {
        s->output_suspended = 1;
        log_line("[proxy<-origin] %s input suspended (%zu buffered)", s->id, s->outbuf.len);
        hc_client_suspend_input(s->client_ex);
    }
}

static void
out_stream_end(hc_client_handler *self, const hc_headers *trailers)
{
    exchange_state *s = OUTGOING(self)->state;

    (void)trailers;
    log_line("[proxy<-origin] %s end of input", s->id);
    s->output_end = 1;
    flush_response_side(s);

    /* Java: clientEndpoint.releaseAndReuse() */
    if (s->endpoint) {
        hc_endpoint_release_and_reuse(s->endpoint);
        s->endpoint = NULL;
    }
}

static void
out_failed(hc_client_handler *self, const char *reason)
{
    exchange_state *s = OUTGOING(self)->state;

    log_line("[proxy<-origin] %s error: %s", s->id, reason);

    /* Java: clientEndpoint.releaseAndDiscard() */
    if (s->endpoint) {
        hc_endpoint_release_and_discard(s->endpoint);
        s->endpoint = NULL;
    }

    if (s->response_channel == NULL) {
        return;
    }

    if (!s->response_sent) {
        /* Java: 503 for I/O problems. Everything here is I/O. */
        s->response_sent = 1;
        hc_response_send_simple(s->response_channel, 503, "text/plain", reason, 1);
    } else if (s->server_ex) {
        /* Response already under way: all we can do is cut it off. */
        hc_server_abort(s->server_ex);
    }
}

static void
out_release(hc_client_handler *self)
{
    outgoing_handler *o = OUTGOING(self);
    exchange_state   *s = o->state;

    s->client_ex     = NULL;
    s->request_data  = NULL;
    s->upstream_gone = 1;

    if (s->endpoint) {          /* neither stream_end nor failed released it */
        hc_endpoint_release_and_discard(s->endpoint);
        s->endpoint = NULL;
    }

    /* The origin no longer wants request bytes: stop holding the client. */
    if (s->input_suspended && s->server_ex) {
        s->input_suspended = 0;
        hc_server_resume_input(s->server_ex);
    }

    pbuf_free(&s->inbuf);
    free(o);
    state_unref(s);
}

static const hc_client_handler_vtbl outgoing_vtbl = {
    .consume_information = out_consume_information,
    .produce_request  = out_produce_request,
    .consume_response = out_consume_response,
    .consume          = out_consume,
    .stream_end       = out_stream_end,
    .produce          = out_produce,
    .failed           = out_failed,
    .release          = out_release,
};

/* ------------------------------------------------------------------------ */
/* IncomingExchangeHandler (client -> proxy): an hc_server_handler          */
/* ------------------------------------------------------------------------ */

typedef struct incoming_handler {
    hc_server_handler base;    /* must be first: the "interface" */
    exchange_state   *state;
} incoming_handler;

#define INCOMING(self) ((incoming_handler *)(void *)(self))

/* Java: the FutureCallback<AsyncClientEndpoint> passed to connect(). */
static void
on_connected(void *arg, hc_endpoint *ep, const char *error)
{
    exchange_state   *s = arg;
    outgoing_handler *o;

    s->connect_op = NULL;

    if (error) {
        log_line("[proxy->origin] %s connect failed: %s", s->id, error);
        s->upstream_gone = 1;

        if (s->response_channel && !s->response_sent) {
            s->response_sent = 1;
            hc_response_send_simple(s->response_channel, 503, "text/plain", error, 1);
        }

        state_unref(s);
        return;
    }

    if (s->downstream_gone || (o = calloc(1, sizeof(*o))) == NULL) {
        hc_endpoint_release_and_reuse(ep);      /* unused: fine to keep */
        s->upstream_gone = 1;

        if (s->response_channel && !s->response_sent) {
            s->response_sent = 1;
            hc_response_send_simple(s->response_channel, 503, "text/plain", "out of memory", 1);
        }

        state_unref(s);
        return;
    }

    log_line("[proxy->origin] %s connection leased", s->id);
    o->base.vtbl  = &outgoing_vtbl;
    o->state      = s;                  /* takes over the connect reference */
    s->endpoint   = ep;
    s->client_ex  = hc_endpoint_execute(ep, &o->base);

    if (s->client_ex == NULL) {
        free(o);
        hc_endpoint_release_and_discard(ep);
        s->endpoint      = NULL;
        s->upstream_gone = 1;

        if (s->response_channel && !s->response_sent) {
            s->response_sent = 1;
            hc_response_send_simple(s->response_channel, 503, "text/plain",
                                    "cannot execute on origin connection", 1);
        }

        state_unref(s);
    }
}

static void
in_handle_request(hc_server_handler *self, hc_server_exchange *ex,
                  const hc_request_head *head, const hc_entity *entity,
                  hc_response_channel *rc)
{
    exchange_state   *s = INCOMING(self)->state;

    s->server_ex          = ex;
    s->response_channel   = rc;
    s->method             = dup_str(head->method);
    s->target             = dup_str(head->target);
    s->version            = head->version;
    s->request_headers    = copy_end_to_end_headers(head->headers);
    s->has_request_entity = entity != NULL;
    s->input_end          = entity == NULL;

    if (entity) {
        s->request_entity              = *entity;
        s->request_entity.content_type = NULL;  /* only valid during call */
    }

    log_line("[client->proxy] %s %s %s", s->id, head->method, head->target);

    /* Java: requester.connect(targetHost, Timeout.ofSeconds(30), ...).
     * The pending connect holds a reference to the state; on success it is
     * handed to the outgoing handler. */
    s->refs++;
    s->connect_op = hc_requester_connect(requester, &target,
                                         CONNECT_TIMEOUT_MS, on_connected, s);

    if (s->connect_op == NULL) {
        s->refs--;
        s->upstream_gone = 1;
        s->response_sent = 1;
        hc_response_send_simple(rc, 503, "text/plain", "cannot connect to origin", 1);
    }
}

static void
in_consume(hc_server_handler *self, const void *data, size_t len)
{
    exchange_state *s = INCOMING(self)->state;

    log_line("[client->proxy] %s %zu bytes received", s->id, len);

    if (s->upstream_gone) {
        return;                 /* origin finished or failed: discard */
    }

    if (s->request_data && s->inbuf.len == 0) {
        size_t room = hc_data_capacity(s->request_data);
        size_t n    = len < room ? len : room;

        if (n && hc_data_write(s->request_data, data, n) == (long)n) {
            log_line("[proxy->origin] %s %zu bytes sent", s->id, n);
            data = (const unsigned char *)data + n;
            len -= n;
        }
    }

    if (len) {
        pbuf_append(&s->inbuf, data, len);
        hc_data_request_output(s->request_data);   /* NULL-safe */
    }

    if (s->inbuf.len >= MAX_BUFFERED && !s->input_suspended) {
        s->input_suspended = 1;
        log_line("[client->proxy] %s input suspended (%zu buffered)", s->id, s->inbuf.len);
        hc_server_suspend_input(s->server_ex);
    }
}

static void
in_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    exchange_state *s = INCOMING(self)->state;

    (void)trailers;

    if (s->has_request_entity) {
        log_line("[client->proxy] %s end of input", s->id);
    }

    s->input_end = 1;
    flush_request_side(s);
}

static void
in_produce(hc_server_handler *self, hc_data_channel *ch)
{
    (void)ch;
    flush_response_side(INCOMING(self)->state);
}

static void
in_failed(hc_server_handler *self, const char *reason)
{
    exchange_state *s = INCOMING(self)->state;

    log_line("[client<-proxy] %s error: %s", s->id, reason);

    if (s->client_ex) {
        hc_client_cancel(s->client_ex);
    }
}

static void
in_release(hc_server_handler *self)
{
    incoming_handler *in = INCOMING(self);
    exchange_state   *s  = in->state;

    s->server_ex        = NULL;
    s->response_channel = NULL;
    s->response_data    = NULL;
    s->downstream_gone  = 1;

    if (s->client_ex) {         /* nobody left to deliver the response to */
        hc_client_cancel(s->client_ex);
    }

    if (s->connect_op) {        /* still waiting for a pooled connection */
        hc_connect_cancel(s->connect_op);
        s->connect_op = NULL;
        state_unref(s);         /* the connect's reference */
    }

    log_line("[client<-proxy] %s exchange completed", s->id);
    free(in);
    state_unref(s);
}

static const hc_server_handler_vtbl incoming_vtbl = {
    .handle_request = in_handle_request,
    .consume        = in_consume,
    .stream_end     = in_stream_end,
    .produce        = in_produce,
    .failed         = in_failed,
    .release        = in_release,
};

/* Java: () -> new IncomingExchangeHandler(targetHost, requester) */
static hc_server_handler *
incoming_factory(void *arg)
{
    static unsigned long counter;
    incoming_handler    *in = calloc(1, sizeof(*in));
    exchange_state      *s  = calloc(1, sizeof(*s));

    (void)arg;

    if (!in || !s) {
        free(in);
        free(s);
        return NULL;
    }

    snprintf(s->id, sizeof(s->id), "%010lu", ++counter);
    s->refs      = 1;
    in->base.vtbl = &incoming_vtbl;
    in->state     = s;

    return &in->base;
}

/* Java: the ConnPoolListener passed to AsyncRequesterBootstrap. */
static void
pool_on_lease(void *arg, const hc_pool_entry *e, hc_pool_stats t)
{
    (void)arg;
    char route[300];

    hc_host_to_string(hc_pool_entry_route(e), route, sizeof(route));
    log_line("[proxy->origin] connection leased %s [leased: %d][available: %d][pending: %d][max: %d]",
             route, t.leased, t.available, t.pending, t.max);
}

static void
pool_on_release(void *arg, const hc_pool_entry *e, int reusable, hc_pool_stats t)
{
    (void)arg;
    char route[300];

    hc_host_to_string(hc_pool_entry_route(e), route, sizeof(route));
    log_line("[proxy->origin] connection %s %s [leased: %d][available: %d][pending: %d][max: %d]",
             reusable ? "released" : "closed", route, t.leased, t.available, t.pending, t.max);
}

/* ------------------------------------------------------------------------ */

int
main(int argc, char **argv)
{
    const char *target_arg = NULL;
    int         port       = 8080;
    hc_server  *server;
    char        target_str[300];

    /* Java: AsyncRequesterBootstrap ... setMaxTotal(100)
     *                                  .setDefaultMaxPerRoute(20) */
    hc_requester_config rcfg = { .max_total = 100, .default_max_per_route = 20 };

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--quiet") == 0) {
            quiet = 1;
        } else if (strcmp(argv[i], "--cafile") == 0 && i + 1 < argc) {
            rcfg.tls_ca_file = argv[++i];
        } else if (strcmp(argv[i], "--insecure") == 0) {
            rcfg.tls_no_verify = 1;
        } else if (target_arg == NULL) {
            target_arg = argv[i];
        } else {
            port = atoi(argv[i]);
        }
    }

    /* Java: HttpHost.create(args[0]) */
    if (target_arg == NULL || hc_host_parse(&target, target_arg) != 0) {
        fprintf(stderr, "Usage: %s [--cafile PEM] [--insecure] [--quiet] "
                        "<[http|https://]hostname[:port]> [listener port]\n", argv[0]);
        return 1;
    }

    hc_host_authority(&target, target_authority, sizeof(target_authority));
    hc_host_to_string(&target, target_str, sizeof(target_str));
    hc_pool_listener    plis = { pool_on_lease, pool_on_release, NULL };

    if ((reactor = hc_reactor_new()) == NULL ||
        (requester = hc_requester_new(reactor, &rcfg)) == NULL ||
        (server = hc_server_new(reactor, incoming_factory, NULL)) == NULL) {
        fprintf(stderr, "initialization failed\n");
        return 1;
    }

    hc_pool_set_listener(hc_requester_pool(requester), &plis);

    hc_reactor_stop_on_signals(reactor);

    if (hc_server_listen(server, "0.0.0.0", (uint16_t)port) != 0) {
        fprintf(stderr, "cannot listen on port %d\n", port);
        return 1;
    }

    log_line("Listening on %d, proxying to %s", port, target_str);

    hc_reactor_run(reactor);

    log_line("Shutting down");
    hc_server_free(server);
    hc_requester_free(requester);
    hc_reactor_free(reactor);

    return 0;
}
