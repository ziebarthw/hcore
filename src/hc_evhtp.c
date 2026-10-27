/*
 * hc_evhtp.c - drives the hc.h exchange-handler interfaces from libevhtp.
 *
 * This is the only file that knows about libevhtp/libevent. It translates
 * libevhtp's hooks (on_headers, on_read, request-complete callback,
 * on_request_fini, connection events) into the handler callbacks declared
 * in hc.h, and the application's channel calls back into libevhtp.
 *
 * libevhtp facts this file depends on (checked against libevhtp 1.2.18):
 *
 *  - A server request must be paused (evhtp_request_pause) when its handler
 *    returns without replying; otherwise pipelined input is parsed into a
 *    new request. It is resumed once the reply is fully queued, and
 *    libevhtp then frees the request when the output has drained.
 *  - While paused, libevhtp's write callback (and so the on_write hook) does
 *    not run. Output drain is therefore observed with an evbuffer callback
 *    on the bufferevent's output buffer instead.
 *  - evhtp_send_reply_end() only sets a flag. If the output buffer is
 *    already empty nothing would ever look at that flag again, so the write
 *    callback is triggered explicitly.
 *  - Freeing a connection from inside one of its own callbacks is unsafe
 *    (libevhtp keeps using it after the hook returns). Everything
 *    destructive is deferred to a per-exchange "task" event that runs on the
 *    next loop iteration, outside libevhtp's call stack.
 *  - evhtp_connection_free() frees the request before the bufferevent, so
 *    the request-fini hook can still unregister from the output buffer.
 */
#include "hc.h"
#include "hc_processor.h"
#include "hc_pool.h"
#include "htstring/htstring.h"     /* third_party/htstring */

#include <errno.h>
#include <time.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/dns.h>
#include <event2/event.h>
#include <event2/util.h>
#include <evhtp/evhtp.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#ifndef EVHTP_DISABLE_SSL
#include <event2/bufferevent_ssl.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#define HC_HAVE_TLS 1
#endif

#define HC_HIGH_WATER (64u * 1024u)
#define HC_LOW_WATER  (16u * 1024u)

/* ======================================================================== */
/* Reactor                                                                  */
/* ======================================================================== */

struct hc_reactor {
    struct event_base *base;
    struct evdns_base *dns;
    struct event      *sig[2];
    int                owns_base;  /* 0: hc_reactor_new_on_base() */
};

static hc_reactor *
reactor_new(struct event_base *base)
{
    hc_reactor *r = calloc(1, sizeof(*r));

    if (r == NULL) {
        return NULL;
    }

    signal(SIGPIPE, SIG_IGN);

    if (base) {
        r->base = base;
    } else if ((r->base = event_base_new()) != NULL) {
        r->owns_base = 1;
    } else {
        free(r);
        return NULL;
    }

    /* Name resolution is optional: without it, hosts must be numeric. */
    r->dns = evdns_base_new(r->base, EVDNS_BASE_INITIALIZE_NAMESERVERS);

    if (r->dns == NULL) {
        r->dns = evdns_base_new(r->base, 0);
    }

    return r;
}

hc_reactor *
hc_reactor_new(void)
{
    return reactor_new(NULL);
}

hc_reactor *
hc_reactor_new_on_base(void *event_base)
{
    return event_base ? reactor_new(event_base) : NULL;
}

int
hc_reactor_run(hc_reactor *r)
{
    return event_base_dispatch(r->base);
}

void
hc_reactor_stop(hc_reactor *r)
{
    event_base_loopbreak(r->base);
}

static void
signal_cb(evutil_socket_t sig, short what, void *arg)
{
    (void)sig;
    (void)what;
    hc_reactor_stop(arg);
}

int
hc_reactor_stop_on_signals(hc_reactor *r)
{
    r->sig[0] = evsignal_new(r->base, SIGINT, signal_cb, r);
    r->sig[1] = evsignal_new(r->base, SIGTERM, signal_cb, r);

    if (!r->sig[0] || !r->sig[1]) {
        return -1;
    }

    return (event_add(r->sig[0], NULL) || event_add(r->sig[1], NULL)) ? -1 : 0;
}

int64_t
hc_reactor_now_ms(hc_reactor *r)
{
    struct timespec ts;

    (void)r;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

struct hc_timer {
    struct event *ev;
    hc_timer_cb   cb;
    void         *arg;
};

static void
timer_fire_(evutil_socket_t fd, short what, void *arg)
{
    hc_timer *t = arg;

    (void)fd;
    (void)what;
    t->cb(t->arg);              /* may free t */
}

hc_timer *
hc_timer_new(hc_reactor *r, hc_timer_cb cb, void *arg)
{
    hc_timer *t = calloc(1, sizeof(*t));

    if (t == NULL) {
        return NULL;
    }

    t->cb  = cb;
    t->arg = arg;

    if ((t->ev = evtimer_new(r->base, timer_fire_, t)) == NULL) {
        free(t);
        return NULL;
    }

    return t;
}

void
hc_timer_start(hc_timer *t, int64_t delay_ms)
{
    struct timeval tv;

    if (delay_ms < 0) {
        delay_ms = 0;
    }

    tv.tv_sec  = (time_t)(delay_ms / 1000);
    tv.tv_usec = (suseconds_t)((delay_ms % 1000) * 1000);
    evtimer_add(t->ev, &tv);
}

void
hc_timer_stop(hc_timer *t)
{
    if (t) {
        evtimer_del(t->ev);
    }
}

void
hc_timer_free(hc_timer *t)
{
    if (t) {
        event_free(t->ev);
        free(t);
    }
}

void
hc_reactor_free(hc_reactor *r)
{
    if (r == NULL) {
        return;
    }

    for (int i = 0; i < 2; i++) {
        if (r->sig[i]) {
            event_free(r->sig[i]);
        }
    }

    if (r->dns) {
        evdns_base_free(r->dns, 0);
    }

    if (r->owns_base) {
        event_base_free(r->base);
    }

    free(r);
}

/* ======================================================================== */
/* Headers                                                                  */
/* ======================================================================== */

struct hc_headers {
    size_t n;
    size_t cap;
    char **kv;                  /* kv[2i] = name, kv[2i+1] = value */
};

hc_headers *
hc_headers_new(void)
{
    return calloc(1, sizeof(hc_headers));
}

void
hc_headers_free(hc_headers *h)
{
    if (h == NULL) {
        return;
    }

    for (size_t i = 0; i < 2 * h->n; i++) {
        free(h->kv[i]);
    }

    free(h->kv);
    free(h);
}

static char *
xstrdup(const char *s)
{
    size_t len = strlen(s) + 1;
    char  *p   = malloc(len);

    return p ? memcpy(p, s, len) : NULL;
}

int
hc_headers_add(hc_headers *h, const char *name, const char *value)
{
    if (h->n == h->cap) {
        size_t ncap = h->cap ? h->cap * 2 : 16;
        char **nkv  = realloc(h->kv, 2 * ncap * sizeof(char *));

        if (nkv == NULL) {
            return -1;
        }

        h->kv  = nkv;
        h->cap = ncap;
    }

    char *k = xstrdup(name);
    char *v = xstrdup(value ? value : "");

    if (!k || !v) {
        free(k);
        free(v);
        return -1;
    }

    h->kv[2 * h->n]     = k;
    h->kv[2 * h->n + 1] = v;
    h->n++;

    return 0;
}

size_t
hc_headers_remove(hc_headers *h, const char *name)
{
    size_t out = 0, removed = 0;

    for (size_t i = 0; i < h->n; i++) {
        if (strcasecmp(h->kv[2 * i], name) == 0) {
            free(h->kv[2 * i]);
            free(h->kv[2 * i + 1]);
            removed++;
            continue;
        }

        h->kv[2 * out]     = h->kv[2 * i];
        h->kv[2 * out + 1] = h->kv[2 * i + 1];
        out++;
    }

    h->n = out;

    return removed;
}

const char *
hc_headers_get(const hc_headers *h, const char *name)
{
    for (size_t i = 0; h && i < h->n; i++) {
        if (strcasecmp(h->kv[2 * i], name) == 0) {
            return h->kv[2 * i + 1];
        }
    }

    return NULL;
}

hc_headers *
hc_headers_copy(const hc_headers *h)
{
    hc_headers *c = hc_headers_new();

    for (size_t i = 0; c && h && i < h->n; i++) {
        if (hc_headers_add(c, h->kv[2 * i], h->kv[2 * i + 1]) != 0) {
            hc_headers_free(c);
            return NULL;
        }
    }

    return c;
}

size_t
hc_headers_count(const hc_headers *h)
{
    return h ? h->n : 0;
}

int
hc_headers_at(const hc_headers *h, size_t i, const char **name, const char **value)
{
    if (h == NULL || i >= h->n) {
        return -1;
    }

    *name  = h->kv[2 * i];
    *value = h->kv[2 * i + 1];

    return 0;
}

static int
copy_evhtp_header_(evhtp_kv_t *kv, void *arg)
{
    hc_headers_add(arg, kv->key, kv->val ? kv->val : "");

    return 0;
}

static hc_headers *
headers_from_evhtp(evhtp_headers_t *in)
{
    hc_headers *h = hc_headers_new();

    if (h && in) {
        evhtp_headers_for_each(in, copy_evhtp_header_, h);
    }

    return h;
}

static int
is_framing_header(const char *name)
{
    return strcasecmp(name, "content-length") == 0 ||
           strcasecmp(name, "transfer-encoding") == 0 ||
           strcasecmp(name, "connection") == 0;
}

static void
headers_to_evhtp(const hc_headers *h, evhtp_headers_t *out)
{
    const char *k, *v;

    for (size_t i = 0; hc_headers_at(h, i, &k, &v) == 0; i++) {
        if (!is_framing_header(k)) {
            evhtp_headers_add_header(out, evhtp_header_new(k, v, 1, 1));
        }
    }
}

static int
has_token(const char *value, const char *token)
{
    size_t tl = strlen(token);

    for (const char *p = value; p && *p;) {
        while (*p == ' ' || *p == '\t' || *p == ',') {
            p++;
        }

        if (strncasecmp(p, token, tl) == 0 &&
            (p[tl] == '\0' || p[tl] == ',' || p[tl] == ' ' || p[tl] == ';')) {
            return 1;
        }

        p = strchr(p, ',');
    }

    return 0;
}

/* Fills *e from framing headers. Returns e, or NULL for "no body".
 * *close_delimited is set when the body runs until the peer closes. */
static const hc_entity *
entity_from_headers(const hc_headers *h, hc_entity *e, int body_possible,
                    int is_response, int *close_delimited)
{
    const char *te = hc_headers_get(h, "transfer-encoding");
    const char *cl = hc_headers_get(h, "content-length");

    *close_delimited  = 0;
    e->content_type   = hc_headers_get(h, "content-type");
    e->content_length = -1;

    if (!body_possible) {
        return NULL;
    }

    if (te && has_token(te, "chunked")) {
        return e;
    }

    if (cl) {
        e->content_length = strtoll(cl, NULL, 10);
        return e->content_length > 0 ? e : NULL;
    }

    if (is_response) {          /* RFC 9112 6.3: read until close */
        *close_delimited = 1;
        return e;
    }

    return NULL;
}

/* ======================================================================== */
/* Data channel (outgoing body)                                             */
/* ======================================================================== */

struct hc_data_channel {
    struct bufferevent       *bev;     /* NULL once detached                 */
    struct evbuffer          *out;     /* bev's output; HTTP/2: the stream's
                                        * req->buffer_out                    */
    evhtp_request_t          *h2req;   /* HTTP/2 server reply, else NULL     */
    struct evbuffer          *stage;   /* HTTP/2: hands data to libevhtp     */
    struct evbuffer_cb_entry *cbe;
    int                       chunked;
    int                       discard; /* HEAD response: accept, send nothing */
    int64_t                   remaining; /* Content-Length mode, else -1     */
    int                       ended;
    int                       full;
    void                     *owner;
    void                    (*on_produce)(void *owner);
    void                    (*on_end)(void *owner);
};

static void
chan_drain_cb(struct evbuffer *buf, const struct evbuffer_cb_info *info, void *arg)
{
    hc_data_channel *ch = arg;

    if (info->n_deleted > 0 && ch->full && !ch->ended &&
        evbuffer_get_length(buf) < HC_LOW_WATER) {
        ch->full = 0;
        ch->on_produce(ch->owner);
    }
}

static void
chan_attach(hc_data_channel *ch, struct bufferevent *bev, int chunked,
            int64_t length, void *owner, void (*on_produce)(void *),
            void (*on_end)(void *))
{
    ch->bev        = bev;
    ch->out        = bufferevent_get_output(bev);
    ch->h2req      = NULL;
    ch->stage      = NULL;
    ch->chunked    = chunked;
    ch->remaining  = chunked ? -1 : length;
    ch->ended      = 0;
    ch->full       = 0;
    ch->owner      = owner;
    ch->on_produce = on_produce;
    ch->on_end     = on_end;
    ch->cbe        = evbuffer_add_cb(ch->out, chan_drain_cb, ch);
}

/* HTTP/2 server reply: the body goes through evhtp_send_reply_body() into
 * the stream's own buffer (req->buffer_out), which libevhtp empties as the
 * peer's flow-control window allows; watching that buffer gives per-stream
 * backpressure. No chunk framing: DATA frames delimit the body. */
static void
chan_attach_h2(hc_data_channel *ch, evhtp_request_t *req, int64_t length,
               void *owner, void (*on_produce)(void *), void (*on_end)(void *))
{
    ch->bev        = evhtp_connection_get_bev(evhtp_request_get_connection(req));
    ch->out        = req->buffer_out;
    ch->h2req      = req;
    ch->stage      = evbuffer_new();
    ch->chunked    = 0;
    ch->remaining  = length;
    ch->ended      = 0;
    ch->full       = 0;
    ch->owner      = owner;
    ch->on_produce = on_produce;
    ch->on_end     = on_end;
    ch->cbe        = evbuffer_add_cb(ch->out, chan_drain_cb, ch);
}

static void
chan_detach(hc_data_channel *ch)
{
    if (ch->bev && ch->cbe) {
        evbuffer_remove_cb_entry(ch->out, ch->cbe);
    }

    if (ch->stage) {
        evbuffer_free(ch->stage);
    }

    ch->bev   = NULL;
    ch->out   = NULL;
    ch->cbe   = NULL;
    ch->h2req = NULL;
    ch->stage = NULL;
}

long
hc_data_write(hc_data_channel *ch, const void *data, size_t len)
{
    if (ch == NULL || ch->bev == NULL || ch->ended) {
        return -1;
    }

    if (len == 0) {
        return 0;
    }

    if (ch->discard) {
        return (long)len;
    }

    if (ch->remaining >= 0) {
        if ((int64_t)len > ch->remaining) {
            return -1;          /* would exceed the declared Content-Length */
        }

        ch->remaining -= (int64_t)len;
    }

    if (ch->h2req) {
        /* appends to req->buffer_out and wakes the stream */
        evbuffer_add(ch->stage, data, len);
        evhtp_send_reply_body(ch->h2req, ch->stage);
    } else if (ch->chunked) {
        evbuffer_add_printf(ch->out, "%zx\r\n", len);
        evbuffer_add(ch->out, data, len);
        evbuffer_add(ch->out, "\r\n", 2);
    } else {
        evbuffer_add(ch->out, data, len);
    }

    if (evbuffer_get_length(ch->out) >= HC_HIGH_WATER) {
        ch->full = 1;
    }

    return (long)len;
}

size_t
hc_data_capacity(hc_data_channel *ch)
{
    size_t queued;

    if (ch == NULL || ch->bev == NULL || ch->ended) {
        return 0;
    }

    queued = evbuffer_get_length(ch->out);

    if (queued >= HC_HIGH_WATER) {
        ch->full = 1;
        return 0;
    }

    return HC_HIGH_WATER - queued;
}

void
hc_data_request_output(hc_data_channel *ch)
{
    if (ch && ch->bev && !ch->ended) {
        ch->on_produce(ch->owner);
    }
}

int
hc_data_end(hc_data_channel *ch, const hc_headers *trailers)
{
    if (ch == NULL || ch->bev == NULL || ch->ended) {
        return -1;
    }

    if (ch->remaining > 0 && !ch->discard) {
        return -1;              /* fewer bytes than declared */
    }

    /* Trailers go out with HTTP/1.1 chunked framing only; an HTTP/2 reply
     * ends without them (libevhtp has no HTTP/2 trailer support). */
    if (ch->chunked && !ch->discard) {
        const char *k, *v;

        evbuffer_add(ch->out, "0\r\n", 3);

        for (size_t i = 0; hc_headers_at(trailers, i, &k, &v) == 0; i++) {
            evbuffer_add_printf(ch->out, "%s: %s\r\n", k, v);
        }

        evbuffer_add(ch->out, "\r\n", 2);
    }

    ch->ended = 1;
    ch->on_end(ch->owner);

    return 0;
}

/* ======================================================================== */
/* Deferred tasks                                                           */
/* ======================================================================== */

enum {
    T_START   = 1u << 0,
    T_PRODUCE = 1u << 1,
    T_ABORT   = 1u << 2,
    T_FINISH  = 1u << 3,
    T_RAWREAD = 1u << 4,
};

static void
schedule(struct event *task, unsigned *pending, unsigned what)
{
    *pending |= what;
    event_active(task, EV_TIMEOUT, 1);
}

/* ======================================================================== */
/* Server side                                                              */
/* ======================================================================== */

/* An HTTP/2 connection (the ziebarthw libevhtp fork, built with nghttp2).
 * Its requests are streams: libevhtp frames their replies, so everything
 * this engine does at the connection level for HTTP/1.x - pausing it,
 * enabling/disabling reads, poking the write callback, writing body bytes
 * and chunk framing to its output - must not happen; a request's body goes
 * through evhtp_send_reply_body() and waits in req->buffer_out until the
 * stream's flow-control window takes it. */
#ifdef EVHTP_CONN_FLAG_IS_HTTP2
#define HC_HAVE_H2 1
#define conn_is_h2(c) ((c) != NULL && ((c)->flags & EVHTP_CONN_FLAG_IS_HTTP2) != 0)
#else
#define conn_is_h2(c) 0
#endif

/* Accepted connections, so hc_server_free() can close them: libevhtp does
 * not track them and evhtp_free() leaves them open. */
typedef struct srv_conn {
    struct srv_conn    *prev;
    struct srv_conn    *next;
    hc_server          *srv;
    evhtp_connection_t *c;
    unsigned            h2_active; /* HTTP/2 streams under way (read timeout off) */
    int                 tls;       /* counted in cx_ssl_active          */
    int                 h2;        /* counted as HTTP/2 (else HTTP/1)   */
    struct evbuffer          *rx, *tx;          /* the bufferevent's buffers */
    struct evbuffer_cb_entry *rx_cbe, *tx_cbe;  /* byte counters             */
} srv_conn;

/* A registered pattern, classified once at registration so a lookup is only
 * length and byte comparisons: "*" (any), "*suffix", "prefix*", or a path
 * that only matches exactly. */
typedef enum { ROUTE_EXACT, ROUTE_PREFIX, ROUTE_SUFFIX, ROUTE_ANY } route_kind;

typedef struct srv_route {
    char                     *pattern;      /* owned copy */
    htstring_t                whole;        /* all of pattern */
    htstring_t                stem;         /* pattern without its '*' */
    route_kind                kind;
    hc_server_handler_factory factory;
    void                     *arg;
} srv_route;

struct hc_server {
    hc_reactor               *r;
    evhtp_t                  *htp;
    srv_conn                 *conns;
    size_t                    n_conns;
    int                       closing;  /* hc_server_free() in progress */
    int                       draining; /* hc_server_drain(): no more keep-alive */
    struct hc_server_exchange *exchanges; /* live exchanges (drain: busy connections) */
    struct event             *drain_ev; /* closes idle connections, from the loop */
    evhtp_callback_t         *cb;
    hc_server_handler_factory factory;  /* unmatched requests (NULL: 404) */
    void                     *arg;
    srv_route                *routes;
    size_t                    n_routes;
    hc_server_listener        listener;
    int                       http2;   /* offer h2 in TLS ALPN (if built in) */
    hc_processor             *processor; /* interceptors (own reference) */
    struct timeval            read_tv;  /* client timeouts (0: none) */
    struct timeval            write_tv;
    int64_t                   max_body; /* request body limit (0: none) */
    hc_server_stats           stats;
#ifdef HC_HAVE_TLS
    evhtp_ssl_cfg_t           tls;     /* libevhtp keeps a pointer to it */
#endif
};

struct hc_response_channel {
    hc_server_exchange *ex;
};

struct hc_server_exchange {
    hc_server           *srv;
    struct hc_server_exchange *prev, *next;  /* srv->exchanges */
    evhtp_request_t     *req;          /* NULL once libevhtp has freed it */
    evhtp_connection_t  *conn;         /* valid while req != NULL         */
    hc_server_handler   *h;
    hc_response_channel  rc;
    hc_data_channel      out;
    struct event        *task;
    unsigned             pending;
    int                  is_head;
    int                  req_done;
    int                  resp_started;
    int                  resp_done;
    int                  paused;
    int                  finished;
    int                  failed;
    int                  released;
    int                  input_suspended;
    int                  keep_alive;   /* connection stays open afterwards */
    int                  h2;           /* an HTTP/2 stream                 */
    int                  h2_counted;   /* in its srv_conn's h2_active      */
    int                  reset;        /* HTTP/2 stream reset sent         */
    int                  too_large;    /* body over the limit: 413 / close */
    int64_t              body_read;    /* request body bytes so far        */
    struct evbuffer_cb_entry *flush_cbe; /* close once the output is sent  */
    hc_processor        *proc;         /* the server's when it began (ref) */
    hc_context          *ctx;          /* interceptors' and handler's     */
    const char          *reason;
    char                 remote[64];   /* "address:port", for the listener */
    struct sockaddr_storage remote_addr;   /* copied when the request arrives */
    socklen_t            remote_len;   /* 0: unknown                       */
    struct sockaddr_storage local_addr;    /* looked up on first use          */
    socklen_t            local_len;
    int                  local_state;  /* 0 not looked up, 1 known, -1 unknown */
};

static void srv_task_cb(evutil_socket_t fd, short what, void *arg);

static void
ms_to_tv(int64_t ms, struct timeval *tv)
{
    if (ms < 0) {
        ms = 0;
    }

    tv->tv_sec  = (time_t)(ms / 1000);
    tv->tv_usec = (suseconds_t)((ms % 1000) * 1000);
}

static int
tv_is_set(const struct timeval *tv)
{
    return tv->tv_sec != 0 || tv->tv_usec != 0;
}

/* The engine's record of an accepted connection (its fini hook's arg);
 * NULL once the connection is being freed. */
static srv_conn *
srv_conn_of(const evhtp_connection_t *c)
{
    return (c && c->hooks) ? (srv_conn *)c->hooks->on_connection_fini_arg : NULL;
}

/* HTTP/2: the connection's read timeout is for a client that leaves the
 * server waiting. While a stream has its request and awaits its response,
 * the server is the one working (a slow handler, a slow origin) and the
 * client may rightly stay silent, so the read timeout is off until the
 * last such stream ends. (HTTP/1.x gets the same effect from reading being
 * stopped while a response is made.) */
static void
srv_h2_busy(hc_server_exchange *ex)
{
    srv_conn *n;

    if (!ex->h2 || ex->h2_counted || !tv_is_set(&ex->srv->read_tv) ||
        (n = srv_conn_of(ex->conn)) == NULL) {
        return;
    }

    ex->h2_counted = 1;

    if (n->h2_active++ == 0) {
        evhtp_connection_set_timeouts(ex->conn, NULL,
            tv_is_set(&ex->srv->write_tv) ? &ex->srv->write_tv : NULL);
    }
}

static void
srv_h2_idle(hc_server_exchange *ex)
{
    srv_conn *n;

    if (!ex->h2_counted) {
        return;
    }

    ex->h2_counted = 0;

    if ((n = srv_conn_of(ex->conn)) == NULL || --n->h2_active > 0 ||
        ex->srv->closing || (ex->conn->flags & EVHTP_CONN_FLAG_ERROR)) {
        return;
    }

    evhtp_connection_set_timeouts(ex->conn, &ex->srv->read_tv,
        tv_is_set(&ex->srv->write_tv) ? &ex->srv->write_tv : NULL);
}

static void
srv_schedule_produce(void *owner)
{
    hc_server_exchange *ex = owner;

    schedule(ex->task, &ex->pending, T_PRODUCE);
}

/* Both directions are complete: hand the request back to libevhtp. */
static void
srv_try_finalize(hc_server_exchange *ex)
{
    struct bufferevent *bev;

    if (!ex->req || !ex->req_done || !ex->resp_done || ex->finished) {
        return;
    }

    ex->finished   = 1;

    if (ex->h2) {
        /* A stream: END_STREAM follows the last DATA frame, libevhtp frees
         * the request when the stream closes, and the connection stays for
         * its other streams (a drain closes it once they are done). */
        ex->keep_alive = 1;
        evhtp_send_reply_end(ex->req);
        return;
    }

    if (ex->srv->draining) {
        ex->req->flags &= ~EVHTP_REQ_FLAG_KEEPALIVE;    /* close once flushed */
    }

    ex->keep_alive = (ex->req->flags & EVHTP_REQ_FLAG_KEEPALIVE) != 0;
    bev            = evhtp_connection_get_bev(ex->conn);

    evhtp_send_reply_end(ex->req);

    if (ex->paused || ex->input_suspended) {
        /* Un-pause synchronously instead of evhtp_request_resume(). That
         * function defers its work to an event; a write callback that runs
         * in between frees the finished request, and the deferred resume
         * then re-enables EV_WRITE on an empty buffer, which makes libevent
         * call the write callback once more with no request ("no request
         * associated with connection") and drop the keep-alive connection.
         * WAITING makes libevhtp's write callback re-enable reading. */
        ex->paused          = 0;
        ex->input_suspended = 0;
        ex->conn->flags    &= ~EVHTP_CONN_FLAG_PAUSED;
        ex->conn->flags    |= EVHTP_CONN_FLAG_WAITING;
    }

    /* Nothing left to flush means libevhtp's write callback would never
     * fire to recycle (keep-alive) or close the connection. Poke it.
     * Deferred bufferevent callbacks are coalesced, so this cannot cause a
     * second write callback if a drain notification is already queued. */
    if (evbuffer_get_length(bufferevent_get_output(bev)) == 0) {
        bufferevent_trigger(bev, EV_WRITE, BEV_TRIG_DEFER_CALLBACKS);
    }
}

static void
srv_flush_cb(struct evbuffer *buf, const struct evbuffer_cb_info *info, void *arg)
{
    hc_server_exchange *ex = arg;

    (void)info;

    if (evbuffer_get_length(buf) == 0) {
        schedule(ex->task, &ex->pending, T_ABORT);
    }
}

/* HTTP/1.x: stop reading, and close the connection once everything written
 * to it has been sent (libevhtp's evhtp_connection_free() drops unsent
 * output in some versions, so it is only called once there is none). A
 * client that does not take the output is closed by the write timeout. */
static void
srv_close_after_flush(hc_server_exchange *ex)
{
    struct bufferevent *bev = evhtp_connection_get_bev(ex->conn);
    struct evbuffer    *out = bufferevent_get_output(bev);

    bufferevent_disable(bev, EV_READ);

    if (evbuffer_get_length(out) == 0) {
        schedule(ex->task, &ex->pending, T_ABORT);
    } else if (ex->flush_cbe == NULL) {
        ex->flush_cbe = evbuffer_add_cb(out, srv_flush_cb, ex);
    }
}

static void
srv_flush_watch_end(hc_server_exchange *ex)
{
    if (ex->flush_cbe && ex->conn) {
        evbuffer_remove_cb_entry(bufferevent_get_output(evhtp_connection_get_bev(ex->conn)),
                                 ex->flush_cbe);
    }

    ex->flush_cbe = NULL;
}

static void
srv_on_response_end(void *owner)
{
    hc_server_exchange *ex = owner;

    ex->resp_done = 1;

    /* The body is over the limit and the rest of it will not be read: close
     * the connection once the response has gone out. HTTP/2 discards the
     * rest of the stream instead. */
    if (ex->too_large && !ex->req_done && !ex->h2 && ex->conn) {
        srv_close_after_flush(ex);
        return;
    }

    srv_try_finalize(ex);
}

hc_data_channel *
hc_response_send(hc_response_channel *rc, const hc_response_head *head,
                 const hc_entity *entity)
{
    hc_server_exchange *ex = rc->ex;
    evhtp_request_t    *req = ex->req;
    const char         *conn_hdr;
    struct bufferevent *bev;
    char                num[32];
    hc_headers         *own_headers = NULL;
    hc_response_head    processed;

    if (req == NULL || ex->resp_started || ex->failed) {
        return NULL;
    }

    /* Interceptors work on a copy (the caller's head is const); they may
     * change the status and headers. A refusal drops the connection: the
     * handler's response cannot be replaced at this point. */
    if (ex->proc) {
        hc_http_response r;

        if ((own_headers = hc_headers_copy(head->headers)) == NULL) {
            hc_server_abort(ex);
            return NULL;
        }

        r.status   = head->status;
        r.version  = head->version;
        r.headers  = own_headers;

        if (hc_processor_process_response(ex->proc, &r, entity, ex->ctx) != 0) {
            hc_headers_free(own_headers);
            hc_server_abort(ex);
            return NULL;
        }

        processed         = *head;
        processed.status  = r.status;
        processed.headers = own_headers;
        head              = &processed;
    }

    ex->resp_started = 1;
    bev              = evhtp_connection_get_bev(ex->conn);

    if (ex->srv->listener.on_response_head) {
        ex->srv->listener.on_response_head(ex->srv->listener.arg, ex->remote, head);
    }

    headers_to_evhtp(head->headers, req->headers_out);

    /* libevhtp writes its own Connection header from the keep-alive flag. */
    conn_hdr = hc_headers_get(head->headers, "connection");

    if ((conn_hdr && has_token(conn_hdr, "close")) || ex->srv->draining) {
        req->flags &= ~EVHTP_REQ_FLAG_KEEPALIVE;    /* "Connection: close" */
    }

    hc_headers_free(own_headers);   /* copied into libevhtp's by now */
    conn_hdr = NULL;

    if (entity == NULL) {
        evhtp_send_reply_start(req, (evhtp_res)head->status);
        srv_on_response_end(ex);
        return NULL;
    }

    if (ex->is_head) {
        if (entity->content_length >= 0) {
            snprintf(num, sizeof(num), "%lld", (long long)entity->content_length);
            evhtp_headers_add_header(req->headers_out,
                evhtp_header_new("Content-Length", num, 0, 1));
        }

        evhtp_send_reply_start(req, (evhtp_res)head->status);
        if (ex->h2) {
            chan_attach_h2(&ex->out, req, -1, ex, srv_schedule_produce, srv_on_response_end);
        } else {
            chan_attach(&ex->out, bev, 0, -1, ex, srv_schedule_produce, srv_on_response_end);
        }
        ex->out.discard = 1;
    } else if (ex->h2) {
        /* DATA frames delimit the body: Content-Length is optional */
        if (entity->content_length >= 0) {
            snprintf(num, sizeof(num), "%lld", (long long)entity->content_length);
            evhtp_headers_add_header(req->headers_out,
                evhtp_header_new("Content-Length", num, 0, 1));
        }

        evhtp_send_reply_start(req, (evhtp_res)head->status);
        chan_attach_h2(&ex->out, req, entity->content_length, ex,
                       srv_schedule_produce, srv_on_response_end);
    } else if (entity->content_length >= 0) {
        snprintf(num, sizeof(num), "%lld", (long long)entity->content_length);
        evhtp_headers_add_header(req->headers_out,
            evhtp_header_new("Content-Length", num, 0, 1));
        evhtp_send_reply_start(req, (evhtp_res)head->status);
        chan_attach(&ex->out, bev, 0, entity->content_length, ex,
                    srv_schedule_produce, srv_on_response_end);
    } else if (req->proto == EVHTP_PROTO_11) {
        evhtp_send_reply_chunk_start(req, (evhtp_res)head->status);
        chan_attach(&ex->out, bev, 1, -1, ex, srv_schedule_produce, srv_on_response_end);
    } else {
        /* HTTP/1.0 client, unknown length: delimit the body by closing. */
        req->flags &= ~EVHTP_REQ_FLAG_KEEPALIVE;
        evhtp_send_reply_start(req, (evhtp_res)head->status);
        chan_attach(&ex->out, bev, 0, -1, ex, srv_schedule_produce, srv_on_response_end);
    }

    schedule(ex->task, &ex->pending, T_PRODUCE);

    return &ex->out;
}

int
hc_response_send_information(hc_response_channel *rc, const hc_response_head *head)
{
    hc_server_exchange *ex = rc->ex;
    struct evbuffer    *out;
    const char         *k, *v, *reason;

    /* 1xx only, before the final response, and never to an HTTP/1.0 client
     * (RFC 9110 15.2). 100 is libevhtp's job; 101 would be an upgrade. */
    if (ex->req == NULL || ex->resp_started || ex->failed || ex->req->proto != EVHTP_PROTO_11 ||
        head->status < 102 || head->status > 199) {
        return -1;
    }

    switch (head->status) {
        case 102: reason = "Processing"; break;
        case 103: reason = "Early Hints"; break;
        default:  reason = "Informational"; break;
    }

    out = bufferevent_get_output(evhtp_connection_get_bev(ex->conn));
    evbuffer_add_printf(out, "HTTP/1.1 %d %s\r\n", head->status, reason);

    for (size_t i = 0; hc_headers_at(head->headers, i, &k, &v) == 0; i++) {
        if (!is_framing_header(k)) {
            evbuffer_add_printf(out, "%s: %s\r\n", k, v);
        }
    }

    evbuffer_add(out, "\r\n", 2);

    return 0;
}

int
hc_response_send_simple(hc_response_channel *rc, int status,
                        const char *content_type, const char *body,
                        int close_connection)
{
    hc_headers      *h   = hc_headers_new();
    size_t           len = body ? strlen(body) : 0;
    hc_entity        e   = { (int64_t)len, content_type };
    hc_response_head head;
    hc_data_channel *ch;
    int              rv = -1;

    if (h == NULL) {
        return -1;
    }

    hc_headers_add(h, "Content-Type", content_type ? content_type : "text/plain");

    if (close_connection) {
        hc_headers_add(h, "Connection", "close");
    }

    head.status  = status;
    head.version = 11;
    head.headers = h;

    ch = hc_response_send(rc, &head, len ? &e : NULL);

    if (len == 0) {
        rv = rc->ex->resp_started ? 0 : -1;
    } else if (ch && hc_data_write(ch, body, len) == (long)len && hc_data_end(ch, NULL) == 0) {
        rv = 0;
    }

    hc_headers_free(h);

    return rv;
}

void
hc_server_suspend_input(hc_server_exchange *ex)
{
    /* HTTP/2: reading the connection serves every stream on it, so it is
     * never stopped for one (as libevhtp's own pause does on HTTP/2); the
     * request body is delivered as it arrives. */
    if (ex->req && !ex->req_done && !ex->input_suspended && !ex->h2) {
        ex->input_suspended = 1;
        bufferevent_disable(evhtp_connection_get_bev(ex->conn), EV_READ);
    }
}

void
hc_server_resume_input(hc_server_exchange *ex)
{
    if (ex->req && !ex->req_done && ex->input_suspended && !ex->h2) {
        ex->input_suspended = 0;
        bufferevent_enable(evhtp_connection_get_bev(ex->conn), EV_READ);
    }
}

void
hc_server_abort(hc_server_exchange *ex)
{
    if (ex->released || ex->req == NULL) {
        return;
    }

    if (!ex->failed) {
        ex->failed = 1;
        ex->reason = "aborted";
    }

    schedule(ex->task, &ex->pending, T_ABORT);
}

/* --- libevhtp hooks (server) -------------------------------------------- */

/* The request body went over hc_server_set_max_body() while arriving. */
static void
srv_body_too_large(hc_server_exchange *ex)
{
    ex->too_large = 1;
    ex->srv->stats.rq_too_large++;

    if (ex->failed || ex->req == NULL) {
        return;                 /* already failing: its body is discarded */
    }

    if (!ex->resp_started) {
        /* HTTP/1.x: also closes once sent (srv_on_response_end) */
        hc_response_send_simple(&ex->rc, 413, "text/plain", "Request body too large\n",
                                !ex->h2);
    }

    ex->failed = 1;
    ex->reason = "request body too large";

    /* A response under way cannot be finished properly: close the
     * connection. HTTP/1.x after a complete response (the 413, or one the
     * handler made earlier): close once it is sent - the rest of the body
     * is not read. */
    if (!ex->resp_done) {
        schedule(ex->task, &ex->pending, T_ABORT);
    } else if (!ex->h2 && !ex->req_done) {
        srv_close_after_flush(ex);
    }
}

static evhtp_res
srv_on_read(evhtp_request_t *req, struct evbuffer *buf, void *arg)
{
    hc_server_exchange *ex  = arg;
    size_t              len = evbuffer_get_length(buf);

    (void)req;

    ex->body_read += (int64_t)len;

    if (ex->srv->max_body > 0 && ex->body_read > ex->srv->max_body && !ex->too_large) {
        srv_body_too_large(ex);
    }

    if (len && !ex->failed && !ex->released) {
        ex->h->vtbl->consume(ex->h, evbuffer_pullup(buf, (ev_ssize_t)len), len);
    }

    /* Draining here keeps libevhtp from accumulating the body. */
    evbuffer_drain(buf, len);

    return EVHTP_RES_OK;
}

static evhtp_res
srv_on_error(evhtp_request_t *req, evhtp_error_flags errtype, void *arg)
{
    hc_server_exchange *ex = arg;
    short               ev = (short)errtype;  /* bufferevent events, or -1 */

    if (!ex->failed) {
        ex->failed = 1;

        if (errtype != (evhtp_error_flags)-1 && (ev & BEV_EVENT_TIMEOUT)) {
            ex->reason = (ev & BEV_EVENT_WRITING) ? "client write timeout" : "client read timeout";
        } else {
            ex->reason = "client connection error";
        }
    }

    chan_detach(&ex->out);

    /* A paused connection is only freed once resumed. */
    if (ex->paused) {
        ex->paused = 0;
        evhtp_request_resume(req);
    }

    return EVHTP_RES_OK;
}

static evhtp_res
srv_on_fini(evhtp_request_t *req, void *arg)
{
    hc_server_exchange *ex = arg;

    (void)req;

    chan_detach(&ex->out);
    srv_h2_idle(ex);
    srv_flush_watch_end(ex);
    ex->req  = NULL;
    ex->conn = NULL;

    if (!ex->finished && !ex->failed) {
        ex->failed = 1;
        ex->reason = ex->srv->closing ? "server shut down" : "client connection closed";
    }

    if (ex->srv->closing) {
        /* hc_server_free(): the loop will not run again, so release the
         * handler now. ex->req is already NULL; nothing touches libevhtp. */
        ex->pending |= T_FINISH;
        srv_task_cb(-1, 0, ex);
    } else {
        schedule(ex->task, &ex->pending, T_FINISH);
    }

    return EVHTP_RES_OK;
}

/* Request fully received (libevhtp's per-request callback). */
static void
srv_on_complete(evhtp_request_t *req, void *arg)
{
    hc_server_exchange *ex = arg;

    if (ex == NULL) {
        return;
    }

    ex->req_done = 1;
    srv_h2_busy(ex);    /* the client now waits for the server */

    if (!ex->failed && !ex->released) {
        ex->h->vtbl->stream_end(ex->h, NULL);
    }

    /* The response may already be complete (e.g. a GET answered from
     * handle_request()): then this is the moment both directions are done.
     * We are inside libevhtp's request callback, which is where libevhtp
     * expects a synchronous reply to be finished, so no pause is needed. */
    srv_try_finalize(ex);

    if (!ex->finished && ex->req && !ex->h2) {
        /* Reply will come later: stop libevhtp from reading the next
         * request on this connection until we are done. (HTTP/2: other
         * streams go on; there is no next request on this one.) */
        ex->paused = 1;
        evhtp_request_pause(req);
    }
}

/* --- routing (Java: UriPatternMatcher) -------------------------------- */

static void
route_classify(srv_route *r)
{
    r->whole = htstring_ctor(r->pattern, strlen(r->pattern));
    r->stem  = r->whole;

    if (r->whole.len == 1 && r->pattern[0] == '*') {
        r->kind = ROUTE_ANY;
    } else if (r->pattern[0] == '*') {
        r->kind = ROUTE_SUFFIX;
        htstring_consume(&r->stem, 1);
    } else if (r->pattern[r->whole.len - 1] == '*') {
        r->kind = ROUTE_PREFIX;
        r->stem.len--;
    } else {
        r->kind = ROUTE_EXACT;
    }
}

/* Wildcard match; exact matches are checked separately, before these. */
static int
route_matches(const srv_route *r, htstring_t path)
{
    switch (r->kind) {
    case ROUTE_ANY:    return 1;
    case ROUTE_SUFFIX: return htstring_has_suffix(path, r->stem);
    case ROUTE_PREFIX: return htstring_has_prefix(path, r->stem);
    case ROUTE_EXACT:  break;
    }

    return 0;
}

static const srv_route *
route_lookup(const hc_server *s, const char *path_cstr)
{
    htstring_t       path = htstring_ctor(path_cstr, strlen(path_cstr));
    const srv_route *best = NULL;

    for (size_t i = 0; i < s->n_routes; i++) {
        if (htstring_equals(s->routes[i].whole, path)) {
            return &s->routes[i];                       /* exact match wins */
        }
    }

    for (size_t i = 0; i < s->n_routes; i++) {
        const srv_route *r = &s->routes[i];

        if (!route_matches(r, path)) {
            continue;
        }

        /* the longest pattern; on equal length, the one ending in '*' */
        if (best == NULL || best->whole.len < r->whole.len ||
            (best->whole.len == r->whole.len && htstring_has_suffix(r->whole, HTSTRING_LIT("*")))) {
            best = r;
        }
    }

    return best;
}

int
hc_server_register(hc_server *s, const char *uri_pattern,
                   hc_server_handler_factory factory, void *arg)
{
    srv_route *r;
    char      *copy;

    if (uri_pattern == NULL || *uri_pattern == '\0' || factory == NULL) {
        return -1;
    }

    for (size_t i = 0; i < s->n_routes; i++) {          /* replace */
        if (htstring_equals(s->routes[i].whole, htstring_ctor(uri_pattern, strlen(uri_pattern)))) {
            s->routes[i].factory = factory;
            s->routes[i].arg     = arg;
            return 0;
        }
    }

    if ((copy = malloc(strlen(uri_pattern) + 1)) == NULL ||
        (r = realloc(s->routes, (s->n_routes + 1) * sizeof(*r))) == NULL) {
        free(copy);
        return -1;
    }

    strcpy(copy, uri_pattern);
    s->routes                   = r;
    s->routes[s->n_routes].pattern = copy;
    s->routes[s->n_routes].factory = factory;
    s->routes[s->n_routes].arg     = arg;
    route_classify(&s->routes[s->n_routes]);
    s->n_routes++;

    return 0;
}

void
hc_server_set_listener(hc_server *s, const hc_server_listener *l)
{
    if (l) {
        s->listener = *l;
    } else {
        memset(&s->listener, 0, sizeof(s->listener));
    }
}

/* Unmatched requests without a default factory: "404 Resource not found",
 * as HttpCore's server bootstrap answers. Stateless, so one shared instance;
 * any request body is read and discarded. */
static void
nf_handle_request(hc_server_handler *self, hc_server_exchange *ex,
                  const hc_request_head *head, const hc_entity *entity,
                  hc_response_channel *rc)
{
    (void)self;
    (void)ex;
    (void)head;
    (void)entity;
    hc_response_send_simple(rc, 404, "text/plain", "Resource not found", 0);
}

static void
nf_consume(hc_server_handler *self, const void *data, size_t len)
{
    (void)self;
    (void)data;
    (void)len;
}

static void
nf_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    (void)self;
    (void)trailers;
}

static void
nf_release(hc_server_handler *self)
{
    (void)self;
}

static const hc_server_handler_vtbl not_found_vtbl = {
    .handle_request = nf_handle_request,
    .consume        = nf_consume,
    .stream_end     = nf_stream_end,
    .release        = nf_release,
};

static hc_server_handler not_found_handler = { &not_found_vtbl };

/* A request an interceptor refused: answered with its status, in place of
 * the application's handler (which is released unused). Any request body
 * is read and discarded. */
typedef struct refused_handler {
    hc_server_handler base;
    int               status;
    int               close;    /* HTTP/1.x: close after the response */
} refused_handler;

static void
rf_handle_request(hc_server_handler *self, hc_server_exchange *ex,
                  const hc_request_head *head, const hc_entity *entity,
                  hc_response_channel *rc)
{
    refused_handler *rf = (refused_handler *)(void *)self;

    (void)head;
    (void)entity;
    hc_response_send_simple(rc, rf->status, "text/plain",
                            rf->status == 413 ? "Request body too large\n" : "Request refused",
                            rf->close && !ex->h2);
}

static void
rf_release(hc_server_handler *self)
{
    free(self);
}

static const hc_server_handler_vtbl refused_vtbl = {
    .handle_request = rf_handle_request,
    .consume        = nf_consume,
    .stream_end     = nf_stream_end,
    .release        = rf_release,
};

static void
format_remote(const evhtp_connection_t *c, char *buf, size_t len)
{
    const struct sockaddr *sa = c ? c->saddr : NULL;
    char                   ip[INET6_ADDRSTRLEN];

    if (sa && sa->sa_family == AF_INET) {
        const struct sockaddr_in *in = (const struct sockaddr_in *)(const void *)sa;

        evutil_inet_ntop(AF_INET, &in->sin_addr, ip, sizeof(ip));
        snprintf(buf, len, "%s:%u", ip, (unsigned)ntohs(in->sin_port));
    } else if (sa && sa->sa_family == AF_INET6) {
        const struct sockaddr_in6 *in6 = (const struct sockaddr_in6 *)(const void *)sa;

        evutil_inet_ntop(AF_INET6, &in6->sin6_addr, ip, sizeof(ip));
        snprintf(buf, len, "[%s]:%u", ip, (unsigned)ntohs(in6->sin6_port));
    } else {
        snprintf(buf, len, "unknown");
    }
}

/* Copies an IPv4/IPv6 address; returns its length, 0 for anything else. */
static socklen_t
copy_sockaddr(struct sockaddr_storage *dst, const struct sockaddr *sa)
{
    socklen_t n;

    switch (sa ? sa->sa_family : AF_UNSPEC) {
    case AF_INET:  n = sizeof(struct sockaddr_in);  break;
    case AF_INET6: n = sizeof(struct sockaddr_in6); break;
    default:       return 0;
    }

    memcpy(dst, sa, n);
    return n;
}

/* getpeername()-style result: copy at most *len bytes, report the full length. */
static int
give_sockaddr(const struct sockaddr_storage *addr, socklen_t n,
              struct sockaddr *out, socklen_t *len)
{
    if (n == 0 || out == NULL || len == NULL) {
        return -1;
    }

    memcpy(out, addr, *len < n ? *len : n);
    *len = n;
    return 0;
}

int
hc_server_exchange_remote_address(hc_server_exchange *ex, struct sockaddr *out, socklen_t *len)
{
    return ex ? give_sockaddr(&ex->remote_addr, ex->remote_len, out, len) : -1;
}

int
hc_server_exchange_local_address(hc_server_exchange *ex, struct sockaddr *out, socklen_t *len)
{
    if (ex == NULL) {
        return -1;
    }

    if (ex->local_state == 0) {
        struct sockaddr_storage ss;
        socklen_t               n = sizeof(ss);

        ex->local_state = -1;

        if (ex->req && ex->conn &&
            getsockname(ex->conn->sock, (struct sockaddr *)&ss, &n) == 0 &&
            (ex->local_len = copy_sockaddr(&ex->local_addr, (struct sockaddr *)&ss)) != 0) {
            ex->local_state = 1;
        }
    }

    return ex->local_state == 1 ? give_sockaddr(&ex->local_addr, ex->local_len, out, len) : -1;
}

static evhtp_res
srv_on_headers(evhtp_request_t *req, evhtp_headers_t *hdrs, void *arg)
{
    hc_server          *srv = arg;
    hc_server_exchange *ex;
    hc_headers         *h;
    hc_request_head     head;
    hc_entity           e;
    const hc_entity    *entity;
    const char         *method, *query;
    char               *target;
    size_t              tlen;
    int                 close_delimited;

    if ((ex = calloc(1, sizeof(*ex))) == NULL) {
        return EVHTP_RES_FATAL;
    }

    ex->srv    = srv;
    ex->req    = req;
    ex->conn   = evhtp_request_get_connection(req);
    ex->rc.ex  = ex;
    ex->task   = event_new(srv->r->base, -1, 0, srv_task_cb, ex);

    {
        const srv_route *route = route_lookup(srv, req->uri->path->full);

        if (route) {
            ex->h = route->factory(route->arg);
        } else if (srv->factory) {
            ex->h = srv->factory(srv->arg);
        } else {
            ex->h = &not_found_handler;
        }
    }

    ex->h2 = conn_is_h2(ex->conn);

    format_remote(ex->conn, ex->remote, sizeof(ex->remote));
    ex->remote_len = copy_sockaddr(&ex->remote_addr, ex->conn ? ex->conn->saddr : NULL);

    if (ex->task == NULL || ex->h == NULL) {
        if (ex->task) {
            event_free(ex->task);
        }

        free(ex);
        return EVHTP_RES_SERVUNAVAIL;
    }

    ex->proc = hc_processor_ref(srv->processor);
    ex->ctx  = hc_context_new(1, NULL, ex->remote);
    hc_context_set_protocol(ex->ctx, ex->h2 ? 20 : req->proto == EVHTP_PROTO_10 ? 10 : 11,
                            ex->conn && ex->conn->ssl != NULL);

    srv->stats.rq_total++;
    srv->stats.rq_active++;
    if (ex->h2) {
        srv->stats.rq_http2_total++;
    } else {
        srv->stats.rq_http1_total++;
    }

    ex->next = srv->exchanges;              /* unlinked in srv_task_cb */
    if (srv->exchanges) {
        srv->exchanges->prev = ex;
    }
    srv->exchanges = ex;

    /* Re-point the per-request hooks at this exchange. */
    evhtp_request_set_hook(req, evhtp_hook_on_read, (evhtp_hook)srv_on_read, ex);
    evhtp_request_set_hook(req, evhtp_hook_on_error, (evhtp_hook)srv_on_error, ex);
    evhtp_request_set_hook(req, evhtp_hook_on_request_fini, (evhtp_hook)srv_on_fini, ex);
    req->cbarg = ex;

    method      = htparser_get_methodstr_m(evhtp_request_get_method(req));
    ex->is_head = strcmp(method, "HEAD") == 0;
    query       = req->uri->query_raw ? (const char *)req->uri->query_raw : NULL;
    tlen        = strlen(req->uri->path->full) + (query ? strlen(query) + 1 : 0) + 1;
    target      = malloc(tlen);
    h           = headers_from_evhtp(hdrs);

    if (target == NULL || h == NULL) {
        free(target);
        hc_headers_free(h);
        return EVHTP_RES_FATAL;   /* fini hook releases the exchange */
    }

    snprintf(target, tlen, "%s%s%s", req->uri->path->full, query ? "?" : "",
             query ? query : "");

    entity         = entity_from_headers(h, &e, 1, 0, &close_delimited);

#ifdef HC_HAVE_H2
    /* HTTP/2: DATA frames delimit the body and Content-Length is optional,
     * so a body follows exactly when the HEADERS frame did not end the
     * stream (an upload of unknown length has neither Content-Length nor
     * chunked encoding, and would otherwise look bodiless). */
    if (ex->h2) {
        if (req->flags & EVHTP_REQ_FLAG_END_STREAM) {
            entity = NULL;
        } else if (entity == NULL) {
            e.content_length = -1;
            entity           = &e;
        }
    }
#endif

    /* Declared body over the limit: answered 413 below, never read. */
    if (srv->max_body > 0 && entity && entity->content_length > srv->max_body) {
        ex->too_large = 1;
        srv->stats.rq_too_large++;
    }

    /* "Expect: 100-continue" is answered here, before the handler runs, so
     * that a response the handler starts from handle_request() can never
     * precede it on the wire. (libevhtp's own 100 is disabled: it writes it
     * after this hook returns, i.e. after such a response head.) */
    {
        const char *expect = hc_headers_get(h, "expect");

        if (expect && has_token(expect, "100-continue") && req->proto == EVHTP_PROTO_11 &&
            entity != NULL && !ex->too_large) {
            evbuffer_add(bufferevent_get_output(evhtp_connection_get_bev(ex->conn)),
                         "HTTP/1.1 100 Continue\r\n\r\n", 25);
        }
    }

    head.method    = method;
    head.target    = target;
    head.authority = hc_headers_get(h, "host");
    head.version   = ex->h2 ? 20 : req->proto == EVHTP_PROTO_10 ? 10 : 11;
    head.headers   = h;

    /* Interceptors: they may change the headers; one may refuse the
     * request, which is then answered with its status instead of going to
     * the handler. The listener and the handler see the result. */
    {
        int rv = 0;

        if (ex->too_large) {
            rv = 413;
        } else if (ex->proc) {
            hc_http_request r = { head.method, head.target, head.authority, head.version, h };

            rv = hc_processor_process_request(ex->proc, &r, entity, ex->ctx);
        }

        if (rv != 0) {
            refused_handler *rf = calloc(1, sizeof(*rf));

            ex->h->vtbl->release(ex->h);    /* never saw the request */

            if (rf) {
                rf->base.vtbl = &refused_vtbl;
                rf->status    = rv;
                rf->close     = ex->too_large;
                ex->h         = &rf->base;
            } else {
                ex->h = &not_found_handler; /* out of memory: still answer */
            }
        }
    }

    if (srv->listener.on_request_head) {
        srv->listener.on_request_head(srv->listener.arg, ex->remote, &head);
    }

    ex->h->vtbl->handle_request(ex->h, ex, &head, entity, &ex->rc);

    free(target);
    hc_headers_free(h);

    return EVHTP_RES_OK;
}

static void
srv_task_cb(evutil_socket_t fd, short what, void *arg)
{
    hc_server_exchange *ex   = arg;
    unsigned            todo = ex->pending;

    (void)fd;
    (void)what;
    ex->pending = 0;

    if (todo & T_ABORT && ex->conn) {
        /* Runs from the event loop, so freeing is safe. The fini hook
         * schedules T_FINISH. */
        srv_flush_watch_end(ex);

#ifdef EVHTP_HAVE_REQUEST_RESET_STREAM
        /* HTTP/2: reset just this stream; the connection's other streams go
         * on. libevhtp frees the request once the reset is sent, and the
         * fini hook schedules T_FINISH as usual. */
        if (ex->h2 && ex->req) {
            if (ex->reset) {
                return;                 /* already on its way */
            }

            chan_detach(&ex->out);      /* nothing more for this stream */

            if (evhtp_request_reset_stream(ex->req, 0x2 /* INTERNAL_ERROR */) == 0) {
                ex->reset = 1;
                return;
            }
        }
#endif

        evhtp_connection_free(ex->conn);
        return;
    }

    if (todo & T_PRODUCE && !ex->failed && !ex->released &&
        ex->out.bev && !ex->out.ended && ex->h->vtbl->produce) {
        ex->h->vtbl->produce(ex->h, &ex->out);
    }

    if (todo & T_FINISH && ex->req == NULL) {
        if (!ex->released) {
            ex->released = 1;

            if (ex->failed && ex->h->vtbl->failed) {
                ex->h->vtbl->failed(ex->h, ex->reason);
            }

            if (ex->finished && !ex->failed) {
                ex->srv->stats.rq_completed++;
            }

            if (ex->finished && !ex->failed && ex->srv->listener.on_exchange_complete) {
                ex->srv->listener.on_exchange_complete(ex->srv->listener.arg, ex->remote,
                                                       ex->keep_alive);
            }

            ex->h->vtbl->release(ex->h);
        }

        ex->srv->stats.rq_active--;

        if (ex->prev) {
            ex->prev->next = ex->next;
        } else {
            ex->srv->exchanges = ex->next;
        }
        if (ex->next) {
            ex->next->prev = ex->prev;
        }

        /* Draining: an HTTP/2 connection does not close by itself after a
         * stream (HTTP/1.x ones do, keep-alive off); look again. */
        if (ex->h2 && ex->srv->draining && ex->srv->drain_ev && !ex->srv->closing) {
            event_active(ex->srv->drain_ev, EV_TIMEOUT, 0);
        }

        hc_context_free(ex->ctx);
        hc_processor_unref(ex->proc);
        event_free(ex->task);
        free(ex);
    }
}

/* Discard a bufferevent's unsent output. libevent freezes the start of a
 * socket bufferevent's output buffer (only the bufferevent may remove data),
 * so a plain evbuffer_drain() silently does nothing: unfreeze around it. */
static void
drop_output(struct bufferevent *bev)
{
    struct evbuffer *out = bufferevent_get_output(bev);

    evbuffer_unfreeze(out, 1);
    evbuffer_drain(out, evbuffer_get_length(out));
    evbuffer_freeze(out, 1);
}

static evhtp_res
srv_conn_fini(evhtp_connection_t *c, void *arg)
{
    srv_conn *n = arg;

    /* Freed because of an error (peer gone, I/O failure): queued output can
     * no longer be delivered, so drop it. Some libevhtp versions (the
     * ziebarthw fork) otherwise defer the free until that output flushes,
     * with no event callback to notice the write failing - the bufferevent
     * and its socket would never be freed. Normal closes still flush. */
    if ((c->flags & EVHTP_CONN_FLAG_ERROR) && c->bev) {
        drop_output(c->bev);
    }

    /* Its requests are freed after this hook: srv_conn_of() must not
     * return the record freed below. */
    if (c->hooks) {
        c->hooks->on_connection_fini_arg = NULL;
    }

    /* The bufferevent may outlive this (libevhtp flushing): stop counting
     * into a record that is about to go. */
    if (n->rx_cbe) {
        evbuffer_remove_cb_entry(n->rx, n->rx_cbe);
    }
    if (n->tx_cbe) {
        evbuffer_remove_cb_entry(n->tx, n->tx_cbe);
    }

    n->srv->stats.cx_active--;
    n->srv->stats.cx_destroy++;
    if (n->h2) {
        n->srv->stats.cx_http2_active--;
    } else {
        n->srv->stats.cx_http1_active--;
    }
    if (n->tls) {
        n->srv->stats.cx_ssl_active--;
    }

    if (n->prev) {
        n->prev->next = n->next;
    } else {
        n->srv->conns = n->next;
    }

    if (n->next) {
        n->next->prev = n->prev;
    }

    n->srv->n_conns--;
    free(n);

    return EVHTP_RES_OK;
}

static void
srv_count_rx(struct evbuffer *buf, const struct evbuffer_cb_info *info, void *arg)
{
    (void)buf;
    ((srv_conn *)arg)->srv->stats.cx_rx_bytes_total += info->n_added;
}

static void
srv_count_tx(struct evbuffer *buf, const struct evbuffer_cb_info *info, void *arg)
{
    (void)buf;
    ((srv_conn *)arg)->srv->stats.cx_tx_bytes_total += info->n_added;
}

/* The TLS handshake is done: ALPN has decided HTTP/2 or HTTP/1.1. */
static evhtp_res
srv_conn_event(evhtp_connection_t *c, short events, void *arg)
{
    srv_conn *n = arg;

    (void)c;    /* unused without HTTP/2 */

    if ((events & BEV_EVENT_CONNECTED) && conn_is_h2(c) && !n->h2) {
        n->h2 = 1;
        n->srv->stats.cx_http1_total--;
        n->srv->stats.cx_http1_active--;
        n->srv->stats.cx_http2_total++;
        n->srv->stats.cx_http2_active++;
    }

    return EVHTP_RES_OK;
}

hc_server_stats
hc_server_get_stats(const hc_server *s)
{
    hc_server_stats none = { 0 };

    return s ? s->stats : none;
}

static evhtp_res
srv_post_accept(evhtp_connection_t *c, void *arg)
{
    hc_server *s = arg;
    srv_conn  *n = calloc(1, sizeof(*n));

    if (n == NULL) {
        return EVHTP_RES_FATAL;     /* libevhtp drops the connection */
    }

    n->srv  = s;
    n->c    = c;
    n->next = s->conns;

    if (s->conns) {
        s->conns->prev = n;
    }

    s->conns = n;
    s->n_conns++;
    evhtp_connection_set_hook(c, evhtp_hook_on_connection_fini, (evhtp_hook)srv_conn_fini, n);
    evhtp_connection_set_hook(c, evhtp_hook_on_event, (evhtp_hook)srv_conn_event, n);

    /* Counted as HTTP/1 until a TLS handshake says h2 (srv_conn_event). */
    n->tls = c->ssl != NULL;
    s->stats.cx_total++;
    s->stats.cx_active++;
    s->stats.cx_http1_total++;
    s->stats.cx_http1_active++;
    if (n->tls) {
        s->stats.cx_ssl_total++;
        s->stats.cx_ssl_active++;
    }

    if (c->bev) {
        n->rx     = bufferevent_get_input(c->bev);
        n->tx     = bufferevent_get_output(c->bev);
        n->rx_cbe = evbuffer_add_cb(n->rx, srv_count_rx, n);
        n->tx_cbe = evbuffer_add_cb(n->tx, srv_count_tx, n);
    }

    return EVHTP_RES_OK;
}

hc_server *
hc_server_new(hc_reactor *r, hc_server_handler_factory factory, void *arg)
{
    hc_server *s = calloc(1, sizeof(*s));

    if (s == NULL) {
        return NULL;
    }

    s->r       = r;
#ifdef HC_HAVE_H2
    s->http2   = 1;             /* as libevhtp: h2 when TLS is on */
#endif
    s->factory = factory;
    s->arg     = arg;

    if ((s->htp = evhtp_new(r->base, NULL)) == NULL) {
        free(s);
        return NULL;
    }

    /* A catch-all callback (unlike the gencb) can carry hooks; on_headers
     * is where each exchange starts. */
    s->cb = evhtp_set_glob_cb(s->htp, "*", srv_on_complete, NULL);
    s->htp->flags &= ~EVHTP_FLAG_ENABLE_100_CONT;   /* see srv_on_headers */
    evhtp_set_post_accept_cb(s->htp, srv_post_accept, s);
    evhtp_callback_set_hook(s->cb, evhtp_hook_on_headers, (evhtp_hook)srv_on_headers, s);

    return s;
}

static char *
dup_cstr(const char *s)
{
    size_t n = strlen(s) + 1;
    char  *p = malloc(n);

    return p ? memcpy(p, s, n) : NULL;
}

#ifdef HC_HAVE_TLS
/* ALPN, HTTP/1.1 only: for builds without HTTP/2 support, and servers with
 * HTTP/2 switched off (hc_server_set_http2). libevhtp builds with HTTP/2
 * register an ALPN callback preferring h2; this replaces it. Without ALPN
 * in the build, it simply answers http/1.1 when a client offers it. */
static int
srv_alpn_http11(SSL *ssl, const unsigned char **out, unsigned char *outlen,
                const unsigned char *in, unsigned int inlen, void *arg)
{
    static const unsigned char http11[] = "\x08http/1.1";

    (void)ssl;
    (void)arg;

    if (SSL_select_next_proto((unsigned char **)out, outlen, http11, sizeof(http11) - 1,
                              in, inlen) == OPENSSL_NPN_NEGOTIATED) {
        return SSL_TLSEXT_ERR_OK;
    }

    return SSL_TLSEXT_ERR_NOACK;    /* client did not offer http/1.1: no ALPN */
}
#endif

int
hc_server_enable_tls(hc_server *s, const char *cert_file, const char *key_file)
{
#ifdef HC_HAVE_TLS
    SSL_CTX *ctx;

    if (s->tls.pemfile) {
        return -1;              /* already enabled */
    }

    s->tls.pemfile         = dup_cstr(cert_file);
    s->tls.privfile        = dup_cstr(key_file);
    s->tls.ssl_opts        = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1;
    s->tls.ssl_ctx_timeout = 60 * 60;
    s->tls.verify_peer     = SSL_VERIFY_NONE;
    s->tls.scache_type     = evhtp_ssl_scache_type_internal;

    if (!s->tls.pemfile || !s->tls.privfile || evhtp_ssl_init(s->htp, &s->tls) != 0) {
        return -1;
    }

    /* evhtp_ssl_init() ignores certificate/key loading errors: check. */
    ctx = s->htp->ssl_ctx;

    if (SSL_CTX_get0_certificate(ctx) == NULL || SSL_CTX_check_private_key(ctx) != 1) {
        return -1;
    }

#ifdef HC_HAVE_H2
    /* Built with HTTP/2: libevhtp's ALPN callback (installed by
     * evhtp_ssl_init(), preferring h2) stays, unless switched off. */
    if (!s->http2)
#endif
    SSL_CTX_set_alpn_select_cb(ctx, srv_alpn_http11, NULL);

    return 0;
#else
    (void)s;
    (void)cert_file;
    (void)key_file;

    return -1;
#endif
}

int
hc_server_set_http2(hc_server *s, int enabled)
{
    if (s == NULL) {
        return -1;
    }

#ifdef HC_HAVE_TLS
    if (s->tls.pemfile) {
        return -1;              /* decided when TLS was enabled */
    }
#endif

#ifdef HC_HAVE_H2
    s->http2 = enabled != 0;
    return 0;
#else
    return enabled ? -1 : 0;    /* this libevhtp has no HTTP/2 */
#endif
}

void
hc_server_set_processor(hc_server *s, hc_processor *p)
{
    if (s) {
        hc_processor *old = s->processor;

        s->processor = hc_processor_ref(p);
        hc_processor_unref(old);
    }
}

void
hc_server_set_timeouts(hc_server *s, int64_t read_ms, int64_t write_ms)
{
    ms_to_tv(read_ms, &s->read_tv);
    ms_to_tv(write_ms, &s->write_tv);
    /* zero means none: libevhtp only sets the non-zero ones on accept */
    evhtp_set_timeouts(s->htp, &s->read_tv, &s->write_tv);
}

void
hc_server_set_max_body(hc_server *s, int64_t max_bytes)
{
    s->max_body = max_bytes > 0 ? max_bytes : 0;
}

hc_context *
hc_server_exchange_context(hc_server_exchange *ex)
{
    if (ex && ex->ctx == NULL) {
        ex->ctx = hc_context_new(1, NULL, ex->remote);
    }

    return ex ? ex->ctx : NULL;
}

int
hc_server_listen(hc_server *s, const char *addr, uint16_t port)
{
    return hc_server_listen_opts(s, addr, port, NULL);
}

int
hc_server_listen_opts(hc_server *s, const char *addr, uint16_t port,
                      const hc_listen_options *opts)
{
    int backlog = opts && opts->backlog > 0 ? opts->backlog : 1024;

    if (opts && opts->reuse_port) {
        evhtp_enable_flag(s->htp, EVHTP_FLAG_ENABLE_REUSEPORT);
    }

    if (opts && opts->defer_accept) {
        evhtp_enable_flag(s->htp, EVHTP_FLAG_ENABLE_DEFER_ACCEPT);
    }

    return evhtp_bind_socket(s->htp, addr, port, backlog);
}

void
hc_server_unlisten(hc_server *s)
{
    if (s && s->htp->server) {
        evhtp_unbind_socket(s->htp);
        s->htp->server = NULL;
    }
}

int
hc_server_pause_accept(hc_server *s)
{
    return s && s->htp->server ? evconnlistener_disable(s->htp->server) : -1;
}

int
hc_server_resume_accept(hc_server *s)
{
    if (s == NULL) {
        return -1;
    }

    s->draining = 0;
    return s->htp->server ? evconnlistener_enable(s->htp->server) : -1;
}

/* From the loop, not from the caller's stack: the caller may be inside one of
 * these connections' callbacks. */
static void
srv_drain_cb(evutil_socket_t fd, short what, void *arg)
{
    hc_server *s = arg;
    srv_conn  *n, *next;

    (void)fd;
    (void)what;

    if (!s->draining) {
        return;                         /* resumed before we ran */
    }

    for (n = s->conns; n; n = next) {
        evhtp_connection_t *c    = n->c;
        hc_server_exchange *busy = NULL;

        next = n->next;                 /* freeing c unlinks n */

        /* Busy while one of our exchanges still has its request: libevhtp
         * frees the request once the response has gone out. (Not
         * c->request: some libevhtp versions keep an empty one on idle
         * keep-alive connections.) Drains are rare; the scan is fine. */
        for (hc_server_exchange *ex = s->exchanges; ex; ex = ex->next) {
            if (ex->conn == c && ex->req) {
                busy = ex;
                break;
            }
        }

        if (busy == NULL && conn_is_h2(c)) {
            /* HTTP/2 with no stream of ours left: close it. Its output may
             * still hold the last streams' frames, so it is not dropped
             * (libevhtp flushes before freeing). */
            evhtp_connection_free(c);
        } else if (busy == NULL) {
            /* Idle: between keep-alive requests, or a request whose head
             * hasn't fully arrived - like any server closing an idle
             * keep-alive connection, the client retries. */
            drop_output(evhtp_connection_get_bev(c));
            evhtp_connection_free(c);
        } else if (busy->h2) {
            /* Streams under way: closed when the last one is done (see
             * srv_task_cb). */
        } else if (busy->finished) {
            /* Finished before the drain, still flushing: don't let
             * libevhtp keep it alive for another request. */
            busy->req->flags &= ~EVHTP_REQ_FLAG_KEEPALIVE;
        }
        /* Otherwise the exchange is under way: srv_try_finalize() closes it. */
    }
}

void
hc_server_drain(hc_server *s)
{
    if (s == NULL) {
        return;
    }

    s->draining = 1;
    hc_server_pause_accept(s);

    if (s->drain_ev == NULL) {
        s->drain_ev = event_new(s->r->base, -1, 0, srv_drain_cb, s);
    }

    if (s->drain_ev) {
        event_active(s->drain_ev, EV_TIMEOUT, 0);
    }
}

size_t
hc_server_connection_count(const hc_server *s)
{
    return s ? s->n_conns : 0;
}

void
hc_server_free(hc_server *s)
{
    if (s) {
        if (s->drain_ev) {
            event_free(s->drain_ev);
        }

        evhtp_unbind_socket(s->htp);
        s->closing = 1;

        while (s->conns) {
            evhtp_connection_t *c = s->conns->c;

            /* Hard close: drop unsent output. Some libevhtp versions (the
             * ziebarthw fork) defer freeing a connection until its output
             * has flushed, which needs a loop that will not run again. */
            drop_output(evhtp_connection_get_bev(c));
            evhtp_connection_free(c);               /* fini unlinks it */
        }

        evhtp_free(s->htp);
#ifdef HC_HAVE_TLS
        free(s->tls.pemfile);
        free(s->tls.privfile);
#endif
        for (size_t i = 0; i < s->n_routes; i++) {
            free(s->routes[i].pattern);
        }

        free(s->routes);
        hc_processor_unref(s->processor);
        free(s);
    }
}

/* ======================================================================== */
/* Client side                                                              */
/* ======================================================================== */

/*
 * Object model:
 *
 *   hc_pconn            one upstream TCP connection (evhtp_connection_t)
 *                       plus the state pooling needs. Lives as long as the
 *                       pool entry that owns it (pooled) or as long as the
 *                       one exchange that uses it (hc_client_execute).
 *   hc_client_exchange  one request/response on a pconn; owns the
 *                       evhtp_request_t for that exchange.
 *   hc_endpoint         a leased pconn as seen by the application
 *                       (Java: AsyncClientEndpoint).
 *   hc_connect_op       an in-progress hc_requester_connect(): pool lease,
 *                       then TCP connect if the entry had no connection.
 *
 * libevhtp hooks on the connection (on_event, on_connection_fini) always
 * point at the pconn, which forwards to whatever exchange is running. Hooks
 * on the request point at the exchange. While a pconn sits idle in the pool
 * the engine swaps the bufferevent callbacks for its own (libevhtp's
 * parser must not see bytes with no request attached): any input or EOF
 * then marks the connection dead and evicts it from the pool.
 */

typedef struct hc_pconn hc_pconn;

struct hc_requester {
    hc_reactor *r;
    hc_pool    *pool;
    int64_t     default_keep_alive_ms;
    hc_timer   *housekeeping;
#ifdef HC_HAVE_TLS
    SSL_CTX    *tls_ctx;
    int         tls_no_verify;
#endif
    /* Everything hc_requester_free() must finish before the pool goes:
     * its tasks and timers would otherwise run later on the loop. */
    struct hc_connect_op *ops;
    struct hc_endpoint   *eps;
    int                   closing;  /* hc_requester_free() in progress */
    hc_processor         *processor; /* interceptors (own reference) */
    int64_t               read_timeout_ms;  /* exchange defaults (0: none) */
    int64_t               write_timeout_ms;
};

struct hc_connect_op {
    hc_requester  *rq;
    hc_connect_cb  cb;
    void          *arg;
    hc_host        target;
    int64_t        deadline;     /* 0: none */
    hc_pool_lease_req *lease;        /* while waiting for a pool slot */
    hc_pool_entry *entry;
    hc_pconn      *pc;           /* while connecting */
    hc_timer      *timer;        /* connect timeout */
    hc_connect_op *prev, *next;  /* rq->ops */
};

struct hc_endpoint {
    hc_requester       *rq;
    hc_pool_entry      *entry;
    hc_pconn           *pc;
    hc_client_exchange *cx;      /* exchange in flight, if any */
    int                 release_pending;
    int                 reuse;
    hc_timer           *task;
    hc_endpoint        *prev, *next;    /* rq->eps */
};

enum {
    P_CONNECTED      = 1u << 0,
    P_CONNECT_FAILED = 1u << 1,
    P_LOST           = 1u << 2,
};

struct hc_pconn {
    hc_reactor          *r;
    hc_requester        *rq;          /* NULL: unpooled                    */
    hc_host              target;
    struct evdns_getaddrinfo_request *dns_req;  /* https: resolving        */
    int                  resolving;
    char                 err[512];    /* why connecting failed, if known   */
    hc_pool_entry       *entry;
    evhtp_connection_t  *conn;        /* NULL once libevhtp freed it       */
    hc_client_exchange  *cx;
    hc_connect_op       *op;          /* connecting for this op            */
    int                  dead;        /* closed, or idle and broken        */
    int                  closing;     /* pc_destroy() in progress          */
    int                  idle;        /* parked in the pool                */
    int                  reusable;    /* verdict of the last exchange      */
    int64_t              keep_alive_ms;
    bufferevent_data_cb  saved_read;
    bufferevent_data_cb  saved_write;
    bufferevent_event_cb saved_event;
    void                *saved_arg;
    struct event        *task;
    unsigned             pending;
    void                *attachment;   /* hc_endpoint_set_attachment()  */
    void               (*attachment_free)(void *);
};

struct hc_request_channel {
    hc_client_exchange *cx;
};

struct hc_client_exchange {
    hc_reactor         *r;
    hc_client_handler  *h;
    hc_pconn           *pc;            /* NULL if its connection was destroyed */
    hc_endpoint        *ep;            /* NULL: unpooled exchange            */
    evhtp_request_t    *req;           /* NULL once freed                    */
    int                 req_attached;  /* conn owns req after make_request   */
    hc_request_channel  rc;
    hc_data_channel     out;
    struct event       *task;
    unsigned            pending;
    int                 head_sent;
    int                 resp_head;
    int                 close_delimited;
    int                 raw_mode;      /* reading a read-until-close body    */
    int                 interposed;    /* our read cb sits before libevhtp's */
    bufferevent_data_cb  ip_read;      /* libevhtp's callbacks, while so     */
    bufferevent_data_cb  ip_write;
    bufferevent_event_cb ip_event;
    void                *ip_arg;
    int                 resp_keepalive;
    int64_t             keep_alive_ms;
    int                 done;          /* response complete                  */
    int                 end_delivered;
    int                 failed;
    int                 released;
    int                 input_suspended;
    const char         *reason;
    int64_t             read_ms;       /* hc_client_set_timeouts(); -1: default */
    int64_t             write_ms;
    int                 req_sent;      /* request complete: read timeout runs */
    int                 timeouts_on;   /* set on the connection             */
    hc_context         *ctx;           /* interceptors' and handler's       */
    const hc_processor *proc;          /* the requester's (borrowed), or NULL */
};

static void cli_task_cb(evutil_socket_t fd, short what, void *arg);
static void ep_task_cb(void *arg);
static void pc_task_cb(evutil_socket_t fd, short what, void *arg);
static void cli_on_complete(evhtp_request_t *req, void *arg);
static evhtp_res cli_on_headers(evhtp_request_t *req, evhtp_headers_t *hdrs, void *arg);
static evhtp_res cli_on_read(evhtp_request_t *req, struct evbuffer *buf, void *arg);
static evhtp_res cli_on_req_fini(evhtp_request_t *req, void *arg);
static evhtp_res cli_conn_event(hc_client_exchange *cx, short events);
static void cli_conn_gone(hc_client_exchange *cx);

static evhtp_connection_t *
cx_conn(const hc_client_exchange *cx)
{
    return cx->pc ? cx->pc->conn : NULL;
}

static void
cli_schedule_produce(void *owner)
{
    hc_client_exchange *cx = owner;

    schedule(cx->task, &cx->pending, T_PRODUCE);
}

static void cx_apply_timeouts(hc_client_exchange *cx);

static void
cli_on_request_end(void *owner)
{
    hc_client_exchange *cx = owner;

    cx->req_sent = 1;           /* the response may still flow */
    cx_apply_timeouts(cx);
}

static void
cli_fail(hc_client_exchange *cx, const char *reason)
{
    if (!cx->done && !cx->failed) {
        cx->failed = 1;
        cx->reason = reason;
    }
}

/* Method name -> libevhtp enum, by enumerator name: the numeric order
 * differs between libevhtp versions (upstream puts htp_method_UNKNOWN last,
 * the ziebarthw fork first), so never iterate over the enum. */
static int
method_from_string(const char *m)
{
    static const struct {
        const char *name;
        htp_method  method;
    } methods[] = {
        { "GET", htp_method_GET },         { "HEAD", htp_method_HEAD },
        { "POST", htp_method_POST },       { "PUT", htp_method_PUT },
        { "DELETE", htp_method_DELETE },   { "OPTIONS", htp_method_OPTIONS },
        { "PATCH", htp_method_PATCH },     { "TRACE", htp_method_TRACE },
        { "CONNECT", htp_method_CONNECT }, { "MKCOL", htp_method_MKCOL },
        { "COPY", htp_method_COPY },       { "MOVE", htp_method_MOVE },
        { "PROPFIND", htp_method_PROPFIND }, { "PROPPATCH", htp_method_PROPPATCH },
        { "LOCK", htp_method_LOCK },       { "UNLOCK", htp_method_UNLOCK },
    };

    for (size_t i = 0; i < sizeof(methods) / sizeof(methods[0]); i++) {
        if (strcmp(methods[i].name, m) == 0) {
            return (int)methods[i].method;
        }
    }

    return -1;
}

/* "Keep-Alive: timeout=5, max=100" -> 5000; -1 if absent. */
static int64_t
keep_alive_timeout_ms(const char *v)
{
    for (const char *p = v; p && *p; p++) {
        if (strncasecmp(p, "timeout", 7) == 0) {
            const char *q = p + 7;

            while (*q == ' ') {
                q++;
            }

            if (*q == '=') {
                long long s = strtoll(q + 1, NULL, 10);

                return s >= 0 ? (int64_t)s * 1000 : -1;
            }
        }
    }

    return -1;
}

/* --- pooled connection ------------------------------------------------------ */

/* Why did connecting fail? Called from the event hook, while the
 * connection (and its SSL object and errno) are still current. */
static void
pc_note_connect_error(hc_pconn *pc, evhtp_connection_t *c, short events)
{
    int sockerr = EVUTIL_SOCKET_ERROR();

    if (pc->err[0]) {
        return;
    }

#ifdef HC_HAVE_TLS
    if (c->ssl) {
        long          v = SSL_get_verify_result(c->ssl);
        unsigned long e = bufferevent_get_openssl_error(evhtp_connection_get_bev(c));

        if (v != X509_V_OK) {
            snprintf(pc->err, sizeof(pc->err), "TLS certificate verification failed: %s",
                     X509_verify_cert_error_string(v));
            return;
        }

        if (e != 0) {
            const char *why = ERR_reason_error_string(e);

            snprintf(pc->err, sizeof(pc->err), "TLS handshake failed: %s",
                     why ? why : "unknown error");
            return;
        }
    }
#endif

    {
        int dnserr = bufferevent_socket_get_dns_error(evhtp_connection_get_bev(c));

        if (dnserr) {
            snprintf(pc->err, sizeof(pc->err), "cannot resolve %s: %s", pc->target.name,
                     evutil_gai_strerror(dnserr));
            return;
        }
    }

    if (events & BEV_EVENT_TIMEOUT) {
        snprintf(pc->err, sizeof(pc->err), "cannot connect to origin: timeout");
    } else if ((events & BEV_EVENT_ERROR) && sockerr) {
        snprintf(pc->err, sizeof(pc->err), "cannot connect to origin: %s",
                 evutil_socket_error_to_string(sockerr));
    } else {
        snprintf(pc->err, sizeof(pc->err), "cannot connect to origin: connection closed");
    }
}

static evhtp_res
pc_on_event(evhtp_connection_t *c, short events, void *arg)
{
    hc_pconn *pc = arg;

    if (events & BEV_EVENT_CONNECTED) {
        if (pc->op) {
            schedule(pc->task, &pc->pending, P_CONNECTED);
        }

        return EVHTP_RES_OK;
    }

    if (pc->op && !pc->cx) {
        pc_note_connect_error(pc, c, events);
    }

    return pc->cx ? cli_conn_event(pc->cx, events) : EVHTP_RES_OK;
}

/* libevhtp is freeing the connection: connect/I-O/parse error, or us. */
static evhtp_res
pc_on_conn_fini(evhtp_connection_t *c, void *arg)
{
    hc_pconn *pc = arg;

    /* Unsent request data is useless once the connection goes, and an
     * origin that stopped reading (write timeout) would never take it:
     * some libevhtp versions (the ziebarthw fork) keep the bufferevent and
     * its socket until the output is flushed, so they would never be
     * freed. Drop it. */
    if (c->bev) {
        drop_output(c->bev);
    }

    pc->conn = NULL;
    pc->dead = 1;

    if (pc->closing) {
        return EVHTP_RES_OK;
    }

    if (pc->cx) {
        cli_conn_gone(pc->cx);
    }

    if (pc->op) {
        schedule(pc->task, &pc->pending, P_CONNECT_FAILED);
    }

    if (pc->idle) {
        schedule(pc->task, &pc->pending, P_LOST);
    }

    return EVHTP_RES_OK;
}

static void
pc_hook(hc_pconn *pc)
{
    evhtp_connection_set_hook(pc->conn, evhtp_hook_on_event, (evhtp_hook)pc_on_event, pc);
    evhtp_connection_set_hook(pc->conn, evhtp_hook_on_connection_fini,
                              (evhtp_hook)pc_on_conn_fini, pc);
}

/* Connecting failed before libevhtp had a connection to report it on. */
static void
pc_fail_early(hc_pconn *pc, const char *why)
{
    snprintf(pc->err, sizeof(pc->err), "%s", why);
    pc->dead = 1;
    schedule(pc->task, &pc->pending, P_CONNECT_FAILED);
}

#ifdef HC_HAVE_TLS
static int
is_ip_literal(const char *name)
{
    unsigned char buf[16];

    return evutil_inet_pton(AF_INET, name, buf) == 1 ||
           evutil_inet_pton(AF_INET6, name, buf) == 1;
}

/* https: evhtp_connection_ssl_new() only takes a numeric IPv4 address, so
 * resolve first. Then configure SNI and peer verification on the SSL
 * object; the handshake only starts once the TCP connect completes, on a
 * later loop iteration, so this is still in time. */
static void
pc_resolved(int err, struct evutil_addrinfo *res, void *arg)
{
    hc_pconn          *pc = arg;
    char               ip[INET_ADDRSTRLEN];
    char               why[512];
    SSL               *ssl;
    const char        *tls_name;

    if (err == EVUTIL_EAI_CANCEL) {         /* pc is being destroyed */
        if (res) {
            evutil_freeaddrinfo(res);
        }

        return;
    }

    pc->resolving = 0;
    pc->dns_req   = NULL;

    if (err || res == NULL) {
        snprintf(why, sizeof(why), "cannot resolve %s: %s", pc->target.name,
                 evutil_gai_strerror(err));
        pc_fail_early(pc, why);
        return;
    }

    evutil_inet_ntop(AF_INET, &((struct sockaddr_in *)(void *)res->ai_addr)->sin_addr,
                     ip, sizeof(ip));
    evutil_freeaddrinfo(res);

    if ((pc->conn = evhtp_connection_ssl_new(pc->r->base, ip, pc->target.port,
                                             pc->rq->tls_ctx)) == NULL) {
        pc_fail_early(pc, "cannot create TLS connection");
        return;
    }

    ssl = pc->conn->ssl;

    /* Offer only http/1.1: libevhtp builds with HTTP/2 offer h2 first, and
     * an origin that picks it would move the connection to a protocol this
     * engine does not drive (see srv_alpn_http11). */
    SSL_set_alpn_protos(ssl, (const unsigned char *)"\x08http/1.1", 9);

    /* The name the site goes by: the target's tls_name (connecting to an
     * address, presenting a host name), else the name connected to. */
    tls_name = hc_host_tls_name(&pc->target);

    if (!is_ip_literal(tls_name)) {
        SSL_set_tlsext_host_name(ssl, tls_name);            /* SNI */
    }

    if (!pc->rq->tls_no_verify) {
        if (is_ip_literal(tls_name)) {
            X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), tls_name);
        } else {
            SSL_set1_host(ssl, tls_name);
        }
    }

    pc_hook(pc);
}
#endif

static hc_pconn *
pc_new(hc_reactor *r, hc_requester *rq, const hc_host *target)
{
    hc_pconn *pc = calloc(1, sizeof(*pc));

    if (pc == NULL) {
        return NULL;
    }

    pc->r        = r;
    pc->rq       = rq;
    pc->target   = *target;
    pc->reusable = 1;               /* a fresh connection is reusable */
    pc->keep_alive_ms = rq ? rq->default_keep_alive_ms : 0;

    if ((pc->task = event_new(r->base, -1, 0, pc_task_cb, pc)) == NULL) {
        free(pc);
        return NULL;
    }

    if (!hc_host_is_tls(target)) {
        if ((pc->conn = evhtp_connection_new_dns(r->base, r->dns, target->name,
                                                 target->port)) == NULL) {
            event_free(pc->task);
            free(pc);
            return NULL;
        }

        pc_hook(pc);
        return pc;
    }

#ifdef HC_HAVE_TLS
    if (rq == NULL || rq->tls_ctx == NULL) {
        pc_fail_early(pc, "https needs a requester (hc_requester_connect)");
    } else {
        struct evutil_addrinfo            hints;
        struct evdns_getaddrinfo_request *req;

        memset(&hints, 0, sizeof(hints));
        hints.ai_family   = AF_INET;        /* evhtp_connection_ssl_new: IPv4 only */
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;

        pc->resolving = 1;
        req           = evdns_getaddrinfo(r->dns, target->name, NULL, &hints, pc_resolved, pc);

        if (pc->resolving) {                /* not answered synchronously */
            pc->dns_req = req;
        }
    }
#else
    pc_fail_early(pc, "https is not supported (libevhtp built without TLS)");
#endif

    return pc;
}

/* Close (if still open) and free. The pool's hc_pool_conn_ops.close. */
static void
pc_destroy(hc_pconn *pc)
{
    if (pc->cx) {               /* only at shutdown with exchanges in flight */
        pc->cx->pc = NULL;
        cli_fail(pc->cx, "connection pool shut down");
        schedule(pc->cx->task, &pc->cx->pending, T_FINISH);
    }

    if (pc->dns_req) {
        evdns_getaddrinfo_cancel(pc->dns_req);  /* callback sees CANCEL */
        pc->dns_req = NULL;
    }

    if (pc->conn) {
        pc->closing = 1;
        evhtp_connection_free(pc->conn);
    }

    if (pc->attachment_free) {
        pc->attachment_free(pc->attachment);
    }

    event_free(pc->task);
    free(pc);
}

static void
pool_close_conn(void *ctx, void *conn)
{
    (void)ctx;
    pc_destroy(conn);
}

/* Idle in the pool: nothing may arrive. Anything that does - data, EOF,
 * an error - means the connection cannot be reused. */
static void
pc_idle_broken(hc_pconn *pc)
{
    if (pc->dead) {
        return;
    }

    pc->dead = 1;
    bufferevent_disable(evhtp_connection_get_bev(pc->conn), EV_READ | EV_WRITE);
    schedule(pc->task, &pc->pending, P_LOST);
}

static void
pc_idle_read(struct bufferevent *bev, void *arg)
{
    (void)bev;
    pc_idle_broken(arg);
}

static void
pc_idle_event(struct bufferevent *bev, short events, void *arg)
{
    (void)bev;
    (void)events;
    pc_idle_broken(arg);
}

static void
pc_set_idle(hc_pconn *pc)
{
    struct bufferevent *bev = evhtp_connection_get_bev(pc->conn);

    if (pc->idle) {
        return;                 /* already parked: keep libevhtp's saved cbs */
    }

    bufferevent_getcb(bev, &pc->saved_read, &pc->saved_write, &pc->saved_event,
                      &pc->saved_arg);
    bufferevent_setcb(bev, pc_idle_read, NULL, pc_idle_event, pc);
    bufferevent_enable(bev, EV_READ);   /* notice the peer closing */
    pc->idle = 1;
}

static void
pc_set_active(hc_pconn *pc)
{
    if (!pc->idle) {
        return;
    }

    bufferevent_setcb(evhtp_connection_get_bev(pc->conn), pc->saved_read,
                      pc->saved_write, pc->saved_event, pc->saved_arg);
    pc->idle = 0;
}

/* --- exchanges ---------------------------------------------------------------- */

static hc_client_exchange *
cx_new(hc_reactor *r, hc_client_handler *h)
{
    hc_client_exchange *cx = calloc(1, sizeof(*cx));

    if (cx == NULL) {
        return NULL;
    }

    cx->r        = r;
    cx->h        = h;
    cx->rc.cx    = cx;
    cx->read_ms  = -1;
    cx->write_ms = -1;
    cx->task  = event_new(r->base, -1, 0, cli_task_cb, cx);
    cx->req   = evhtp_request_new(cli_on_complete, cx);

    if (!cx->task || !cx->req) {
        if (cx->req) {
            evhtp_request_free(cx->req);
        }

        if (cx->task) {
            event_free(cx->task);
        }

        free(cx);
        return NULL;
    }

    evhtp_request_set_hook(cx->req, evhtp_hook_on_headers, (evhtp_hook)cli_on_headers, cx);
    evhtp_request_set_hook(cx->req, evhtp_hook_on_read, (evhtp_hook)cli_on_read, cx);
    evhtp_request_set_hook(cx->req, evhtp_hook_on_request_fini, (evhtp_hook)cli_on_req_fini, cx);

    return cx;
}

/* --- interim (1xx) responses -------------------------------------------- *
 * htparser cannot parse them: "100 Continue" without header lines makes it
 * stall one byte per call (libevhtp then tries to resume a connection that
 * is not paused, a no-op, and the exchange hangs), and a 1xx with header
 * lines is rejected as an invalid request line. So from the moment the
 * request is sent until the final response starts, the engine's read
 * callback sits in front of libevhtp's: complete 1xx messages are parsed
 * here and passed to consume_information(); as soon as the input begins
 * with a final status line, libevhtp's callbacks are restored and it takes
 * over. 101 counts as final (upgrades are not supported).
 */
#define HC_MAX_INTERIM_HEAD (64 * 1024)

static void
cli_interpose_end(hc_client_exchange *cx, struct bufferevent *bev)
{
    if (cx->interposed) {
        cx->interposed = 0;
        bufferevent_setcb(bev, cx->ip_read, cx->ip_write, cx->ip_event, cx->ip_arg);
    }
}

/* Parse "HTTP/1.x SSS reason\r\n(Name: value\r\n)*\r\n" (len bytes, the
 * final empty line included) and deliver it. */
static void
cli_deliver_interim(hc_client_exchange *cx, const char *msg, size_t len)
{
    hc_headers      *h = hc_headers_new();
    hc_response_head head;
    const char      *line = msg, *end = msg + len;

    if (h == NULL) {
        return;
    }

    head.version = msg[7] == '0' ? 10 : 11;
    head.status  = (msg[9] - '0') * 100 + (msg[10] - '0') * 10 + (msg[11] - '0');
    head.headers = h;

    /* skip the status line, then one header per line until the blank one */
    for (line = memchr(line, '\n', (size_t)(end - line)); line && ++line < end;) {
        const char *eol = memchr(line, '\n', (size_t)(end - line));
        const char *colon, *v, *ve;
        char        name[256], value[4096];

        if (eol == NULL || eol == line || (eol == line + 1 && *line == '\r')) {
            break;
        }

        ve    = eol > line && eol[-1] == '\r' ? eol - 1 : eol;
        colon = memchr(line, ':', (size_t)(ve - line));

        if (colon && (size_t)(colon - line) < sizeof(name)) {
            for (v = colon + 1; v < ve && (*v == ' ' || *v == '\t'); v++) {
            }

            while (ve > v && (ve[-1] == ' ' || ve[-1] == '\t')) {
                ve--;
            }

            if ((size_t)(ve - v) < sizeof(value)) {
                memcpy(name, line, (size_t)(colon - line));
                name[colon - line] = '\0';
                memcpy(value, v, (size_t)(ve - v));
                value[ve - v] = '\0';
                hc_headers_add(h, name, value);
            }
        }

        line = eol;
    }

    if (!cx->failed && !cx->released && cx->h->vtbl->consume_information) {
        cx->h->vtbl->consume_information(cx->h, &head);
    }

    hc_headers_free(h);
}

static void
cli_pre_read(struct bufferevent *bev, void *arg)
{
    hc_client_exchange *cx = arg;
    struct evbuffer    *in = bufferevent_get_input(bev);
    char                start[12];

    for (;;) {
        size_t                 len = evbuffer_get_length(in);
        struct evbuffer_ptr    eom;
        int                    status;

        if (len < sizeof(start)) {
            if (len >= 5 && evbuffer_copyout(in, start, 5) == 5 && memcmp(start, "HTTP/", 5) != 0) {
                break;                      /* not a status line: libevhtp's problem */
            }

            return;                         /* wait for the status code */
        }

        evbuffer_copyout(in, start, sizeof(start));

        if (memcmp(start, "HTTP/1.", 7) != 0 || start[8] != ' ' ||
            start[9] != '1' || (start[10] == '0' && start[11] == '1')) {
            break;                          /* final response (or garbage) */
        }

        eom = evbuffer_search(in, "\r\n\r\n", 4, NULL);

        if (eom.pos < 0) {
            if (len > HC_MAX_INTERIM_HEAD) {
                cli_fail(cx, "interim response head too large");
                schedule(cx->task, &cx->pending, T_ABORT);
                return;
            }

            return;                         /* incomplete 1xx: wait */
        }

        status = (int)eom.pos + 4;
        cli_deliver_interim(cx, (const char *)evbuffer_pullup(in, status), (size_t)status);
        evbuffer_drain(in, (size_t)status);
    }

    /* The final response begins: from here on it is libevhtp's. */
    cli_interpose_end(cx, bev);

    if (evbuffer_get_length(in) > 0) {
        cx->ip_read(bev, cx->ip_arg);
    }
}

static void
cli_pre_write(struct bufferevent *bev, void *arg)
{
    hc_client_exchange *cx = arg;

    if (cx->ip_write) {
        cx->ip_write(bev, cx->ip_arg);
    }
}

static void
cli_pre_event(struct bufferevent *bev, short events, void *arg)
{
    hc_client_exchange *cx = arg;

    cx->ip_event(bev, events, cx->ip_arg);      /* may free the connection */
}

static void
cli_interpose_begin(hc_client_exchange *cx, struct bufferevent *bev)
{
    bufferevent_getcb(bev, &cx->ip_read, &cx->ip_write, &cx->ip_event, &cx->ip_arg);
    bufferevent_setcb(bev, cli_pre_read, cli_pre_write, cli_pre_event, cx);
    cx->interposed = 1;
}

hc_data_channel *
hc_request_send(hc_request_channel *rc, const hc_request_head *head,
                const hc_entity *entity)
{
    hc_client_exchange *cx   = rc->cx;
    evhtp_connection_t *conn = cx_conn(cx);
    int                 m    = method_from_string(head->method);
    hc_headers         *own_headers = NULL;
    char                num[32];

    if (cx->head_sent || conn == NULL || cx->req == NULL || cx->failed) {
        return NULL;
    }

    if (m < 0) {
        cli_fail(cx, "unsupported request method");
        schedule(cx->task, &cx->pending, T_ABORT);
        return NULL;
    }

    cx->head_sent = 1;

    /* Interceptors work on a copy of the headers: the caller's are const. */
    if (cx->proc) {
        hc_http_request r;

        if ((own_headers = hc_headers_copy(head->headers)) == NULL) {
            cli_fail(cx, "out of memory");
            schedule(cx->task, &cx->pending, T_ABORT);
            return NULL;
        }

        r.method    = head->method;
        r.target    = head->target;
        r.authority = head->authority;
        r.version   = head->version;
        r.headers   = own_headers;
        hc_context_set_protocol(cx->ctx, head->version, cx->pc && hc_host_is_tls(&cx->pc->target));

        if (hc_processor_process_request(cx->proc, &r, entity, cx->ctx) != 0) {
            hc_headers_free(own_headers);
            cli_fail(cx, "request refused by interceptor");
            schedule(cx->task, &cx->pending, T_ABORT);
            return NULL;
        }
    }

    if (head->authority) {
        evhtp_headers_add_header(cx->req->headers_out,
            evhtp_header_new("Host", head->authority, 0, 1));
    }

    headers_to_evhtp(own_headers ? own_headers : head->headers, cx->req->headers_out);
    hc_headers_free(own_headers);

    if (entity && entity->content_length >= 0) {
        snprintf(num, sizeof(num), "%lld", (long long)entity->content_length);
        evhtp_headers_add_header(cx->req->headers_out,
            evhtp_header_new("Content-Length", num, 0, 1));
    } else if (entity) {
        evhtp_headers_add_header(cx->req->headers_out,
            evhtp_header_new("Transfer-Encoding", "chunked", 0, 0));
    }

    if (head->version == 10) {
        cx->req->proto = EVHTP_PROTO_10;
    }

    evhtp_make_request(conn, cx->req, (htp_method)m, head->target);
    cx->req_attached = 1;
    cli_interpose_begin(cx, evhtp_connection_get_bev(conn));

    if (entity == NULL) {
        cx->req_sent = 1;
        cx_apply_timeouts(cx);
        return NULL;
    }

    chan_attach(&cx->out, evhtp_connection_get_bev(conn),
                entity->content_length < 0, entity->content_length, cx,
                cli_schedule_produce, cli_on_request_end);

    if (entity->content_length == 0) {
        cx->out.ended = 1;
        cx->req_sent  = 1;
    }

    cx_apply_timeouts(cx);

    schedule(cx->task, &cx->pending, T_PRODUCE);

    return &cx->out;
}

void
hc_client_suspend_input(hc_client_exchange *cx)
{
    if (cx_conn(cx) && !cx->done && !cx->input_suspended) {
        cx->input_suspended = 1;
        bufferevent_disable(evhtp_connection_get_bev(cx_conn(cx)), EV_READ);
    }
}

void
hc_client_resume_input(hc_client_exchange *cx)
{
    if (cx_conn(cx) && !cx->done && cx->input_suspended) {
        cx->input_suspended = 0;
        bufferevent_enable(evhtp_connection_get_bev(cx_conn(cx)), EV_READ);
    }
}

hc_context *
hc_client_exchange_context(hc_client_exchange *cx)
{
    if (cx && cx->ctx == NULL) {
        cx->ctx = hc_context_new(0, cx->pc ? &cx->pc->target : NULL, NULL);
    }

    return cx ? cx->ctx : NULL;
}

/* The exchange's timeouts onto its connection: write from the request
 * head on, read once the request is complete (see hc_client_set_timeouts). */
static void
cx_apply_timeouts(hc_client_exchange *cx)
{
    evhtp_connection_t *c = cx_conn(cx);
    int64_t             r = cx->read_ms, w = cx->write_ms;
    struct timeval      rt, wt;

    if (c == NULL || !cx->head_sent || cx->done || cx->failed || cx->released) {
        return;
    }

    if (r < 0) {
        r = cx->ep ? cx->ep->rq->read_timeout_ms : 0;
    }

    if (w < 0) {
        w = cx->ep ? cx->ep->rq->write_timeout_ms : 0;
    }

    if (!cx->req_sent) {
        r = 0;
    }

    if (r <= 0 && w <= 0 && !cx->timeouts_on) {
        return;
    }

    ms_to_tv(r, &rt);
    ms_to_tv(w, &wt);
    bufferevent_set_timeouts(evhtp_connection_get_bev(c), r > 0 ? &rt : NULL,
                             w > 0 ? &wt : NULL);
    cx->timeouts_on = r > 0 || w > 0;
}

void
hc_client_set_timeouts(hc_client_exchange *cx, int64_t read_ms, int64_t write_ms)
{
    if (cx == NULL || cx->released) {
        return;
    }

    cx->read_ms  = read_ms < 0 ? -1 : read_ms;
    cx->write_ms = write_ms < 0 ? -1 : write_ms;
    cx_apply_timeouts(cx);
}

void
hc_client_cancel(hc_client_exchange *cx)
{
    if (cx == NULL || cx->released) {
        return;
    }

    cli_fail(cx, "cancelled");
    schedule(cx->task, &cx->pending, T_ABORT);
}

/* --- libevhtp hooks (client request) ------------------------------------------ */

static evhtp_res
cli_on_headers(evhtp_request_t *req, evhtp_headers_t *hdrs, void *arg)
{
    hc_client_exchange *cx = arg;
    evhtp_connection_t *c  = evhtp_request_get_connection(req);
    hc_headers         *h;
    hc_response_head    head;
    hc_entity           e;
    const hc_entity    *entity;
    const char         *v;
    int                 status = (int)evhtp_request_status(req);
    int                 body_possible;

    if (cx->resp_head || cx->done) {
        return EVHTP_RES_ERROR; /* unsolicited second response: drop conn */
    }

    if ((h = headers_from_evhtp(hdrs)) == NULL) {
        return EVHTP_RES_FATAL;
    }

    body_possible = !(status < 200 || status == 204 || status == 304);
    entity        = entity_from_headers(h, &e, body_possible, 1, &cx->close_delimited);

    /* A HEAD response describes the entity but carries no body bytes.
     * Report the entity so a proxy can forward the Content-Length, but
     * never wait for a read-until-close body. */
    if (req->method == htp_method_HEAD) {
        cx->close_delimited = 0;
    }

    head.status   = status;
    head.version  = htparser_get_minor(c->parser) == 0 ? 10 : 11;
    head.headers  = h;
    cx->resp_head = 1;

    /* Persistence (RFC 9112 9.3) and keep-alive duration (httpcore5's
     * DefaultConnectionKeepAliveStrategy). */
    v = hc_headers_get(h, "connection");
    cx->resp_keepalive = head.version == 11 ? !(v && has_token(v, "close"))
                                            : (v && has_token(v, "keep-alive"));

    if (status == 101) {
        cx->resp_keepalive = 0;     /* the connection now speaks something else */
    }
    v = hc_headers_get(h, "keep-alive");
    cx->keep_alive_ms = v ? keep_alive_timeout_ms(v) : -1;

    if (cx->keep_alive_ms < 0 && cx->pc && cx->pc->rq) {
        cx->keep_alive_ms = cx->pc->rq->default_keep_alive_ms;
    }

    /* Interceptors: after the engine has read framing and persistence from
     * the headers as received, before the handler sees them. */
    if (cx->proc && !cx->failed && !cx->released) {
        hc_http_response r = { head.status, head.version, h };

        hc_context_set_protocol(cx->ctx, head.version, cx->pc && hc_host_is_tls(&cx->pc->target));

        if (hc_processor_process_response(cx->proc, &r, entity, cx->ctx) != 0) {
            cli_fail(cx, "response refused by interceptor");
            cx->resp_keepalive = 0;
            schedule(cx->task, &cx->pending, T_ABORT);
            hc_headers_free(h);
            return EVHTP_RES_OK;
        }

        head.status = r.status;
    }

    if (!cx->failed && !cx->released) {
        cx->h->vtbl->consume_response(cx->h, &head, entity);
    }

    hc_headers_free(h);

    /* htparser does not know the request was HEAD and would wait for the
     * Content-Length bytes forever. The exchange is complete at the head;
     * T_FINISH delivers stream_end and resets the parser for reuse. */
    if (req->method == htp_method_HEAD && !cx->failed) {
        cx->done = 1;
        schedule(cx->task, &cx->pending, T_FINISH);
    }

    return EVHTP_RES_OK;
}

static evhtp_res
cli_on_read(evhtp_request_t *req, struct evbuffer *buf, void *arg)
{
    hc_client_exchange *cx  = arg;
    size_t              len = evbuffer_get_length(buf);

    (void)req;

    if (len && !cx->failed && !cx->released && !cx->done) {
        cx->h->vtbl->consume(cx->h, evbuffer_pullup(buf, (ev_ssize_t)len), len);
    }

    evbuffer_drain(buf, len);

    return EVHTP_RES_OK;
}

static evhtp_res
cli_on_req_fini(evhtp_request_t *req, void *arg)
{
    hc_client_exchange *cx = arg;

    (void)req;
    cx->req = NULL;

    return EVHTP_RES_OK;
}

/* --- read-until-close bodies --------------------------------------------- *
 * htparser treats a response with neither Content-Length nor chunked
 * encoding as having an empty body, so it reports completion right after
 * the head and would then misparse the body as the next response. For such
 * responses the engine pauses the parser after the head (pausing makes
 * libevhtp stop and leave the remaining bytes in the input buffer) and
 * reads the socket itself until EOF. Such a connection is never reused.
 */
static void
cli_raw_read(struct bufferevent *bev, void *arg)
{
    hc_client_exchange *cx = arg;
    struct evbuffer    *in = bufferevent_get_input(bev);
    size_t              len;

    while ((len = evbuffer_get_length(in)) > 0) {
        if (!cx->failed && !cx->released) {
            cx->h->vtbl->consume(cx->h, evbuffer_pullup(in, (ev_ssize_t)len), len);
        }

        evbuffer_drain(in, len);
    }
}

static void
cli_raw_event(struct bufferevent *bev, short events, void *arg)
{
    hc_client_exchange *cx = arg;

    if (events & BEV_EVENT_CONNECTED) {
        return;
    }

    bufferevent_disable(bev, EV_READ | EV_WRITE);

    if (events & BEV_EVENT_EOF) {
        cli_raw_read(bev, cx);
        cx->done = 1;
    } else if (events & BEV_EVENT_TIMEOUT) {
        cli_fail(cx, (events & BEV_EVENT_WRITING) ? "upstream write timeout"
                                                  : "upstream read timeout");
    } else {
        cli_fail(cx, "upstream connection error mid-response");
    }

    schedule(cx->task, &cx->pending, T_FINISH);
}

/* Response complete. */
static void
cli_on_complete(evhtp_request_t *req, void *arg)
{
    hc_client_exchange *cx = arg;

    if (cx->failed || cx->done) {
        return;
    }

    if (cx->close_delimited) {
        struct bufferevent *bev = evhtp_connection_get_bev(cx_conn(cx));

        cx->raw_mode = 1;
        evhtp_request_pause(req);   /* parser stops; body stays buffered */
        bufferevent_setcb(bev, cli_raw_read, NULL, cli_raw_event, cx);

        if (!cx->input_suspended) {
            bufferevent_enable(bev, EV_READ);
        }

        /* Bytes already read are consumed on the next loop iteration, once
         * libevhtp has removed the response head from the buffer. */
        schedule(cx->task, &cx->pending, T_RAWREAD);
        return;
    }

    cx->done = 1;
    cx->end_delivered = 1;
    cx->h->vtbl->stream_end(cx->h, NULL);

    /* Recycle or close the connection outside libevhtp's call stack. */
    schedule(cx->task, &cx->pending, T_FINISH);
}

/* Connection event (not CONNECTED) while this exchange runs. */
static evhtp_res
cli_conn_event(hc_client_exchange *cx, short events)
{
    /* Mirrors libevhtp: a spurious EAGAIN EOF is not a close. */
    if (events == (BEV_EVENT_EOF | BEV_EVENT_READING) && errno == EAGAIN) {
        return EVHTP_RES_OK;
    }

    if (events & BEV_EVENT_TIMEOUT) {
        cli_fail(cx, (events & BEV_EVENT_WRITING) ? "upstream write timeout"
                                                  : "upstream read timeout");
    } else if ((events & BEV_EVENT_EOF) && cx->resp_head && cx->close_delimited) {
        cx->done = 1;           /* end of a read-until-close body */
    } else if (!cx->head_sent || !cx->resp_head) {
        cli_fail(cx, "connection to upstream failed or closed");
    } else {
        cli_fail(cx, "upstream closed connection mid-response");
    }

    return EVHTP_RES_OK;
}

/* The connection under this exchange is being freed by libevhtp. */
static void
cli_conn_gone(hc_client_exchange *cx)
{
    chan_detach(&cx->out);

    if (!cx->done) {
        cli_fail(cx, "upstream connection closed");
    }

    schedule(cx->task, &cx->pending, T_FINISH);
}

/* Can the connection carry another exchange? */
static int
cx_left_conn_reusable(const hc_client_exchange *cx)
{
    evhtp_connection_t *c = cx_conn(cx);
    struct bufferevent *bev;

    if (c == NULL || cx->pc->dead || cx->pc->rq == NULL) {
        return 0;
    }

    bev = evhtp_connection_get_bev(c);

    return cx->done && !cx->failed && !cx->raw_mode && cx->resp_keepalive &&
           cx->head_sent && cx->req_attached &&
           (cx->out.bev == NULL || cx->out.ended) &&            /* body sent */
           evbuffer_get_length(bufferevent_get_output(bev)) == 0 &&
           evbuffer_get_length(bufferevent_get_input(bev)) == 0;
}

static void
cli_task_cb(evutil_socket_t fd, short what, void *arg)
{
    hc_client_exchange *cx   = arg;
    unsigned            todo = cx->pending;

    (void)fd;
    (void)what;
    cx->pending = 0;

    if (todo & T_START && !cx->failed && !cx->released) {
        cx->h->vtbl->produce_request(cx->h, &cx->rc);
    }

    if (todo & T_PRODUCE && !cx->failed && !cx->released &&
        cx->out.bev && !cx->out.ended && cx->h->vtbl->produce) {
        cx->h->vtbl->produce(cx->h, &cx->out);
    }

    if (todo & T_RAWREAD && cx_conn(cx) && !cx->done) {
        cli_raw_read(evhtp_connection_get_bev(cx_conn(cx)), cx);
    }

    if (todo & (T_ABORT | T_FINISH)) {
        hc_pconn *pc       = cx->pc;
        int       reusable;

        if (cx->interposed && cx_conn(cx)) {
            cli_interpose_end(cx, evhtp_connection_get_bev(cx_conn(cx)));
        }

        reusable = cx_left_conn_reusable(cx) && !cx->interposed;

        chan_detach(&cx->out);

        if (pc) {
            pc->cx       = NULL;
            pc->reusable = reusable;

            if (reusable) {
                evhtp_connection_t *c   = pc->conn;
                struct bufferevent *bev = evhtp_connection_get_bev(c);

                /* Detach this exchange's request and give the next one a
                 * clean parser (the HEAD case leaves it expecting a body). */
                evhtp_request_free(cx->req);
                htparser_init(c->parser, htp_type_response);
                htparser_set_userdata(c->parser, c);

                if (cx->input_suspended) {
                    bufferevent_enable(bev, EV_READ);
                }

                pc->keep_alive_ms = cx->keep_alive_ms;

                if (cx->timeouts_on) {
                    bufferevent_set_timeouts(bev, NULL, NULL);  /* idle: none */
                }

                /* No request is attached any more, so libevhtp's parser
                 * must not see input until the next exchange starts. */
                pc_set_idle(pc);
            } else if (pc->conn) {
                /* Not reusable (or unpooled): close now. Frees the
                 * request with it. */
                evhtp_connection_free(pc->conn);
            }
        }

        if (cx->req && !cx->req_attached) {
            evhtp_request_free(cx->req);    /* never handed to libevhtp */
        }

        if (!cx->released) {
            cx->released = 1;

            if (cx->done && !cx->failed) {
                if (!cx->end_delivered) {
                    cx->end_delivered = 1;
                    cx->h->vtbl->stream_end(cx->h, NULL);
                }
            } else if (cx->h->vtbl->failed) {
                cx->h->vtbl->failed(cx->h, cx->reason ? cx->reason : "failed");
            }

            cx->h->vtbl->release(cx->h);
        }

        if (cx->ep) {
            hc_endpoint *ep = cx->ep;

            ep->cx = NULL;

            /* Hand the connection back now, not on a later timer: a handler
             * that starts its next request from ->release() (the usual
             * pattern for sequential exchanges) has queued a lease, and the
             * pool must see this connection as idle before that lease is
             * served. We are on the event loop, outside libevhtp's stack. */
            if (ep->release_pending) {
                ep_task_cb(ep);
            }
        } else if (pc) {
            pc_destroy(pc);     /* unpooled: the connection was ours alone */
        }

        hc_context_free(cx->ctx);
        event_free(cx->task);
        free(cx);
    }
}

/* Unpooled: one connection for one exchange. */
hc_client_exchange *
hc_client_execute(hc_reactor *r, const hc_host *target, hc_client_handler *h)
{
    hc_client_exchange *cx = cx_new(r, h);
    hc_pconn           *pc;

    if (cx && hc_host_is_tls(target)) {
        /* TLS settings live in a requester; fail the exchange normally. */
        cli_fail(cx, "https needs a requester (hc_requester_execute)");
        schedule(cx->task, &cx->pending, T_FINISH);
        return cx;
    }

    pc = cx ? pc_new(r, NULL, target) : NULL;

    if (pc == NULL) {
        if (cx) {
            evhtp_request_free(cx->req);
            event_free(cx->task);
            free(cx);
        }

        return NULL;
    }

    cx->pc  = pc;
    cx->ctx = hc_context_new(0, target, NULL);
    pc->cx  = cx;
    schedule(cx->task, &cx->pending, T_START);

    return cx;
}

/* ======================================================================== */
/* Requester, connect, endpoints                                            */
/* ======================================================================== */


/* rq->ops / rq->eps: intrusive doubly linked lists, head insertion. */
#define RQ_LINK(head, n) do {                                              \
        (n)->prev = NULL;                                                  \
        (n)->next = (head);                                                \
        if (head) { (head)->prev = (n); }                                  \
        (head) = (n);                                                      \
    } while (0)

#define RQ_UNLINK(head, n) do {                                            \
        if ((n)->prev) { (n)->prev->next = (n)->next; }                    \
        else { (head) = (n)->next; }                                       \
        if ((n)->next) { (n)->next->prev = (n)->prev; }                    \
        (n)->prev = (n)->next = NULL;                                      \
    } while (0)

static hc_endpoint *
ep_new(hc_requester *rq, hc_pool_entry *e, hc_pconn *pc)
{
    hc_endpoint *ep = calloc(1, sizeof(*ep));

    if (ep == NULL) {
        return NULL;
    }

    ep->rq    = rq;
    ep->entry = e;
    ep->pc    = pc;

    if ((ep->task = hc_timer_new(rq->r, ep_task_cb, ep)) == NULL) {
        free(ep);
        return NULL;
    }

    RQ_LINK(rq->eps, ep);

    return ep;
}

static void
op_finish(hc_connect_op *op, hc_endpoint *ep, const char *error)
{
    hc_connect_cb cb  = op->cb;
    void         *arg = op->arg;

    char          why[512];

    /* error may point into memory freed below (the pconn's message) */
    snprintf(why, sizeof(why), "%s", error ? error : ep ? "" : "out of memory");

    RQ_UNLINK(op->rq->ops, op);
    hc_timer_free(op->timer);
    free(op);
    cb(arg, ep, ep ? NULL : why);
}

/* Give a leased entry back without ever having handed out an endpoint. */
static void
op_fail(hc_connect_op *op, const char *reason)
{
    if (op->pc) {
        op->pc->op = NULL;
    }

    hc_pool_release(op->rq->pool, op->entry, 0);    /* closes the pconn */
    op_finish(op, NULL, reason);
}

static void
op_connected(hc_connect_op *op)
{
    hc_endpoint *ep;

    op->pc->op = NULL;
    pc_set_idle(op->pc);        /* until the first exchange */

    if ((ep = ep_new(op->rq, op->entry, op->pc)) == NULL) {
        hc_pool_release(op->rq->pool, op->entry, 0);
    }

    op_finish(op, ep, NULL);
}

static void
op_timeout_cb(void *arg)
{
    op_fail(arg, "connect timeout");
}

static void
op_on_lease(void *arg, hc_pool_entry *e, const char *error)
{
    hc_connect_op *op = arg;
    hc_pconn      *pc;
    hc_endpoint   *ep;

    op->lease = NULL;

    if (error) {
        op_finish(op, NULL, error);
        return;
    }

    op->entry = e;
    pc        = hc_pool_entry_conn(e);

    if (pc && !pc->dead && pc->conn) {
        /* Pooled connection: ready now. It stays parked (idle callbacks)
         * until an exchange is executed on it. */
        if ((ep = ep_new(op->rq, e, pc)) == NULL) {
            hc_pool_release(op->rq->pool, e, 0);
        }

        op_finish(op, ep, NULL);
        return;
    }

    if (pc) {
        hc_pool_entry_discard(e);   /* stale or expired: reconnect */
    }

    if ((pc = pc_new(op->rq->r, op->rq, &op->target)) == NULL) {
        op_fail(op, "cannot create connection");
        return;
    }

    pc->entry = e;
    pc->op    = op;
    op->pc    = pc;
    hc_pool_entry_assign(e, pc);

    if (op->deadline) {
        int64_t left = op->deadline - hc_reactor_now_ms(op->rq->r);

        hc_timer_start(op->timer, left > 0 ? left : 0);
    }
}

static void
pc_task_cb(evutil_socket_t fd, short what, void *arg)
{
    hc_pconn *pc   = arg;
    unsigned  todo = pc->pending;

    (void)fd;
    (void)what;
    pc->pending = 0;

    if (todo & P_CONNECTED && pc->op && !pc->dead) {
        op_connected(pc->op);
    } else if (todo & (P_CONNECTED | P_CONNECT_FAILED) && pc->op) {
        char why[sizeof(pc->err)];

        snprintf(why, sizeof(why), "%s", pc->err[0] ? pc->err : "cannot connect to origin");
        op_fail(pc->op, why);
        return;                 /* pc was destroyed */
    }

    if (todo & P_LOST && pc->idle && pc->entry) {
        /* Frees pc if the entry is idle in the pool; if it is leased (an
         * endpoint holds it between exchanges) the pool leaves it and the
         * endpoint's next execute/release sees pc->dead. */
        hc_pool_connection_lost(pc->rq->pool, pc->entry);
    }
}

hc_connect_op *
hc_requester_connect(hc_requester *rq, const hc_host *target, int64_t timeout_ms,
                     hc_connect_cb cb, void *arg)
{
    hc_connect_op *op;

    if (rq->closing || (op = calloc(1, sizeof(*op))) == NULL) {
        return NULL;
    }

    op->rq       = rq;
    op->cb       = cb;
    op->arg      = arg;
    op->target   = *target;
    op->deadline = timeout_ms > 0 ? hc_reactor_now_ms(rq->r) + timeout_ms : 0;
    op->timer    = hc_timer_new(rq->r, op_timeout_cb, op);

    if (op->timer) {
        op->lease = hc_pool_lease(rq->pool, target, NULL, timeout_ms, op_on_lease, op);
    }

    if (!op->timer || !op->lease) {
        hc_timer_free(op->timer);
        free(op);
        return NULL;
    }

    RQ_LINK(rq->ops, op);

    return op;
}

void
hc_connect_cancel(hc_connect_op *op)
{
    if (op->lease) {
        hc_pool_lease_cancel(op->lease);
    } else if (op->entry) {
        if (op->pc) {
            op->pc->op = NULL;
        }

        hc_pool_release(op->rq->pool, op->entry, 0);
    }

    RQ_UNLINK(op->rq->ops, op);
    hc_timer_free(op->timer);
    free(op);
}

hc_client_exchange *
hc_endpoint_execute(hc_endpoint *ep, hc_client_handler *h)
{
    hc_client_exchange *cx;

    if (ep->cx || ep->release_pending || ep->rq->closing ||
        (cx = cx_new(ep->rq->r, h)) == NULL) {
        return NULL;
    }

    cx->pc     = ep->pc;
    cx->ep     = ep;
    cx->proc   = ep->rq->processor;     /* the requester outlives its exchanges */
    cx->ctx    = hc_context_new(0, &ep->pc->target, NULL);
    ep->cx     = cx;
    ep->pc->cx = cx;

    if (ep->pc->conn == NULL || ep->pc->dead) {
        cli_fail(cx, "upstream connection closed");
        schedule(cx->task, &cx->pending, T_FINISH);
    } else {
        pc_set_active(ep->pc);
        schedule(cx->task, &cx->pending, T_START);
    }

    return cx;
}

void
hc_endpoint_set_attachment(hc_endpoint *ep, void *data, void (*free_fn)(void *))
{
    hc_pconn *pc = ep ? ep->pc : NULL;

    if (pc == NULL) {
        if (free_fn) {
            free_fn(data);
        }
        return;
    }

    if (pc->attachment_free && pc->attachment != data) {
        pc->attachment_free(pc->attachment);
    }

    pc->attachment      = data;
    pc->attachment_free = free_fn;
}

void *
hc_endpoint_get_attachment(const hc_endpoint *ep)
{
    return (ep && ep->pc) ? ep->pc->attachment : NULL;
}

static void
ep_release(hc_endpoint *ep, int reuse)
{
    if (ep->release_pending) {
        return;
    }

    ep->release_pending = 1;
    ep->reuse           = reuse;
    hc_timer_start(ep->task, 0);  /* never from inside libevhtp's stack */
}

void
hc_endpoint_release_and_reuse(hc_endpoint *ep)
{
    ep_release(ep, 1);
}

void
hc_endpoint_release_and_discard(hc_endpoint *ep)
{
    ep_release(ep, 0);
}

static void
ep_task_cb(void *arg)
{
    hc_endpoint *ep = arg;
    hc_pconn    *pc = ep->pc;
    int          keep;

    if (ep->cx) {
        /* Exchange still running: finish (or cancel) it first; its
         * T_FINISH re-runs this task. */
        if (!ep->reuse) {
            hc_client_cancel(ep->cx);
        }

        return;
    }

    keep = ep->reuse && pc->conn && !pc->dead && pc->reusable;

    if (keep) {
        pc_set_idle(pc);        /* no-op if already parked */
        hc_pool_entry_update_expiry(ep->entry, pc->keep_alive_ms);
    }

    hc_pool_release(ep->rq->pool, ep->entry, keep);    /* !keep: closes pc */
    RQ_UNLINK(ep->rq->eps, ep);
    hc_timer_free(ep->task);
    free(ep);
}

static void
housekeeping_cb(void *arg)
{
    hc_requester *rq = arg;

    hc_pool_close_expired(rq->pool);
    hc_timer_start(rq->housekeeping, 1000);
}

/* Client TLS context shared by every https connection of the requester
 * (Java: the requester's TlsStrategy). Peer verification is on unless
 * explicitly disabled; per-connection host checks are set in pc_resolved. */
static int
requester_tls_init(hc_requester *rq, const hc_requester_config *c)
{
#ifdef HC_HAVE_TLS
    SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());

    if (ctx == NULL) {
        return -1;
    }

    SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
    rq->tls_no_verify = c->tls_no_verify;

    if (c->tls_no_verify) {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
    } else {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);

        if (c->tls_ca_file ? SSL_CTX_load_verify_locations(ctx, c->tls_ca_file, NULL) != 1
                           : SSL_CTX_set_default_verify_paths(ctx) != 1) {
            SSL_CTX_free(ctx);
            return -1;
        }
    }

    rq->tls_ctx = ctx;
#else
    (void)rq;

    if (c->tls_ca_file) {
        return -1;              /* asked for TLS settings we cannot honour */
    }
#endif

    return 0;
}

hc_requester *
hc_requester_new(hc_reactor *r, const hc_requester_config *cfg)
{
    static const hc_pool_conn_ops ops = { pool_close_conn };
    hc_requester_config           c   = cfg ? *cfg : (hc_requester_config){ 0 };
    hc_pool_config                pc;
    hc_requester                 *rq  = calloc(1, sizeof(*rq));

    if (rq == NULL) {
        return NULL;
    }

    pc.max_total             = c.max_total > 0 ? c.max_total : 25;
    pc.default_max_per_route = c.default_max_per_route > 0 ? c.default_max_per_route : 5;
    pc.time_to_live_ms       = c.time_to_live_ms;
    pc.reuse_policy          = c.fifo ? HC_POOL_REUSE_FIFO : HC_POOL_REUSE_LIFO;

    rq->r                     = r;
    rq->processor             = hc_processor_ref(c.processor);
    rq->read_timeout_ms       = c.read_timeout_ms > 0 ? c.read_timeout_ms : 0;
    rq->write_timeout_ms      = c.write_timeout_ms > 0 ? c.write_timeout_ms : 0;
    rq->default_keep_alive_ms = c.default_keep_alive_ms > 0 ? c.default_keep_alive_ms : 180000;
    rq->pool                  = hc_pool_new(r, &pc, &ops, rq);
    rq->housekeeping          = hc_timer_new(r, housekeeping_cb, rq);

    if (!rq->pool || !rq->housekeeping || requester_tls_init(rq, &c) != 0) {
        hc_pool_free(rq->pool);
        hc_timer_free(rq->housekeeping);
#ifdef HC_HAVE_TLS
        SSL_CTX_free(rq->tls_ctx);
#endif
        hc_processor_unref(rq->processor);
        free(rq);
        return NULL;
    }

    hc_timer_start(rq->housekeeping, 1000);

    return rq;
}

hc_pool *
hc_requester_pool(hc_requester *rq)
{
    return rq->pool;
}

/* Finish an exchange now, as its queued T_FINISH would later: the handler
 * sees failed + release, and the endpoint (if released) is handed back. */
static void
cx_finish_now(hc_client_exchange *cx, const char *reason)
{
    cli_fail(cx, reason);
    cx->pending = T_FINISH;     /* nothing else queued may run now */
    cli_task_cb(-1, 0, cx);     /* frees cx (and its queued task) */
}

void
hc_requester_free(hc_requester *rq)
{
    static const char why[] = "requester shut down";

    if (rq == NULL || rq->closing) {
        return;
    }

    /* The application's loop may keep running after this (an embedding
     * that replaces its requester, e.g. on a config reload), so everything
     * still queued on it for this requester is finished here and now.
     * Callbacks run from here cannot start anything new on it. */
    rq->closing = 1;
    hc_timer_free(rq->housekeeping);
    rq->housekeeping = NULL;

    /* Connect requests: waiting for a pool slot, or connecting. */
    while (rq->ops) {
        hc_connect_op *op = rq->ops;

        if (op->lease) {
            hc_pool_lease_cancel(op->lease);
            op->lease = NULL;
            op_finish(op, NULL, why);
        } else {
            op_fail(op, why);       /* closes the connection being made */
        }
    }

    /* Endpoints: an exchange in flight (perhaps with its abort queued) is
     * finished first; that hands a released endpoint back. One still held
     * by the application is discarded: its pointer is invalid from now. */
    while (rq->eps) {
        hc_endpoint *ep = rq->eps;

        if (ep->cx) {
            cx_finish_now(ep->cx, why);     /* sets ep->cx = NULL */
            continue;
        }

        ep->reuse = ep->release_pending ? ep->reuse : 0;
        ep_task_cb(ep);                     /* unlinks and frees ep */
    }

    hc_pool_free(rq->pool);     /* closes the idle connections */
#ifdef HC_HAVE_TLS
    SSL_CTX_free(rq->tls_ctx);
#endif
    hc_processor_unref(rq->processor);  /* after its exchanges are done */
    free(rq);
}

hc_client_exchange *
hc_request_channel_exchange(hc_request_channel *rc)
{
    return rc ? rc->cx : NULL;
}

/* ======================================================================== */
/* hc_requester_execute: connect + execute + release in one call            */
/* ======================================================================== */

/* A client handler that forwards every call to the application's handler
 * and manages the endpoint underneath it (Java: the internal handler
 * HttpAsyncRequester.execute() wraps around the caller's). */
typedef struct exec_handler {
    hc_client_handler  base;      /* must be first */
    hc_client_handler *inner;
    hc_endpoint       *ep;        /* NULL once released */
} exec_handler;

#define EXEC(self) ((exec_handler *)(void *)(self))

static void
exec_release_ep(exec_handler *x, int reuse)
{
    if (x->ep) {
        if (reuse) {
            hc_endpoint_release_and_reuse(x->ep);
        } else {
            hc_endpoint_release_and_discard(x->ep);
        }

        x->ep = NULL;
    }
}

static void
exec_produce_request(hc_client_handler *self, hc_request_channel *rc)
{
    hc_client_handler *in = EXEC(self)->inner;

    in->vtbl->produce_request(in, rc);
}

static void
exec_consume_response(hc_client_handler *self, const hc_response_head *head,
                      const hc_entity *entity)
{
    hc_client_handler *in = EXEC(self)->inner;

    in->vtbl->consume_response(in, head, entity);
}

static void
exec_consume(hc_client_handler *self, const void *data, size_t len)
{
    hc_client_handler *in = EXEC(self)->inner;

    in->vtbl->consume(in, data, len);
}

static void
exec_stream_end(hc_client_handler *self, const hc_headers *trailers)
{
    hc_client_handler *in = EXEC(self)->inner;

    in->vtbl->stream_end(in, trailers);
    exec_release_ep(EXEC(self), 1);     /* Java: releaseAndReuse() */
}

static void
exec_produce(hc_client_handler *self, hc_data_channel *ch)
{
    hc_client_handler *in = EXEC(self)->inner;

    if (in->vtbl->produce) {
        in->vtbl->produce(in, ch);
    }
}

static void
exec_failed(hc_client_handler *self, const char *reason)
{
    hc_client_handler *in = EXEC(self)->inner;

    exec_release_ep(EXEC(self), 0);     /* Java: releaseAndDiscard() */

    if (in->vtbl->failed) {
        in->vtbl->failed(in, reason);
    }
}

static void
exec_release(hc_client_handler *self)
{
    exec_handler      *x  = EXEC(self);
    hc_client_handler *in = x->inner;

    exec_release_ep(x, 0);              /* neither end nor failure seen */
    free(x);
    in->vtbl->release(in);
}

static void
exec_consume_information(hc_client_handler *self, const hc_response_head *head)
{
    hc_client_handler *in = EXEC(self)->inner;

    if (in->vtbl->consume_information) {
        in->vtbl->consume_information(in, head);
    }
}

static const hc_client_handler_vtbl exec_vtbl = {
    .consume_information = exec_consume_information,
    .produce_request  = exec_produce_request,
    .consume_response = exec_consume_response,
    .consume          = exec_consume,
    .stream_end       = exec_stream_end,
    .produce          = exec_produce,
    .failed           = exec_failed,
    .release          = exec_release,
};

static void
exec_on_connected(void *arg, hc_endpoint *ep, const char *error)
{
    exec_handler      *x  = arg;
    hc_client_handler *in = x->inner;

    if (error) {
        free(x);

        if (in->vtbl->failed) {
            in->vtbl->failed(in, error);
        }

        in->vtbl->release(in);
        return;
    }

    x->ep = ep;

    if (hc_endpoint_execute(ep, &x->base) == NULL) {
        hc_endpoint_release_and_discard(ep);
        free(x);

        if (in->vtbl->failed) {
            in->vtbl->failed(in, "cannot start exchange");
        }

        in->vtbl->release(in);
    }
}

int
hc_requester_execute(hc_requester *rq, const hc_host *target, int64_t timeout_ms,
                     hc_client_handler *h)
{
    exec_handler *x = calloc(1, sizeof(*x));

    if (x == NULL) {
        return -1;
    }

    x->base.vtbl = &exec_vtbl;
    x->inner     = h;

    if (hc_requester_connect(rq, target, timeout_ms, exec_on_connected, x) == NULL) {
        free(x);
        return -1;
    }

    return 0;
}
