/*
 * hc.h - a small async HTTP "exchange handler" API in standard C (C99).
 *
 * Modelled on Apache HttpCore 5's async API:
 *
 *   Java (httpcore5)                     C (this header)
 *   -----------------------------------  ------------------------------------
 *   AsyncServerExchangeHandler           hc_server_handler   (+ _vtbl)
 *     handleRequest(head, entity, chan)    ->handle_request()
 *     consume(ByteBuffer)                  ->consume()
 *     streamEnd(trailers)                  ->stream_end()
 *     produce(DataStreamChannel)           ->produce()
 *     failed(Exception)                    ->failed()
 *     releaseResources()                   ->release()
 *   AsyncClientExchangeHandler           hc_client_handler   (+ _vtbl)
 *     produceRequest(RequestChannel)       ->produce_request()
 *     consumeResponse(head, entity)        ->consume_response()
 *     consume / streamEnd / produce        ->consume / ->stream_end / ->produce
 *     failed / releaseResources / cancel   ->failed / ->release
 *     consumeInformation(response)         ->consume_information()
 *   ResponseChannel.sendResponse()       hc_response_send()
 *   ResponseChannel.sendInformation()    hc_response_send_information()
 *   AsyncServerBootstrap.register()      hc_server_register()
 *   Http1StreamListener (server)         hc_server_listener
 *   HttpHost                             hc_host
 *   HttpAsyncRequester (pooled)          hc_requester_* (pool: hc_pool.h)
 *   RequestChannel.sendRequest()         hc_request_send()
 *   DataStreamChannel.write/endStream    hc_data_write() / hc_data_end()
 *   DataStreamChannel.requestOutput()    hc_data_request_output()
 *   CapacityChannel.update()             hc_*_suspend_input / hc_*_resume_input
 *
 * "Interfaces" are expressed the usual C way: a struct whose first member is
 * a pointer to a table of function pointers. An application embeds the
 * interface struct as the FIRST member of its own handler struct and
 * recovers its struct inside callbacks with a cast (or HC_CONTAINER_OF).
 *
 * Nothing in this header depends on libevhtp or libevent. The engine that
 * drives these callbacks (hc_evhtp.c) is the only file that does, so an
 * application written against hc.h can be moved to another engine.
 *
 * Threading: all callbacks for a reactor run on the thread that called
 * hc_reactor_run(). None of the hc_* functions are thread-safe.
 *
 * Re-entrancy: callbacks are never invoked from inside an hc_* call made by
 * the application; they are always dispatched from the event loop. Every
 * hc_* function may therefore be called from inside any callback.
 */
#ifndef HC_H
#define HC_H

#include <stddef.h>
#include <stdint.h>
#include <sys/socket.h>

#ifdef __cplusplus
extern "C" {
#endif

#define HC_CONTAINER_OF(ptr, type, member) \
    ((type *)(void *)((char *)(ptr) - offsetof(type, member)))

/* ------------------------------------------------------------------------ */
/* Reactor (event loop)                                                     */
/* ------------------------------------------------------------------------ */

typedef struct hc_reactor hc_reactor;

hc_reactor *hc_reactor_new(void);
/* A reactor on an event loop the application already runs - e.g. one per
 * worker thread. |event_base| is a libevent struct event_base *; it stays the
 * application's: it keeps running the loop (don't call hc_reactor_run()), and
 * hc_reactor_free() leaves the base alone. Everything hcore does for this
 * reactor then happens in that loop's thread. */
hc_reactor *hc_reactor_new_on_base(void *event_base);
int         hc_reactor_run(hc_reactor *r);           /* blocks until stopped */
void        hc_reactor_stop(hc_reactor *r);          /* safe from callbacks  */
void        hc_reactor_free(hc_reactor *r);

/* Arrange for SIGINT/SIGTERM to stop the reactor. */
int         hc_reactor_stop_on_signals(hc_reactor *r);

/* Monotonic clock in milliseconds. */
int64_t     hc_reactor_now_ms(hc_reactor *r);

/* One-shot timers. A delay of 0 runs the callback on the next loop
 * iteration, which is also the way to defer work out of a callback.
 * Starting a pending timer re-arms it. */
typedef struct hc_timer hc_timer;
typedef void (*hc_timer_cb)(void *arg);

hc_timer   *hc_timer_new(hc_reactor *r, hc_timer_cb cb, void *arg);
void        hc_timer_start(hc_timer *t, int64_t delay_ms);
void        hc_timer_stop(hc_timer *t);
void        hc_timer_free(hc_timer *t);

/* ------------------------------------------------------------------------ */
/* Headers and message heads                                                */
/* ------------------------------------------------------------------------ */

typedef struct hc_headers hc_headers;               /* ordered name/value list */

hc_headers *hc_headers_new(void);
void        hc_headers_free(hc_headers *h);
int         hc_headers_add(hc_headers *h, const char *name, const char *value);
size_t      hc_headers_remove(hc_headers *h, const char *name); /* all matches */
const char *hc_headers_get(const hc_headers *h, const char *name); /* first */
size_t      hc_headers_count(const hc_headers *h);
int         hc_headers_at(const hc_headers *h, size_t i,
                          const char **name, const char **value);

typedef struct hc_request_head {
    const char       *method;     /* "GET", "POST", ...                     */
    const char       *target;     /* request-target: "/path?query"          */
    const char       *authority;  /* Host header value (may be NULL)        */
    int               version;    /* 10 or 11                               */
    const hc_headers *headers;
} hc_request_head;

typedef struct hc_response_head {
    int               status;
    int               version;    /* 10 or 11                               */
    const hc_headers *headers;
} hc_response_head;

/* Describes a message body. A NULL hc_entity pointer means "no body"
 * (Java: EntityDetails == null). */
typedef struct hc_entity {
    int64_t     content_length;   /* -1 = unknown (sent chunked)            */
    const char *content_type;     /* may be NULL                            */
} hc_entity;

/* ------------------------------------------------------------------------ */
/* Hosts and routes (Java: HttpHost)                                        */
/* ------------------------------------------------------------------------ */

/* A target host: scheme + name + port. A plain value (no allocation): copy
 * it freely. It is also the pool's route key, so http://h:443 and
 * https://h:443 are different routes. */
typedef struct hc_host {
    char     scheme[8];         /* "http" or "https"                       */
    char     name[256];         /* host name or IP literal, no brackets    */
    uint16_t port;              /* never 0 once initialised                */
} hc_host;

/* port 0: the scheme's default (80/443). Returns -1 for an unknown scheme
 * or a name that does not fit. */
int    hc_host_init(hc_host *h, const char *scheme, const char *name, uint16_t port);
/* "[scheme://]name[:port]", scheme defaulting to http; IPv6 literals in
 * brackets. Java: HttpHost.create(). */
int    hc_host_parse(hc_host *h, const char *s);
/* "scheme://authority[/path...]" -> host + request-target ("/" if none). */
int    hc_uri_parse(const char *uri, hc_host *h, char *target, size_t target_len);
int    hc_host_is_tls(const hc_host *h);
int    hc_host_equals(const hc_host *a, const hc_host *b);  /* name: case-insensitive */
/* Host header value: name, with ":port" unless it is the scheme default. */
size_t hc_host_authority(const hc_host *h, char *buf, size_t len);
/* "scheme://authority" */
size_t hc_host_to_string(const hc_host *h, char *buf, size_t len);

/* ------------------------------------------------------------------------ */
/* Channels handed to the application                                       */
/* ------------------------------------------------------------------------ */

/* Outgoing body stream (response body on the server side, request body on
 * the client side). Writes are always accepted and queued; use
 * hc_data_capacity() to avoid queueing without bound. When the channel has
 * room again after being full, or after hc_data_request_output(), the owning
 * handler's ->produce() is called. */
typedef struct hc_data_channel hc_data_channel;

long   hc_data_write(hc_data_channel *ch, const void *data, size_t len);
int    hc_data_end(hc_data_channel *ch, const hc_headers *trailers);
/* Bytes that can be written before the channel counts as full. A return of
 * 0 also arms a ->produce() call for when the channel drains. */
size_t hc_data_capacity(hc_data_channel *ch);
void   hc_data_request_output(hc_data_channel *ch);  /* schedule ->produce() */

typedef struct hc_response_channel hc_response_channel;
typedef struct hc_request_channel  hc_request_channel;

/* Send the response head. Returns the body channel, or NULL when entity is
 * NULL (no body) or on error. The channel stays valid until ->release(). */
hc_data_channel *hc_response_send(hc_response_channel *ch,
                                  const hc_response_head *head,
                                  const hc_entity *entity);

/* Send an interim (1xx) response ahead of the final one; may be called
 * several times before hc_response_send(). 100 is sent automatically by the
 * engine when a request carries "Expect: 100-continue", and 101 is not
 * supported; both return -1. Java: ResponseChannel.sendInformation(). */
int hc_response_send_information(hc_response_channel *ch, const hc_response_head *head);

/* Convenience: complete response with a small in-memory body. */
int hc_response_send_simple(hc_response_channel *ch, int status,
                            const char *content_type, const char *body,
                            int close_connection);

/* Send the request head (client side). Same return convention. */
hc_data_channel *hc_request_send(hc_request_channel *ch,
                                 const hc_request_head *head,
                                 const hc_entity *entity);

/* ------------------------------------------------------------------------ */
/* Server side: AsyncServerExchangeHandler                                  */
/* ------------------------------------------------------------------------ */

typedef struct hc_server_exchange hc_server_exchange;  /* opaque, engine side */
typedef struct hc_server_handler  hc_server_handler;

typedef struct hc_server_handler_vtbl {
    /* Request head received (body, if any, has NOT been read yet).
     * Respond now or later via hc_response_send(rc, ...). */
    void (*handle_request)(hc_server_handler *self, hc_server_exchange *ex,
                           const hc_request_head *head,
                           const hc_entity *entity,      /* NULL: no body */
                           hc_response_channel *rc);
    /* A chunk of the request body. Not called after stream_end/failed. */
    void (*consume)(hc_server_handler *self, const void *data, size_t len);
    /* Request body complete (also called for bodiless requests). */
    void (*stream_end)(hc_server_handler *self, const hc_headers *trailers);
    /* Response body channel can take data (see hc_data_channel). Optional. */
    void (*produce)(hc_server_handler *self, hc_data_channel *ch);
    /* The exchange failed (I/O error, client went away, ...). */
    void (*failed)(hc_server_handler *self, const char *reason);
    /* Last call. The handler may free itself; channels are now invalid. */
    void (*release)(hc_server_handler *self);
} hc_server_handler_vtbl;

struct hc_server_handler {
    const hc_server_handler_vtbl *vtbl;
};

/* Called once per incoming exchange (Java: Supplier<AsyncServerExchangeHandler>). */
typedef hc_server_handler *(*hc_server_handler_factory)(void *arg);

/* Flow control for the incoming request body. Advisory: data already read
 * from the socket (at most one read buffer) may still be delivered. */
void hc_server_suspend_input(hc_server_exchange *ex);
void hc_server_resume_input(hc_server_exchange *ex);
/* Drop the client connection. ->failed() and ->release() follow. */
void hc_server_abort(hc_server_exchange *ex);

/* The exchange's connection: the client's address (remote), and our end of
 * it, the address the client connected to (local). Like getpeername() and
 * getsockname(): copies at most *len bytes into out and sets *len to the
 * address's full length (a struct sockaddr_storage always fits). Returns 0,
 * or -1 when the address is unknown. Callable from handle_request() until
 * release(); the remote address stays available even after the connection
 * has gone. */
int hc_server_exchange_remote_address(hc_server_exchange *ex, struct sockaddr *out, socklen_t *len);
int hc_server_exchange_local_address(hc_server_exchange *ex, struct sockaddr *out, socklen_t *len);

typedef struct hc_server hc_server;

/* factory handles every request that no hc_server_register() pattern
 * matches. It may be NULL: unmatched requests then get
 * "404 Resource not found" (as with Java's AsyncServerBootstrap). */
hc_server *hc_server_new(hc_reactor *r, hc_server_handler_factory factory,
                         void *arg);

/* Route requests by URI path. Java: AsyncServerBootstrap.register(uriPattern,
 * supplier), matched like UriPatternMatcher: a pattern is an exact path
 * ("/echo"), a prefix ("/api*"), a suffix ("*.html") or "*". An exact match
 * wins; otherwise the longest matching pattern, and between equally long
 * ones the pattern ending in '*'. The query string is not part of the path.
 * Registering the same pattern again replaces it. Returns -1 on failure. */
int        hc_server_register(hc_server *s, const char *uri_pattern,
                              hc_server_handler_factory factory, void *arg);

/* Java: Http1StreamListener on the server. remote is "address:port". Every
 * callback is optional. on_exchange_complete reports exchanges that finished
 * normally, and whether the connection stays open for another request. */
typedef struct hc_server_listener {
    void (*on_request_head)(void *arg, const char *remote, const hc_request_head *head);
    void (*on_response_head)(void *arg, const char *remote, const hc_response_head *head);
    void (*on_exchange_complete)(void *arg, const char *remote, int keep_alive);
    void  *arg;
} hc_server_listener;

void       hc_server_set_listener(hc_server *s, const hc_server_listener *l);
/* Serve HTTPS: PEM certificate (chain) and private key files. Call before
 * hc_server_listen(). Returns -1 if they cannot be loaded or TLS support
 * was not compiled in. */
int        hc_server_enable_tls(hc_server *s, const char *cert_file, const char *key_file);
int        hc_server_listen(hc_server *s, const char *addr, uint16_t port);
void       hc_server_free(hc_server *s);

/* hc_server_listen() with socket options. NULL options = defaults. */
typedef struct hc_listen_options {
    int backlog;        /* listen() backlog; 0: 1024                          */
    int reuse_port;     /* SO_REUSEPORT: several servers - one per thread -   */
                        /* listen on the same address; the kernel spreads     */
                        /* new connections between them                       */
    int defer_accept;   /* TCP_DEFER_ACCEPT: wake up only once data arrives   */
} hc_listen_options;

int    hc_server_listen_opts(hc_server *s, const char *addr, uint16_t port,
                             const hc_listen_options *opts);
/* Close the listening socket. Open connections carry on. */
void   hc_server_unlisten(hc_server *s);
/* Stop / restart accepting new connections; the socket stays bound. Return
 * 0, or -1 when the server isn't listening. */
int    hc_server_pause_accept(hc_server *s);
int    hc_server_resume_accept(hc_server *s);
/*
 * Graceful drain: stop accepting, close idle keep-alive connections now, and
 * close every other connection once its current exchange is complete (a
 * response not yet started gets "Connection: close"). Nothing is cut off.
 * Watch hc_server_connection_count() reach 0, then free the server.
 * hc_server_resume_accept() ends a drain (e.g. a rolled-back reload).
 */
void   hc_server_drain(hc_server *s);
size_t hc_server_connection_count(const hc_server *s);

/* ------------------------------------------------------------------------ */
/* Client side: AsyncClientExchangeHandler                                  */
/* ------------------------------------------------------------------------ */

typedef struct hc_client_exchange hc_client_exchange;  /* opaque */
typedef struct hc_client_handler  hc_client_handler;

typedef struct hc_client_handler_vtbl {
    /* Connection is being set up; send the request head now (or later)
     * with hc_request_send(rc, ...). */
    void (*produce_request)(hc_client_handler *self, hc_request_channel *rc);
    /* Response head received. entity == NULL: no body follows. For a HEAD
     * request the entity describes the representation (so Content-Length
     * can be forwarded) but no body bytes follow; stream_end comes next. */
    void (*consume_response)(hc_client_handler *self,
                             const hc_response_head *head,
                             const hc_entity *entity);
    /* A chunk of the response body. */
    void (*consume)(hc_client_handler *self, const void *data, size_t len);
    /* Response complete. */
    void (*stream_end)(hc_client_handler *self, const hc_headers *trailers);
    /* Request body channel can take data. Optional. */
    void (*produce)(hc_client_handler *self, hc_data_channel *ch);
    /* Connect/I/O/protocol failure, or cancelled. */
    void (*failed)(hc_client_handler *self, const char *reason);
    /* Last call. */
    void (*release)(hc_client_handler *self);
    /* Optional: an interim (1xx) response arrived before the final one, e.g.
     * "100 Continue" or "103 Early Hints". Java: consumeInformation(). A
     * "101 Switching Protocols" is not interim here: it is delivered to
     * consume_response() and ends the exchange (upgrades are unsupported). */
    void (*consume_information)(hc_client_handler *self, const hc_response_head *head);
} hc_client_handler_vtbl;

struct hc_client_handler {
    const hc_client_handler_vtbl *vtbl;
};

/* Unpooled: one new connection for one exchange (name resolved async).
 * http only; use a requester for https. Returns NULL only on immediate
 * resource failure (handler is NOT released in that case). Otherwise
 * ->release() is eventually called. */
hc_client_exchange *hc_client_execute(hc_reactor *r, const hc_host *target,
                                      hc_client_handler *h);
void hc_client_suspend_input(hc_client_exchange *cx);
void hc_client_resume_input(hc_client_exchange *cx);
/* Abort the exchange: ->failed("cancelled") then ->release() (unless the
 * exchange already completed). */
void hc_client_cancel(hc_client_exchange *cx);

/* ------------------------------------------------------------------------ */
/* Pooled connections: requester + endpoints                                */
/* (Java: HttpAsyncRequester.connect() -> AsyncClientEndpoint)              */
/* ------------------------------------------------------------------------ */

typedef struct hc_pool       hc_pool;       /* see hc_pool.h */
typedef struct hc_requester  hc_requester;
typedef struct hc_endpoint   hc_endpoint;
typedef struct hc_connect_op hc_connect_op;

typedef struct hc_requester_config {
    int     max_total;             /* 0 -> 25                                */
    int     default_max_per_route; /* 0 -> 5                                 */
    int64_t time_to_live_ms;       /* 0 -> unlimited connection lifetime     */
    int64_t default_keep_alive_ms; /* 0 -> 180000; used when the response
                                    * has no "Keep-Alive: timeout=N"        */
    int     fifo;                  /* 0 -> LIFO reuse (httpcore5 default)    */
    /* TLS for https targets (Java: TlsStrategy / SSLContext). The server's
     * certificate is verified against the system trust store, or against
     * tls_ca_file (PEM) when set, and must match the host name (or IP). */
    const char *tls_ca_file;
    int         tls_no_verify;     /* 1: skip all verification (testing only) */
} hc_requester_config;

hc_requester *hc_requester_new(hc_reactor *r, const hc_requester_config *cfg);
/* Closes every pooled connection; outstanding endpoints must already have
 * been released. */
void          hc_requester_free(hc_requester *rq);
/* The underlying pool: limits, statistics, listener, close_idle. */
hc_pool      *hc_requester_pool(hc_requester *rq);

/* Lease a connection to target, reusing an idle one when possible.
 * timeout_ms bounds the wait for a pool slot plus name resolution, TCP
 * connect and TLS handshake (<= 0: no limit). Exactly one callback:
 * (ep, NULL) or (NULL, error); TLS errors name the reason, e.g.
 * "TLS certificate verification failed: Hostname mismatch". */
typedef void (*hc_connect_cb)(void *arg, hc_endpoint *ep, const char *error);

hc_connect_op *hc_requester_connect(hc_requester *rq, const hc_host *target,
                                    int64_t timeout_ms, hc_connect_cb cb, void *arg);
/* Only valid before the callback has run; the callback will not run. */
void           hc_connect_cancel(hc_connect_op *op);

/* Run one exchange on the endpoint (one at a time; HTTP/1.1, no
 * pipelining). The handler gets ->release() as usual; the endpoint stays
 * leased until released. */
hc_client_exchange *hc_endpoint_execute(hc_endpoint *ep, hc_client_handler *h);

/* Give the connection back. "reuse" is a request: the engine also checks
 * that the last exchange left the connection reusable (complete response,
 * request fully sent, no "Connection: close", ...). Releasing while an
 * exchange is in flight takes effect once it ends (discard cancels it).
 * The endpoint pointer is invalid after either call; extra calls are
 * ignored only if made before the endpoint is actually released, so drop
 * your pointer once you have called one of them. */
void hc_endpoint_release_and_reuse(hc_endpoint *ep);
void hc_endpoint_release_and_discard(hc_endpoint *ep);

/* Java: HttpAsyncRequester.execute(handler, timeout, context).
 * Leases a pooled connection to target, runs the handler on it, and
 * releases it afterwards: for reuse after a complete exchange, discarded
 * after a failure. The engine's reusability checks apply as usual.
 *
 * The handler sees the normal client callback sequence. If no connection
 * can be obtained (pool timeout, connect failure, connect timeout), it gets
 * ->failed(reason) and ->release() without ->produce_request().
 *
 * Returns 0, or -1 on immediate resource failure, in which case the
 * handler is not called at all (and not released). */
int hc_requester_execute(hc_requester *rq, const hc_host *target,
                         int64_t timeout_ms, hc_client_handler *h);

/* The exchange a request channel belongs to: lets a handler started with
 * hc_requester_execute() (which returns no exchange) use
 * hc_client_suspend_input/resume_input/cancel. Valid until ->release(). */
hc_client_exchange *hc_request_channel_exchange(hc_request_channel *rc);

#ifdef __cplusplus
}
#endif

#endif /* HC_H */
