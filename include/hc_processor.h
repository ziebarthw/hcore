/*
 * hc_processor.h - request/response interceptors and the HTTP processor
 * that runs them (Java: org.apache.hc.core5.http.protocol: HttpProcessor,
 * HttpRequestInterceptor, HttpResponseInterceptor, HttpProcessorBuilder,
 * HttpCoreContext, and the standard interceptors).
 *
 *   HttpRequestInterceptor.process()     hc_request_interceptor_fn
 *   HttpResponseInterceptor.process()    hc_response_interceptor_fn
 *   HttpProcessorBuilder                 hc_processor_builder
 *   HttpProcessor (immutable)            hc_processor
 *   HttpProcessors.client()/server()     hc_processor_client_default() / _server_default()
 *   HttpContext / HttpCoreContext        hc_context
 *   RequestTargetHost                    hc_processor_builder_add_target_host()
 *   RequestUserAgent                     hc_processor_builder_add_user_agent()
 *   RequestDate                          hc_processor_builder_add_request_date()
 *   (default request headers)            hc_processor_builder_add_default_headers()
 *   ResponseDate                         hc_processor_builder_add_response_date()
 *   ResponseServer                       hc_processor_builder_add_response_server()
 *
 * A processor is attached to a requester (hc_requester_config.processor)
 * or a server (hc_server_set_processor()); it then runs on every exchange:
 *
 *   client  request interceptors   just before the request head is sent
 *           response interceptors  when the final response head arrives,
 *                                  before the handler's consume_response()
 *   server  request interceptors   after the request head is parsed,
 *                                  before the handler's handle_request()
 *           response interceptors  in hc_response_send(), before the head
 *                                  is written
 *
 * Interceptors see the message head with mutable headers (and status, for
 * responses), the entity details, and the exchange's context. They run in
 * order: request interceptors first to last, response interceptors first
 * to last. Message framing (Content-Length, Transfer-Encoding, Connection)
 * is the engine's: changes to those headers are ignored.
 *
 * A processor is immutable once built and reference counted (atomically),
 * so one can serve every requester and server, on every thread.
 */
#ifndef HC_PROCESSOR_H
#define HC_PROCESSOR_H

#include "hc.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------------ */
/* Context: per exchange                                                    */
/* ------------------------------------------------------------------------ */

/* Attributes: named values the interceptors and the handler share for one
 * exchange (e.g. a request id an interceptor makes up and the handler logs).
 * Setting a name again replaces (and frees) the old value. free_fn, if not
 * NULL, frees the value when it is replaced or the exchange ends. */
int   hc_context_set_attribute(hc_context *ctx, const char *name, void *value,
                               void (*free_fn)(void *));
void *hc_context_get_attribute(const hc_context *ctx, const char *name);
void  hc_context_remove_attribute(hc_context *ctx, const char *name);

/* What the exchange is (Java: HttpCoreContext's endpoint details). */
int            hc_context_is_server(const hc_context *ctx);
int            hc_context_protocol_version(const hc_context *ctx); /* 10, 11, 20 */
int            hc_context_is_tls(const hc_context *ctx);
/* Client: the target connected to. Server: NULL. */
const hc_host *hc_context_target(const hc_context *ctx);
/* Server: the client's "address:port". Client: NULL. */
const char    *hc_context_remote(const hc_context *ctx);

/* ------------------------------------------------------------------------ */
/* Interceptors                                                             */
/* ------------------------------------------------------------------------ */

/* The request as an interceptor sees it: headers can be changed; method,
 * target and authority cannot (the authority is sent as Host). */
typedef struct hc_http_request {
    const char *method;
    const char *target;         /* request-target: "/path?query"          */
    const char *authority;      /* Host (may be NULL: then Host may come
                                 * from the headers)                       */
    int         version;        /* 10, 11 or 20                            */
    hc_headers *headers;        /* mutable                                 */
} hc_http_request;

/* The response as an interceptor sees it: status and headers can be
 * changed. */
typedef struct hc_http_response {
    int         status;
    int         version;
    hc_headers *headers;        /* mutable                                 */
} hc_http_response;

/*
 * Return 0 to go on. Return an HTTP status (400..599) to stop the exchange:
 *   client request   the exchange fails ("request refused by interceptor")
 *                    and nothing is sent
 *   client response  the exchange fails ("response refused by
 *                    interceptor"); the connection is not reused
 *   server request   the client gets that status; the handler never runs
 *   server response  the connection is dropped (the response cannot be
 *                    replaced once the handler has made it)
 * Any other non-zero value counts as 500 (server request) or as a failure.
 * Later interceptors do not run once one has stopped the exchange.
 */
typedef int (*hc_request_interceptor_fn)(void *arg, hc_http_request *request,
                                         const hc_entity *entity, /* NULL: none */
                                         hc_context *ctx);
typedef int (*hc_response_interceptor_fn)(void *arg, hc_http_response *response,
                                          const hc_entity *entity, /* NULL: none */
                                          hc_context *ctx);

/* ------------------------------------------------------------------------ */
/* Builder and processor                                                    */
/* ------------------------------------------------------------------------ */

typedef struct hc_processor_builder hc_processor_builder;

hc_processor_builder *hc_processor_builder_new(void);
/* free_arg, if not NULL, frees arg with the processor (or the builder, if
 * it is never built). Each returns 0, or -1 on allocation failure (then
 * free_arg has already run). */
int hc_processor_builder_add_request_first(hc_processor_builder *b, hc_request_interceptor_fn fn,
                                           void *arg, void (*free_arg)(void *));
int hc_processor_builder_add_request_last(hc_processor_builder *b, hc_request_interceptor_fn fn,
                                          void *arg, void (*free_arg)(void *));
int hc_processor_builder_add_response_first(hc_processor_builder *b, hc_response_interceptor_fn fn,
                                            void *arg, void (*free_arg)(void *));
int hc_processor_builder_add_response_last(hc_processor_builder *b, hc_response_interceptor_fn fn,
                                           void *arg, void (*free_arg)(void *));

/* Standard interceptors, added last. Each only adds its header when the
 * message does not have it already. */
/* Host from the target, when the request has no authority and no Host. */
int hc_processor_builder_add_target_host(hc_processor_builder *b);
/* User-Agent: agent (copied). */
int hc_processor_builder_add_user_agent(hc_processor_builder *b, const char *agent);
/* Date, on requests with a body (as httpcore5's RequestDate). */
int hc_processor_builder_add_request_date(hc_processor_builder *b);
/* Each of these headers (copied), e.g. Accept or Authorization. */
int hc_processor_builder_add_default_headers(hc_processor_builder *b, const hc_headers *headers);
/* Date, on responses with status >= 200. */
int hc_processor_builder_add_response_date(hc_processor_builder *b);
/* Server: name (copied). */
int hc_processor_builder_add_response_server(hc_processor_builder *b, const char *name);

/* The processor; frees the builder either way. NULL on allocation failure. */
hc_processor *hc_processor_builder_build(hc_processor_builder *b);
/* Frees the builder and everything added (when not building). */
void          hc_processor_builder_free(hc_processor_builder *b);

/* Java: HttpProcessors.client(agent) - target host, user agent ("hcore"
 * if NULL) - and HttpProcessors.server(name) - date, server ("hcore" if
 * NULL). */
hc_processor *hc_processor_client_default(const char *agent);
hc_processor *hc_processor_server_default(const char *name);

/* Reference counting: requesters and servers take their own reference. */
hc_processor *hc_processor_ref(hc_processor *p);
void          hc_processor_unref(hc_processor *p);

/* Runs the interceptors (what the engine does; also usable directly). */
int hc_processor_process_request(const hc_processor *p, hc_http_request *request,
                                 const hc_entity *entity, hc_context *ctx);
int hc_processor_process_response(const hc_processor *p, hc_http_response *response,
                                  const hc_entity *entity, hc_context *ctx);

/* ------------------------------------------------------------------------ */
/* For the engine                                                           */
/* ------------------------------------------------------------------------ */

/* A context for one exchange. target is copied (client); remote too
 * (server). */
hc_context *hc_context_new(int is_server, const hc_host *target, const char *remote);
void        hc_context_free(hc_context *ctx);
void        hc_context_set_protocol(hc_context *ctx, int version, int is_tls);

#ifdef __cplusplus
}
#endif

#endif /* HC_PROCESSOR_H */
