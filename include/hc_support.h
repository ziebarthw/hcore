/*
 * hc_support.h - request producers, response consumers and a basic client
 * exchange handler built from them. Modelled on httpcore5's
 * org.apache.hc.core5.http.nio.support and .nio.entity packages.
 *
 * Engine-independent: implemented purely on top of hc.h.
 *
 *   Java (httpcore5)                          C
 *   ----------------------------------------  ------------------------------
 *   AsyncEntityProducer                       hc_entity_producer (+ _vtbl)
 *   BasicAsyncEntityProducer (byte[])         hc_bytes_entity_producer_new()
 *   StringAsyncEntityProducer                 hc_string_entity_producer_new()
 *   AsyncRequestBuilder / BasicRequestProducer  hc_request_producer
 *   AsyncEntityConsumer<T>                    hc_entity_consumer (+ _vtbl)
 *   BasicAsyncEntityConsumer / StringAsync... hc_buffer_entity_consumer_new()
 *   BasicResponseConsumer<T>                  built into the basic handler
 *   Message<HttpResponse, T>                  hc_message
 *   FutureCallback<T>                         hc_result_callback
 *   BasicClientExchangeHandler<T>             hc_basic_client_handler_new()
 *   HttpAsyncRequester.execute(producer,
 *       consumer, timeout, callback)          hc_requester_execute_request()
 *
 * Ownership: producers and consumers handed to a handler (or to
 * hc_requester_execute_request()) belong to it from then on and are
 * released through their ->release() when the exchange ends.
 */
#ifndef HC_SUPPORT_H
#define HC_SUPPORT_H

#include "hc.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------------ */
/* Entity producers (request bodies)                                         */
/* ------------------------------------------------------------------------ */

typedef struct hc_entity_producer hc_entity_producer;

typedef struct hc_entity_producer_vtbl {
    /* Write body bytes while hc_data_capacity(ch) > 0, and hc_data_end()
     * once everything is written. Called again whenever the channel has
     * room, until the body has ended. */
    void (*produce)(hc_entity_producer *self, hc_data_channel *ch);
    /* Optional: the exchange failed before the body was complete. */
    void (*failed)(hc_entity_producer *self, const char *reason);
    /* Last call. */
    void (*release)(hc_entity_producer *self);
} hc_entity_producer_vtbl;

struct hc_entity_producer {
    const hc_entity_producer_vtbl *vtbl;
    hc_entity                      details;  /* length (-1: chunked), type */
};

/* The data is copied. content_type may be NULL. */
hc_entity_producer *hc_bytes_entity_producer_new(const void *data, size_t len,
                                                 const char *content_type);
/* content_type NULL -> "text/plain; charset=UTF-8" */
hc_entity_producer *hc_string_entity_producer_new(const char *text,
                                                  const char *content_type);

/* ------------------------------------------------------------------------ */
/* Request producer (method, URI, headers, optional body)                    */
/* ------------------------------------------------------------------------ */

typedef struct hc_request_producer hc_request_producer;

/* uri: "http[s]://host[:port][/path?query]". Takes ownership of entity
 * (may be NULL), also when it returns NULL for a bad URI. */
hc_request_producer *hc_request_producer_new(const char *method, const char *uri,
                                             hc_entity_producer *entity);
int            hc_request_producer_add_header(hc_request_producer *p,
                                              const char *name, const char *value);
const hc_host *hc_request_producer_target(const hc_request_producer *p);
/* Only for a producer that was never handed to a handler. */
void           hc_request_producer_free(hc_request_producer *p);

/* ------------------------------------------------------------------------ */
/* Entity consumers (response bodies)                                        */
/* ------------------------------------------------------------------------ */

typedef struct hc_entity_consumer hc_entity_consumer;

typedef struct hc_entity_consumer_vtbl {
    /* Optional: a body follows, described by entity. */
    void (*stream_start)(hc_entity_consumer *self, const hc_entity *entity);
    /* Body bytes. Return 0 to continue, -1 to abort the exchange (the
     * result callback then fails with "response body rejected"). */
    int  (*consume)(hc_entity_consumer *self, const void *data, size_t len);
    /* Optional: body complete. */
    void (*stream_end)(hc_entity_consumer *self, const hc_headers *trailers);
    /* Optional: the exchange failed. */
    void (*failed)(hc_entity_consumer *self, const char *reason);
    /* Last call. */
    void (*release)(hc_entity_consumer *self);
} hc_entity_consumer_vtbl;

struct hc_entity_consumer {
    const hc_entity_consumer_vtbl *vtbl;
};

/* Collects the body in memory, up to max_bytes (0: 16 MiB); more aborts the
 * exchange. Java: BasicAsyncEntityConsumer / StringAsyncEntityConsumer. */
hc_entity_consumer *hc_buffer_entity_consumer_new(size_t max_bytes);
/* The body so far; always NUL-terminated (not counted in *len). */
const char         *hc_buffer_entity_consumer_data(const hc_entity_consumer *c, size_t *len);

/* ------------------------------------------------------------------------ */
/* Basic client exchange handler + result callback                           */
/* ------------------------------------------------------------------------ */

/* Java: Message<HttpResponse, T>. Valid only during ->completed(). */
typedef struct hc_message {
    const hc_response_head *head;
    hc_entity_consumer     *body;   /* the consumer given; NULL if none     */
} hc_message;

/* Java: FutureCallback<T>; exactly one of completed/failed is called.
 * information is optional (1xx responses, Java: consumeInformation). */
typedef struct hc_result_callback {
    void (*completed)(void *arg, const hc_message *response);
    void (*failed)(void *arg, const char *reason);
    void (*information)(void *arg, const hc_response_head *head);
    /* Optional: called last, after completed/failed, when everything the
     * exchange owned has been released (e.g. to start the next request). */
    void (*released)(void *arg);
    void  *arg;
} hc_result_callback;

/* An hc_client_handler that sends the producer's request and feeds the
 * response body to the consumer (NULL: discard the body). Returns NULL on
 * allocation failure (nothing is taken over then). */
hc_client_handler *hc_basic_client_handler_new(hc_request_producer *request,
                                               hc_entity_consumer *body,
                                               const hc_result_callback *cb);

/* Java: HttpAsyncRequester.execute(requestProducer, responseConsumer,
 * timeout, callback). The target is the request producer's URI host; the
 * connection comes from the requester's pool. Returns 0 (the callback will
 * run), or -1 with nothing taken over. */
int hc_requester_execute_request(hc_requester *rq, hc_request_producer *request,
                                 hc_entity_consumer *body, int64_t timeout_ms,
                                 const hc_result_callback *cb);

#ifdef __cplusplus
}
#endif

#endif /* HC_SUPPORT_H */
