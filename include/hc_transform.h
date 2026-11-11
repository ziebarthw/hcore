/*
 * hc_transform.h - transforming message bodies on their way in or out of a
 * handler: decompressing a request, compressing a response, and the like.
 *
 * A transformation is written once, as hc_transform_ops: bytes in, bytes
 * out, with no knowledge of channels, framing or flow control. A transform
 * handler wraps an application's handler and runs a transformation on
 * either body of its exchanges:
 *
 *   incoming body   ->consume() / ->stream_end() of the wrapped handler
 *                   receive the transformed bytes
 *   outgoing body   what the wrapped handler writes with hc_data_write() /
 *                   hc_data_end() is transformed before it is sent
 *
 * Java has no single counterpart: HttpClient's ContentCompressionAsyncExec does
 * this for responses with an InflatingAsyncDataConsumer, and requests are
 * compressed with a DeflatingAsyncEntityProducer. Here one mechanism serves
 * both sides of both kinds of exchange.
 *
 *   Java                                  C (this header)
 *   ------------------------------------  ------------------------------------
 *   ContentCompressionAsyncExec (client)  hc_client_transform_handler_new()
 *   InflatingAsyncDataConsumer            an incoming hc_transform
 *   DeflatingAsyncEntityProducer          an outgoing hc_transform
 *
 * Which body is transformed, and how, is decided per exchange once its head
 * is known, by the choose functions in the config: they see the head with
 * its headers in a copy they may change (e.g. remove Content-Encoding from
 * a request about to be decompressed, add it to a response about to be
 * compressed) and return a transformation, or none.
 *
 * Once a body is transformed its length is no longer known: the wrapped
 * handler sees an incoming body of unknown length (-1) without a
 * Content-Length header, and an outgoing one is sent with unknown length
 * (chunked on HTTP/1.1, plain DATA frames on HTTP/2).
 *
 * Flow control: an outgoing transformation keeps what the real channel
 * cannot take yet in a pending buffer. hc_data_capacity() on the wrapped
 * handler's channel is the room left below a high-water mark (config, 64
 * KiB by default); at the mark it is 0, and the wrapped handler's
 * ->produce() is called once the pending bytes have been sent. A single
 * write can take the buffer past the mark by as much as that write expands
 * to, so a handler that respects the capacity keeps memory bounded by the
 * mark times the transformation's expansion.
 *
 * Errors: a transformation that fails (update, finish or flush returns -1)
 * aborts the exchange: the wrapped handler's ->failed() gets "body
 * transformation failed", then ->release().
 */
#ifndef HC_TRANSFORM_H
#define HC_TRANSFORM_H

#include "hc.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------------ */
/* Byte buffers                                                             */
/* ------------------------------------------------------------------------ */

/* A growable byte buffer: what a transformation appends its output to.
 * Zero-initialised it is empty and owns nothing. */
typedef struct hc_bytes {
    unsigned char *data;
    size_t         len;     /* bytes in use, from data[0] */
    size_t         cap;     /* bytes allocated */
} hc_bytes;

/* Append n bytes. 0, or -1 when out of memory (b unchanged). */
int   hc_bytes_append(hc_bytes *b, const void *p, size_t n);
/* For codecs that write in place (zlib and the like): room for at least n
 * more bytes, returned as a pointer to the first of them (NULL when out
 * of memory); then hc_bytes_commit() with the number actually written. */
void *hc_bytes_reserve(hc_bytes *b, size_t n);
void  hc_bytes_commit(hc_bytes *b, size_t n);
/* Drop the first n bytes (all, if n >= len). */
void  hc_bytes_consume(hc_bytes *b, size_t n);
/* Free the memory; b is empty again. */
void  hc_bytes_free(hc_bytes *b);

/* ------------------------------------------------------------------------ */
/* Transformations                                                          */
/* ------------------------------------------------------------------------ */

typedef struct hc_transform_ops {
    /* Turn len input bytes into output appended to out (possibly none:
     * a codec may hold data back). 0, or -1 on failure. */
    int  (*update)(void *state, const void *in, size_t len, hc_bytes *out);
    /* End of the body: append whatever is still held back. 0, or -1. */
    int  (*finish)(void *state, hc_bytes *out);
    /* Optional: append what is held back so far without ending (a
     * compressor's sync flush), for streamed responses such as server-sent
     * events. Used after every write when flush_each_write is set. */
    int  (*flush)(void *state, hc_bytes *out);
    /* Free the state. Called exactly once for every transformation a
     * choose function returns, whether or not it was used. */
    void (*free)(void *state);
} hc_transform_ops;

/* What a choose function returns: ops NULL means "leave this body as it is". */
typedef struct hc_transform {
    const hc_transform_ops *ops;
    void                   *state;
} hc_transform;

/* ------------------------------------------------------------------------ */
/* Server side                                                              */
/* ------------------------------------------------------------------------ */

/* Every choose function is optional (NULL: that body is never transformed)
 * and runs on the exchange's reactor thread, once for every head - also
 * one without a body (entity NULL), so that it can still change headers;
 * a transformation returned for no body is freed unused. headers is the
 * head's headers, a copy the function may change: the other side sees the
 * result. It returns 0, with t->ops set or left NULL, or - as interceptors
 * do (hc_processor.h) - a status from 400 to 599 to refuse the exchange:
 *
 *   server request   answered with that status; the wrapped handler never
 *                    sees it (it gets ->release() only)
 *   server response  the exchange is aborted (the head cannot be replaced)
 *   client request   not sent; the exchange fails
 *   client response  the exchange fails
 *
 * A refused exchange that fails reports "body transformation refused" to
 * the wrapped handler's ->failed(). */
typedef struct hc_server_transform_config {
    /* When the request head arrives. */
    int (*choose_request)(void *arg, const hc_request_head *request, hc_headers *headers,
                          const hc_entity *entity, hc_transform *t);
    /* When the wrapped handler sends the response head. request is the
     * request head as the wrapped handler saw it. *status may be changed
     * too. */
    int (*choose_response)(void *arg, const hc_request_head *request, int *status,
                           hc_headers *headers, const hc_entity *entity, hc_transform *t);
    void  *arg;
    size_t high_water;          /* 0: 64 KiB */
    int    flush_each_write;    /* flush() after every write (if it has one) */
} hc_server_transform_config;

/* Wrap inner: the returned handler stands in for it, and owns it (inner's
 * ->release() is called from the wrapper's). cfg is not copied: it must
 * outlive the handler. NULL when out of memory (inner is then untouched). */
hc_server_handler *hc_server_transform_handler_new(hc_server_handler *inner,
                                                   const hc_server_transform_config *cfg);

/* The same as an hc_server_decorator (hc.h), with the config as its arg:
 *   hc_server_add_decorator_last(s, "gzip", hc_server_transform_decorator, &cfg); */
hc_server_handler *hc_server_transform_decorator(void *cfg, hc_server_handler *inner);

/* ------------------------------------------------------------------------ */
/* Client side                                                              */
/* ------------------------------------------------------------------------ */

typedef struct hc_client_transform_config {
    /* When the wrapped handler sends the request head. */
    int (*choose_request)(void *arg, const hc_request_head *request, hc_headers *headers,
                          const hc_entity *entity, hc_transform *t);
    /* When the response head arrives. request is the request head as
     * sent: with the headers choose_request left (e.g. the Accept-Encoding
     * it added). */
    int (*choose_response)(void *arg, const hc_request_head *request, int status,
                           hc_headers *headers, const hc_entity *entity, hc_transform *t);
    void  *arg;
    size_t high_water;          /* 0: 64 KiB */
    int    flush_each_write;
} hc_client_transform_config;

/* As hc_server_transform_handler_new(). Interim (1xx) responses pass
 * through unchanged. */
hc_client_handler *hc_client_transform_handler_new(hc_client_handler *inner,
                                                   const hc_client_transform_config *cfg);

/* As an hc_client_decorator, for hc_requester_add_decorator_*(). */
hc_client_handler *hc_client_transform_decorator(void *cfg, hc_client_handler *inner);

#ifdef __cplusplus
}
#endif

#endif /* HC_TRANSFORM_H */
