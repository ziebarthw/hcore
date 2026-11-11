/*
 * hc_channel.h - channels implemented by the application: a response,
 * request or data channel whose calls go to functions you supply instead
 * of to the engine (Java: your own implementation of the interfaces
 * ResponseChannel, RequestChannel and DataStreamChannel).
 *
 *   Java (httpcore5)                     C (this header)
 *   -----------------------------------  ------------------------------------
 *   implements DataStreamChannel         hc_data_channel_new(ops, arg)
 *   implements ResponseChannel           hc_response_channel_new(ops, arg)
 *   implements RequestChannel            hc_request_channel_new(ops, arg)
 *   ResponseChannel.sendResponse()       hc_response_submit()
 *   RequestChannel.sendRequest()         hc_request_submit()
 *
 * The usual use is a wrapper: a handler that sits in front of another
 * handler gives it its own channel, looks at or changes what the inner
 * handler sends (a header, the body), and passes it on to the channel the
 * engine gave the wrapper. The inner handler cannot tell the difference:
 * hc_response_send(), hc_data_write() and the rest work on either kind.
 *
 * A channel made here is the caller's: the engine never frees or keeps it,
 * and never hands it to a handler itself. Free it with the matching
 * _free() once nothing will call it again - for a channel given to an
 * inner handler, after that handler's ->release().
 *
 * Message framing stays the engine's: a wrapper that changes a body's
 * length sends the head on with an entity of unknown length (-1), and the
 * engine picks chunked encoding (HTTP/1.1) or plain DATA frames (HTTP/2).
 * Response interceptors (hc_processor.h) run inside the engine's channel,
 * so they see the head as it is finally sent, after every wrapper.
 */
#ifndef HC_CHANNEL_H
#define HC_CHANNEL_H

#include "hc.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Send a response head; on success *body is the body channel, or NULL when
 * entity is NULL. Returns 0, or -1 on failure (then *body is NULL).
 * hc_response_send() is this without the status: it returns NULL both for
 * "sent, no body" and for a failure. Java: ResponseChannel.sendResponse(). */
int hc_response_submit(hc_response_channel *ch, const hc_response_head *head,
                       const hc_entity *entity, hc_data_channel **body);

/* The same for a request head (client side). Java: RequestChannel.sendRequest(). */
int hc_request_submit(hc_request_channel *ch, const hc_request_head *head,
                      const hc_entity *entity, hc_data_channel **body);

/* ------------------------------------------------------------------------ */
/* Data channels                                                            */
/* ------------------------------------------------------------------------ */

/* hc_data_write() and the others call these with the arg given to
 * hc_data_channel_new(). Each has the contract of the function it stands
 * for (see hc.h), which is what the handler writing to the channel relies
 * on. In particular: write() returns the bytes taken, or -1; capacity()
 * returning 0 promises a ->produce() call to the writing handler once
 * there is room again. write, end and capacity are required;
 * request_output may be NULL (hc_data_request_output() then does nothing). */
typedef struct hc_data_channel_ops {
    long   (*write)(void *arg, const void *data, size_t len);
    int    (*end)(void *arg, const hc_headers *trailers);
    size_t (*capacity)(void *arg);
    void   (*request_output)(void *arg);
} hc_data_channel_ops;

/* NULL if a required function is missing or out of memory. ops is not
 * copied: it must outlive the channel (a static table, normally). */
hc_data_channel *hc_data_channel_new(const hc_data_channel_ops *ops, void *arg);
/* Frees a channel made by hc_data_channel_new(); does nothing for NULL or
 * for a channel the engine handed out. */
void             hc_data_channel_free(hc_data_channel *ch);

/* ------------------------------------------------------------------------ */
/* Response channels (server side)                                          */
/* ------------------------------------------------------------------------ */

/* send is required; it has hc_response_submit()'s contract, and is what
 * hc_response_send() and hc_response_send_simple() call too.
 * send_information may be NULL: hc_response_send_information() then
 * returns -1. */
typedef struct hc_response_channel_ops {
    int (*send)(void *arg, const hc_response_head *head, const hc_entity *entity,
                hc_data_channel **body);
    int (*send_information)(void *arg, const hc_response_head *head);
} hc_response_channel_ops;

hc_response_channel *hc_response_channel_new(const hc_response_channel_ops *ops, void *arg);
void                 hc_response_channel_free(hc_response_channel *ch);

/* ------------------------------------------------------------------------ */
/* Request channels (client side)                                           */
/* ------------------------------------------------------------------------ */

/* send is required (hc_request_submit()'s contract; hc_request_send()
 * calls it too). exchange may be NULL: hc_request_channel_exchange() then
 * returns NULL. A wrapper normally returns the exchange of the channel it
 * wraps, so the inner handler can still suspend input or cancel. */
typedef struct hc_request_channel_ops {
    int                 (*send)(void *arg, const hc_request_head *head,
                                const hc_entity *entity, hc_data_channel **body);
    hc_client_exchange *(*exchange)(void *arg);
} hc_request_channel_ops;

hc_request_channel *hc_request_channel_new(const hc_request_channel_ops *ops, void *arg);
void                hc_request_channel_free(hc_request_channel *ch);

#ifdef __cplusplus
}
#endif

#endif /* HC_CHANNEL_H */
