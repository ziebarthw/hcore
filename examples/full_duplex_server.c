/*
 * full_duplex_server.c - a C port of httpcore5's AsyncFullDuplexServerExample.
 *
 * Usage: full_duplex_server [port] [--tls CERT.pem KEY.pem]
 *
 * An HTTP/1.1 server with one handler, registered at "/echo", that streams
 * the request body back while it is still arriving: the response head is
 * sent as soon as the request head is in, and every chunk of request body
 * is written straight back. Other paths get "404 Resource not found".
 *
 * Java -> C:
 *   AsyncServerBootstrap.bootstrap()            hc_server_new(r, NULL, NULL)
 *     .register("/echo", () -> new handler)     hc_server_register(s, "/echo", ...)
 *     .setStreamListener(Http1StreamListener)   hc_server_set_listener()
 *   AsyncServerExchangeHandler (anonymous)      echo_handler + echo_vtbl
 *   ByteBuffer.allocate(2048) + ensureCapacity  relay_buffer (grows from 2048)
 *   CapacityChannel.update(...)                 hc_server_suspend_input /
 *                                               hc_server_resume_input
 *   DataStreamChannel.requestOutput()           hc_data_request_output()
 *   server.listen(new InetSocketAddress(port))  hc_server_listen(s, "0.0.0.0", port)
 *   shutdown hook                               SIGINT/SIGTERM -> "HTTP server shutting down"
 *
 * --tls is an addition: the Java example is plain HTTP.
 */
#include "hc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INIT_BUFFER_SIZE 2048   /* Java: ByteBuffer.allocate(2048) */

/* ------------------------------------------------------------------------ */
/* Relay buffer (Java: the handler's ByteBuffer)                            */
/* ------------------------------------------------------------------------ */

typedef struct relay_buffer {
    unsigned char *data;
    size_t         off;     /* start of unsent bytes */
    size_t         len;     /* unsent bytes */
    size_t         cap;
} relay_buffer;

/* Java: ensureCapacity(chunk) - grow by at least INIT_BUFFER_SIZE. */
static int
relay_append(relay_buffer *b, const void *src, size_t n)
{
    if (b->off + b->len + n > b->cap) {
        if (b->off) {
            memmove(b->data, b->data + b->off, b->len);
            b->off = 0;
        }

        if (b->len + n > b->cap) {
            size_t         ncap = b->len + (n > INIT_BUFFER_SIZE ? n : INIT_BUFFER_SIZE);
            unsigned char *nd   = realloc(b->data, ncap);

            if (nd == NULL) {
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

/* Write what the channel will take; returns bytes written. */
static size_t
relay_flush(relay_buffer *b, hc_data_channel *ch)
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

/* ------------------------------------------------------------------------ */
/* The /echo handler (Java: the anonymous AsyncServerExchangeHandler)       */
/* ------------------------------------------------------------------------ */

typedef struct echo_handler {
    hc_server_handler   base;           /* the "interface"; must be first */
    hc_server_exchange *ex;
    hc_data_channel    *out;            /* Java: outputDataChannel */
    relay_buffer        buf;            /* Java: buffer */
    int                 end_stream;     /* Java: endStream */
    int                 input_suspended;/* Java: inputCapacityChannel != null */
} echo_handler;

#define ECHO(self) ((echo_handler *)(void *)(self))

/* Java: handleRequest - the response goes out at once, with the request's
 * entity details, before any of the body has arrived. */
static void
echo_handle_request(hc_server_handler *self, hc_server_exchange *ex,
                    const hc_request_head *head, const hc_entity *entity,
                    hc_response_channel *rc)
{
    echo_handler    *e = ECHO(self);
    hc_headers      *h = hc_headers_new();
    hc_response_head response;

    (void)head;
    e->ex = ex;

    if (entity && entity->content_type) {
        hc_headers_add(h, "Content-Type", entity->content_type);
    }

    response.status  = 200;
    response.version = 11;
    response.headers = h;
    e->out           = hc_response_send(rc, &response, entity);   /* NULL: no body */
    hc_headers_free(h);
}

/* Java: consume - write through while nothing is queued, buffer the rest. */
static void
echo_consume(hc_server_handler *self, const void *data, size_t len)
{
    echo_handler        *e = ECHO(self);
    const unsigned char *p = data;

    if (e->out == NULL) {
        return;
    }

    if (e->buf.len == 0) {
        size_t room = hc_data_capacity(e->out);
        size_t n    = len < room ? len : room;

        if (n && hc_data_write(e->out, p, n) == (long)n) {
            p   += n;
            len -= n;
        }
    }

    if (len) {
        relay_append(&e->buf, p, len);
        hc_data_request_output(e->out);
    }

    /* Java: updateCapacity() with no room left keeps the capacity channel
     * for later, so the client is not read until produce() has drained. */
    if (e->buf.len >= INIT_BUFFER_SIZE && !e->input_suspended) {
        e->input_suspended = 1;
        hc_server_suspend_input(e->ex);
    }
}

/* Java: streamEnd */
static void
echo_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    echo_handler *e = ECHO(self);

    (void)trailers;
    e->end_stream = 1;

    if (e->out == NULL) {
        return;
    }

    if (e->buf.len == 0) {
        hc_data_end(e->out, NULL);
        e->out = NULL;
    } else {
        hc_data_request_output(e->out);
    }
}

/* Java: produce - flush the buffer, end the stream once drained, and hand
 * capacity back to the input side. */
static void
echo_produce(hc_server_handler *self, hc_data_channel *ch)
{
    echo_handler *e = ECHO(self);

    relay_flush(&e->buf, ch);

    if (e->buf.len == 0 && e->end_stream && e->out) {
        hc_data_end(ch, NULL);
        e->out = NULL;
    }

    if (e->input_suspended && e->buf.len < INIT_BUFFER_SIZE) {
        e->input_suspended = 0;
        hc_server_resume_input(e->ex);
    }
}

/* Java: failed - print, except for the client simply going away
 * (SocketException there). */
static void
echo_failed(hc_server_handler *self, const char *reason)
{
    (void)self;

    if (strcmp(reason, "client connection closed") != 0) {
        printf("exchange failed: %s\n", reason);
        fflush(stdout);
    }
}

/* Java: releaseResources */
static void
echo_release(hc_server_handler *self)
{
    echo_handler *e = ECHO(self);

    free(e->buf.data);
    free(e);
}

static const hc_server_handler_vtbl echo_vtbl = {
    .handle_request = echo_handle_request,
    .consume        = echo_consume,
    .stream_end     = echo_stream_end,
    .produce        = echo_produce,
    .failed         = echo_failed,
    .release        = echo_release,
};

/* Java: () -> new AsyncServerExchangeHandler() { ... } */
static hc_server_handler *
echo_factory(void *arg)
{
    echo_handler *e = calloc(1, sizeof(*e));

    (void)arg;

    if (e == NULL) {
        return NULL;
    }

    e->base.vtbl = &echo_vtbl;

    return &e->base;
}

/* ------------------------------------------------------------------------ */
/* Java: Http1StreamListener                                                */
/* ------------------------------------------------------------------------ */

static void
on_request_head(void *arg, const char *remote, const hc_request_head *head)
{
    (void)arg;
    printf("%s %s %s HTTP/%s\n", remote, head->method, head->target,
           head->version == 10 ? "1.0" : "1.1");
    fflush(stdout);
}

static void
on_response_head(void *arg, const char *remote, const hc_response_head *head)
{
    (void)arg;
    printf("%s HTTP/%s %d\n", remote, head->version == 10 ? "1.0" : "1.1", head->status);
    fflush(stdout);
}

static void
on_exchange_complete(void *arg, const char *remote, int keep_alive)
{
    (void)arg;
    printf("%s exchange completed (connection %s)\n", remote,
           keep_alive ? "kept alive" : "closed");
    fflush(stdout);
}

/* ------------------------------------------------------------------------ */

int
main(int argc, char **argv)
{
    int                port = 8080;
    const char        *cert = NULL, *key = NULL;
    hc_reactor        *r;
    hc_server         *s;
    hc_server_listener l = { on_request_head, on_response_head, on_exchange_complete, NULL };

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--tls") == 0 && i + 2 < argc) {
            cert = argv[++i];
            key  = argv[++i];
        } else {
            port = atoi(argv[i]);       /* Java: Integer.parseInt(args[0]) */
        }
    }

    if ((r = hc_reactor_new()) == NULL ||
        (s = hc_server_new(r, NULL, NULL)) == NULL ||   /* unmatched: 404 */
        hc_server_register(s, "/echo", echo_factory, NULL) != 0) {
        fprintf(stderr, "initialization failed\n");
        return 1;
    }

    hc_server_set_listener(s, &l);

    if (cert && hc_server_enable_tls(s, cert, key) != 0) {
        fprintf(stderr, "cannot load TLS certificate %s / key %s\n", cert, key);
        return 1;
    }

    if (hc_server_listen(s, "0.0.0.0", (uint16_t)port) != 0) {
        fprintf(stderr, "cannot listen on port %d\n", port);
        return 1;
    }

    hc_reactor_stop_on_signals(r);      /* Java: the shutdown hook */
    printf("Listening on 0.0.0.0:%d%s\n", port, cert ? " (https)" : "");
    fflush(stdout);

    hc_reactor_run(r);                  /* Java: server.awaitShutdown(...) */

    printf("HTTP server shutting down\n");
    hc_server_free(s);
    hc_reactor_free(r);

    return 0;
}
