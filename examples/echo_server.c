/*
 * echo_server.c - smallest useful hc_server_handler.
 *
 * Streams the request body straight back as a chunked response while it is
 * still arriving, suspending input whenever the client reads too slowly.
 * Requests without a body get a short text reply.
 *
 * Usage: echo_server [--tls CERT.pem KEY.pem] [port]
 */
#include "hc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct echo_handler {
    hc_server_handler   base;      /* the "interface"; must be first */
    hc_server_exchange *ex;
    hc_data_channel    *out;
    int                 suspended;
} echo_handler;

#define ECHO(self) ((echo_handler *)(void *)(self))

static void
echo_handle_request(hc_server_handler *self, hc_server_exchange *ex,
                    const hc_request_head *head, const hc_entity *entity,
                    hc_response_channel *rc)
{
    echo_handler    *e = ECHO(self);
    hc_headers      *h;
    hc_response_head resp;
    hc_entity        body = { -1, NULL };   /* unknown length: chunked */
    char             msg[512];

    e->ex = ex;

    if (entity == NULL) {
        snprintf(msg, sizeof(msg), "%s %s: send me a body and I will echo it\n",
                 head->method, head->target);
        hc_response_send_simple(rc, 200, "text/plain", msg, 0);
        return;
    }

    h = hc_headers_new();
    hc_headers_add(h, "Content-Type",
                   entity->content_type ? entity->content_type : "application/octet-stream");
    resp.status  = 200;
    resp.version = 11;
    resp.headers = h;
    e->out       = hc_response_send(rc, &resp, &body);
    hc_headers_free(h);
}

static void
echo_consume(hc_server_handler *self, const void *data, size_t len)
{
    echo_handler *e = ECHO(self);

    if (e->out == NULL) {
        return;
    }

    hc_data_write(e->out, data, len);

    if (hc_data_capacity(e->out) == 0 && !e->suspended) {
        e->suspended = 1;           /* resumed from echo_produce() */
        hc_server_suspend_input(e->ex);
    }
}

static void
echo_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    echo_handler *e = ECHO(self);

    (void)trailers;

    if (e->out) {
        hc_data_end(e->out, NULL);
    }
}

static void
echo_produce(hc_server_handler *self, hc_data_channel *ch)
{
    echo_handler *e = ECHO(self);

    if (e->suspended && hc_data_capacity(ch) > 0) {
        e->suspended = 0;
        hc_server_resume_input(e->ex);
    }
}

static void
echo_failed(hc_server_handler *self, const char *reason)
{
    (void)self;
    fprintf(stderr, "echo: exchange failed: %s\n", reason);
}

static void
echo_release(hc_server_handler *self)
{
    free(ECHO(self));
}

static const hc_server_handler_vtbl echo_vtbl = {
    .handle_request = echo_handle_request,
    .consume        = echo_consume,
    .stream_end     = echo_stream_end,
    .produce        = echo_produce,
    .failed         = echo_failed,
    .release        = echo_release,
};

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

int
main(int argc, char **argv)
{
    int         port = 8081;
    const char *cert = NULL, *key = NULL;
    hc_reactor *r    = hc_reactor_new();
    hc_server  *s    = r ? hc_server_new(r, echo_factory, NULL) : NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--tls") == 0 && i + 2 < argc) {
            cert = argv[++i];
            key  = argv[++i];
        } else {
            port = atoi(argv[i]);
        }
    }

    if (s == NULL) {
        return 1;
    }

    if (cert && hc_server_enable_tls(s, cert, key) != 0) {
        fprintf(stderr, "cannot load TLS certificate %s / key %s\n", cert, key);
        return 1;
    }

    if (hc_server_listen(s, "0.0.0.0", (uint16_t)port) != 0) {
        fprintf(stderr, "cannot start echo server on port %d\n", port);
        return 1;
    }

    hc_reactor_stop_on_signals(r);
    printf("echo server listening on %d%s\n", port, cert ? " (https)" : "");
    fflush(stdout);
    hc_reactor_run(r);
    hc_server_free(s);
    hc_reactor_free(r);

    return 0;
}
