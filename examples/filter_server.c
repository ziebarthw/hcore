/*
 * filter_server.c - port of HttpCore 5's AsyncServerFilterExample: request
 * filters in front of an application handler, as handler decorators
 * (hc.h, "Handler decorators").
 *
 *   Java (AsyncServerFilterExample)          C (this file)
 *   ---------------------------------------  ---------------------------------
 *   addFilterFirst("my-filter", ...)         hc_server_add_decorator_first(s,
 *                                              "my-filter", my_filter, NULL)
 *   replaceFilter(EXPECT_CONTINUE,           hc_server_add_decorator_last(s,
 *     new AbstractAsyncServerAuthFilter)       "auth", auth_filter, &AUTH)
 *   AsyncFilterChain.proceed()               calling the inner handler
 *   wrapped ResponseTrigger.submitResponse   a wrapper response channel
 *                                              (hc_channel.h) adding X-Filter
 *   register("*", AsyncServerRequestHandler  hc_basic_server_factory: the
 *     with a StringAsyncEntityConsumer)        whole body, then "Hello"
 *   IOReactorConfig soTimeout 15 s           hc_server_set_timeouts()
 *   shutdown hook, close(GRACEFUL)           SIGINT/SIGTERM: drain, then stop
 *
 * The filters run in list order, the first outermost: my-filter, then auth,
 * then the application. So /back-door is answered by my-filter before auth
 * is asked, and every other response - the application's "Hello" and auth's
 * 401 alike - passes back through my-filter, which adds "X-Filter:
 * My-Filter".
 *
 * One difference: in HttpCore, "Expect: 100-continue" is a filter, and the
 * Java example replaces it with the auth filter, so an unauthorized client
 * gets 401 instead of "100 Continue". In hcore the engine answers
 * 100-continue before any decorator runs, so such a client is sent "100
 * Continue", uploads its body, and then gets the 401. Pushes (pushPromise)
 * have no counterpart: hcore has no server push.
 *
 * Try it:
 *   curl -i http://127.0.0.1:8080/back-door                    200 Welcome
 *   curl -i http://127.0.0.1:8080/                             401
 *   curl -i -H 'Authorization: let me pass' http://127.0.0.1:8080/   200 Hello
 *
 * Usage: filter_server [port]
 */
#include "hc.h"
#include "hc_channel.h"
#include "hc_support.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

/* ======================================================================== */
/* my-filter: answers /back-door itself, marks every other response         */
/* ======================================================================== */

typedef struct my_filter {
    hc_server_handler    base;       /* must be first */
    hc_server_handler   *inner;      /* the rest of the chain */
    hc_response_channel *real;       /* the channel we were given */
    hc_response_channel *rc;         /* the one the chain gets */
    int                  proceeded;
} my_filter;

/* Java: the wrapped ResponseTrigger's submitResponse() - add the header,
 * then pass the response on. */
static int
my_filter_send(void *arg, const hc_response_head *head, const hc_entity *entity,
               hc_data_channel **body)
{
    my_filter       *f  = arg;
    hc_response_head h2 = *head;
    hc_headers      *h  = head->headers ? hc_headers_copy(head->headers) : hc_headers_new();
    int              rv;

    if (h == NULL) {
        return -1;
    }

    hc_headers_add(h, "X-Filter", "My-Filter");
    h2.headers = h;
    rv         = hc_response_submit(f->real, &h2, entity, body);
    hc_headers_free(h);
    return rv;
}

/* Java: sendInformation(), passed on unchanged. */
static int
my_filter_send_information(void *arg, const hc_response_head *head)
{
    return hc_response_send_information(((my_filter *)arg)->real, head);
}

static const hc_response_channel_ops MY_FILTER_RC = {
    my_filter_send, my_filter_send_information
};

static void
my_filter_handle_request(hc_server_handler *self, hc_server_exchange *ex,
                         const hc_request_head *head, const hc_entity *entity,
                         hc_response_channel *rc)
{
    my_filter *f = (my_filter *)self;

    if (strcmp(head->target, "/back-door") == 0) {
        hc_response_send_simple(rc, 200, "text/plain", "Welcome", 0);
        return;     /* not proceeding: the chain never sees this request */
    }

    f->real = rc;

    if ((f->rc = hc_response_channel_new(&MY_FILTER_RC, f)) == NULL) {
        hc_server_abort(ex);
        return;
    }

    f->proceeded = 1;   /* Java: chain.proceed(..., wrapped trigger) */
    f->inner->vtbl->handle_request(f->inner, ex, head, entity, f->rc);
}

static void
my_filter_consume(hc_server_handler *self, const void *data, size_t len)
{
    my_filter *f = (my_filter *)self;

    if (f->proceeded) {
        f->inner->vtbl->consume(f->inner, data, len);
    }
}

static void
my_filter_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    my_filter *f = (my_filter *)self;

    if (f->proceeded) {
        f->inner->vtbl->stream_end(f->inner, trailers);
    }
}

static void
my_filter_produce(hc_server_handler *self, hc_data_channel *ch)
{
    my_filter *f = (my_filter *)self;

    if (f->proceeded && f->inner->vtbl->produce) {
        f->inner->vtbl->produce(f->inner, ch);
    }
}

static void
my_filter_failed(hc_server_handler *self, const char *reason)
{
    my_filter *f = (my_filter *)self;

    if (f->proceeded) {
        f->inner->vtbl->failed(f->inner, reason);
    }
}

static void
my_filter_release(hc_server_handler *self)
{
    my_filter *f = (my_filter *)self;

    f->inner->vtbl->release(f->inner);     /* the chain is ours to release */
    hc_response_channel_free(f->rc);       /* after its last use */
    free(f);
}

static const hc_server_handler_vtbl MY_FILTER_VTBL = {
    .handle_request = my_filter_handle_request,
    .consume        = my_filter_consume,
    .stream_end     = my_filter_stream_end,
    .produce        = my_filter_produce,
    .failed         = my_filter_failed,
    .release        = my_filter_release,
};

/* hc_server_decorator: wraps the chain for one exchange. */
static hc_server_handler *
my_filter_decorator(void *arg, hc_server_handler *inner)
{
    my_filter *f = calloc(1, sizeof(*f));

    (void)arg;

    if (f == NULL) {
        return NULL;    /* the server answers 503 */
    }

    f->base.vtbl = &MY_FILTER_VTBL;
    f->inner     = inner;
    return &f->base;
}

/* ======================================================================== */
/* auth: AbstractAsyncServerAuthFilter                                       */
/* ======================================================================== */

/* The abstract methods, as functions. Java's parseChallengeResponse()
 * returns the Authorization value as it is, so authenticate() and
 * generate_challenge() get that value (NULL without the header). */
typedef struct auth_config {
    int         respond_immediately;
    int         (*authenticate)(const char *challenge_response, const char *authority,
                                const char *request_uri);
    const char *(*generate_challenge)(const char *challenge_response, const char *authority,
                                      const char *request_uri);
} auth_config;

typedef struct auth_filter {
    hc_server_handler    base;
    hc_server_handler   *inner;
    const auth_config   *cfg;
    hc_response_channel *rc;
    char                *challenge;     /* a 401 owed at the end of the body */
    int                  proceeded;
} auth_filter;

static void
auth_send_unauthorized(hc_response_channel *rc, const char *challenge)
{
    hc_headers      *h    = hc_headers_new();
    hc_response_head head = { 401, 11, h };
    hc_entity        e    = { 12, "text/plain" };
    hc_data_channel *body;

    hc_headers_add(h, "WWW-Authenticate", challenge);

    /* Java: generateResponseContent() */
    if ((body = hc_response_send(rc, &head, &e)) != NULL) {
        hc_data_write(body, "Unauthorized", 12);
        hc_data_end(body, NULL);
    }

    hc_headers_free(h);
}

static void
auth_handle_request(hc_server_handler *self, hc_server_exchange *ex,
                    const hc_request_head *head, const hc_entity *entity,
                    hc_response_channel *rc)
{
    auth_filter *a    = (auth_filter *)self;
    const char  *cred = hc_headers_get(head->headers, "Authorization");
    const char  *expect;

    if (a->cfg->authenticate(cred, head->authority, head->target)) {
        /* Java sends "100 Continue" here first; hcore's engine already has
         * (see the top of the file). */
        a->proceeded = 1;
        a->inner->vtbl->handle_request(a->inner, ex, head, entity, rc);
        return;
    }

    expect = hc_headers_get(head->headers, "Expect");

    if (a->cfg->respond_immediately || entity == NULL ||
        (expect && strcasecmp(expect, "100-continue") == 0)) {
        auth_send_unauthorized(rc, a->cfg->generate_challenge(cred, head->authority,
                                                              head->target));
        return;
    }

    /* Read the body to the end, discarding it, then answer. */
    a->rc        = rc;
    a->challenge = strdup(a->cfg->generate_challenge(cred, head->authority, head->target));
}

static void
auth_consume(hc_server_handler *self, const void *data, size_t len)
{
    auth_filter *a = (auth_filter *)self;

    if (a->proceeded) {
        a->inner->vtbl->consume(a->inner, data, len);
    }   /* else: discarded */
}

static void
auth_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    auth_filter *a = (auth_filter *)self;

    if (a->proceeded) {
        a->inner->vtbl->stream_end(a->inner, trailers);
    } else if (a->challenge) {
        auth_send_unauthorized(a->rc, a->challenge);
    }
}

static void
auth_produce(hc_server_handler *self, hc_data_channel *ch)
{
    auth_filter *a = (auth_filter *)self;

    if (a->proceeded && a->inner->vtbl->produce) {
        a->inner->vtbl->produce(a->inner, ch);
    }
}

static void
auth_failed(hc_server_handler *self, const char *reason)
{
    auth_filter *a = (auth_filter *)self;

    if (a->proceeded) {
        a->inner->vtbl->failed(a->inner, reason);
    }
}

static void
auth_release(hc_server_handler *self)
{
    auth_filter *a = (auth_filter *)self;

    a->inner->vtbl->release(a->inner);
    free(a->challenge);
    free(a);
}

static const hc_server_handler_vtbl AUTH_VTBL = {
    .handle_request = auth_handle_request,
    .consume        = auth_consume,
    .stream_end     = auth_stream_end,
    .produce        = auth_produce,
    .failed         = auth_failed,
    .release        = auth_release,
};

static hc_server_handler *
auth_decorator(void *arg, hc_server_handler *inner)
{
    auth_filter *a = calloc(1, sizeof(*a));

    if (a == NULL) {
        return NULL;
    }

    a->base.vtbl = &AUTH_VTBL;
    a->inner     = inner;
    a->cfg       = arg;
    return &a->base;
}

/* The example's anonymous subclass. */
static int
example_authenticate(const char *challenge_response, const char *authority,
                     const char *request_uri)
{
    (void)authority;
    (void)request_uri;
    return challenge_response && strcmp(challenge_response, "let me pass") == 0;
}

static const char *
example_challenge(const char *challenge_response, const char *authority,
                  const char *request_uri)
{
    (void)challenge_response;
    (void)authority;
    (void)request_uri;
    return "who goes there?";
}

static const auth_config AUTH = { 1, example_authenticate, example_challenge };

/* ======================================================================== */
/* The application                                                          */
/* ======================================================================== */

/* Java: a BasicRequestConsumer with a StringAsyncEntityConsumer - the
 * basic server hands over the whole request, body included. */
static void
app_request(void *arg, hc_basic_request *r)
{
    (void)arg;
    /* do something useful */
    hc_response_send_simple(hc_basic_request_channel(r), 200, "text/plain", "Hello", 0);
}

static hc_basic_server_config APP = { .request = app_request };

/* ======================================================================== */

/* Java: the shutdown hook's close(GRACEFUL). The first SIGINT/SIGTERM
 * stops the loop; main() then drains the server (no new connections, the
 * exchanges in progress finish) and runs the loop again until the last
 * connection has closed. A second signal ends the wait. */
typedef struct drain {
    hc_reactor *r;
    hc_server  *s;
    hc_timer   *t;
} drain;

static void
drain_check(void *arg)
{
    drain *d = arg;

    if (hc_server_connection_count(d->s) == 0) {
        hc_reactor_stop(d->r);
    } else {
        hc_timer_start(d->t, 100);
    }
}

int
main(int argc, char **argv)
{
    int         port = argc >= 2 ? atoi(argv[1]) : 8080;
    hc_reactor *r    = hc_reactor_new();
    hc_server  *s    = r ? hc_server_new(r, hc_basic_server_factory, &APP) : NULL;
    drain       d;

    if (s == NULL) {
        return 1;
    }

    hc_server_set_timeouts(s, 15000, 15000);   /* Java: setSoTimeout(15 s) */

    /* Filters, outermost first: my-filter, then auth, then the application. */
    if (hc_server_add_decorator_last(s, "auth", auth_decorator, (void *)&AUTH) != 0 ||
        hc_server_add_decorator_first(s, "my-filter", my_filter_decorator, NULL) != 0) {
        return 1;
    }

    if (hc_server_listen(s, "0.0.0.0", (uint16_t)port) != 0) {
        fprintf(stderr, "cannot listen on port %d\n", port);
        return 1;
    }

    hc_reactor_stop_on_signals(r);
    printf("Listening on 0.0.0.0:%d\n", port);
    fflush(stdout);
    hc_reactor_run(r);

    printf("HTTP server shutting down\n");
    fflush(stdout);
    d.r = r;
    d.s = s;
    d.t = hc_timer_new(r, drain_check, &d);
    hc_server_drain(s);
    hc_timer_start(d.t, 0);
    hc_reactor_run(r);

    hc_timer_free(d.t);
    hc_server_free(s);
    hc_reactor_free(r);
    return 0;
}
