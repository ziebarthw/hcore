/*
 * full_duplex_client.c - a C port of httpcore5's AsyncFullDuplexClientExample.
 *
 * Usage: full_duplex_client [-n COUNT] [--duplex MB] [--expect]
 *                           [--cafile PEM] [--insecure] <http[s]://host[:port]/path>
 *
 * Default mode is the Java example: POST the body "stuff" with a request
 * producer (Java: AsyncRequestBuilder.post(uri).setEntity("stuff")) and
 * collect the response with a buffering consumer (Java:
 * StringAsyncEntityConsumer), printing interim and final status codes.
 *
 * The Java example sends too little to show full duplex, so --duplex MB
 * plugs two custom implementations into the same basic handler: an entity
 * producer that streams MB megabytes of generated data, and an entity
 * consumer that verifies every echoed byte and records how far into the
 * upload the response started arriving. Against examples/echo_server that
 * is long before the upload ends: request and response stream at the same
 * time over one connection.
 *
 *   -n COUNT    run COUNT exchanges one after another (each starts when the
 *               previous one has released everything; one pooled connection)
 *   --expect    send "Expect: 100-continue": the server's "100 Continue"
 *               arrives through the information callback
 *   --cafile    trust this PEM CA/certificate for https (default: system)
 *   --insecure  skip TLS certificate verification
 *
 * Java -> C:
 *   AsyncRequesterBootstrap ... .create()          hc_requester_new()
 *   AsyncRequestBuilder ... setEntity("stuff")     hc_request_producer_new() +
 *                                                  hc_string_entity_producer_new()
 *   StringAsyncEntityConsumer                      hc_buffer_entity_consumer_new()
 *   anonymous AsyncClientExchangeHandler           hc_basic_client_handler (inside
 *     consumeInformation / consumeResponse ...       hc_requester_execute_request)
 *   requester.execute(handler, timeout, ctx)       hc_requester_execute_request()
 *   .setStreamListener(Http1StreamListener)        hc_requester_set_listener()
 *     onRequestHead / onResponseHead /               on_request_head / on_response_head /
 *     onExchangeComplete                             on_exchange_complete
 *   (no Java counterpart)                          pool listener: lease / release lines
 *   CountDownLatch / latch.await()                 hc_reactor_stop() when done
 */
#include "hc.h"
#include "hc_pool.h"
#include "hc_support.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define EXECUTE_TIMEOUT_MS 30000   /* Java: Timeout.ofSeconds(30) */
#define WRITE_SLICE        (16 * 1024)

static hc_reactor   *reactor;
static hc_requester *requester;
static const char   *url;
static int64_t       duplex_bytes;  /* 0: Java mode ("stuff") */
static int           send_expect;
static int           remaining_runs = 1;
static int           failures;

/* Deterministic request body: byte i of the stream. */
static unsigned char
pattern_at(int64_t i)
{
    return (unsigned char)((i * 7 + (i >> 11)) & 0xff);
}

static double
ms_since(int64_t t0)
{
    return (double)(hc_reactor_now_ms(reactor) - t0);
}

static void start_exchange(void);

/* ------------------------------------------------------------------------ */
/* Per-exchange state shared by the duplex producer and consumer            */
/* ------------------------------------------------------------------------ */

typedef struct run_stats {
    int64_t t0;
    int64_t sent;
    int64_t received;
    int64_t sent_at_first_byte;     /* -1: no response byte yet */
    double  ms_at_first_byte;
    double  ms_upload_done;
    int     mismatch;
} run_stats;

/* --- a custom AsyncEntityProducer: generated data, streamed ------------ */

typedef struct pattern_producer {
    hc_entity_producer base;        /* the "interface"; must be first */
    run_stats         *st;
} pattern_producer;

static void
pattern_produce(hc_entity_producer *self, hc_data_channel *ch)
{
    run_stats    *st = ((pattern_producer *)(void *)self)->st;
    unsigned char buf[WRITE_SLICE];
    size_t        room;

    while (st->sent < duplex_bytes && (room = hc_data_capacity(ch)) > 0) {
        int64_t left = duplex_bytes - st->sent;
        size_t  n    = room < sizeof(buf) ? room : sizeof(buf);

        if ((int64_t)n > left) {
            n = (size_t)left;
        }

        for (size_t i = 0; i < n; i++) {
            buf[i] = pattern_at(st->sent + (int64_t)i);
        }

        if (hc_data_write(ch, buf, n) < 0) {
            return;
        }

        st->sent += (int64_t)n;
    }
    /* capacity 0: produce() runs again once the socket drains - also while
     * the response is already streaming back */

    if (st->sent == duplex_bytes) {
        hc_data_end(ch, NULL);
        st->ms_upload_done = ms_since(st->t0);
    }
}

static void
pattern_release(hc_entity_producer *self)
{
    free(self);
}

static const hc_entity_producer_vtbl pattern_vtbl = {
    .produce = pattern_produce,
    .release = pattern_release,
};

/* --- a custom AsyncEntityConsumer: verifies the echo ---------------------- */

typedef struct verify_consumer {
    hc_entity_consumer base;        /* must be first */
    run_stats         *st;
} verify_consumer;

static int
verify_consume(hc_entity_consumer *self, const void *data, size_t len)
{
    run_stats           *st = ((verify_consumer *)(void *)self)->st;
    const unsigned char *p  = data;

    if (st->sent_at_first_byte < 0) {
        st->sent_at_first_byte = st->sent;
        st->ms_at_first_byte   = ms_since(st->t0);
    }

    for (size_t i = 0; i < len && !st->mismatch; i++) {
        if (p[i] != pattern_at(st->received + (int64_t)i)) {
            st->mismatch = 1;
            printf("  response differs from the request at byte %lld (not an echo server?)\n",
                   (long long)(st->received + (int64_t)i));
        }
    }

    st->received += (int64_t)len;

    return 0;   /* keep going even on mismatch, to report the totals */
}

static void
verify_release(hc_entity_consumer *self)
{
    free(self);
}

static const hc_entity_consumer_vtbl verify_vtbl = {
    .consume = verify_consume,
    .release = verify_release,
};

/* ------------------------------------------------------------------------ */
/* Result callback (Java: FutureCallback + the handler's print statements)  */
/* ------------------------------------------------------------------------ */

typedef struct run {
    run_stats st;
    int       ok;
} run;

/* Java: consumeInformation -> System.out.println(requestUri + "->" + code) */
static void
on_information(void *arg, const hc_response_head *head)
{
    const char *k, *v;

    (void)arg;
    printf("%s->%d (interim)", url, head->status);

    for (size_t i = 0; hc_headers_at(head->headers, i, &k, &v) == 0; i++) {
        printf(" [%s: %s]", k, v);
    }

    putchar('\n');
}

static void
on_completed(void *arg, const hc_message *response)
{
    run       *r  = arg;
    run_stats *st = &r->st;

    /* Java: consumeResponse -> System.out.println(requestUri + "->" + code) */
    printf("%s->%d\n", url, response->head->status);
    r->ok = response->head->status >= 200 && response->head->status < 300;

    if (!duplex_bytes) {
        size_t      len;
        const char *text = hc_buffer_entity_consumer_data(response->body, &len);

        printf("%s%s", text, len && text[len - 1] != '\n' ? "\n" : "");
        return;
    }

    r->ok = r->ok && !st->mismatch && st->received == duplex_bytes;
    printf("  sent %lld bytes, received %lld bytes, echo %s\n",
           (long long)st->sent, (long long)st->received,
           st->mismatch ? "MISMATCH" : st->received == duplex_bytes ? "verified" : "incomplete");

    if (st->sent_at_first_byte >= 0) {
        printf("  first response byte after %.0f ms, with %lld of %lld request bytes sent (%.1f%%)\n",
               st->ms_at_first_byte, (long long)st->sent_at_first_byte, (long long)duplex_bytes,
               100.0 * (double)st->sent_at_first_byte / (double)duplex_bytes);
    }

    printf("  upload finished after %.0f ms, response after %.0f ms -> %s\n",
           st->ms_upload_done, ms_since(st->t0),
           st->sent_at_first_byte >= 0 && st->sent_at_first_byte < duplex_bytes
               ? "full duplex: both directions streamed at the same time"
               : "half duplex: the server answered only after the whole request");
}

/* Java: failed(Exception) -> System.out.println(requestUri + "->" + cause) */
static void
on_failed(void *arg, const char *reason)
{
    (void)arg;
    printf("%s->%s\n", url, reason);
}

/* Java: releaseResources() -> latch.countDown() */
static void
on_released(void *arg)
{
    run *r = arg;

    if (!r->ok) {
        failures++;
    }

    free(r);

    if (--remaining_runs > 0) {
        start_exchange();       /* sequential: same pooled connection */
    } else {
        hc_reactor_stop(reactor);
    }
}

static void
start_exchange(void)
{
    run                 *r = calloc(1, sizeof(*r));
    hc_entity_producer  *entity;
    hc_entity_consumer  *body;
    hc_request_producer *req;
    hc_result_callback   cb = { on_completed, on_failed, on_information, on_released, r };

    if (r == NULL) {
        goto fail;
    }

    r->st.t0                 = hc_reactor_now_ms(reactor);
    r->st.sent_at_first_byte = -1;

    if (duplex_bytes) {
        pattern_producer *pp = calloc(1, sizeof(*pp));
        verify_consumer  *vc = calloc(1, sizeof(*vc));

        if (!pp || !vc) {
            free(pp);
            free(vc);
            goto fail;
        }

        pp->base.vtbl                   = &pattern_vtbl;
        pp->base.details.content_length = -1;           /* stream it: chunked */
        pp->base.details.content_type   = "application/octet-stream";
        pp->st                          = &r->st;
        vc->base.vtbl                   = &verify_vtbl;
        vc->st                          = &r->st;
        entity                          = &pp->base;
        body                            = &vc->base;
    } else {
        entity = hc_string_entity_producer_new("stuff", NULL);
        body   = hc_buffer_entity_consumer_new(0);
    }

    if ((req = hc_request_producer_new("POST", url, entity)) == NULL) {    /* takes entity */
        if (body) {
            body->vtbl->release(body);
        }

        goto fail;
    }

    if (send_expect) {
        hc_request_producer_add_header(req, "Expect", "100-continue");
    }

    if (hc_requester_execute_request(requester, req, body, EXECUTE_TIMEOUT_MS, &cb) != 0) {
        hc_request_producer_free(req);
        body->vtbl->release(body);
        goto fail;
    }

    return;

fail:
    free(r);
    failures++;
    hc_reactor_stop(reactor);
}

/* ------------------------------------------------------------------------ */
/* Java: .setStreamListener(new Http1StreamListener() { ... })              */
/* ------------------------------------------------------------------------ */

/* System.out.println(connection.getRemoteAddress() + " " + new RequestLine(request)) */
static void
on_request_head(void *arg, const char *remote, const hc_request_head *head)
{
    (void)arg;
    printf("%s %s %s HTTP/%s\n", remote, head->method, head->target,
           head->version == 10 ? "1.0" : "1.1");
}

/* System.out.println(connection.getRemoteAddress() + " " + new StatusLine(response));
 * interim responses (100 Continue, 103 Early Hints) come through here too */
static void
on_response_head(void *arg, const char *remote, const hc_response_head *head)
{
    (void)arg;
    printf("%s HTTP/%s %d\n", remote, head->version == 10 ? "1.0" : "1.1", head->status);
}

static void
on_exchange_complete(void *arg, const char *remote, int keep_alive)
{
    (void)arg;
    printf("%s exchange completed (connection %s)\n", remote,
           keep_alive ? "kept alive" : "closed");
}

/* ------------------------------------------------------------------------ */
/* Pool listener (an addition: shows the connection being leased again)     */
/* ------------------------------------------------------------------------ */

/* The lease is given back once the handler has released everything. */
static void
on_pool_release(void *arg, const hc_pool_entry *e, int reusable, hc_pool_stats t)
{
    char route[300];

    (void)arg;
    hc_host_to_string(hc_pool_entry_route(e), route, sizeof(route));
    printf("%s connection %s [leased: %d][available: %d]\n", route,
           reusable ? "kept alive" : "closed", t.leased, t.available);
}

/* An entry that already carries a connection is a reused idle one. */
static void
on_pool_lease(void *arg, const hc_pool_entry *e, hc_pool_stats t)
{
    char route[300];

    (void)arg;
    hc_host_to_string(hc_pool_entry_route(e), route, sizeof(route));
    printf("%s %s [leased: %d][available: %d]\n", route,
           hc_pool_entry_conn(e) ? "reusing pooled connection" : "opening new connection",
           t.leased, t.available);
}

/* ------------------------------------------------------------------------ */

int
main(int argc, char **argv)
{
    hc_requester_config   cfg = { 0 };
    hc_requester_listener sl  = { on_request_head, on_response_head, on_exchange_complete, NULL };
    hc_pool_listener      l   = { on_pool_lease, on_pool_release, NULL };
    hc_host               probe;
    char                  target[2048];

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--duplex") == 0 && i + 1 < argc) {
            duplex_bytes = (int64_t)(atof(argv[++i]) * 1024 * 1024);
        } else if (strcmp(argv[i], "-n") == 0 && i + 1 < argc) {
            remaining_runs = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--cafile") == 0 && i + 1 < argc) {
            cfg.tls_ca_file = argv[++i];
        } else if (strcmp(argv[i], "--insecure") == 0) {
            cfg.tls_no_verify = 1;
        } else if (strcmp(argv[i], "--expect") == 0) {
            send_expect = 1;
        } else {
            url = argv[i];
        }
    }

    if (url == NULL || hc_uri_parse(url, &probe, target, sizeof(target)) != 0 ||
        remaining_runs < 1) {
        fprintf(stderr,
                "Usage: %s [-n COUNT] [--duplex MB] [--expect] [--cafile PEM] [--insecure]\n"
                "          <http[s]://host[:port]/path>\n"
                "  e.g. %s http://httpbin.org/post\n"
                "       %s --duplex 8 http://127.0.0.1:8081/echo   (examples/echo_server)\n",
                argv[0], argv[0], argv[0]);
        return 2;
    }

    if ((reactor = hc_reactor_new()) == NULL ||
        (requester = hc_requester_new(reactor, &cfg)) == NULL) {
        fprintf(stderr, "initialization failed%s\n",
                cfg.tls_ca_file ? " (cannot load --cafile?)" : "");
        return 1;
    }

    hc_requester_set_listener(requester, &sl);  /* Java: .setStreamListener() */
    hc_pool_set_listener(hc_requester_pool(requester), &l);
    hc_reactor_stop_on_signals(reactor);

    start_exchange();
    hc_reactor_run(reactor);    /* Java: latch.await(); returns on stop */

    printf("Shutting down I/O reactor\n");
    hc_requester_free(requester);
    hc_reactor_free(reactor);

    return failures ? 1 : 0;
}
