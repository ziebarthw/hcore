/*
 * pipelined_client.c - a C port of httpcore5's
 * AsyncPipelinedRequestExecutionExample.
 *
 * Usage: pipelined_client [--cafile PEM] [--insecure] [http[s]://host[:port]]
 *        (default: http://httpbin.org, as the Java example)
 *
 * Leases one connection to the target, then executes four GET requests on
 * it at once (HTTP/1.1 pipelining): all four requests go out on the wire
 * one after another without waiting, and the responses come back in the
 * same order. The stream listener shows it - the four request lines come
 * before the first status line.
 *
 * Java -> C:
 *   IOReactorConfig.setSoTimeout(5 s)              cfg.read_timeout_ms = 5000
 *   AsyncRequesterBootstrap ... .create()          hc_requester_new()
 *   .setStreamListener(Http1StreamListener)        hc_requester_set_listener()
 *   requester.connect(target, 5 s).get()           hc_requester_connect() + callback
 *   AsyncRequestBuilder.get().setHttpHost(target)  hc_request_producer_new("GET", url)
 *     .setPath(requestUri)
 *   BasicResponseConsumer<>(                       hc_buffer_entity_consumer_new()
 *     new StringAsyncEntityConsumer())
 *   clientEndpoint.execute(producer, consumer,     hc_endpoint_execute(ep,
 *     callback)                                      hc_basic_client_handler_new())
 *   FutureCallback completed/failed/cancelled      hc_result_callback completed/failed
 *   CountDownLatch / latch.await()                 a counter; stop the reactor at 0
 *   clientEndpoint.releaseAndDiscard()             hc_endpoint_release_and_discard()
 */
#include "hc.h"
#include "hc_support.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CONNECT_TIMEOUT_MS 5000    /* Java: Timeout.ofSeconds(5) */

static const char *const request_uris[] = { "/", "/ip", "/user-agent", "/headers" };
#define N_REQUESTS (sizeof(request_uris) / sizeof(request_uris[0]))

static hc_reactor  *reactor;
static hc_endpoint *endpoint;
static hc_host      target;
static size_t       latch = N_REQUESTS;    /* Java: CountDownLatch */
static int          failures;

/* ------------------------------------------------------------------------ */
/* Java: .setStreamListener(new Http1StreamListener() { ... })              */
/* ------------------------------------------------------------------------ */

static void
on_request_head(void *arg, const char *remote, const hc_request_head *head)
{
    (void)arg;
    printf("%s %s %s HTTP/%s\n", remote, head->method, head->target,
           head->version == 10 ? "1.0" : "1.1");
}

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
/* Java: new FutureCallback<Message<HttpResponse, String>>() { ... }        */
/* ------------------------------------------------------------------------ */

/* completed: System.out.println(requestUri + "->" + code); then the body */
static void
on_completed(void *arg, const hc_message *response)
{
    const char *uri = arg;
    size_t      len;
    const char *text = hc_buffer_entity_consumer_data(response->body, &len);

    printf("%s->%d\n", uri, response->head->status);
    printf("%s%s", text, len && text[len - 1] != '\n' ? "\n" : "");
}

/* failed: System.out.println(requestUri + "->" + ex). Java's cancelled()
 * has no separate C callback: a cancelled exchange fails with "cancelled". */
static void
on_failed(void *arg, const char *reason)
{
    printf("%s->%s\n", (const char *)arg, reason);
    failures++;
}

/* latch.countDown(); after the last one: latch.await() returns */
static void
on_released(void *arg)
{
    (void)arg;

    if (--latch == 0) {
        printf("Shutting down I/O reactor\n");
        hc_endpoint_release_and_discard(endpoint);  /* Java: releaseAndDiscard() */
        endpoint = NULL;
        hc_reactor_stop(reactor);
    }
}

/* ------------------------------------------------------------------------ */

/* Java: final AsyncClientEndpoint clientEndpoint = future.get(); then the
 * loop over requestUris, each one executed on the endpoint at once. */
static void
on_connected(void *arg, hc_endpoint *ep, const char *error)
{
    char base[300];

    (void)arg;

    if (error) {
        fprintf(stderr, "cannot connect: %s\n", error);
        failures++;
        hc_reactor_stop(reactor);
        return;
    }

    endpoint = ep;
    hc_host_to_string(&target, base, sizeof(base));

    for (size_t i = 0; i < N_REQUESTS; i++) {
        hc_result_callback   cb = { on_completed, on_failed, NULL, on_released,
                                    (void *)request_uris[i] };
        char                 url[512];
        hc_request_producer *req;
        hc_entity_consumer  *body;
        hc_client_handler   *h;

        snprintf(url, sizeof(url), "%s%s", base, request_uris[i]);

        req  = hc_request_producer_new("GET", url, NULL);
        body = hc_buffer_entity_consumer_new(0);
        h    = req && body ? hc_basic_client_handler_new(req, body, &cb) : NULL;

        if (h == NULL || hc_endpoint_execute(ep, h) == NULL) {
            /* nothing was taken over */
            free(h);

            if (req) {
                hc_request_producer_free(req);
            }

            if (body) {
                body->vtbl->release(body);
            }

            printf("%s->cannot start the request\n", request_uris[i]);
            failures++;
            on_released(NULL);
        }
    }
}

int
main(int argc, char **argv)
{
    hc_requester_config   cfg = { 0 };
    hc_requester_listener l   = { on_request_head, on_response_head, on_exchange_complete, NULL };
    hc_requester         *requester;
    const char           *host = "http://httpbin.org";     /* Java: new HttpHost("httpbin.org") */

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--cafile") == 0 && i + 1 < argc) {
            cfg.tls_ca_file = argv[++i];
        } else if (strcmp(argv[i], "--insecure") == 0) {
            cfg.tls_no_verify = 1;
        } else if (argv[i][0] != '-') {
            host = argv[i];
        } else {
            fprintf(stderr, "Usage: %s [--cafile PEM] [--insecure] [http[s]://host[:port]]\n",
                    argv[0]);
            return 2;
        }
    }

    if (hc_host_parse(&target, host) != 0) {
        fprintf(stderr, "bad target: %s\n", host);
        return 2;
    }

    cfg.read_timeout_ms = 5000;     /* Java: IOReactorConfig.setSoTimeout(5 s) */

    if ((reactor = hc_reactor_new()) == NULL ||
        (requester = hc_requester_new(reactor, &cfg)) == NULL) {
        fprintf(stderr, "initialization failed%s\n",
                cfg.tls_ca_file ? " (cannot load --cafile?)" : "");
        return 1;
    }

    hc_requester_set_listener(requester, &l);
    hc_reactor_stop_on_signals(reactor);    /* Java: the shutdown hook */

    if (hc_requester_connect(requester, &target, CONNECT_TIMEOUT_MS, on_connected, NULL) == NULL) {
        fprintf(stderr, "cannot start connecting\n");
        return 1;
    }

    hc_reactor_run(reactor);                /* Java: latch.await() */

    hc_requester_free(requester);           /* Java: requester.initiateShutdown() */
    hc_reactor_free(reactor);

    return failures || latch ? 1 : 0;
}
