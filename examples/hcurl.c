/*
 * hcurl.c - a basic url fetcher based on hcore, the C port of httpcore5.
 *
 * Usage: hcurl <http[s]://host[:port]/path>
 */

#include "hc.h"
#include "hc_pool.h"
#include "hc_support.h"

#include <stdio.h>
#include <stdlib.h>

#define EXECUTE_TIMEOUT_MS 30000   /* Java: Timeout.ofSeconds(30) */

/* ------------------------------------------------------------------------ */
/* Result callback (Java: FutureCallback + the handler's print statements)  */
/* ------------------------------------------------------------------------ */

typedef struct run {
    hc_reactor *reactor;
    const char *url;
} run;

/* Java: consumeInformation -> System.out.println(requestUri + "->" + code) */
static void
on_information(void *arg, const hc_response_head *head)
{
    const char *k, *v;

    run *r = arg;
    printf("%s->%d (interim)", r->url, head->status);

    for (size_t i = 0; hc_headers_at(head->headers, i, &k, &v) == 0; i++) {
        printf(" [%s: %s]", k, v);
    }

    putchar('\n');
}

static void
on_completed(void *arg, const hc_message *response)
{
    run                    *r    = arg;
    const hc_response_head *head = response->head;

    /* Java: consumeResponse -> System.out.println(requestUri + "->" + code) */
    printf("%s->%d\n", r->url, head->status);

    size_t len;
    const char *text = hc_buffer_entity_consumer_data(response->body, &len);

    const char *k, *v;
    for (size_t i = 0; hc_headers_at(head->headers, i, &k, &v) == 0; i++) {
        printf("%s: %s\n", k, v);
    }

    putchar('\n');
    printf("%s%s", text, len && text[len - 1] != '\n' ? "\n" : "");
}

/* Java: failed(Exception) -> System.out.println(requestUri + "->" + cause) */
static void
on_failed(void *arg, const char *reason)
{
    run *r = arg;
    fprintf(stderr, "%s->%s\n", r->url, reason);
}

/* Java: releaseResources() -> latch.countDown() */
static void
on_released(void *arg)
{
    run *r = arg;
    hc_reactor_stop(r->reactor);
}

/* Java: Http1StreamListener.onExchangeComplete reports "connection kept
 * alive" / "connection closed"; here the pool reports it on release. */
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
    if (argc < 2) {
        fprintf(stderr,
                "Usage: %s <http[s]://host[:port]/path>\n", argv[0]);
        return 2;
    }

    const char *url = argv[1];
    hc_host     host;
    char        target[2048];

    if (hc_uri_parse(url, &host, target, sizeof(target)) != 0) {
        fprintf(stderr, "invalid url \"%s\"\n", url);
        return 2;
    }

    printf("host \"%s\"\n", host.name);
    printf("port %d\n", host.port);

    hc_reactor *reactor = hc_reactor_new();
    if (!reactor) {
        fprintf(stderr, "alloc failed\n");
        return 1;
    }

    hc_requester_config cfg = { 0 };
    hc_requester *requester = hc_requester_new(reactor, &cfg);

    hc_pool_listener l = {
        on_pool_lease,
        on_pool_release,
        NULL
    };

    hc_pool_set_listener(hc_requester_pool(requester), &l);
    hc_reactor_stop_on_signals(reactor);

    hc_request_producer *request = hc_request_producer_new("GET", url, NULL);

    run r = {
        .reactor = reactor,
        .url = url
    };
    hc_result_callback cb = {
        on_completed,
        on_failed,
        on_information,
        on_released,
        &r
    };

    hc_entity_consumer *body = hc_buffer_entity_consumer_new(0);

    if (hc_requester_execute_request(requester, request, body, EXECUTE_TIMEOUT_MS, &cb) != 0) {
        fprintf(stderr, "failed!\n");
        body->vtbl->release(body);
        hc_request_producer_free(request);
        hc_requester_free(requester);
        hc_reactor_free(reactor);
        return 1;
    }

    hc_reactor_run(reactor);    /* Java: latch.await(); returns on stop */

    hc_requester_free(requester);
    hc_reactor_free(reactor);

    return 0;
}
