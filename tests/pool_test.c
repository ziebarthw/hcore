/*
 * pool_test.c - unit tests for hc_pool with fake connections.
 *
 * The pool only ever sees connections through hc_pool_conn_ops, so these
 * tests check its semantics (limits, queuing, eviction, expiry, reuse
 * order, cancellation, shutdown) without any sockets.
 */
#include "hc_pool.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static hc_reactor *R;
static int         failures, checks;

#define CHECK(cond)                                                          \
    do {                                                                     \
        checks++;                                                            \
        if (!(cond)) {                                                       \
            failures++;                                                      \
            fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        }                                                                    \
    } while (0)

/* --- fake connections --------------------------------------------------------- */

typedef struct fake_conn {
    int id;
    int closed;
} fake_conn;

static fake_conn conns[64];
static int       next_conn, closes;

static fake_conn *
new_conn(void)
{
    fake_conn *c = &conns[next_conn];

    c->id     = next_conn++;
    c->closed = 0;

    return c;
}

static void
fake_close(void *ctx, void *conn)
{
    (void)ctx;
    ((fake_conn *)conn)->closed = 1;
    closes++;
}

static const hc_pool_conn_ops fake_ops = { fake_close };

/* http://NAME:80 as a route. Rotating static storage: valid for a few calls. */
static const hc_host *
route_of(const char *name)
{
    static hc_host slots[8];
    static int     next;
    hc_host       *h = &slots[next++ % 8];

    hc_host_init(h, "http", name, 80);

    return h;
}

/* --- lease results ------------------------------------------------------------ */

typedef struct result {
    int            done;
    hc_pool_entry *e;
    const char    *error;
} result;

static void
on_lease(void *arg, hc_pool_entry *e, const char *error)
{
    result *r = arg;

    r->done  = 1;
    r->e     = e;
    r->error = error;
}

static void
stop_cb(void *arg)
{
    hc_reactor_stop(arg);
}

/* Run the loop for ms milliseconds (lets deferred work happen). */
static void
run_for(int64_t ms)
{
    hc_timer *t = hc_timer_new(R, stop_cb, R);

    hc_timer_start(t, ms);
    hc_reactor_run(R);
    hc_timer_free(t);
}

static hc_pool *
make_pool(int max_total, int max_per_route, int64_t ttl, hc_pool_reuse_policy pol)
{
    hc_pool_config c = { max_total, max_per_route, ttl, pol };

    next_conn = closes = 0;

    return hc_pool_new(R, &c, &fake_ops, NULL);
}

/* Lease and, if the entry is empty, "connect" it. */
static hc_pool_entry *
lease_now(hc_pool *p, const char *host, const void *state, result *r)
{
    memset(r, 0, sizeof(*r));
    hc_pool_lease(p, route_of(host), state, 0, on_lease, r);
    run_for(5);

    if (r->e && hc_pool_entry_conn(r->e) == NULL) {
        hc_pool_entry_assign(r->e, new_conn());
    }

    return r->e;
}

static int
conn_id(hc_pool_entry *e)
{
    return e && hc_pool_entry_conn(e) ? ((fake_conn *)hc_pool_entry_conn(e))->id : -1;
}

/* --- tests ---------------------------------------------------------------------- */

static void
test_reuse(void)
{
    hc_pool      *p = make_pool(10, 2, 0, HC_POOL_REUSE_LIFO);
    result        r1, r2;
    hc_pool_stats s;

    puts("reuse: released connection is handed out again");
    lease_now(p, "a", NULL, &r1);
    CHECK(r1.done && r1.e && !r1.error);
    CHECK(hc_pool_get_total_stats(p).leased == 1);

    hc_pool_release(p, r1.e, 1);
    s = hc_pool_get_total_stats(p);
    CHECK(s.leased == 0 && s.available == 1);

    lease_now(p, "a", NULL, &r2);
    CHECK(conn_id(r2.e) == 0);          /* same connection */
    CHECK(next_conn == 1 && closes == 0);

    hc_pool_release(p, r2.e, 0);        /* not reusable: closed, slot freed */
    s = hc_pool_get_total_stats(p);
    CHECK(closes == 1 && s.leased == 0 && s.available == 0);
    hc_pool_free(p);
}

static void
test_route_limit_queues(void)
{
    hc_pool *p = make_pool(10, 2, 0, HC_POOL_REUSE_LIFO);
    result   a, b, c;

    puts("max_per_route: third lease waits, then gets the released connection");
    lease_now(p, "a", NULL, &a);
    lease_now(p, "a", NULL, &b);
    lease_now(p, "a", NULL, &c);
    CHECK(a.e && b.e && !c.done);
    CHECK(hc_pool_get_route_stats(p, route_of("a")).pending == 1);

    /* Other routes are not blocked by route a's limit. */
    {
        result d;

        lease_now(p, "b", NULL, &d);
        CHECK(d.e != NULL);
        hc_pool_release(p, d.e, 1);
    }

    hc_pool_release(p, a.e, 1);
    run_for(5);
    CHECK(c.done && conn_id(c.e) == conn_id(a.e));
    CHECK(hc_pool_get_route_stats(p, route_of("a")).pending == 0);

    hc_pool_release(p, b.e, 1);
    hc_pool_release(p, c.e, 1);
    hc_pool_free(p);
}

static void
test_total_limit_evicts_other_route(void)
{
    hc_pool *p = make_pool(3, 3, 0, HC_POOL_REUSE_LIFO);
    result   a1, a2, a3, b;

    puts("max_total: lease for a new route closes another route's LRU idle connection");
    lease_now(p, "a", NULL, &a1);
    lease_now(p, "a", NULL, &a2);
    lease_now(p, "a", NULL, &a3);
    hc_pool_release(p, a1.e, 1);        /* oldest idle */
    hc_pool_release(p, a2.e, 1);
    CHECK(hc_pool_get_total_stats(p).available == 2);

    lease_now(p, "b", NULL, &b);
    CHECK(b.e != NULL);
    CHECK(closes == 1 && conns[0].closed);      /* a1 was least recently used */
    CHECK(hc_pool_get_route_stats(p, route_of("a")).available == 1);

    hc_pool_release(p, a3.e, 1);
    hc_pool_release(p, b.e, 1);
    hc_pool_free(p);
}

static void
test_lease_timeout(void)
{
    hc_pool *p = make_pool(10, 1, 0, HC_POOL_REUSE_LIFO);
    result   a, b;

    puts("lease timeout: waiting request fails after its timeout");
    lease_now(p, "a", NULL, &a);
    memset(&b, 0, sizeof(b));
    hc_pool_lease(p, route_of("a"), NULL, 30, on_lease, &b);
    run_for(10);
    CHECK(!b.done);
    run_for(60);
    CHECK(b.done && b.e == NULL && b.error != NULL);
    CHECK(hc_pool_get_total_stats(p).pending == 0);

    hc_pool_release(p, a.e, 1);
    hc_pool_free(p);
}

static void
test_reuse_policy(void)
{
    hc_pool_reuse_policy pol[2] = { HC_POOL_REUSE_LIFO, HC_POOL_REUSE_FIFO };
    int                  want[2] = { 1, 0 };

    puts("reuse policy: LIFO hands out the most recent, FIFO the oldest");

    for (int i = 0; i < 2; i++) {
        hc_pool *p = make_pool(10, 5, 0, pol[i]);
        result   a, b, c;

        lease_now(p, "a", NULL, &a);
        lease_now(p, "a", NULL, &b);
        hc_pool_release(p, a.e, 1);     /* conn 0 first */
        hc_pool_release(p, b.e, 1);     /* conn 1 second */
        lease_now(p, "a", NULL, &c);
        CHECK(conn_id(c.e) == want[i]);
        hc_pool_release(p, c.e, 1);
        hc_pool_free(p);
    }
}

static void
test_expiry(void)
{
    hc_pool *p = make_pool(10, 5, 0, HC_POOL_REUSE_LIFO);
    result   a, b;

    puts("expiry: keep-alive and time-to-live retire connections");

    /* Keep-alive of 0: expired on release, closed rather than kept. */
    lease_now(p, "a", NULL, &a);
    hc_pool_entry_update_expiry(a.e, 0);
    hc_pool_release(p, a.e, 1);
    CHECK(closes == 1 && hc_pool_get_total_stats(p).available == 0);

    /* Keep-alive of 30 ms: kept, then found expired at lease time; the
     * caller gets the slot without a connection and reconnects. */
    lease_now(p, "a", NULL, &a);
    hc_pool_entry_update_expiry(a.e, 30);
    hc_pool_release(p, a.e, 1);
    CHECK(hc_pool_get_total_stats(p).available == 1);
    run_for(50);
    memset(&b, 0, sizeof(b));
    hc_pool_lease(p, route_of("a"), NULL, 0, on_lease, &b);
    run_for(5);
    CHECK(b.e && hc_pool_entry_conn(b.e) == NULL && closes == 2);
    hc_pool_entry_assign(b.e, new_conn());
    hc_pool_release(p, b.e, 1);

    /* close_expired() does the same for idle connections proactively. */
    lease_now(p, "a", NULL, &a);
    hc_pool_entry_update_expiry(a.e, 20);
    hc_pool_release(p, a.e, 1);
    run_for(40);
    hc_pool_close_expired(p);
    CHECK(closes == 3 && hc_pool_get_route_stats(p, route_of("a")).available == 0);

    hc_pool_free(p);

    /* Time to live caps everything. */
    p = make_pool(10, 5, 30, HC_POOL_REUSE_LIFO);
    lease_now(p, "a", NULL, &a);
    hc_pool_entry_update_expiry(a.e, 100000);
    CHECK(hc_pool_entry_expiry_ms(a.e) <= hc_pool_entry_created_ms(a.e) + 30);
    hc_pool_release(p, a.e, 1);
    run_for(50);
    hc_pool_close_expired(p);
    CHECK(closes == 1 && hc_pool_get_total_stats(p).available == 0);
    hc_pool_free(p);
}

static void
test_close_idle_and_lost(void)
{
    hc_pool *p = make_pool(10, 5, 0, HC_POOL_REUSE_LIFO);
    result   a, b, c;

    puts("close_idle / connection_lost remove idle connections");
    lease_now(p, "a", NULL, &a);
    lease_now(p, "a", NULL, &b);
    lease_now(p, "a", NULL, &c);
    hc_pool_release(p, a.e, 1);
    hc_pool_release(p, b.e, 1);

    hc_pool_connection_lost(p, a.e);    /* idle: removed and closed */
    CHECK(closes == 1 && conns[0].closed);
    hc_pool_connection_lost(p, c.e);    /* leased: left to its holder */
    CHECK(closes == 1 && hc_pool_get_total_stats(p).leased == 1);

    run_for(20);
    hc_pool_close_idle(p, 10);
    CHECK(closes == 2 && hc_pool_get_total_stats(p).available == 0);

    hc_pool_release(p, c.e, 1);
    hc_pool_free(p);
}

static void
test_cancel_and_state(void)
{
    hc_pool           *p = make_pool(10, 1, 0, HC_POOL_REUSE_LIFO);
    result             a, b, c, d;
    hc_pool_lease_req *l;
    static const int   alice = 1, bob = 2;

    puts("cancel: a cancelled request never completes and does not block others");
    lease_now(p, "a", NULL, &a);
    memset(&b, 0, sizeof(b));
    memset(&c, 0, sizeof(c));
    l = hc_pool_lease(p, route_of("a"), NULL, 0, on_lease, &b);
    hc_pool_lease(p, route_of("a"), NULL, 0, on_lease, &c);
    run_for(5);
    hc_pool_lease_cancel(l);
    hc_pool_release(p, a.e, 1);
    run_for(5);
    CHECK(!b.done && c.done && conn_id(c.e) == 0);
    hc_pool_release(p, c.e, 1);
    hc_pool_free(p);

    puts("state: an idle connection is matched by state, stateless as fallback");
    p = make_pool(10, 5, 0, HC_POOL_REUSE_LIFO);
    lease_now(p, "a", &alice, &a);      /* conn 0 for alice */
    lease_now(p, "a", NULL, &b);        /* conn 1 stateless */
    hc_pool_release(p, a.e, 1);
    hc_pool_release(p, b.e, 1);
    lease_now(p, "a", &bob, &c);        /* no bob conn: takes stateless */
    CHECK(conn_id(c.e) == 1);
    lease_now(p, "a", &alice, &d);
    CHECK(conn_id(d.e) == 0 && hc_pool_entry_state(d.e) == &alice);
    hc_pool_release(p, c.e, 1);
    hc_pool_release(p, d.e, 1);
    hc_pool_free(p);
}

static int lease_events, release_events;

static void
on_l(void *arg, const hc_pool_entry *e, hc_pool_stats t)
{
    (void)arg;
    (void)e;
    (void)t;
    lease_events++;
}

static void
on_r(void *arg, const hc_pool_entry *e, int reusable, hc_pool_stats t)
{
    (void)arg;
    (void)e;
    (void)reusable;
    (void)t;
    release_events++;
}

static void
test_listener_and_shutdown(void)
{
    hc_pool         *p = make_pool(10, 1, 0, HC_POOL_REUSE_LIFO);
    hc_pool_listener l = { on_l, on_r, NULL };
    result           a, b;

    puts("listener + shutdown: events fire; free fails waiting leases and closes all");
    hc_pool_set_listener(p, &l);
    lease_now(p, "a", NULL, &a);
    memset(&b, 0, sizeof(b));
    hc_pool_lease(p, route_of("a"), NULL, 0, on_lease, &b);
    run_for(5);
    CHECK(lease_events == 1 && !b.done);

    hc_pool_free(p);                    /* a is still leased */
    CHECK(b.done && b.e == NULL && b.error != NULL);
    CHECK(closes == 1);
    CHECK(release_events == 0);
}

static void
test_scheme_is_part_of_route(void)
{
    hc_pool *p = make_pool(10, 1, 0, HC_POOL_REUSE_LIFO);
    hc_host  http, https;
    result   a, b, c;

    puts("routes: http://h:8443 and https://h:8443 never share connections");
    hc_host_init(&http, "http", "h", 8443);
    hc_host_init(&https, "https", "H", 8443);   /* name compares case-insensitively */

    memset(&a, 0, sizeof(a));
    hc_pool_lease(p, &http, NULL, 0, on_lease, &a);
    run_for(5);
    hc_pool_entry_assign(a.e, new_conn());
    hc_pool_release(p, a.e, 1);

    memset(&b, 0, sizeof(b));
    hc_pool_lease(p, &https, NULL, 0, on_lease, &b);
    run_for(5);
    CHECK(b.e && hc_pool_entry_conn(b.e) == NULL);          /* not the http one */
    CHECK(hc_host_is_tls(hc_pool_entry_route(b.e)));
    CHECK(hc_pool_get_route_stats(p, &http).available == 1);
    CHECK(hc_pool_get_route_stats(p, &https).leased == 1);  /* own per-route limit */
    hc_pool_entry_assign(b.e, new_conn());
    hc_pool_release(p, b.e, 1);

    hc_host_init(&https, "https", "h", 8443);
    memset(&c, 0, sizeof(c));
    hc_pool_lease(p, &https, NULL, 0, on_lease, &c);
    run_for(5);
    CHECK(conn_id(c.e) == 1);                               /* reuses the https one */
    hc_pool_release(p, c.e, 1);
    hc_pool_free(p);
}

static void
test_tls_name_is_part_of_route(void)
{
    hc_pool *p = make_pool(10, 1, 0, HC_POOL_REUSE_LIFO);
    hc_host  a_site, b_site, plain, same;
    result   a, b, c;

    puts("routes: one address, different TLS server names -> separate connections");
    hc_host_init(&a_site, "https", "192.0.2.1", 443);
    hc_host_init(&b_site, "https", "192.0.2.1", 443);
    CHECK(hc_host_set_tls_name(&a_site, "a.example") == 0);
    CHECK(hc_host_set_tls_name(&b_site, "b.example") == 0);
    CHECK(!hc_host_equals(&a_site, &b_site));
    CHECK(strcmp(hc_host_tls_name(&a_site), "a.example") == 0);

    hc_host_init(&plain, "https", "a.example", 443);
    hc_host_init(&same, "https", "a.example", 443);
    CHECK(hc_host_set_tls_name(&same, "A.EXAMPLE") == 0);   /* = its name */
    CHECK(hc_host_equals(&plain, &same));                   /* unset == name */
    CHECK(strcmp(hc_host_tls_name(&plain), "a.example") == 0);
    CHECK(hc_host_set_tls_name(&same, NULL) == 0 && same.tls_name[0] == '\0');
    {
        char long_name[300];

        memset(long_name, 'x', sizeof(long_name) - 1);
        long_name[sizeof(long_name) - 1] = '\0';
        CHECK(hc_host_set_tls_name(&same, long_name) == -1);
    }
    CHECK(hc_host_parse(&same, "https://a.example") == 0 && same.tls_name[0] == '\0');

    memset(&a, 0, sizeof(a));
    hc_pool_lease(p, &a_site, NULL, 0, on_lease, &a);
    run_for(5);
    hc_pool_entry_assign(a.e, new_conn());
    hc_pool_release(p, a.e, 1);

    memset(&b, 0, sizeof(b));
    hc_pool_lease(p, &b_site, NULL, 0, on_lease, &b);
    run_for(5);
    CHECK(b.e && hc_pool_entry_conn(b.e) == NULL);          /* not a.example's */
    CHECK(strcmp(hc_pool_entry_route(b.e)->tls_name, "b.example") == 0);
    hc_pool_entry_assign(b.e, new_conn());
    hc_pool_release(p, b.e, 1);

    memset(&c, 0, sizeof(c));
    hc_pool_lease(p, &a_site, NULL, 0, on_lease, &c);
    run_for(5);
    CHECK(c.e && hc_pool_entry_conn(c.e) != NULL);          /* a.example's again */
    CHECK(strcmp(hc_pool_entry_route(c.e)->tls_name, "a.example") == 0);
    hc_pool_release(p, c.e, 1);
    hc_pool_free(p);
}

static void
test_host_parsing(void)
{
    hc_host h;
    char    buf[300], target[64];

    puts("hc_host: parse, defaults, authority, URIs");
    CHECK(hc_host_parse(&h, "example.org") == 0 && h.port == 80 && strcmp(h.scheme, "http") == 0);
    CHECK(hc_host_parse(&h, "https://example.org") == 0 && h.port == 443);
    CHECK(hc_host_parse(&h, "HTTPS://example.org:8443") == 0 && h.port == 8443 &&
          strcmp(h.scheme, "https") == 0);
    hc_host_authority(&h, buf, sizeof(buf));
    CHECK(strcmp(buf, "example.org:8443") == 0);
    CHECK(hc_host_parse(&h, "https://example.org:443") == 0);
    hc_host_authority(&h, buf, sizeof(buf));
    CHECK(strcmp(buf, "example.org") == 0);                 /* default port omitted */
    CHECK(hc_host_parse(&h, "[::1]:8080") == 0 && strcmp(h.name, "::1") == 0 && h.port == 8080);
    hc_host_to_string(&h, buf, sizeof(buf));
    CHECK(strcmp(buf, "http://[::1]:8080") == 0);
    CHECK(hc_host_parse(&h, "ftp://x") != 0);
    CHECK(hc_host_parse(&h, "x:0") != 0 && hc_host_parse(&h, "x:99999") != 0);
    CHECK(hc_host_parse(&h, "x:80a") != 0 && hc_host_parse(&h, "") != 0);

    CHECK(hc_uri_parse("https://a.b:9443/p/q?x=1#frag", &h, target, sizeof(target)) == 0 &&
          h.port == 9443 && strcmp(target, "/p/q?x=1") == 0);
    CHECK(hc_uri_parse("http://a.b", &h, target, sizeof(target)) == 0 && strcmp(target, "/") == 0);
    CHECK(hc_uri_parse("http://a.b?q", &h, target, sizeof(target)) == 0 && strcmp(target, "/?q") == 0);
    CHECK(hc_uri_parse("a.b/p", &h, target, sizeof(target)) != 0);
}

int
main(void)
{
    R = hc_reactor_new();

    test_reuse();
    test_route_limit_queues();
    test_total_limit_evicts_other_route();
    test_lease_timeout();
    test_reuse_policy();
    test_expiry();
    test_close_idle_and_lost();
    test_cancel_and_state();
    test_listener_and_shutdown();
    test_scheme_is_part_of_route();
    test_tls_name_is_part_of_route();
    test_host_parsing();

    hc_reactor_free(R);
    printf("%d checks, %d failed\n", checks, failures);

    return failures ? 1 : 0;
}
