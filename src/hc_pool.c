/*
 * hc_pool.c - route-based connection pool (httpcore5 StrictConnPool model).
 *
 * Depends only on hc.h (for timers); see hc_pool.h for the API.
 *
 * Bookkeeping follows StrictConnPool:
 *   - per route: available (idle) entries, leased entries, pending count
 *   - globally:  available entries in LRU order (for evicting another
 *                route's idle connection when max_total is reached),
 *                pending lease requests in arrival order
 * An entry counts against the limits from the moment it is leased, even
 * before the caller has connected it.
 */
#include "hc_pool.h"

#include <stdlib.h>
#include <string.h>
#include <strings.h>

/* --- intrusive circular lists -------------------------------------------- */

typedef struct hc_link {
    struct hc_link *prev;
    struct hc_link *next;
} hc_link;

static void
l_init(hc_link *h)
{
    h->prev = h->next = h;
}

static int
l_empty(const hc_link *h)
{
    return h->next == h;
}

static void
l_remove(hc_link *n)
{
    n->prev->next = n->next;
    n->next->prev = n->prev;
    n->prev = n->next = n;
}

static void
l_push_front(hc_link *h, hc_link *n)
{
    n->next       = h->next;
    n->prev       = h;
    h->next->prev = n;
    h->next       = n;
}

static void
l_push_back(hc_link *h, hc_link *n)
{
    n->prev       = h->prev;
    n->next       = h;
    h->prev->next = n;
    h->prev       = n;
}

/* --- types ---------------------------------------------------------------- */

typedef struct route_pool {
    hc_link  link;               /* in pool->routes                       */
    hc_host  route;
    int      max;                /* < 0: pool default                     */
    hc_link  available;          /* head = next to hand out               */
    hc_link  leased;
    int      n_available;
    int      n_leased;
    int      n_pending;
} route_pool;

struct hc_pool_entry {
    hc_link     route_link;      /* rp->available or rp->leased           */
    hc_link     lru_link;        /* pool->available while idle            */
    route_pool *rp;
    hc_pool    *pool;
    void       *conn;
    const void *state;
    int         leased;
    int64_t     created;
    int64_t     updated;
    int64_t     valid_until;     /* created + time_to_live                */
    int64_t     expiry;          /* min(valid_until, last keep-alive)     */
};

struct hc_pool_lease_req {
    hc_link          link;       /* pool->pending or pool->completed      */
    hc_pool         *pool;
    route_pool      *rp;
    const void      *state;
    hc_pool_lease_cb cb;
    void            *arg;
    hc_timer        *timer;      /* lease timeout                         */
    hc_pool_entry   *entry;
    const char      *error;
    int              completed;
};

struct hc_pool {
    hc_reactor      *r;
    hc_pool_config   cfg;
    hc_pool_conn_ops ops;
    void            *ops_ctx;
    hc_pool_listener listener;
    hc_link          routes;
    hc_link          available;  /* all routes; head = most recently used */
    hc_link          pending;
    hc_link          completed;  /* served, callback not yet delivered    */
    int              n_leased;
    int              n_available;
    int              n_pending;
    hc_timer        *service;
    int              shutdown;
};

#define ENTRY_OF_ROUTE(lnk) HC_CONTAINER_OF(lnk, hc_pool_entry, route_link)
#define ENTRY_OF_LRU(lnk)   HC_CONTAINER_OF(lnk, hc_pool_entry, lru_link)
#define LEASE_OF(lnk)       HC_CONTAINER_OF(lnk, hc_pool_lease_req, link)
#define ROUTE_OF(lnk)       HC_CONTAINER_OF(lnk, route_pool, link)

static const char *const ERR_TIMEOUT  = "timeout waiting for a connection from the pool";
static const char *const ERR_SHUTDOWN = "connection pool shut down";

static void service_cb(void *arg);

static int64_t
now_ms(const hc_pool *p)
{
    return hc_reactor_now_ms(p->r);
}

static void
schedule_service(hc_pool *p)
{
    if (!p->shutdown) {
        hc_timer_start(p->service, 0);
    }
}

static hc_pool_stats
totals(const hc_pool *p)
{
    hc_pool_stats s = { p->n_leased, p->n_pending, p->n_available, p->cfg.max_total };

    return s;
}

/* --- routes ---------------------------------------------------------------- */

static route_pool *
get_route(hc_pool *p, const hc_host *route, int create)
{
    route_pool *rp;

    for (hc_link *it = p->routes.next; it != &p->routes; it = it->next) {
        rp = ROUTE_OF(it);

        if (hc_host_equals(&rp->route, route)) {
            return rp;
        }
    }

    if (!create || (rp = calloc(1, sizeof(*rp))) == NULL) {
        return NULL;
    }

    rp->route = *route;
    rp->max   = -1;
    l_init(&rp->available);
    l_init(&rp->leased);
    l_push_back(&p->routes, &rp->link);

    return rp;
}

static int
route_max(const hc_pool *p, const route_pool *rp)
{
    return rp->max >= 0 ? rp->max : p->cfg.default_max_per_route;
}

/* --- entries ---------------------------------------------------------------- */

static void
entry_close_conn(hc_pool_entry *e)
{
    void *conn = e->conn;

    if (conn) {
        e->conn = NULL;
        e->pool->ops.close(e->pool->ops_ctx, conn);
    }
}

/* Remove an idle entry entirely (close + free). */
static void
entry_drop_available(hc_pool *p, hc_pool_entry *e)
{
    l_remove(&e->route_link);
    l_remove(&e->lru_link);
    e->rp->n_available--;
    p->n_available--;
    entry_close_conn(e);
    free(e);
}

static void
entry_mark_leased(hc_pool *p, route_pool *rp, hc_pool_entry *e)
{
    e->leased = 1;
    l_push_back(&rp->leased, &e->route_link);
    rp->n_leased++;
    p->n_leased++;
}

/* An idle entry for this state: exact match first, then a stateless one. */
static hc_pool_entry *
find_free(route_pool *rp, const void *state)
{
    hc_link *it;

    for (it = rp->available.next; it != &rp->available; it = it->next) {
        if (ENTRY_OF_ROUTE(it)->state == state) {
            return ENTRY_OF_ROUTE(it);
        }
    }

    for (it = rp->available.next; it != &rp->available; it = it->next) {
        if (ENTRY_OF_ROUTE(it)->state == NULL) {
            return ENTRY_OF_ROUTE(it);
        }
    }

    return NULL;
}

/* --- lease processing (StrictConnPool.processPendingRequest) --------------- */

static void
lease_complete(hc_pool_lease_req *l, hc_pool_entry *e, const char *error)
{
    hc_pool *p = l->pool;

    l_remove(&l->link);             /* from pending */
    p->n_pending--;
    l->rp->n_pending--;
    l->completed = 1;
    l->entry     = e;
    l->error     = error;
    hc_timer_stop(l->timer);
    l_push_back(&p->completed, &l->link);

    if (e && p->listener.on_lease) {
        p->listener.on_lease(p->listener.arg, e, totals(p));
    }
}

static int
try_serve(hc_pool *p, hc_pool_lease_req *l)
{
    route_pool    *rp  = l->rp;
    int64_t        now = now_ms(p);
    hc_pool_entry *e;
    int            max, excess, free_capacity;

    /* 1. Reuse an idle connection of this route. */
    if ((e = find_free(rp, l->state)) != NULL) {
        l_remove(&e->route_link);
        l_remove(&e->lru_link);
        rp->n_available--;
        p->n_available--;

        if (now >= e->expiry) {
            entry_close_conn(e);    /* keep the slot, caller reconnects */
        }

        entry_mark_leased(p, rp, e);
        lease_complete(l, e, NULL);
        return 1;
    }

    /* 2. Make room within the route limit by closing idle connections
     *    (only possible if they had a different state). */
    max    = route_max(p, rp);
    excess = rp->n_available + rp->n_leased + 1 - max;

    while (excess-- > 0 && !l_empty(&rp->available)) {
        entry_drop_available(p, ENTRY_OF_ROUTE(rp->available.prev));
    }

    if (rp->n_available + rp->n_leased >= max) {
        return 0;
    }

    /* 3. Allocate a new slot within max_total, closing the least recently
     *    used idle connection of any route if that is what it takes. */
    free_capacity = p->cfg.max_total - p->n_leased;

    if (free_capacity <= 0) {
        return 0;
    }

    if (p->n_available > free_capacity - 1 && !l_empty(&p->available)) {
        entry_drop_available(p, ENTRY_OF_LRU(p->available.prev));
    }

    if ((e = calloc(1, sizeof(*e))) == NULL) {
        return 0;                   /* try again on the next service run */
    }

    e->rp          = rp;
    e->pool        = p;
    e->state       = l->state;
    e->created     = now;
    e->updated     = now;
    e->valid_until = p->cfg.time_to_live_ms > 0 ? now + p->cfg.time_to_live_ms : INT64_MAX;
    e->expiry      = e->valid_until;
    l_init(&e->route_link);
    l_init(&e->lru_link);

    entry_mark_leased(p, rp, e);
    lease_complete(l, e, NULL);

    return 1;
}

static void
deliver_completed(hc_pool *p)
{
    while (!l_empty(&p->completed)) {
        hc_pool_lease_req *l = LEASE_OF(p->completed.next);

        l_remove(&l->link);
        hc_timer_free(l->timer);
        l->cb(l->arg, l->entry, l->error);
        free(l);
    }
}

static void
service_cb(void *arg)
{
    hc_pool *p = arg;
    hc_link *it, *next;

    for (it = p->pending.next; it != &p->pending; it = next) {
        next = it->next;
        try_serve(p, LEASE_OF(it));
    }

    deliver_completed(p);
}

static void
lease_timeout_cb(void *arg)
{
    hc_pool_lease_req *l = arg;

    if (!l->completed) {
        lease_complete(l, NULL, ERR_TIMEOUT);
        deliver_completed(l->pool);
    }
}

/* --- public API -------------------------------------------------------------- */

hc_pool *
hc_pool_new(hc_reactor *r, const hc_pool_config *cfg,
            const hc_pool_conn_ops *ops, void *ops_ctx)
{
    hc_pool *p = calloc(1, sizeof(*p));

    if (p == NULL) {
        return NULL;
    }

    p->r       = r;
    p->cfg     = *cfg;
    p->ops     = *ops;
    p->ops_ctx = ops_ctx;
    l_init(&p->routes);
    l_init(&p->available);
    l_init(&p->pending);
    l_init(&p->completed);

    if ((p->service = hc_timer_new(r, service_cb, p)) == NULL) {
        free(p);
        return NULL;
    }

    return p;
}

void
hc_pool_set_listener(hc_pool *p, const hc_pool_listener *l)
{
    if (l) {
        p->listener = *l;
    } else {
        memset(&p->listener, 0, sizeof(p->listener));
    }
}

hc_pool_lease_req *
hc_pool_lease(hc_pool *p, const hc_host *route, const void *state,
              int64_t timeout_ms, hc_pool_lease_cb cb, void *arg)
{
    hc_pool_lease_req *l;
    route_pool    *rp;

    if (p->shutdown || (rp = get_route(p, route, 1)) == NULL ||
        (l = calloc(1, sizeof(*l))) == NULL) {
        return NULL;
    }

    l->pool  = p;
    l->rp    = rp;
    l->state = state;
    l->cb    = cb;
    l->arg   = arg;
    l->timer = hc_timer_new(p->r, lease_timeout_cb, l);

    if (l->timer == NULL) {
        free(l);
        return NULL;
    }

    l_push_back(&p->pending, &l->link);
    p->n_pending++;
    rp->n_pending++;

    if (timeout_ms > 0) {
        hc_timer_start(l->timer, timeout_ms);
    }

    schedule_service(p);

    return l;
}

void
hc_pool_lease_cancel(hc_pool_lease_req *l)
{
    hc_pool *p = l->pool;

    if (l->completed) {
        /* Served but not yet delivered: hand the slot straight back. */
        l_remove(&l->link);

        if (l->entry) {
            hc_pool_release(p, l->entry, 1);
        }
    } else {
        l_remove(&l->link);
        p->n_pending--;
        l->rp->n_pending--;
        schedule_service(p);    /* a later request may now be servable */
    }

    hc_timer_free(l->timer);
    free(l);
}

void
hc_pool_release(hc_pool *p, hc_pool_entry *e, int reusable)
{
    route_pool *rp = e->rp;
    int         keep;

    l_remove(&e->route_link);
    e->leased = 0;
    rp->n_leased--;
    p->n_leased--;

    keep = reusable && e->conn && !p->shutdown && now_ms(p) < e->expiry;

    if (keep) {
        e->updated = now_ms(p);

        if (p->cfg.reuse_policy == HC_POOL_REUSE_FIFO) {
            l_push_back(&rp->available, &e->route_link);
        } else {
            l_push_front(&rp->available, &e->route_link);
        }

        l_push_front(&p->available, &e->lru_link);   /* LRU order regardless */
        rp->n_available++;
        p->n_available++;
    }

    if (p->listener.on_release) {
        p->listener.on_release(p->listener.arg, e, keep, totals(p));
    }

    if (!keep) {
        entry_close_conn(e);
        free(e);
    }

    schedule_service(p);
}

void
hc_pool_connection_lost(hc_pool *p, hc_pool_entry *e)
{
    if (e->leased) {
        return;                 /* the lessee finds out and releases it */
    }

    entry_drop_available(p, e);
    schedule_service(p);
}

void
hc_pool_close_idle(hc_pool *p, int64_t idle_ms)
{
    int64_t  deadline = now_ms(p) - idle_ms;
    hc_link *it, *next;

    for (it = p->available.next; it != &p->available; it = next) {
        next = it->next;

        if (ENTRY_OF_LRU(it)->updated <= deadline) {
            entry_drop_available(p, ENTRY_OF_LRU(it));
        }
    }

    schedule_service(p);
}

void
hc_pool_close_expired(hc_pool *p)
{
    int64_t  now = now_ms(p);
    hc_link *it, *next;

    for (it = p->available.next; it != &p->available; it = next) {
        next = it->next;

        if (now >= ENTRY_OF_LRU(it)->expiry) {
            entry_drop_available(p, ENTRY_OF_LRU(it));
        }
    }

    schedule_service(p);
}

void
hc_pool_set_max_total(hc_pool *p, int max)
{
    p->cfg.max_total = max;
    schedule_service(p);
}

void
hc_pool_set_default_max_per_route(hc_pool *p, int max)
{
    p->cfg.default_max_per_route = max;
    schedule_service(p);
}

void
hc_pool_set_max_per_route(hc_pool *p, const hc_host *route, int max)
{
    route_pool *rp = get_route(p, route, 1);

    if (rp) {
        rp->max = max;
        schedule_service(p);
    }
}

int
hc_pool_get_max_per_route(hc_pool *p, const hc_host *route)
{
    route_pool *rp = get_route(p, route, 0);

    return rp ? route_max(p, rp) : p->cfg.default_max_per_route;
}

hc_pool_stats
hc_pool_get_total_stats(const hc_pool *p)
{
    return totals(p);
}

hc_pool_stats
hc_pool_get_route_stats(hc_pool *p, const hc_host *route)
{
    route_pool   *rp = get_route(p, route, 0);
    hc_pool_stats s  = { 0, 0, 0, p->cfg.default_max_per_route };

    if (rp) {
        s.leased    = rp->n_leased;
        s.pending   = rp->n_pending;
        s.available = rp->n_available;
        s.max       = route_max(p, rp);
    }

    return s;
}

void
hc_pool_free(hc_pool *p)
{
    hc_link *it, *next;

    if (p == NULL) {
        return;
    }

    p->shutdown = 1;
    hc_timer_stop(p->service);

    /* Fail everything still waiting; deliver results already served. */
    for (it = p->pending.next; it != &p->pending; it = next) {
        next = it->next;
        lease_complete(LEASE_OF(it), NULL, ERR_SHUTDOWN);
    }

    deliver_completed(p);

    while (!l_empty(&p->routes)) {
        route_pool *rp = ROUTE_OF(p->routes.next);

        while (!l_empty(&rp->available)) {
            entry_drop_available(p, ENTRY_OF_ROUTE(rp->available.next));
        }

        while (!l_empty(&rp->leased)) {
            hc_pool_entry *e = ENTRY_OF_ROUTE(rp->leased.next);

            l_remove(&e->route_link);
            entry_close_conn(e);
            free(e);
        }

        l_remove(&rp->link);
        free(rp);
    }

    hc_timer_free(p->service);
    free(p);
}

/* --- entries ---------------------------------------------------------------- */

const hc_host *
hc_pool_entry_route(const hc_pool_entry *e)
{
    return &e->rp->route;
}

const void *
hc_pool_entry_state(const hc_pool_entry *e)
{
    return e->state;
}

void
hc_pool_entry_set_state(hc_pool_entry *e, const void *state)
{
    e->state = state;
}

void *
hc_pool_entry_conn(const hc_pool_entry *e)
{
    return e->conn;
}

/* Java: PoolEntry.assignConnection - a new connection starts a new life,
 * even in a slot whose previous connection expired. */
void
hc_pool_entry_assign(hc_pool_entry *e, void *conn)
{
    int64_t now = now_ms(e->pool);
    int64_t ttl = e->pool->cfg.time_to_live_ms;

    e->conn        = conn;
    e->created     = now;
    e->updated     = now;
    e->valid_until = ttl > 0 ? now + ttl : INT64_MAX;
    e->expiry      = e->valid_until;
}

void
hc_pool_entry_discard(hc_pool_entry *e)
{
    entry_close_conn(e);
}

void
hc_pool_entry_update_expiry(hc_pool_entry *e, int64_t keep_alive_ms)
{
    int64_t now = now_ms(e->pool);

    e->updated = now;

    if (keep_alive_ms < 0) {
        e->expiry = e->valid_until;
    } else {
        int64_t until = now + keep_alive_ms;

        e->expiry = until < e->valid_until ? until : e->valid_until;
    }
}

int64_t
hc_pool_entry_created_ms(const hc_pool_entry *e)
{
    return e->created;
}

int64_t
hc_pool_entry_updated_ms(const hc_pool_entry *e)
{
    return e->updated;
}

int64_t
hc_pool_entry_expiry_ms(const hc_pool_entry *e)
{
    return e->expiry;
}
