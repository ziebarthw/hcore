/*
 * hc_pool.h - a generic, route-based connection pool modelled on
 * httpcore5's StrictConnPool (org.apache.hc.core5.pool).
 *
 * The pool manages *entries*; a connection is an opaque void * that only
 * the transport layer understands (here: the libevhtp engine). The pool
 * never opens connections. A lease completes with an entry that either
 * already carries a pooled connection or is empty, and the caller connects
 * and calls hc_pool_entry_assign(). It closes connections only through
 * hc_pool_conn_ops.close, mirroring ManagedConnPool<T, C extends
 * ModalCloseable>.
 *
 *   Java (StrictConnPool)                 C
 *   ------------------------------------  ----------------------------------
 *   lease(route, state, timeout, cb)      hc_pool_lease()   (route: hc_host,
 *                                                            Java: HttpHost)
 *   Future.cancel()                       hc_pool_lease_cancel()
 *   release(entry, reusable)              hc_pool_release()
 *   PoolEntry.assignConnection            hc_pool_entry_assign()
 *   PoolEntry.discardConnection           hc_pool_entry_discard()
 *   PoolEntry.updateExpiry(keepAlive)     hc_pool_entry_update_expiry()
 *   setMaxTotal / setMaxPerRoute          hc_pool_set_max_total/_per_route()
 *   getTotalStats / getStats(route)       hc_pool_get_total_stats/_route_stats()
 *   closeIdle / closeExpired              hc_pool_close_idle/_expired()
 *   PoolReusePolicy LIFO / FIFO           hc_pool_config.reuse_policy
 *   ConnPoolListener                      hc_pool_listener
 *
 * Single-threaded, so no locking: use it on the reactor's thread. Lease
 * callbacks are always delivered from the event loop (never from inside
 * hc_pool_lease()), consistent with hc.h.
 */
#ifndef HC_POOL_H
#define HC_POOL_H

#include "hc.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct hc_pool_entry hc_pool_entry;
typedef struct hc_pool_lease_req hc_pool_lease_req;

typedef enum {
    HC_POOL_REUSE_LIFO = 0,     /* most recently used first: fewer, hotter conns */
    HC_POOL_REUSE_FIFO          /* round-robin over idle connections             */
} hc_pool_reuse_policy;

typedef struct hc_pool_config {
    int                  max_total;
    int                  default_max_per_route;
    int64_t              time_to_live_ms;   /* <= 0: unlimited */
    hc_pool_reuse_policy reuse_policy;
} hc_pool_config;

/* The pool's only view of a connection. close() must release everything
 * the connection holds; it may be called for a connection that has already
 * died, and is called from the event loop (pool tasks, release, timers). */
typedef struct hc_pool_conn_ops {
    void (*close)(void *ctx, void *conn);
} hc_pool_conn_ops;

typedef struct hc_pool_stats {
    int leased;
    int pending;
    int available;
    int max;
} hc_pool_stats;

/* Java: ConnPoolListener. Either callback may be NULL. */
typedef struct hc_pool_listener {
    void (*on_lease)(void *arg, const hc_pool_entry *e, hc_pool_stats totals);
    void (*on_release)(void *arg, const hc_pool_entry *e, int reusable,
                       hc_pool_stats totals);
    void  *arg;
} hc_pool_listener;

typedef struct hc_pool hc_pool;

hc_pool *hc_pool_new(hc_reactor *r, const hc_pool_config *cfg,
                     const hc_pool_conn_ops *ops, void *ops_ctx);
/* Fails pending leases ("pool shut down"), closes idle and leased
 * connections and frees every entry. Leased entries become invalid. */
void     hc_pool_free(hc_pool *p);
void     hc_pool_set_listener(hc_pool *p, const hc_pool_listener *l);

/* --- leasing ------------------------------------------------------------ */

/* entry != NULL and error == NULL on success. state is matched by pointer
 * (Java: Objects.equals on the state object); NULL is the common case. */
typedef void (*hc_pool_lease_cb)(void *arg, hc_pool_entry *e, const char *error);

/* timeout_ms <= 0: wait for a slot indefinitely. The handle is valid until
 * the callback runs or hc_pool_lease_cancel() is called. */
hc_pool_lease_req *hc_pool_lease(hc_pool *p, const hc_host *route,
                             const void *state, int64_t timeout_ms,
                             hc_pool_lease_cb cb, void *arg);
void           hc_pool_lease_cancel(hc_pool_lease_req *l);

/* Give back a leased entry. Kept only if reusable, it still has a
 * connection, it has not expired and the pool is not shutting down;
 * otherwise its connection is closed and the slot freed. */
void hc_pool_release(hc_pool *p, hc_pool_entry *e, int reusable);

/* An *idle* entry's connection died on its own (peer closed, unexpected
 * bytes). The entry is removed and ops.close() is called to free it. */
void hc_pool_connection_lost(hc_pool *p, hc_pool_entry *e);

/* --- housekeeping, limits, stats ------------------------------------------ */

void hc_pool_close_idle(hc_pool *p, int64_t idle_ms);
void hc_pool_close_expired(hc_pool *p);

void hc_pool_set_max_total(hc_pool *p, int max);
void hc_pool_set_default_max_per_route(hc_pool *p, int max);
void hc_pool_set_max_per_route(hc_pool *p, const hc_host *route, int max);
int  hc_pool_get_max_per_route(hc_pool *p, const hc_host *route);

hc_pool_stats hc_pool_get_total_stats(const hc_pool *p);
hc_pool_stats hc_pool_get_route_stats(hc_pool *p, const hc_host *route);

/* --- entries (Java: PoolEntry) -------------------------------------------- */

const hc_host *hc_pool_entry_route(const hc_pool_entry *e);
const void *hc_pool_entry_state(const hc_pool_entry *e);
void       *hc_pool_entry_conn(const hc_pool_entry *e);  /* NULL: connect first */
void        hc_pool_entry_assign(hc_pool_entry *e, void *conn);
void        hc_pool_entry_discard(hc_pool_entry *e);     /* closes the connection */
void        hc_pool_entry_set_state(hc_pool_entry *e, const void *state);
/* Expire at min(now + keep_alive_ms, created + time_to_live).
 * keep_alive_ms < 0: no keep-alive limit. */
void        hc_pool_entry_update_expiry(hc_pool_entry *e, int64_t keep_alive_ms);
int64_t     hc_pool_entry_created_ms(const hc_pool_entry *e);
int64_t     hc_pool_entry_updated_ms(const hc_pool_entry *e);
int64_t     hc_pool_entry_expiry_ms(const hc_pool_entry *e);   /* INT64_MAX: never */

#ifdef __cplusplus
}
#endif

#endif /* HC_POOL_H */
