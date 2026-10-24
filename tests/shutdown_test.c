/*
 * shutdown_test.c - freeing a requester while the application's event loop
 * keeps running (htm8 does this on every config reload: the old listener
 * generation is freed, the worker thread's loop carries on).
 *
 * hc_requester_free() must finish everything it started before it returns:
 * exchanges in flight, cancelled exchanges whose abort is still queued,
 * connect requests waiting for a pool slot, endpoints released but not yet
 * handed back, endpoints still held. Each handler sees failed + release
 * (or its connect callback an error) exactly once, during the free, and
 * nothing of the requester is left on the loop afterwards. Run under
 * valgrind (make check does) to see the use-after-free this guards against.
 *
 * The origin accepts connections and reads requests but never answers, so
 * exchanges stay in flight for as long as the test wants.
 */
#include "hc.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>

#define PORT 9320

static int passed, failed;

#define CHECK(c) do {                                                      \
        if (c) { passed++; }                                               \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } \
    } while (0)

static struct event_base *base;
static hc_reactor        *R;
static hc_host            ORIGIN;

/* ======================================================================== */
/* Origin: reads, never answers                                             */
/* ======================================================================== */

#define MAX_CONNS 16
static struct bufferevent *conns[MAX_CONNS];
static int                 n_conns;

static void
origin_read(struct bufferevent *bev, void *arg)
{
    (void)arg;
    evbuffer_drain(bufferevent_get_input(bev), evbuffer_get_length(bufferevent_get_input(bev)));
}

static void
origin_event(struct bufferevent *bev, short what, void *arg)
{
    (void)what;
    (void)arg;

    for (int i = 0; i < n_conns; i++) {
        if (conns[i] == bev) {
            conns[i] = NULL;
        }
    }

    bufferevent_free(bev);
}

static void
origin_accept(struct evconnlistener *l, evutil_socket_t fd, struct sockaddr *sa,
              int salen, void *arg)
{
    struct bufferevent *bev = bufferevent_socket_new(evconnlistener_get_base(l), fd,
                                                     BEV_OPT_CLOSE_ON_FREE);

    (void)sa;
    (void)salen;
    (void)arg;
    bufferevent_setcb(bev, origin_read, NULL, origin_event, NULL);
    bufferevent_enable(bev, EV_READ);

    if (n_conns < MAX_CONNS) {
        conns[n_conns++] = bev;
    }
}

/* ======================================================================== */
/* Client handler: records what it was told                                 */
/* ======================================================================== */

typedef struct rec {
    hc_client_handler   base;
    hc_requester       *rq;
    hc_client_exchange *cx;
    int                 sent;
    int                 failed;
    int                 ended;
    int                 released;
    int                 reexec;      /* hc_requester_execute() from failed() */
    char                reason[64];
} rec;

#define REC(self) ((rec *)(void *)(self))

static void
rec_produce_request(hc_client_handler *self, hc_request_channel *rc)
{
    rec            *r = REC(self);
    hc_headers     *h = hc_headers_new();
    hc_request_head head = { "GET", "/never", "127.0.0.1", 11, h };

    r->cx = hc_request_channel_exchange(rc);
    hc_request_send(rc, &head, NULL);
    hc_headers_free(h);
    r->sent = 1;
}

static void
rec_consume_response(hc_client_handler *self, const hc_response_head *head,
                     const hc_entity *e)
{
    (void)self;
    (void)head;
    (void)e;
}

static void rec_consume(hc_client_handler *s, const void *d, size_t n) { (void)s; (void)d; (void)n; }

static void
rec_stream_end(hc_client_handler *self, const hc_headers *t)
{
    (void)t;
    REC(self)->ended++;
}

static rec spare;   /* a handler for the re-execute attempt; never runs */

static void
rec_failed(hc_client_handler *self, const char *reason)
{
    rec *r = REC(self);

    r->failed++;
    snprintf(r->reason, sizeof(r->reason), "%s", reason);

    /* What an application might do on failure: retry. The requester is
     * shutting down, so it must refuse. */
    if (r->rq) {
        r->reexec = hc_requester_execute(r->rq, &ORIGIN, 1000, &spare.base);
    }
}

static void
rec_release(hc_client_handler *self)
{
    REC(self)->released++;
}

static const hc_client_handler_vtbl rec_vtbl = {
    .produce_request  = rec_produce_request,
    .consume_response = rec_consume_response,
    .consume          = rec_consume,
    .stream_end       = rec_stream_end,
    .failed           = rec_failed,
    .release          = rec_release,
};

static void
rec_init(rec *r, hc_requester *rq)
{
    memset(r, 0, sizeof(*r));
    r->base.vtbl = &rec_vtbl;
    r->rq        = rq;
    r->reexec    = 99;
}

/* Raw connect callbacks. */
typedef struct conn_rec {
    hc_endpoint *ep;
    int          calls;
    char         error[64];
} conn_rec;

static void
on_connected(void *arg, hc_endpoint *ep, const char *error)
{
    conn_rec *c = arg;

    c->calls++;
    c->ep = ep;
    snprintf(c->error, sizeof(c->error), "%s", error ? error : "");
}

/* ======================================================================== */
/* Loop helpers                                                             */
/* ======================================================================== */

static void
stop_cb(evutil_socket_t fd, short what, void *arg)
{
    (void)fd;
    (void)what;
    event_base_loopbreak(arg);
}

/* Run the application's loop for ms milliseconds. */
static void
run_for(int ms)
{
    struct timeval tv = { ms / 1000, (ms % 1000) * 1000 };
    struct event  *ev = evtimer_new(base, stop_cb, base);

    evtimer_add(ev, &tv);
    event_base_dispatch(base);
    event_free(ev);
}

static hc_requester *
new_rq(int max_total)
{
    hc_requester_config c = { 0 };

    c.max_total             = max_total;
    c.default_max_per_route = max_total;

    return hc_requester_new(R, &c);
}

/* ======================================================================== */
/* Cases                                                                    */
/* ======================================================================== */

/* An exchange waiting for its response. */
static void
case_in_flight(void)
{
    hc_requester *rq = new_rq(0);
    rec           a;

    printf("in flight\n");
    rec_init(&a, rq);
    CHECK(hc_requester_execute(rq, &ORIGIN, 5000, &a.base) == 0);
    run_for(150);
    CHECK(a.sent && a.failed == 0 && a.released == 0);

    hc_requester_free(rq);
    CHECK(a.failed == 1 && a.released == 1 && a.ended == 0);
    CHECK(a.reexec == -1);
    printf("  failed: %s\n", a.reason);

    run_for(100);               /* the loop carries on: nothing may fire */
    CHECK(a.failed == 1 && a.released == 1);
}

/* htm8's case: the server side cancels the upstream exchange (abort
 * queued on the loop), then the requester is freed before it runs. */
static void
case_cancel_then_free(void)
{
    hc_requester *rq = new_rq(0);
    rec           a;

    printf("cancel, then free\n");
    rec_init(&a, rq);
    CHECK(hc_requester_execute(rq, &ORIGIN, 5000, &a.base) == 0);
    run_for(150);
    CHECK(a.sent && a.cx);

    hc_client_cancel(a.cx);
    hc_requester_free(rq);
    CHECK(a.failed == 1 && a.released == 1);

    run_for(100);
    CHECK(a.failed == 1 && a.released == 1);
}

/* Connect requests still waiting for a pool slot (one in flight holds the
 * only slot), and one whose lease has not even been served yet. */
static void
case_waiting_for_slot(void)
{
    hc_requester *rq = new_rq(1);
    rec           a, b, c;

    printf("waiting for a pool slot\n");
    rec_init(&a, rq);
    rec_init(&b, rq);
    CHECK(hc_requester_execute(rq, &ORIGIN, 5000, &a.base) == 0);
    CHECK(hc_requester_execute(rq, &ORIGIN, 5000, &b.base) == 0);
    run_for(150);
    CHECK(a.sent && !b.sent);

    rec_init(&c, rq);
    CHECK(hc_requester_execute(rq, &ORIGIN, 5000, &c.base) == 0);   /* not yet served */

    hc_requester_free(rq);
    CHECK(a.failed == 1 && a.released == 1);
    CHECK(b.failed == 1 && b.released == 1 && !b.sent);
    CHECK(c.failed == 1 && c.released == 1 && !c.sent);
    printf("  waiting: %s\n", b.reason);

    run_for(100);
    CHECK(b.failed == 1 && b.released == 1 && c.failed == 1 && c.released == 1);
}

/* Raw endpoints: one released (hand-back queued on the loop), one still
 * held by the application, and a connect request not yet answered. */
static void
case_endpoints(void)
{
    hc_requester *rq = new_rq(0);
    conn_rec      e1 = { 0 }, e2 = { 0 }, e3 = { 0 };

    printf("endpoints\n");
    CHECK(hc_requester_connect(rq, &ORIGIN, 5000, on_connected, &e1) != NULL);
    CHECK(hc_requester_connect(rq, &ORIGIN, 5000, on_connected, &e2) != NULL);
    run_for(150);
    CHECK(e1.calls == 1 && e1.ep && e2.calls == 1 && e2.ep);

    hc_endpoint_release_and_reuse(e1.ep);   /* queued, not yet run */
    CHECK(hc_requester_connect(rq, &ORIGIN, 5000, on_connected, &e3) != NULL);

    hc_requester_free(rq);                  /* e2.ep is now invalid */
    CHECK(e3.calls == 1 && e3.ep == NULL && e3.error[0]);
    printf("  connect: %s\n", e3.error);

    run_for(100);
    CHECK(e3.calls == 1);
}

/* htm8's reload, end to end: a reverse proxy (server + requester on the
 * application's loop) with a request in flight to the origin. The server
 * handler cancels its upstream exchange when released, as htm8's does.
 * Free the server, then the requester; the loop keeps running. */
static hc_requester *proxy_rq;
static rec            upstream;
static int            proxy_released;

typedef struct proxy_handler {
    hc_server_handler base;
} proxy_handler;

static void
ph_handle_request(hc_server_handler *self, hc_server_exchange *ex,
                  const hc_request_head *head, const hc_entity *entity,
                  hc_response_channel *rc)
{
    (void)self;
    (void)ex;
    (void)head;
    (void)entity;
    (void)rc;
    rec_init(&upstream, NULL);
    CHECK(hc_requester_execute(proxy_rq, &ORIGIN, 5000, &upstream.base) == 0);
}

static void ph_consume(hc_server_handler *s, const void *d, size_t n) { (void)s; (void)d; (void)n; }
static void ph_stream_end(hc_server_handler *s, const hc_headers *t) { (void)s; (void)t; }

static void
ph_release(hc_server_handler *self)
{
    if (upstream.cx && !upstream.released) {
        hc_client_cancel(upstream.cx);      /* queued on the loop */
    }

    proxy_released++;
    free(self);
}

static const hc_server_handler_vtbl ph_vtbl = {
    .handle_request = ph_handle_request,
    .consume        = ph_consume,
    .stream_end     = ph_stream_end,
    .release        = ph_release,
};

static hc_server_handler *
ph_factory(void *arg)
{
    proxy_handler *p = calloc(1, sizeof(*p));

    (void)arg;
    p->base.vtbl = &ph_vtbl;
    return &p->base;
}

static void
case_proxy_reload(void)
{
    static const char  req[] = "GET /slow HTTP/1.1\r\nHost: test\r\n\r\n";
    struct sockaddr_in sin = { .sin_family = AF_INET, .sin_port = htons(PORT + 1) };
    hc_server         *srv = hc_server_new(R, ph_factory, NULL);
    int                fd  = socket(AF_INET, SOCK_STREAM, 0);

    printf("proxy reload\n");
    proxy_rq = new_rq(0);
    CHECK(srv && proxy_rq && hc_server_listen(srv, "127.0.0.1", PORT + 1) == 0);

    inet_pton(AF_INET, "127.0.0.1", &sin.sin_addr);
    CHECK(connect(fd, (struct sockaddr *)&sin, sizeof(sin)) == 0);  /* backlog */
    CHECK(write(fd, req, sizeof(req) - 1) == (ssize_t)(sizeof(req) - 1));
    run_for(200);
    CHECK(upstream.sent && upstream.failed == 0 && proxy_released == 0);

    hc_server_free(srv);
    CHECK(proxy_released == 1);
    hc_requester_free(proxy_rq);
    CHECK(upstream.failed == 1 && upstream.released == 1);

    run_for(100);
    CHECK(upstream.failed == 1 && upstream.released == 1);
    close(fd);
}

/* After all that, a fresh requester on the same loop works normally. */
static void
case_still_usable(void)
{
    hc_requester *rq = new_rq(0);
    rec           a;

    printf("loop still usable\n");
    rec_init(&a, NULL);
    CHECK(hc_requester_execute(rq, &ORIGIN, 5000, &a.base) == 0);
    run_for(150);
    CHECK(a.sent && a.failed == 0);

    hc_requester_free(rq);
    CHECK(a.failed == 1 && a.released == 1);
}

int
main(void)
{
    struct sockaddr_in      sin;
    struct evconnlistener  *l;

    base = event_base_new();
    R    = hc_reactor_new_on_base(base);
    hc_host_init(&ORIGIN, "http", "127.0.0.1", PORT);

    memset(&sin, 0, sizeof(sin));
    sin.sin_family      = AF_INET;
    sin.sin_port        = htons(PORT);
    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    l = evconnlistener_new_bind(base, origin_accept, NULL,
                                LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE, -1,
                                (struct sockaddr *)&sin, sizeof(sin));

    if (!base || !R || !l) {
        fprintf(stderr, "setup failed\n");
        return 1;
    }

    case_in_flight();
    case_cancel_then_free();
    case_waiting_for_slot();
    case_endpoints();
    case_proxy_reload();
    case_still_usable();

    run_for(50);                /* origin sees the closes */

    for (int i = 0; i < n_conns; i++) {
        if (conns[i]) {
            bufferevent_free(conns[i]);
        }
    }

    evconnlistener_free(l);
    hc_reactor_free(R);
    event_base_free(base);

    printf("%d checks, %d failed\n", passed + failed, failed);
    return failed ? 1 : 0;
}
