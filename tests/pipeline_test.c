/*
 * pipeline_test.c - HTTP/1.1 pipelining on an endpoint (hc_endpoint_execute
 * with several exchanges queued), against a scripted origin.
 *
 * The origin either waits until it has received N complete requests and
 * then writes every response in one go - which a client that does not
 * pipeline could never satisfy, and which leaves later responses already
 * buffered when an earlier one completes - or answers each request as it
 * arrives.
 */
#include "hc.h"
#include "hc_pool.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>

#define PORT    9330
#define MAX_X   8

static int passed, failed;

#define CHECK(c) do {                                                      \
        if (c) { passed++; }                                               \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } \
    } while (0)

static struct event_base *base;
static hc_reactor        *R;
static hc_requester      *RQ;
static hc_host            ORIGIN;

/* ======================================================================== */
/* Scripted origin                                                          */
/* ======================================================================== */

static struct {
    int                 wait_for;       /* >0: answer once this many arrived  */
    const char         *all;            /* ...with this, in one write         */
    const char         *each[MAX_X];    /* else: response i when request i is in */
    int                 close_after;    /* close once the responses are out   */
    int                 responded;      /* something was written              */
    /* observed */
    int                 accepts;
    int                 requests;
    char                log[1024];      /* "Q /a|A1|..."                      */
    char                bodies[256];
    struct bufferevent *bev;
} O;

static void
olog(const char *s)
{
    strncat(O.log, s, sizeof(O.log) - strlen(O.log) - 1);
}

static void
origin_write(struct bufferevent *bev, const char *s)
{
    evbuffer_add(bufferevent_get_output(bev), s, strlen(s));
    O.responded = 1;
}

static void
origin_flushed(struct bufferevent *bev, void *arg)
{
    (void)arg;

    if (O.close_after && O.responded && evbuffer_get_length(bufferevent_get_output(bev)) == 0) {
        bufferevent_free(bev);
        O.bev = NULL;
    }
}

/* Complete requests out of the input, each one logged and maybe answered. */
static void
origin_read(struct bufferevent *bev, void *arg)
{
    struct evbuffer *in = bufferevent_get_input(bev);

    (void)arg;

    for (;;) {
        struct evbuffer_ptr eoh = evbuffer_search(in, "\r\n\r\n", 4, NULL);
        size_t              hlen, blen = 0;
        char                head[2048], line[256], *cl;

        if (eoh.pos < 0) {
            return;
        }

        hlen = (size_t)eoh.pos + 4;

        if (hlen >= sizeof(head)) {
            return;
        }

        evbuffer_copyout(in, head, hlen);
        head[hlen] = '\0';

        if ((cl = strstr(head, "\r\nContent-Length:")) != NULL) {   /* as hcore writes it */
            blen = (size_t)strtoul(cl + 17, NULL, 10);
        }

        if (evbuffer_get_length(in) < hlen + blen) {
            return;                         /* body not all here yet */
        }

        evbuffer_drain(in, hlen);

        if (blen) {
            char body[128] = { 0 };

            evbuffer_remove(in, body, blen < sizeof(body) - 1 ? blen : sizeof(body) - 1);
            strncat(O.bodies, body, sizeof(O.bodies) - strlen(O.bodies) - 1);
        }

        /* "GET /a HTTP/1.1" -> "Q GET /a|" */
        sscanf(head, "%*s %255s", line);
        olog("Q ");
        olog(line);
        olog("|");
        O.requests++;

        if (O.wait_for > 0 && O.requests == O.wait_for) {
            origin_write(bev, O.all);
            olog("A*|");
        } else if (O.wait_for == 0 && O.requests <= MAX_X && O.each[O.requests - 1]) {
            char tag[16];

            origin_write(bev, O.each[O.requests - 1]);
            snprintf(tag, sizeof(tag), "A%d|", O.requests);
            olog(tag);
        }
    }
}

static void
origin_event(struct bufferevent *bev, short what, void *arg)
{
    (void)what;
    (void)arg;
    bufferevent_free(bev);

    if (O.bev == bev) {
        O.bev = NULL;
    }
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
    O.accepts++;
    O.bev = bev;
    bufferevent_setcb(bev, origin_read, origin_flushed, origin_event, NULL);
    bufferevent_enable(bev, EV_READ | EV_WRITE);
}

/* New script; the connection (if the client kept it) stays. */
static void
origin_script(int wait_for, const char *all, int close_after)
{
    O.wait_for    = wait_for;
    O.all         = all;
    O.close_after = close_after;
    O.responded   = 0;
    O.requests    = 0;
    O.log[0]      = '\0';
    O.bodies[0]   = '\0';
    memset(O.each, 0, sizeof(O.each));
}

/* ======================================================================== */
/* Client side: one recording handler per exchange                          */
/* ======================================================================== */

typedef struct xch {
    hc_client_handler   base;           /* must be first */
    const char         *method;
    const char         *path;
    const char         *body;           /* NULL: no body */
    hc_client_exchange *cx;
    int                 status;
    int                 info;           /* interim responses seen */
    char                got[64];
    int                 ended, failed_n, released;
    char                reason[64];
    /* cancel this other exchange from produce_request (once sent) */
    struct xch         *cancel_other;
} xch;

static xch      X[MAX_X];
static int      n_x, n_released;
static hc_endpoint *EP;
static void   (*on_all_released)(void);

#define XCH(self) ((xch *)(void *)(self))

static void
x_produce_request(hc_client_handler *self, hc_request_channel *rc)
{
    xch             *x = XCH(self);
    hc_headers      *h = hc_headers_new();
    hc_request_head  head = { x->method, x->path, "127.0.0.1", 11, h };
    hc_entity        e    = { x->body ? (int64_t)strlen(x->body) : 0, "text/plain" };
    hc_data_channel *ch;

    x->cx = hc_request_channel_exchange(rc);
    ch    = hc_request_send(rc, &head, x->body ? &e : NULL);
    hc_headers_free(h);

    if (ch) {
        hc_data_write(ch, x->body, strlen(x->body));
        hc_data_end(ch, NULL);
    }

    if (x->cancel_other && x->cancel_other->cx) {
        hc_client_cancel(x->cancel_other->cx);
    }
}

static void
x_consume_response(hc_client_handler *self, const hc_response_head *head, const hc_entity *e)
{
    (void)e;
    XCH(self)->status = head->status;
}

static void
x_consume(hc_client_handler *self, const void *d, size_t n)
{
    xch *x = XCH(self);

    strncat(x->got, d, n < sizeof(x->got) - strlen(x->got) - 1 ? n : sizeof(x->got) - strlen(x->got) - 1);
}

static void
x_stream_end(hc_client_handler *self, const hc_headers *t)
{
    (void)t;
    XCH(self)->ended++;
}

static void
x_failed(hc_client_handler *self, const char *reason)
{
    xch *x = XCH(self);

    x->failed_n++;
    snprintf(x->reason, sizeof(x->reason), "%s", reason);
}

static void
next_cb(evutil_socket_t fd, short what, void *arg)
{
    (void)fd;
    (void)what;
    (void)arg;
    on_all_released();
}

static void
x_release(hc_client_handler *self)
{
    XCH(self)->released++;

    if (++n_released == n_x) {
        /* out of the engine's stack before the next case */
        event_base_once(base, -1, EV_TIMEOUT, next_cb, NULL, NULL);
    }
}

static void
x_info(hc_client_handler *self, const hc_response_head *head)
{
    (void)head;
    XCH(self)->info++;
}

static const hc_client_handler_vtbl x_vtbl = {
    .produce_request     = x_produce_request,
    .consume_response    = x_consume_response,
    .consume             = x_consume,
    .stream_end          = x_stream_end,
    .failed              = x_failed,
    .release             = x_release,
    .consume_information = x_info,
};

static xch *
x_add(const char *method, const char *path, const char *body)
{
    xch *x = &X[n_x++];

    memset(x, 0, sizeof(*x));
    x->base.vtbl = &x_vtbl;
    x->method    = method;
    x->path      = path;
    x->body      = body;
    return x;
}

/* Execute every prepared exchange on the endpoint at once. */
static void
run_all(void)
{
    for (int i = 0; i < n_x; i++) {
        CHECK(hc_endpoint_execute(EP, &X[i].base) != NULL);
    }
}

static int
ok(const xch *x, int status, const char *body)
{
    int good = x->ended == 1 && x->failed_n == 0 && x->released == 1 &&
               x->status == status && strcmp(x->got, body) == 0;

    if (!good) {
        fprintf(stderr, "    %s %s: status %d ended %d failed %d (%s) body \"%s\"\n",
                x->method, x->path, x->status, x->ended, x->failed_n, x->reason, x->got);
    }

    return good;
}

static int
failed_with(const xch *x, const char *reason)
{
    int good = x->failed_n == 1 && x->ended == 0 && x->released == 1 &&
               strstr(x->reason, reason) != NULL;

    if (!good) {
        fprintf(stderr, "    %s %s: expected failure \"%s\", got failed %d (%s) ended %d\n",
                x->method, x->path, reason, x->failed_n, x->reason, x->ended);
    }

    return good;
}

/* ======================================================================== */
/* Cases                                                                    */
/* ======================================================================== */

static int step;
static void next_case(void);

static void
connected(void *arg, hc_endpoint *ep, const char *error)
{
    void (*start)(void) = (void (*)(void))arg;

    CHECK(error == NULL);
    EP = ep;

    if (ep) {
        start();
    } else {
        next_case();
    }
}

/* Lease the endpoint, then start(): the case's exchanges. */
static void
with_endpoint(void (*start)(void))
{
    n_x        = 0;
    n_released = 0;
    hc_requester_connect(RQ, &ORIGIN, 5000, connected, (void *)start);
}

static hc_pool_stats
route_stats(void)
{
    return hc_pool_get_route_stats(hc_requester_pool(RQ), &ORIGIN);
}

/* --- A: four requests on the wire before any response ------------------ */
static void
a_start(void)
{
    puts("4 GETs pipelined; origin answers only after all 4 arrived, in one write");
    origin_script(4,
        "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\na"
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nb\r\n0\r\n\r\n"
        "HTTP/1.1 103 Early Hints\r\nLink: </x>\r\n\r\n"
        "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nc"
        "HTTP/1.1 404 Not Found\r\nContent-Length: 1\r\n\r\nd", 0);
    x_add("GET", "/a", NULL);
    x_add("GET", "/b", NULL);
    x_add("GET", "/c", NULL);
    x_add("GET", "/d", NULL);
    run_all();
}

static void
a_check(void)
{
    CHECK(strcmp(O.log, "Q /a|Q /b|Q /c|Q /d|A*|") == 0);
    CHECK(ok(&X[0], 200, "a"));
    CHECK(ok(&X[1], 200, "b"));
    CHECK(ok(&X[2], 200, "c") && X[2].info == 1);  /* 103 at the hand-over */
    CHECK(ok(&X[3], 404, "d"));
    printf("  origin saw: %s\n", O.log);
}

/* --- B: the endpoint is reused afterwards ------------------------------ */
static void
b_start(void)
{
    puts("same connection afterwards: one more exchange on the reused endpoint");
    origin_script(1, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok", 0);
    x_add("GET", "/again", NULL);
    run_all();
}

/* --- C: HEAD is not pipelined behind ----------------------------------- */
static void
c_start(void)
{
    puts("GET, HEAD, GET: the request after HEAD waits for its response");
    origin_script(0, NULL, 0);
    O.each[0] = "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n1";
    O.each[1] = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n";      /* HEAD: no body */
    O.each[2] = "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n3";
    x_add("GET", "/1", NULL);
    x_add("HEAD", "/2", NULL);
    x_add("GET", "/3", NULL);
    run_all();
}

static void
c_check(void)
{
    CHECK(ok(&X[0], 200, "1"));
    CHECK(ok(&X[1], 200, ""));
    CHECK(ok(&X[2], 200, "3"));
    CHECK(strstr(O.log, "A2|Q /3|") != NULL);   /* sent after HEAD's response */
    printf("  origin saw: %s\n", O.log);
}

/* --- D: a request body in the pipeline --------------------------------- */
static void
d_start(void)
{
    puts("GET, POST with a body, GET pipelined: the body follows its head");
    origin_script(3,
        "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nx"
        "HTTP/1.1 201 Created\r\nContent-Length: 1\r\n\r\ny"
        "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nz", 0);
    x_add("GET", "/x", NULL);
    x_add("POST", "/y", "hello world");
    x_add("GET", "/z", NULL);
    run_all();
}

static void
d_check(void)
{
    CHECK(strcmp(O.log, "Q /x|Q /y|Q /z|A*|") == 0);
    CHECK(strcmp(O.bodies, "hello world") == 0);
    CHECK(ok(&X[0], 200, "x") && ok(&X[1], 201, "y") && ok(&X[2], 200, "z"));
}

/* --- E: "Connection: close" ends the pipeline -------------------------- */
static void
e_start(void)
{
    puts("4 pipelined, the 2nd response says Connection: close: 3rd and 4th fail");
    origin_script(4,
        "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n1"
        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 1\r\n\r\n2", 1);
    x_add("GET", "/1", NULL);
    x_add("GET", "/2", NULL);
    x_add("GET", "/3", NULL);
    x_add("GET", "/4", NULL);
    run_all();
}

static void
e_check(void)
{
    CHECK(ok(&X[0], 200, "1") && ok(&X[1], 200, "2"));
    CHECK(failed_with(&X[2], "connection") && failed_with(&X[3], "connection"));
}

/* --- F: the origin just closes ----------------------------------------- */
static void
f_start(void)
{
    puts("3 pipelined, origin closes after 1 response: 2nd and 3rd fail");
    origin_script(3, "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n1", 1);
    x_add("GET", "/1", NULL);
    x_add("GET", "/2", NULL);
    x_add("GET", "/3", NULL);
    run_all();
}

static void
f_check(void)
{
    CHECK(ok(&X[0], 200, "1"));
    CHECK(failed_with(&X[1], "closed") && failed_with(&X[2], "closed"));
}

/* --- G: cancelling an exchange not sent yet ---------------------------- */
static void
g_start(void)
{
    puts("HEAD, GET, GET; the 2nd cancelled before it was sent: only it fails");
    origin_script(0, NULL, 0);
    O.each[0] = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n";
    O.each[1] = "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n3";
    x_add("HEAD", "/1", NULL);
    x_add("GET", "/2", NULL);
    x_add("GET", "/3", NULL);

    for (int i = 0; i < n_x; i++) {
        X[i].cx = hc_endpoint_execute(EP, &X[i].base);
        CHECK(X[i].cx != NULL);
    }

    hc_client_cancel(X[1].cx);  /* queued behind HEAD: not on the wire */
}

static void
g_check(void)
{
    CHECK(ok(&X[0], 200, ""));
    CHECK(failed_with(&X[1], "cancelled"));
    CHECK(ok(&X[2], 200, "3"));
    CHECK(strcmp(O.log, "Q /1|A1|Q /3|A2|") == 0);
    printf("  origin saw: %s\n", O.log);
}

/* --- H: cancelling an exchange already sent ---------------------------- */
static void
h_start(void)
{
    puts("3 pipelined, the 2nd cancelled after it was sent: the connection closes");
    origin_script(99, "", 0);   /* never answers */
    x_add("GET", "/1", NULL);
    x_add("GET", "/2", NULL);
    x_add("GET", "/3", NULL)->cancel_other = &X[1];    /* by then /2 is written */
    run_all();
}

static void
h_check(void)
{
    CHECK(failed_with(&X[1], "cancelled"));
    CHECK(failed_with(&X[0], "closed") && failed_with(&X[2], "closed"));
}

/* --- I: release-and-discard with exchanges queued ---------------------- */
static void
i_start(void)
{
    puts("3 pipelined to an origin that never answers, endpoint discarded: all cancelled");
    origin_script(99, "", 0);
    x_add("GET", "/1", NULL);
    x_add("GET", "/2", NULL);
    x_add("GET", "/3", NULL);
    run_all();
    hc_endpoint_release_and_discard(EP);
    EP = NULL;
}

static void
i_check(void)
{
    CHECK(X[0].failed_n == 1 && X[1].failed_n == 1 && X[2].failed_n == 1);
    CHECK(X[0].released == 1 && X[1].released == 1 && X[2].released == 1);
}

/* ------------------------------------------------------------------------ */

static void
release_ep(int reuse)
{
    if (EP) {
        if (reuse) {
            hc_endpoint_release_and_reuse(EP);
        } else {
            hc_endpoint_release_and_discard(EP);
        }

        EP = NULL;
    }
}

static void
after_release_cb(evutil_socket_t fd, short what, void *arg)
{
    (void)fd;
    (void)what;
    (void)arg;
    next_case();
}

/* Give the endpoint back, then go on once that has happened. */
static void
release_then_next(int reuse)
{
    struct timeval t = { 0, 20000 };

    release_ep(reuse);
    event_base_once(base, -1, EV_TIMEOUT, after_release_cb, NULL, &t);
}

/* Each case: lease the endpoint, run its exchanges, check, give it back
 * (for reuse; a broken connection is closed by the pool). */
static void
next_case(void)
{
    switch (step++) {
        case 0:  on_all_released = next_case; with_endpoint(a_start); break;
        case 1:  a_check(); release_then_next(1); break;
        case 2:  CHECK(route_stats().available == 1);   /* kept alive */
                 with_endpoint(b_start);
                 break;
        case 3:  CHECK(ok(&X[0], 200, "ok"));
                 CHECK(O.accepts == 1);                 /* still the first connection */
                 release_then_next(1);
                 break;
        case 4:  with_endpoint(c_start); break;
        case 5:  c_check(); release_then_next(1); break;
        case 6:  with_endpoint(d_start); break;
        case 7:  d_check();
                 CHECK(O.accepts == 1);                 /* all of it on one connection */
                 release_then_next(1);
                 break;
        case 8:  with_endpoint(e_start); break;
        case 9:  e_check(); release_then_next(1); break;
        case 10: CHECK(route_stats().available == 0);   /* closed, not pooled */
                 with_endpoint(f_start);
                 break;
        case 11: f_check(); release_then_next(1); break;
        case 12: with_endpoint(g_start); break;
        case 13: g_check(); release_then_next(1); break;
        case 14: with_endpoint(h_start); break;
        case 15: h_check(); release_then_next(1); break;
        case 16: with_endpoint(i_start); break;
        case 17: i_check();
                 step = -1;
                 event_base_loopbreak(base);
                 break;
    }
}

static void
watchdog(evutil_socket_t fd, short what, void *arg)
{
    (void)fd;
    (void)what;
    (void)arg;
    fprintf(stderr, "  FAIL: timed out in step %d (origin saw: %s)\n", step, O.log);
    failed++;
    event_base_loopbreak(base);
}

static void
start_cb(evutil_socket_t fd, short what, void *arg)
{
    (void)fd;
    (void)what;
    (void)arg;
    next_case();
}

int
main(void)
{
    hc_requester_config     cfg = { 0 };
    struct sockaddr_in      sin;
    struct evconnlistener  *l;
    struct timeval          limit = { 30, 0 };

    base = event_base_new();
    R    = hc_reactor_new_on_base(base);
    cfg.read_timeout_ms = 3000;
    RQ   = hc_requester_new(R, &cfg);
    hc_host_init(&ORIGIN, "http", "127.0.0.1", PORT);

    memset(&sin, 0, sizeof(sin));
    sin.sin_family      = AF_INET;
    sin.sin_port        = htons(PORT);
    sin.sin_addr.s_addr = htonl(0x7f000001);
    l = evconnlistener_new_bind(base, origin_accept, NULL,
                                LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE, -1,
                                (struct sockaddr *)&sin, sizeof(sin));

    if (!R || !RQ || !l) {
        fprintf(stderr, "setup failed\n");
        return 1;
    }

    event_base_once(base, -1, EV_TIMEOUT, watchdog, NULL, &limit);
    event_base_once(base, -1, EV_TIMEOUT, start_cb, NULL, NULL);
    event_base_dispatch(base);

    release_ep(0);
    hc_requester_free(RQ);
    hc_reactor_free(R);

    if (O.bev) {
        bufferevent_free(O.bev);
    }

    evconnlistener_free(l);
    event_base_free(base);

    printf("pipeline_test: %d checks, %d failed\n", passed + failed, failed);

    return failed || step != -1 ? 1 : 0;
}
