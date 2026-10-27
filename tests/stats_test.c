/*
 * stats_test.c - hc_server_get_stats() and endpoint attachments.
 *
 * Server counters, checked against what happened on the wire:
 *   - plain HTTP/1.1: a connection with two keep-alive requests (bytes in
 *     and out counted exactly), then closed; a request cut off by the
 *     client (not completed); a body over the limit (rq_too_large)
 *   - TLS: an HTTP/2 connection with two streams moves from HTTP/1 to
 *     HTTP/2 when ALPN picks h2; an HTTP/1.1-over-TLS connection stays
 *     HTTP/1 (curl with HTTP/2 and a libevhtp with HTTP/2 only)
 *   - responses by status class (2xx, 3xx, 4xx incl. the 413, 5xx)
 *   - all gauges back to 0 at the end
 * Endpoint attachments: a fresh pooled connection has none; one set on it
 * is found on the next lease of the same connection, a new connection has
 * none, and each is freed once when its connection closes.
 *
 * Usage: stats_test <tests/tls dir>
 */
#define _GNU_SOURCE
#include "hc.h"
#include "hc_pool.h"

#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>

#include <event2/event.h>
#include <event2/thread.h>

#define PORT_H1  9360
#define PORT_TLS 9361
#define PORT_ORG 9362

static int passed, failed;

#define CHECK(c) do {                                                      \
        if (c) { passed++; }                                               \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } \
    } while (0)

#define CHECK_EQ(a, b) do {                                                \
        unsigned long long a_ = (unsigned long long)(a), b_ = (unsigned long long)(b); \
        if (a_ == b_) { passed++; }                                        \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s == %llu, want %llu\n", \
                                 __FILE__, __LINE__, #a, a_, b_); }        \
    } while (0)

static struct event_base *base;
static hc_reactor        *R;

static void
sleep_ms(int ms)
{
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };

    nanosleep(&ts, NULL);
}

/* ======================================================================== */
/* Server handler                                                           */
/* ======================================================================== */

typedef struct th {
    hc_server_handler    base;
    hc_response_channel *rc;
    char                 path[64];
    size_t               sent, total;
} th;

#define TH(self) ((th *)(void *)(self))

static void
h_request(hc_server_handler *self, hc_server_exchange *ex, const hc_request_head *head,
          const hc_entity *entity, hc_response_channel *rc)
{
    th *t = TH(self);

    (void)ex;
    (void)entity;
    t->rc = rc;
    snprintf(t->path, sizeof(t->path), "%s", head->target);

    if (strncmp(t->path, "/big/", 5) == 0) {
        hc_headers      *h  = hc_headers_new();
        hc_response_head rh = { 200, 11, h };
        hc_entity        e  = { -1, "application/octet-stream" };

        t->total = (size_t)atol(t->path + 5);
        hc_response_send(rc, &rh, &e);
        hc_headers_free(h);
    } else if (strcmp(t->path, "/fail") == 0) {
        hc_response_send_simple(rc, 503, "text/plain", "no upstream", 0);
    } else if (strcmp(t->path, "/moved") == 0) {
        hc_response_send_simple(rc, 302, "text/plain", "elsewhere", 0);
    } else if (strcmp(t->path, "/upload") != 0) {
        hc_response_send_simple(rc, 200, "text/plain", "hello", 0);
    }
}

static void h_consume(hc_server_handler *s, const void *d, size_t n) { (void)s; (void)d; (void)n; }

static void
h_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    (void)trailers;

    if (strcmp(TH(self)->path, "/upload") == 0) {
        hc_response_send_simple(TH(self)->rc, 200, "text/plain", "got it", 0);
    }
}

static void
h_produce(hc_server_handler *self, hc_data_channel *ch)
{
    th  *t = TH(self);
    char buf[16384];

    memset(buf, 'x', sizeof(buf));

    while (t->sent < t->total) {
        size_t room = hc_data_capacity(ch);
        size_t n    = t->total - t->sent;

        if (room == 0) {
            return;
        }

        n = n < room ? n : room;
        n = n < sizeof(buf) ? n : sizeof(buf);
        if (hc_data_write(ch, buf, n) != (long)n) {
            return;
        }
        t->sent += n;
    }

    hc_data_end(ch, NULL);
}

static void h_failed(hc_server_handler *s, const char *r) { (void)s; (void)r; }
static void h_release(hc_server_handler *self) { free(self); }

static const hc_server_handler_vtbl vt = {
    .handle_request = h_request,
    .consume        = h_consume,
    .stream_end     = h_stream_end,
    .produce        = h_produce,
    .failed         = h_failed,
    .release        = h_release,
};

static hc_server_handler *
factory(void *arg)
{
    th *t = calloc(1, sizeof(*t));

    (void)arg;
    t->base.vtbl = &vt;
    return &t->base;
}

/* ======================================================================== */
/* Raw sockets, curl                                                        */
/* ======================================================================== */

static int
connect_to(int port)
{
    struct sockaddr_in sa;
    int                fd = socket(AF_INET, SOCK_STREAM, 0);

    memset(&sa, 0, sizeof(sa));
    sa.sin_family      = AF_INET;
    sa.sin_port        = htons((uint16_t)port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) {
        close(fd);
        return -1;
    }

    return fd;
}

static size_t
send_str(int fd, const char *s)
{
    size_t len = strlen(s), done = 0;

    while (done < len) {
        ssize_t n = send(fd, s + done, len - done, MSG_NOSIGNAL);

        if (n <= 0) {
            break;
        }
        done += (size_t)n;
    }

    return done;
}

/* Read until `end` has been seen; returns the bytes read. */
static size_t
read_upto(int fd, char *out, size_t cap, const char *end)
{
    size_t kept = 0;

    out[0] = '\0';

    for (int i = 0; i < 100 && strstr(out, end) == NULL; i++) {
        struct pollfd p = { fd, POLLIN, 0 };
        ssize_t       n;

        if (poll(&p, 1, 50) <= 0) {
            continue;
        }

        if ((n = recv(fd, out + kept, cap - 1 - kept, 0)) <= 0) {
            break;
        }

        kept      += (size_t)n;
        out[kept]  = '\0';
    }

    return kept;
}

static int
curl(const char *args, char *out, size_t cap)
{
    char   cmd[1024];
    FILE  *p;
    size_t n;

    snprintf(cmd, sizeof(cmd), "curl -sS -k --max-time 20 %s 2>&1", args);
    p      = popen(cmd, "r");
    n      = fread(out, 1, cap - 1, p);
    out[n] = '\0';

    return WEXITSTATUS(pclose(p));
}

/* ======================================================================== */
/* Server cases                                                             */
/* ======================================================================== */

static void
server_cases(hc_server *s)
{
    char            out[8192];
    const char     *req1 = "GET /a HTTP/1.1\r\nHost: x\r\n\r\n";
    const char     *req2 = "GET /b HTTP/1.1\r\nHost: x\r\n\r\n";
    size_t          sent = 0, got = 0;
    hc_server_stats st;
    int             fd;

    puts("server, HTTP/1.1:");

    st = hc_server_get_stats(s);
    CHECK_EQ(st.cx_total, 0);

    fd = connect_to(PORT_H1);
    sleep_ms(100);
    st = hc_server_get_stats(s);
    CHECK_EQ(st.cx_total, 1);
    CHECK_EQ(st.cx_active, 1);
    CHECK_EQ(st.cx_http1_total, 1);
    CHECK_EQ(st.cx_http1_active, 1);
    CHECK_EQ(st.cx_ssl_total, 0);

    sent += send_str(fd, req1);
    got  += read_upto(fd, out, sizeof(out), "hello");
    sent += send_str(fd, req2);
    got  += read_upto(fd, out, sizeof(out), "hello");
    sleep_ms(100);

    st = hc_server_get_stats(s);
    CHECK_EQ(st.rq_total, 2);
    CHECK_EQ(st.rq_http1_total, 2);
    CHECK_EQ(st.rq_completed, 2);
    CHECK_EQ(st.rq_active, 0);
    CHECK_EQ(st.cx_rx_bytes_total, sent);
    CHECK_EQ(st.cx_tx_bytes_total, got);
    printf("  two keep-alive requests: rq_total %llu, completed %llu, rx %llu (sent %zu), "
           "tx %llu (got %zu)\n", (unsigned long long)st.rq_total,
           (unsigned long long)st.rq_completed, (unsigned long long)st.cx_rx_bytes_total, sent,
           (unsigned long long)st.cx_tx_bytes_total, got);

    close(fd);
    sleep_ms(100);
    st = hc_server_get_stats(s);
    CHECK_EQ(st.cx_active, 0);
    CHECK_EQ(st.cx_http1_active, 0);
    CHECK_EQ(st.cx_destroy, 1);
    printf("  closed: cx_active %llu, cx_destroy %llu\n", (unsigned long long)st.cx_active,
           (unsigned long long)st.cx_destroy);

    /* a response the client walks away from: not completed */
    fd = connect_to(PORT_H1);
    send_str(fd, "GET /big/67108864 HTTP/1.1\r\nHost: x\r\n\r\n");
    read_upto(fd, out, sizeof(out), "\r\n\r\n");
    sleep_ms(50);
    st = hc_server_get_stats(s);
    CHECK_EQ(st.rq_active, 1);
    close(fd);
    sleep_ms(200);
    st = hc_server_get_stats(s);
    CHECK_EQ(st.rq_total, 3);
    CHECK_EQ(st.rq_completed, 2);
    CHECK_EQ(st.rq_active, 0);
    printf("  response cut off by the client: rq_total %llu, completed %llu\n",
           (unsigned long long)st.rq_total, (unsigned long long)st.rq_completed);

    /* body over the limit */
    fd = connect_to(PORT_H1);
    send_str(fd, "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: 5000\r\n"
                 "Expect: 100-continue\r\n\r\n");
    read_upto(fd, out, sizeof(out), "too large");
    close(fd);
    sleep_ms(100);
    st = hc_server_get_stats(s);
    CHECK(strncmp(out, "HTTP/1.1 413", 12) == 0);
    CHECK_EQ(st.rq_too_large, 1);
    CHECK_EQ(st.rq_active, 0);
    CHECK_EQ(st.cx_active, 0);
    CHECK_EQ(st.cx_total, 3);
    CHECK_EQ(st.cx_destroy, 3);
    CHECK_EQ(st.cx_http2_total, 0);
    printf("  Content-Length over the limit: rq_too_large %llu\n",
           (unsigned long long)st.rq_too_large);

    /* status classes: 200 x2 and the cut-off 200 (its head went out), the
     * 413; then a 302, a 503 and one more 200 */
    CHECK_EQ(st.rq_2xx, 3);
    CHECK_EQ(st.rq_4xx, 1);
    fd = connect_to(PORT_H1);
    send_str(fd, "GET /moved HTTP/1.1\r\nHost: x\r\n\r\n");
    read_upto(fd, out, sizeof(out), "elsewhere");
    send_str(fd, "GET /fail HTTP/1.1\r\nHost: x\r\n\r\n");
    read_upto(fd, out, sizeof(out), "no upstream");
    send_str(fd, "GET /a HTTP/1.1\r\nHost: x\r\n\r\n");
    read_upto(fd, out, sizeof(out), "hello");
    close(fd);
    sleep_ms(100);
    st = hc_server_get_stats(s);
    CHECK_EQ(st.rq_1xx, 0);
    CHECK_EQ(st.rq_2xx, 4);
    CHECK_EQ(st.rq_3xx, 1);
    CHECK_EQ(st.rq_4xx, 1);
    CHECK_EQ(st.rq_5xx, 1);
    printf("  by status class: 2xx %llu, 3xx %llu, 4xx %llu, 5xx %llu\n",
           (unsigned long long)st.rq_2xx, (unsigned long long)st.rq_3xx,
           (unsigned long long)st.rq_4xx, (unsigned long long)st.rq_5xx);
}

static void
tls_cases(hc_server *s)
{
    char            out[4096], args[512];
    hc_server_stats st;

    puts("server, TLS:");

    snprintf(args, sizeof(args), "--http2 -w ' %%{http_version}\\n' https://127.0.0.1:%d/a "
             "https://127.0.0.1:%d/b", PORT_TLS, PORT_TLS);
    CHECK(curl(args, out, sizeof(out)) == 0);

    if (strstr(out, "hello 2") == NULL) {
        printf("  HTTP/2 not available (curl said: %.60s); HTTP/1.1 over TLS only\n", out);
    } else {
        sleep_ms(100);
        st = hc_server_get_stats(s);
        CHECK_EQ(st.cx_total, 1);
        CHECK_EQ(st.cx_ssl_total, 1);
        CHECK_EQ(st.cx_http2_total, 1);
        CHECK_EQ(st.cx_http1_total, 0);
        CHECK_EQ(st.rq_http2_total, 2);
        CHECK_EQ(st.rq_completed, 2);
        CHECK_EQ(st.cx_active, 0);
        CHECK_EQ(st.cx_http2_active, 0);
        CHECK_EQ(st.cx_ssl_active, 0);
        printf("  h2, two streams: cx_http2_total %llu, cx_http1_total %llu, rq_http2_total %llu\n",
               (unsigned long long)st.cx_http2_total, (unsigned long long)st.cx_http1_total,
               (unsigned long long)st.rq_http2_total);
    }

    st = hc_server_get_stats(s);
    snprintf(args, sizeof(args), "--http1.1 https://127.0.0.1:%d/a", PORT_TLS);
    CHECK(curl(args, out, sizeof(out)) == 0);
    sleep_ms(100);
    {
        hc_server_stats st2 = hc_server_get_stats(s);

        CHECK_EQ(st2.cx_ssl_total, st.cx_ssl_total + 1);
        CHECK_EQ(st2.cx_http1_total, st.cx_http1_total + 1);
        CHECK_EQ(st2.cx_http2_total, st.cx_http2_total);
        CHECK_EQ(st2.rq_http1_total, st.rq_http1_total + 1);
        CHECK_EQ(st2.cx_active, 0);
        CHECK_EQ(st2.cx_http1_active, 0);
        CHECK_EQ(st2.cx_ssl_active, 0);
        printf("  HTTP/1.1 over TLS: cx_http1_total %llu, cx_ssl_total %llu\n",
               (unsigned long long)st2.cx_http1_total, (unsigned long long)st2.cx_ssl_total);
    }
}

/* ======================================================================== */
/* Endpoint attachments                                                     */
/* ======================================================================== */

static int frees;
static int tags;

static void tag_free(void *p) { frees++; free(p); }

typedef struct att {
    hc_requester *rq;
    hc_endpoint  *ep;
    const char   *error;
} att;

static void
on_connected(void *arg, hc_endpoint *ep, const char *error)
{
    att *a = arg;

    a->ep    = ep;
    a->error = error;
    hc_reactor_stop(R);
}

typedef struct ch {
    hc_client_handler base;
    int               ok;
} ch_t;

static void
c_produce_request(hc_client_handler *self, hc_request_channel *rc)
{
    hc_headers     *h    = hc_headers_new();
    hc_request_head head = { "GET", "/a", "127.0.0.1", 11, h };

    (void)self;
    hc_request_send(rc, &head, NULL);
    hc_headers_free(h);
}

static void c_resp(hc_client_handler *s, const hc_response_head *h, const hc_entity *e) { (void)s; (void)h; (void)e; }
static void c_consume(hc_client_handler *s, const void *d, size_t n) { (void)s; (void)d; (void)n; }
static void c_end(hc_client_handler *s, const hc_headers *t) { (void)t; ((ch_t *)(void *)s)->ok = 1; }
static void c_failed(hc_client_handler *s, const char *r) { (void)s; (void)r; }
static void c_release(hc_client_handler *s) { (void)s; hc_reactor_stop(R); }

static const hc_client_handler_vtbl cvt = {
    .produce_request  = c_produce_request,
    .consume_response = c_resp,
    .consume          = c_consume,
    .stream_end       = c_end,
    .failed           = c_failed,
    .release          = c_release,
};

/* Lease, run one exchange, release for reuse; returns the attachment the
 * lease found, and tags the connection when it had none. */
static void *
lease_and_use(hc_requester *rq, hc_endpoint **held)
{
    hc_host target;
    att     a = { rq, NULL, NULL };
    ch_t    c = { { &cvt }, 0 };
    void   *found;

    hc_host_init(&target, "http", "127.0.0.1", PORT_ORG);
    hc_requester_connect(rq, &target, 2000, on_connected, &a);
    hc_reactor_run(R);
    CHECK(a.ep != NULL);
    if (a.ep == NULL) {
        return NULL;
    }

    found = hc_endpoint_get_attachment(a.ep);
    if (found == NULL) {
        int *tag = malloc(sizeof(*tag));

        *tag = ++tags;
        hc_endpoint_set_attachment(a.ep, tag, tag_free);
        CHECK(hc_endpoint_get_attachment(a.ep) == tag);
    }

    hc_endpoint_execute(a.ep, &c.base);
    hc_reactor_run(R);
    CHECK(c.ok);

    if (held) {
        *held = a.ep;           /* caller releases */
    } else {
        hc_endpoint_release_and_reuse(a.ep);
    }

    return found;
}

static void
attachment_cases(void)
{
    hc_requester *rq = hc_requester_new(R, NULL);
    hc_endpoint  *held;
    void         *a1, *a2, *a3;

    puts("endpoint attachments:");

    a1 = lease_and_use(rq, NULL);       /* new: tagged 1 */
    a2 = lease_and_use(rq, NULL);       /* reused: finds tag 1 */
    CHECK(a1 == NULL);
    CHECK(a2 != NULL && *(int *)a2 == 1);
    printf("  first lease: %s; second lease of the same connection: tag %d\n",
           a1 ? "attachment?" : "no attachment", a2 ? *(int *)a2 : 0);

    /* hold that connection: the next lease gets a new one, untagged */
    lease_and_use(rq, &held);
    a3 = lease_and_use(rq, NULL);
    CHECK(a3 == NULL && tags == 2);
    hc_endpoint_release_and_reuse(held);
    printf("  connection in use elsewhere -> new connection, no attachment: %s\n",
           a3 == NULL ? "ok" : "no");

    CHECK_EQ(frees, 0);
    hc_requester_free(rq);              /* closes both connections */
    CHECK_EQ(frees, 2);
    printf("  freed with their connections: %d of 2\n", frees);
}

/* ======================================================================== */

static void *
loop_main(void *arg)
{
    event_base_dispatch(arg);
    return NULL;
}

static void
stop_cb(evutil_socket_t fd, short what, void *arg)
{
    (void)fd;
    (void)what;
    event_base_loopbreak(arg);
}

int
main(int argc, char **argv)
{
    char       cert[512], key[512];
    pthread_t  th_loop;
    hc_server *s_h1, *s_tls, *origin;

    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc < 2) {
        fprintf(stderr, "usage: %s <tests/tls dir>\n", argv[0]);
        return 2;
    }

    snprintf(cert, sizeof(cert), "%s/localhost.pem", argv[1]);
    snprintf(key, sizeof(key), "%s/localhost.key", argv[1]);

    evthread_use_pthreads();
    base   = event_base_new();
    R      = hc_reactor_new_on_base(base);
    s_h1   = hc_server_new(R, factory, NULL);
    s_tls  = hc_server_new(R, factory, NULL);
    origin = hc_server_new(R, factory, NULL);

    hc_server_set_max_body(s_h1, 1000);

    if (hc_server_listen(s_h1, "127.0.0.1", PORT_H1) != 0 ||
        hc_server_enable_tls(s_tls, cert, key) != 0 ||
        hc_server_listen(s_tls, "127.0.0.1", PORT_TLS) != 0 ||
        hc_server_listen(origin, "127.0.0.1", PORT_ORG) != 0) {
        fprintf(stderr, "setup failed\n");
        return 1;
    }

    pthread_create(&th_loop, NULL, loop_main, base);
    server_cases(s_h1);
    tls_cases(s_tls);
    event_base_once(base, -1, EV_TIMEOUT, stop_cb, base, NULL);
    pthread_join(th_loop, NULL);

    attachment_cases();

    hc_server_free(s_h1);
    hc_server_free(s_tls);
    hc_server_free(origin);
    hc_reactor_free(R);
    event_base_free(base);

    printf("stats_test: %d checks, %d failed\n", passed + failed, failed);
    return failed ? 1 : 0;
}
