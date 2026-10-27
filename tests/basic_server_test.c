/*
 * basic_server_test.c - hc_basic_server_factory(): whole requests, checked
 * with curl over HTTP/1.1, and over HTTP/2 when curl and libevhtp have it:
 *
 *   - GET with a query: method, target and Host reach ->request()
 *   - POST with Content-Length, and a chunked POST: the whole body, intact
 *   - a reply made later, from a timer
 *   - a streamed reply (server-sent events), produced from a timer
 *   - a body over max_body, by Content-Length and chunked: 413, and the
 *     application never sees the request
 *   - a client that leaves while its reply is pending: on HTTP/2 at once
 *     ->failed() and ->release(), after which nothing touches the request;
 *     on HTTP/1.1 (the connection is not read while a reply is pending) the
 *     exchange ends with the reply, unnoticed - either way it is released
 *   - a streamed reply to a client that has left: ->failed() once a write
 *     fails, ->release()
 *   - every request delivered is released exactly once
 *   - IPv6 (where the system has it): servers on "::1" and "[::1]", and an
 *     IPv4 and an IPv6 server on the same port
 *
 * Usage: basic_server_test <tests/tls dir>
 */
#define _GNU_SOURCE
#include "hc.h"
#include "hc_support.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <event2/event.h>
#include <event2/thread.h>

#define PORT     9380
#define PORT_TLS 9381
#define PORT_V6  9382    /* "::1" */
#define PORT_V6B 9383    /* "[::1]" */
#define PORT_DUAL 9384   /* 127.0.0.1 and ::1 */
#define MAX_BODY 20000

static int passed, failed;

#define CHECK(c) do {                                                      \
        if (c) { passed++; }                                               \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } \
    } while (0)

static hc_reactor  *R;
static hc_server   *V6[4];
static volatile int delivered, released, failures;
static char         last_failure[128];

static void
sleep_ms(int ms)
{
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };

    nanosleep(&ts, NULL);
}

/* ------------------------------------------------------------------------ */
/* The application                                                          */
/* ------------------------------------------------------------------------ */

typedef struct app_req {
    hc_basic_request *r;
    hc_timer         *timer;
    int               events;   /* /stream: sent so far */
    hc_data_channel  *ch;
} app_req;

static unsigned long
checksum(const char *p, size_t len)
{
    unsigned long sum = 0;

    for (size_t i = 0; i < len; i++) {
        sum = sum * 31 + (unsigned char)p[i];
    }

    return sum;
}

static void
reply(hc_basic_request *r, int status, const char *text)
{
    hc_response_send_simple(hc_basic_request_channel(r), status, "text/plain", text, 0);
}

static void
later_cb(void *arg)
{
    app_req *a = arg;

    reply(a->r, 200, "later");
}

static void
stream_cb(void *arg)
{
    app_req *a = arg;
    char     ev[64];

    if (a->ch == NULL) {
        return;
    }

    snprintf(ev, sizeof(ev), "data: event %d\n\n", ++a->events);
    hc_data_write(a->ch, ev, strlen(ev));

    if (a->events == 3 && strcmp(hc_basic_request_target(a->r), "/stream") == 0) {
        hc_data_end(a->ch, NULL);
    } else {
        hc_timer_start(a->timer, 50);
    }
}

static void
app_request(void *arg, hc_basic_request *r)
{
    app_req    *a      = calloc(1, sizeof(*a));
    const char *target = hc_basic_request_target(r);
    char        msg[512];
    size_t      len;
    const char *body   = hc_basic_request_body(r, &len);

    (void)arg;
    delivered++;
    a->r = r;
    hc_basic_request_set_data(r, a);

    if (strncmp(target, "/echo", 5) == 0) {
        snprintf(msg, sizeof(msg), "%s %s host=%s v=%d len=%zu sum=%lu",
                 hc_basic_request_method(r), target,
                 hc_basic_request_authority(r) ? hc_basic_request_authority(r) : "-",
                 hc_basic_request_version(r), len, checksum(body, len));
        reply(r, 200, msg);
    } else if (strcmp(target, "/later") == 0 || strcmp(target, "/later-long") == 0) {
        a->timer = hc_timer_new(R, later_cb, a);
        hc_timer_start(a->timer, target[6] ? 1000 : 150);
    } else if (strcmp(target, "/stream") == 0 || strcmp(target, "/stream-long") == 0) {
        hc_headers      *h  = hc_headers_new();
        hc_response_head rh = { 200, 11, h };
        hc_entity        e  = { -1, "text/event-stream" };

        a->ch    = hc_response_send(hc_basic_request_channel(r), &rh, &e);
        a->timer = hc_timer_new(R, stream_cb, a);
        hc_headers_free(h);
        hc_timer_start(a->timer, 50);
    } else {
        reply(r, 404, "?");
    }
}

static void
app_produce(void *arg, hc_basic_request *r, hc_data_channel *ch)
{
    (void)arg;
    ((app_req *)hc_basic_request_data(r))->ch = ch;     /* events come from the timer */
}

static void
app_failed(void *arg, hc_basic_request *r, const char *reason)
{
    (void)arg;
    (void)r;
    failures++;
    snprintf(last_failure, sizeof(last_failure), "%s", reason);
}

static void
app_release(void *arg, hc_basic_request *r)
{
    app_req *a = hc_basic_request_data(r);

    (void)arg;
    released++;

    if (a->timer) {
        hc_timer_free(a->timer);        /* nothing may answer r now */
    }

    free(a);
}

static hc_basic_server_config CONFIG = {
    .request  = app_request,
    .produce  = app_produce,
    .failed   = app_failed,
    .release  = app_release,
    .max_body = MAX_BODY,
};

/* ------------------------------------------------------------------------ */

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

static unsigned long
make_file(const char *path, size_t len)
{
    FILE         *f   = fopen(path, "wb");
    unsigned long sum = 0;

    for (size_t i = 0; i < len; i++) {
        char c = (char)('a' + (i * 7) % 26);

        fputc(c, f);
        sum = sum * 31 + (unsigned char)c;
    }

    fclose(f);
    return sum;
}

static void
run_cases(const char *proto, const char *scheme, int port, int version)
{
    char          out[4096], args[1024], want[256], small[160], big[160];
    unsigned long sum;
    int           before;
    struct timespec t0, t1;

    printf("%s:\n", proto);

    snprintf(args, sizeof(args), "%s -H 'Host: example.test' '%s://127.0.0.1:%d/echo?a=1&b=2'",
             proto, scheme, port);
    CHECK(curl(args, out, sizeof(out)) == 0);
    snprintf(want, sizeof(want), "GET /echo?a=1&b=2 host=example.test v=%d len=0 sum=0", version);
    CHECK(strcmp(out, want) == 0);
    printf("  GET with query: %s\n", out);

    snprintf(small, sizeof(small), "/tmp/hc_bs_small_%d", (int)getpid());
    sum = make_file(small, 10000);
    snprintf(args, sizeof(args), "%s --data-binary @%s %s://127.0.0.1:%d/echo", proto, small,
             scheme, port);
    CHECK(curl(args, out, sizeof(out)) == 0);
    snprintf(want, sizeof(want), "POST /echo host=127.0.0.1:%d v=%d len=10000 sum=%lu", port,
             version, sum);
    CHECK(strcmp(out, want) == 0);
    printf("  POST, Content-Length 10000: %s\n", strcmp(out, want) == 0 ? "whole body" : out);

    /* from stdin: chunked (HTTP/1.1) / no Content-Length (HTTP/2) */
    snprintf(args, sizeof(args), "%s -X POST -T - %s://127.0.0.1:%d/echo < %s", proto, scheme,
             port, small);
    CHECK(curl(args, out, sizeof(out)) == 0);
    CHECK(strcmp(out, want) == 0);
    printf("  POST, length unknown: %s\n", strstr(out, "len=10000") ? "whole body" : out);

    clock_gettime(CLOCK_MONOTONIC, &t0);
    snprintf(args, sizeof(args), "%s %s://127.0.0.1:%d/later", proto, scheme, port);
    CHECK(curl(args, out, sizeof(out)) == 0);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    CHECK(strcmp(out, "later") == 0);
    CHECK((t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000 >= 140);
    printf("  reply from a timer 150 ms later: %s\n", out);

    snprintf(args, sizeof(args), "%s -N %s://127.0.0.1:%d/stream", proto, scheme, port);
    CHECK(curl(args, out, sizeof(out)) == 0);
    CHECK(strcmp(out, "data: event 1\n\ndata: event 2\n\ndata: event 3\n\n") == 0);
    printf("  streamed events: %s\n",
           strcmp(out, "data: event 1\n\ndata: event 2\n\ndata: event 3\n\n") == 0 ? "3 of 3" : out);

    before = delivered;
    snprintf(big, sizeof(big), "/tmp/hc_bs_big_%d", (int)getpid());
    make_file(big, MAX_BODY + 10000);
    snprintf(args, sizeof(args), "%s -w ' %%{http_code}' --data-binary @%s %s://127.0.0.1:%d/echo",
             proto, big, scheme, port);
    curl(args, out, sizeof(out));
    CHECK(strstr(out, " 413") != NULL);
    snprintf(args, sizeof(args), "%s -w ' %%{http_code}' -X POST -T - %s://127.0.0.1:%d/echo < %s",
             proto, scheme, port, big);
    curl(args, out, sizeof(out));
    CHECK(strstr(out, " 413") != NULL);
    sleep_ms(100);
    CHECK(delivered == before);
    printf("  bodies over the limit: 413 twice, application not called: %s\n",
           delivered == before ? "ok" : "called");

    failures = 0;
    before   = released;
    snprintf(args, sizeof(args), "%s --max-time 0.3 %s://127.0.0.1:%d/later-long", proto, scheme,
             port);
    curl(args, out, sizeof(out));
    sleep_ms(1200);     /* past the moment the reply would have gone */
    CHECK(released == before + 1);
    if (version == 20) {
        CHECK(failures == 1);
    }
    printf("  client left before a pending reply: %s\n",
           failures ? last_failure : "noticed only when the reply went out (HTTP/1.1)");

    /* a long stream to a client that leaves: the writes fail */
    failures = 0;
    before   = released;
    snprintf(args, sizeof(args), "%s -N --max-time 0.3 %s://127.0.0.1:%d/stream-long", proto,
             scheme, port);
    curl(args, out, sizeof(out));
    sleep_ms(1500);
    CHECK(failures == 1 && released == before + 1);
    printf("  stream to a client that left: failed(\"%s\") %s\n", last_failure,
           released == before + 1 ? "and released" : "NOT released");

    CHECK(released == delivered);
    printf("  delivered %d, released %d\n", delivered, released);

    unlink(small);
    unlink(big);
}

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
    char               cert[512], key[512], out[256], args[256];
    struct event_base *base;
    hc_server         *s, *s_tls;
    pthread_t          th;

    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc < 2) {
        fprintf(stderr, "usage: %s <tests/tls dir>\n", argv[0]);
        return 2;
    }

    snprintf(cert, sizeof(cert), "%s/localhost.pem", argv[1]);
    snprintf(key, sizeof(key), "%s/localhost.key", argv[1]);

    evthread_use_pthreads();
    base  = event_base_new();
    R     = hc_reactor_new_on_base(base);
    s     = hc_server_new(R, hc_basic_server_factory, &CONFIG);
    s_tls = hc_server_new(R, hc_basic_server_factory, &CONFIG);

    if (hc_server_listen(s, "127.0.0.1", PORT) != 0 ||
        hc_server_enable_tls(s_tls, cert, key) != 0 ||
        hc_server_listen(s_tls, "127.0.0.1", PORT_TLS) != 0) {
        fprintf(stderr, "setup failed\n");
        return 1;
    }

    /* IPv6, if the system has it */
    {
        int        fd  = socket(AF_INET6, SOCK_STREAM, 0);
        int        has = fd >= 0;
        hc_server *v6 = NULL, *v6b = NULL, *d4 = NULL, *d6 = NULL;

        if (fd >= 0) {
            close(fd);
        }

        if (has) {
            v6  = hc_server_new(R, hc_basic_server_factory, &CONFIG);
            v6b = hc_server_new(R, hc_basic_server_factory, &CONFIG);
            d4  = hc_server_new(R, hc_basic_server_factory, &CONFIG);
            d6  = hc_server_new(R, hc_basic_server_factory, &CONFIG);
            CHECK(hc_server_listen(v6, "::1", PORT_V6) == 0);
            CHECK(hc_server_listen(v6b, "[::1]", PORT_V6B) == 0);
            CHECK(hc_server_listen(d4, "127.0.0.1", PORT_DUAL) == 0);
            CHECK(hc_server_listen(d6, "::1", PORT_DUAL) == 0);
        }

        pthread_create(&th, NULL, loop_main, base);

        if (has) {
            puts("IPv6:");
            snprintf(args, sizeof(args), "-g 'http://[::1]:%d/echo'", PORT_V6);
            CHECK(curl(args, out, sizeof(out)) == 0 && strncmp(out, "GET /echo", 9) == 0);
            printf("  \"::1\": %s\n", out);
            snprintf(args, sizeof(args), "-g 'http://[::1]:%d/echo'", PORT_V6B);
            CHECK(curl(args, out, sizeof(out)) == 0 && strncmp(out, "GET /echo", 9) == 0);
            printf("  \"[::1]\": %s\n", out);
            snprintf(args, sizeof(args), "-g 'http://[::1]:%d/echo'", PORT_DUAL);
            CHECK(curl(args, out, sizeof(out)) == 0 && strncmp(out, "GET /echo", 9) == 0);
            snprintf(args, sizeof(args), "'http://127.0.0.1:%d/echo'", PORT_DUAL);
            CHECK(curl(args, out, sizeof(out)) == 0 && strncmp(out, "GET /echo", 9) == 0);
            printf("  IPv4 and IPv6 servers on port %d: both answer\n", PORT_DUAL);
        } else {
            puts("IPv6: skipped (no IPv6 on this system)");
        }

        /* freed at the end, from the loop's thread being stopped */
        V6[0] = v6;
        V6[1] = v6b;
        V6[2] = d4;
        V6[3] = d6;
    }

    run_cases("--http1.1", "http", PORT, 11);

    snprintf(args, sizeof(args), "--http2 -o /dev/null -w '%%{http_version}' https://127.0.0.1:%d/echo",
             PORT_TLS);
    if (curl(args, out, sizeof(out)) == 0 && strcmp(out, "2") == 0) {
        run_cases("--http2", "https", PORT_TLS, 20);
    } else {
        puts("--http2: skipped (no HTTP/2 in curl or libevhtp)");
    }

    event_base_once(base, -1, EV_TIMEOUT, stop_cb, base, NULL);
    pthread_join(th, NULL);

    hc_server_free(s);
    hc_server_free(s_tls);
    for (int i = 0; i < 4; i++) {
        if (V6[i]) {
            hc_server_free(V6[i]);
        }
    }
    hc_reactor_free(R);
    event_base_free(base);

    CHECK(released == delivered);
    printf("basic_server_test: %d checks, %d failed\n", passed + failed, failed);
    return failed ? 1 : 0;
}
