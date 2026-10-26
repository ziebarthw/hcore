/*
 * h2_test.c - an hcore server over HTTP/2 (the ziebarthw libevhtp fork
 * built with nghttp2), checked with curl, and the same requests over
 * HTTP/1.1 for comparison:
 *
 *   - a small reply, an error reply that asks to close the connection (no
 *     connection-specific header may reach an HTTP/2 client), HEAD
 *   - an upload (3 MB, past the stream and connection windows), counted
 *   - a streamed download of unknown length (8 MB) produced with
 *     backpressure: every byte checked
 *   - six streams at once on one connection
 *   - a drain with a stream under way: the stream completes, then the
 *     connection closes
 *   - hc_server_set_http2(s, 0): the client gets HTTP/1.1
 *
 * Built only against a libevhtp with HTTP/2 (make check skips it otherwise).
 * Usage: h2_test <tests/tls dir>; needs curl with HTTP/2 on PATH (skipped
 * without it).
 */
#define _GNU_SOURCE     /* strcasestr, popen, usleep */
#include "hc.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <event2/event.h>
#include <event2/thread.h>

#define PORT    9340
#define PORT_H1 9341

static int passed, failed;

#define CHECK(c) do {                                                      \
        if (c) { passed++; }                                               \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } \
    } while (0)

static struct event_base *base;
static hc_reactor        *R;
static hc_server         *S, *S_H1;
static volatile int       last_version;     /* request version seen */

static unsigned char
pattern(size_t i)
{
    return (unsigned char)('a' + (i * 7 + i / 251) % 26);
}

/* ======================================================================== */
/* Handlers                                                                 */
/* ======================================================================== */

typedef struct th {
    hc_server_handler    base;
    hc_server_exchange  *ex;
    hc_response_channel *rc;
    char                 path[64];
    size_t               got;           /* upload bytes */
    unsigned long        sum;
    size_t               sent, total;   /* download */
    hc_timer            *timer;
} th;

#define TH(self) ((th *)(void *)(self))

static void
reply_text(th *t, int status, const char *body, int close_conn)
{
    hc_response_send_simple(t->rc, status, "text/plain", body, close_conn);
}

static void
slow_cb(void *arg)
{
    th *t = arg;

    reply_text(t, 200, "slow done", 0);
}

static void
h_request(hc_server_handler *self, hc_server_exchange *ex, const hc_request_head *head,
          const hc_entity *entity, hc_response_channel *rc)
{
    th *t = TH(self);

    (void)entity;
    t->ex = ex;
    t->rc = rc;
    last_version = head->version;
    snprintf(t->path, sizeof(t->path), "%s", head->target);

    if (strcmp(t->path, "/hello") == 0) {
        reply_text(t, 200, "hello", 0);
    } else if (strcmp(t->path, "/error") == 0) {
        reply_text(t, 503, "no upstream", 1);   /* "Connection: close" */
    } else if (strcmp(t->path, "/head") == 0) {
        hc_headers      *h  = hc_headers_new();
        hc_response_head rh = { 200, 11, h };
        hc_entity        e  = { 12345, "text/plain" };
        hc_data_channel *ch = hc_response_send(rc, &rh, &e);

        hc_headers_free(h);
        if (ch) {
            hc_data_write(ch, "ignored", 7);    /* HEAD: discarded */
            hc_data_end(ch, NULL);
        }
    } else if (strncmp(t->path, "/big/", 5) == 0) {
        hc_headers      *h  = hc_headers_new();
        hc_response_head rh = { 200, 11, h };
        hc_entity        e  = { -1, "application/octet-stream" };   /* unknown length */

        t->total = (size_t)atol(t->path + 5);
        hc_response_send(rc, &rh, &e);          /* ->produce() follows */
        hc_headers_free(h);
    } else if (strcmp(t->path, "/slow-drain") == 0) {
        /* drain while this stream is under way; answer 300 ms later */
        hc_server_drain(S);
        t->timer = hc_timer_new(R, slow_cb, t);
        hc_timer_start(t->timer, 300);
    } else if (strcmp(t->path, "/upload") == 0) {
        /* answered at stream_end */
    } else {
        reply_text(t, 404, "?", 0);
    }
}

static void
h_consume(hc_server_handler *self, const void *data, size_t len)
{
    th                  *t = TH(self);
    const unsigned char *p = data;

    for (size_t i = 0; i < len; i++) {
        t->sum = t->sum * 31 + p[i];
    }

    t->got += len;
}

static void
h_stream_end(hc_server_handler *self, const hc_headers *trailers)
{
    th  *t = TH(self);
    char msg[64];

    (void)trailers;

    if (strcmp(t->path, "/upload") == 0) {
        snprintf(msg, sizeof(msg), "n=%zu sum=%lu", t->got, t->sum);
        reply_text(t, 200, msg, 0);
    }
}

static void
h_produce(hc_server_handler *self, hc_data_channel *ch)
{
    th           *t = TH(self);
    unsigned char buf[16384];

    while (t->sent < t->total) {
        size_t room = hc_data_capacity(ch);
        size_t n    = t->total - t->sent;

        if (room == 0) {
            return;                 /* called again once there is room */
        }

        n = n < room ? n : room;
        n = n < sizeof(buf) ? n : sizeof(buf);

        for (size_t i = 0; i < n; i++) {
            buf[i] = pattern(t->sent + i);
        }

        if (hc_data_write(ch, buf, n) != (long)n) {
            return;
        }

        t->sent += n;
    }

    hc_data_end(ch, NULL);
}

static void h_failed(hc_server_handler *s, const char *r) { (void)s; (void)r; }

static void
h_release(hc_server_handler *self)
{
    th *t = TH(self);

    if (t->timer) {
        hc_timer_free(t->timer);
    }

    free(t);
}

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
/* curl                                                                     */
/* ======================================================================== */

static char tmpdir[64];

/* Run curl; *out gets "-w" + headers (-D) + body as configured. */
static int
curl(const char *args, char *out, size_t cap)
{
    char   cmd[1024];
    FILE  *p;
    size_t n;

    snprintf(cmd, sizeof(cmd), "curl -sS -k --max-time 30 %s 2>&1", args);
    p      = popen(cmd, "r");
    n      = fread(out, 1, cap - 1, p);
    out[n] = '\0';

    return WEXITSTATUS(pclose(p));
}

static int
file_is_pattern(const char *path, size_t len)
{
    FILE  *f = fopen(path, "rb");
    size_t i = 0;
    int    c, ok = 1;

    if (!f) {
        return 0;
    }

    while ((c = fgetc(f)) != EOF) {
        if (i >= len || (unsigned char)c != pattern(i)) {
            ok = 0;
            break;
        }
        i++;
    }

    fclose(f);
    return ok && i == len;
}

static void
make_upload(const char *path, size_t len, unsigned long *sum)
{
    FILE *f = fopen(path, "wb");

    *sum = 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = pattern(i * 3 + 1);
        fputc(c, f);
        *sum = *sum * 31 + c;
    }

    fclose(f);
}

static void
run_cases(const char *proto, const char *want_version, int port, int want_hc_version)
{
    char          out[4096], args[512], file[128];
    unsigned long sum;

    printf("%s:\n", proto);

    /* small reply */
    snprintf(args, sizeof(args), "%s -w ' %%{http_version} %%{http_code}' https://127.0.0.1:%d/hello",
             proto, port);
    CHECK(curl(args, out, sizeof(out)) == 0);
    CHECK(strncmp(out, "hello ", 6) == 0 && strstr(out, want_version) && strstr(out, " 200"));
    CHECK(last_version == want_hc_version);
    printf("  small reply: %s\n", out);

    /* error reply asking to close: no Connection header on HTTP/2 */
    snprintf(args, sizeof(args), "%s -D - https://127.0.0.1:%d/error", proto, port);
    CHECK(curl(args, out, sizeof(out)) == 0);
    CHECK(strstr(out, " 503") != NULL && strstr(out, "no upstream") != NULL);
    if (want_hc_version == 20) {
        CHECK(strcasestr(out, "\nconnection:") == NULL);
    }
    printf("  error reply 503 (close requested): ok\n");

    /* HEAD: length kept, no body */
    snprintf(args, sizeof(args), "%s -I https://127.0.0.1:%d/head", proto, port);
    CHECK(curl(args, out, sizeof(out)) == 0);
    CHECK(strstr(out, " 200") && strcasestr(out, "content-length: 12345"));
    printf("  HEAD: ok\n");

    /* upload: 3 MB */
    snprintf(file, sizeof(file), "%s/up.bin", tmpdir);
    make_upload(file, 3u * 1024 * 1024, &sum);
    snprintf(args, sizeof(args), "%s --data-binary @%s -H 'Content-Type: application/octet-stream' "
             "https://127.0.0.1:%d/upload", proto, file, port);
    CHECK(curl(args, out, sizeof(out)) == 0);
    {
        char want[64];

        snprintf(want, sizeof(want), "n=%u sum=%lu", 3u * 1024 * 1024, sum);
        CHECK(strcmp(out, want) == 0);
        printf("  upload 3 MB: %s\n", strcmp(out, want) == 0 ? "ok" : out);
    }

    /* streamed download: 8 MB, unknown length */
    snprintf(file, sizeof(file), "%s/down.bin", tmpdir);
    snprintf(args, sizeof(args), "%s -o %s -w '%%{http_version}' https://127.0.0.1:%d/big/%u",
             proto, file, port, 8u * 1024 * 1024);
    {
        int rc = curl(args, out, sizeof(out));

        CHECK(rc == 0);
        if (rc != 0) {
            fprintf(stderr, "    curl %d: %s\n", rc, out);
        }
    }
    CHECK(strstr(out, want_version) != NULL);
    CHECK(file_is_pattern(file, 8u * 1024 * 1024));
    printf("  download 8 MB (streamed, backpressure): ok\n");
}

static void *
loop_main(void *arg)
{
    event_base_dispatch(arg);
    return NULL;
}

int
main(int argc, char **argv)
{
    char      cert[512], key[512], out[4096], args[2048];
    pthread_t th_loop;

    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc < 2) {
        fprintf(stderr, "usage: %s <tests/tls dir>\n", argv[0]);
        return 2;
    }

    if (system("curl -V 2>/dev/null | grep -q HTTP2") != 0) {
        puts("h2_test: skipped (curl without HTTP/2)");
        return 0;
    }

    snprintf(cert, sizeof(cert), "%s/localhost.pem", argv[1]);
    snprintf(key, sizeof(key), "%s/localhost.key", argv[1]);
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/hc_h2_%d", getpid());
    mkdir(tmpdir, 0700);

    evthread_use_pthreads();
    base = event_base_new();
    R    = hc_reactor_new_on_base(base);
    S    = hc_server_new(R, factory, NULL);
    S_H1 = hc_server_new(R, factory, NULL);

    CHECK(hc_server_set_http2(S_H1, 0) == 0);
    if (hc_server_enable_tls(S, cert, key) != 0 || hc_server_listen(S, "127.0.0.1", PORT) != 0 ||
        hc_server_enable_tls(S_H1, cert, key) != 0 || hc_server_listen(S_H1, "127.0.0.1", PORT_H1) != 0) {
        fprintf(stderr, "setup failed\n");
        return 1;
    }
    CHECK(hc_server_set_http2(S, 0) == -1);     /* too late: TLS is on */

    pthread_create(&th_loop, NULL, loop_main, base);

    run_cases("--http2", "2", PORT, 20);
    run_cases("--http1.1", "1.1", PORT, 11);

    puts("hc_server_set_http2(s, 0):");
    snprintf(args, sizeof(args), "--http2 -w ' %%{http_version}' https://127.0.0.1:%d/hello", PORT_H1);
    CHECK(curl(args, out, sizeof(out)) == 0);
    CHECK(strcmp(out, "hello 1.1") == 0);
    printf("  client asked for h2, got: %s\n", out);

    puts("HTTP/2: six streams at once on one connection");
    {
        int n = snprintf(args, sizeof(args), "--http2 --parallel --parallel-max 6 -w '%%{num_connects}\\n'");

        for (int i = 0; i < 6; i++) {
            n += snprintf(args + n, sizeof(args) - n, " -o %s/p%d.bin https://127.0.0.1:%d/big/%d",
                          tmpdir, i, PORT, (i + 1) * 300000);
        }

        CHECK(curl(args, out, sizeof(out)) == 0);
        {
            int conns = 0;

            for (char *l = out; l && *l; l = strchr(l, '\n') ? strchr(l, '\n') + 1 : NULL) {
                conns += atoi(l);
            }
            CHECK(conns == 1);
            printf("  connections opened: %d\n", conns);
        }

        for (int i = 0; i < 6; i++) {
            char f[128];

            snprintf(f, sizeof(f), "%s/p%d.bin", tmpdir, i);
            CHECK(file_is_pattern(f, (size_t)(i + 1) * 300000));
            unlink(f);
        }
        printf("  six downloads correct\n");
    }

    puts("HTTP/2: drain with a stream under way");
    {
        snprintf(args, sizeof(args), "--http2 -w ' %%{http_version}' https://127.0.0.1:%d/slow-drain", PORT);
        CHECK(curl(args, out, sizeof(out)) == 0);
        CHECK(strcmp(out, "slow done 2") == 0);
        printf("  in-flight stream: %s\n", out);
        usleep(300 * 1000);
        CHECK(hc_server_connection_count(S) == 0);
        printf("  connections after drain: %zu\n", hc_server_connection_count(S));
        snprintf(args, sizeof(args), "--http2 https://127.0.0.1:%d/hello", PORT);
        CHECK(curl(args, out, sizeof(out)) != 0);   /* not accepting */
    }

    event_base_loopbreak(base);
    pthread_join(th_loop, NULL);
    hc_server_free(S);
    hc_server_free(S_H1);
    hc_reactor_free(R);
    event_base_free(base);

    {
        char f[160];

        snprintf(f, sizeof(f), "%s/up.bin", tmpdir);
        unlink(f);
        snprintf(f, sizeof(f), "%s/down.bin", tmpdir);
        unlink(f);
        rmdir(tmpdir);
    }

    printf("h2_test: %d checks, %d failed\n", passed + failed, failed);
    return failed ? 1 : 0;
}
