/*
 * file_server.c - a port of HttpCore 5's AsyncFileServerExample, as a
 * scaffold to finish.
 *
 *   Java: httpcore5/src/test/java/org/apache/hc/core5/http/examples/
 *         AsyncFileServerExample.java (rel/v5.1)
 *
 * The plumbing is done: arguments, the server and its timeouts, listening,
 * Ctrl-C, the logging. It builds and runs as it is; every request gets
 * "501 Not Implemented" until the TODOs are filled in. Each TODO says which
 * part of the Java example it is and which hcore calls do the job.
 *
 * Suggested order (each step can be tried with curl before the next):
 *   1. content_type_for()          warm-up, plain C
 *   2. send_error_page()           the first response
 *   3. resolve_path()              target -> file name, safely
 *   4. file_request()              404 / 403 / start a 200
 *   5. struct file_xfer,
 *      file_produce(),
 *      file_release()              stream the file without loading it
 *
 * Usage: file_server DOC_ROOT [port]           (port 8080 by default)
 *
 * Try:
 *   curl -i  localhost:8080/some.txt                     200, the file
 *   curl -i  localhost:8080/nope                         404
 *   curl -i  localhost:8080/a-directory                  403
 *   curl -i --path-as-is localhost:8080/../../etc/passwd 400, not the file!
 *   curl -s  localhost:8080/big.bin | cmp - DOC_ROOT/big.bin   a large file
 *   curl --limit-rate 100k --max-time 1 localhost:8080/big.bin -o /dev/null
 *                                         client gives up: ->failed, ->release
 */
#include "hc.h"
#include "hc_support.h"
#include "htstring/htstring.h"
#include "htstring/parsed_uri.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static const char *doc_root;    /* Java: docRoot */

/* ------------------------------------------------------------------------ */
/* Done: helpers                                                            */
/* ------------------------------------------------------------------------ */

/* Java: println(): the date (HttpDateGenerator's format), then the message. */
static void
println(const char *fmt, ...)
{
    char      date[64];
    time_t    now = time(NULL);
    struct tm tm;
    va_list   ap;

    gmtime_r(&now, &tm);
    strftime(date, sizeof(date), "%a, %d %b %Y %H:%M:%S GMT", &tm);
    printf("%s | ", date);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    putchar('\n');
    fflush(stdout);
}

/* Java: coreContext.getEndpointDetails() in the log line: the client's
 * "address:port". */
static const char *
remote_address(hc_basic_request *r, char *buf, size_t size)
{
    struct sockaddr_storage ss;
    socklen_t               len  = sizeof(ss);
    char                    host[INET6_ADDRSTRLEN] = "?";
    int                     port = 0;

    if (hc_server_exchange_remote_address(hc_basic_request_exchange(r),
                                          (struct sockaddr *)&ss, &len) == 0) {
        if (ss.ss_family == AF_INET) {
            struct sockaddr_in *in = (struct sockaddr_in *)&ss;

            inet_ntop(AF_INET, &in->sin_addr, host, sizeof(host));
            port = ntohs(in->sin_port);
        } else if (ss.ss_family == AF_INET6) {
            struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)&ss;

            inet_ntop(AF_INET6, &in6->sin6_addr, host, sizeof(host));
            port = ntohs(in6->sin6_port);
        }
    }

    snprintf(buf, size, "%s:%d", host, port);
    return buf;
}

/* ------------------------------------------------------------------------ */
/* TODO 1: the content type                                                 */
/* ------------------------------------------------------------------------ */

/* Java: the if/else on filename.endsWith(...):
 *   .txt -> "text/plain", .html/.htm -> "text/html", .xml -> "text/xml",
 *   anything else -> "application/octet-stream" (ContentType.DEFAULT_BINARY).
 * Java lower-cases the name first; strcasecmp() on the extension does the
 * same. strrchr(name, '.') finds the extension. */
static const char *
content_type_for(const char *name)
{
    htstring_t s = htstring_ctor(name, strlen(name));
    if (htstring_has_suffixi(s, HTSTRING_LIT(".txt")))
        return "text/plain";
    else if (htstring_has_suffixi(s, HTSTRING_LIT(".html")) ||
                htstring_has_suffixi(s, HTSTRING_LIT(".htm"))) {
        return "text/html";
    } else if (htstring_has_suffixi(s, HTSTRING_LIT(".xml")))
        return "text/xml";
    else
        return "application/octet-stream";
}

/* ------------------------------------------------------------------------ */
/* TODO 2: an error page                                                    */
/* ------------------------------------------------------------------------ */

static const htstring_t escaped[256] = {
    ['<'] = { "&lt;", 4 },  ['>'] = { "&gt;", 4 },
    ['&'] = { "&amp;", 5 }, ['"'] = { "&quot;", 6 },
};                          /* every other entry is zero: { NULL, 0 } */

static inline size_t
calc_escaped_len(const char *msg)
{
    size_t len = 0;
    for (const char *curp = msg; curp[0]; ++curp) {
        size_t n = escaped[(unsigned char)curp[0]].len;
        if (n > 0)
            len += n;
        else
            ++len;
    }
    return len;
}

/* NULL when out of memory. */
static inline char*
escape_message(const char *msg)
{
    size_t len = calc_escaped_len(msg);
    char  *out = (char*)malloc(len + 1); // Could be htstring_t rather than c string.
    char  *dst = out;

    if (out == NULL)
        return NULL;

    for (const char *curp = msg; curp[0]; ++curp) {
        htstring_t e = escaped[(unsigned char)curp[0]];
        if (e.len > 0) {
            memcpy(dst, e.startp, e.len);
            dst += e.len;
        } else {
            *dst++ = curp[0];
        }
    }
    *dst = '\0';
    return out;
}

/* Java: println(msg) and
 *   responseTrigger.submitResponse(AsyncResponseBuilder.create(status)
 *       .setEntity("<html><body><h1>" + msg + "</h1></body></html>",
 *                  ContentType.TEXT_HTML).build(), context);
 *
 * hcore: a small, complete response in one call:
 *   hc_response_send_simple(hc_basic_request_channel(r), status,
 *                           "text/html", body, 0);
 * (the last argument: 1 to close the connection afterwards). Build the body
 * with snprintf() into a local buffer; hcore copies it. */
/* Out of memory, or anything else that leaves no proper answer: a fixed
 * text, so sending it allocates nothing of ours. */
static void
send_internal_error(hc_basic_request *r, const char *what)
{
    println("internal error: %s", what);
    hc_response_send_simple(hc_basic_request_channel(r), 500, "text/plain",
                            "Internal Server Error\n", 0);
}

static void
send_error_page(hc_basic_request *r, int status, const char *msg)
{
    ht_scratch_auto(tbuf);
    char       *escaped_msg = escape_message(msg);
    const char *body = NULL;

    if (escaped_msg)
        body = htstring_join_cstr_v(&tbuf,
                        HTSTRING_LIT("<html><body><h1>"),
                        htstring_ctor(escaped_msg, strlen(escaped_msg)),
                        HTSTRING_LIT("</h1></body></html>"));
    println("%s", msg);
    if (body)
        hc_response_send_simple(hc_basic_request_channel(r), status, "text/html",
                                body, 0);
    else
        send_internal_error(r, "out of memory (error page)");
    free(escaped_msg);
}

/* ------------------------------------------------------------------------ */
/* TODO 3: from request-target to file name                                 */
/* ------------------------------------------------------------------------ */

/* Java: requestUri.getPath(), then new File(docRoot, path).
 *
 * hc_basic_request_target(r) is the raw request-target: "/dir/a%20b.txt?x=1".
 * Into out, write doc_root + the path:
 *   - stop at '?' (the query; Java's getPath() leaves it out);
 *   - decode %XX escapes ("%20" -> ' '), as getPath() does; reject a bad
 *     escape and "%00";
 *   - REJECT any ".." path segment, *after* decoding: "/../etc/passwd" and
 *     "/..%2f..%2fetc/passwd" must not leave doc_root. (The Java example
 *     doesn't check; a real server must.)
 *   - reject a target that doesn't start with '/', and anything too long
 *     for out.
 * Return 0, or -1 for a target to refuse (the caller answers 400). */
static int
resolve_path(const char *target, char *out, size_t size)
{
    struct parsed_uri u = parse_uri_view(htstring_ctor(target, strlen(target)), false);
    htstring_t        path = parsed_uri_get_path(target, &u);

    char              dec[2048];
    ptrdiff_t         n = htstring_pct_decode(path, dec, sizeof(dec));
    if (n < 0) return -1;
    if (n == 0) return -1;
    if (dec[0] != '/') return -1;
    if (memchr(dec, 0, n)) return -1;

    htstring_t        seg[64];
    ptrdiff_t         ns = htstring_split(htstring_ctor(dec, (size_t)n), '/', seg, 64);
    if (ns < 0) return -1;
    for (ptrdiff_t i = 0; i < ns; i++) {
        if (htstring_equals(seg[i], HTSTRING_LIT(".."))) return -1;
    }
    size_t doc_root_len = strlen(doc_root);
    if (n + doc_root_len >= size) return -1;

    ht_scratch_auto(tbuf);
    const char *file_path = htstring_join_cstr_v(&tbuf,
                            htstring_ctor(doc_root, doc_root_len),
                            htstring_ctor(dec, n));

    memcpy(out, file_path, n + doc_root_len);
    out[n + doc_root_len] = '\0';
    return 0;
}

/* ------------------------------------------------------------------------ */
/* TODO 5 (state): one file being sent                                      */
/* ------------------------------------------------------------------------ */

/* Java hides this in AsyncEntityProducers.create(file, contentType). In C
 * the request carries it: hc_basic_request_set_data(r, xfer) in
 * file_request(), hc_basic_request_data(r) in the callbacks after it.
 * It must be freed (and the file closed) in file_release(), which is
 * called once per request whatever happened: sent, client gone, timeout. */
typedef struct file_xfer {
    int fd;
    /* TODO: anything else you need */
} file_xfer;

/* ------------------------------------------------------------------------ */
/* TODO 4: the request                                                      */
/* ------------------------------------------------------------------------ */

/* Java: handle(): the whole request has arrived (the server's basic factory
 * plays BasicRequestConsumer; any request body is read and ignored, as
 * NoopEntityConsumer does).
 *
 *   1. resolve_path() fails       -> hc_response_send_simple(..., 400, ...)
 *   2. stat() fails               -> send_error_page(r, 404, "File ... not found")
 *   3. a directory, or open() fails -> send_error_page(r, 403, "Cannot read file ...")
 *   4. otherwise: 200 with the file as the body, streamed:
 *        - a file_xfer for the open fd, attached with hc_basic_request_set_data();
 *        - println("%s | serving file %s", remote_address(...), path);
 *        - the head: hc_response_head { 200, 11, headers } with a
 *          "Content-Type" header (hc_headers_new / hc_headers_add /
 *          hc_headers_free after sending - hcore copies them);
 *        - the entity: hc_entity { st.st_size, content type } - a known
 *          length, so the reply has Content-Length rather than chunks;
 *        - hc_response_send(hc_basic_request_channel(r), &head, &entity):
 *          hcore then calls ->produce(), which sends the bytes.
 *      Don't read the file here: file_produce() does, as the client takes it.
 */
static void
file_request(void *arg, hc_basic_request *r)
{
    (void)arg;
    struct stat sbuf;
    char msg[4096];
    char path[2048];
    int fd;
    if (resolve_path(hc_basic_request_target(r), path, sizeof(path)) != 0) {
        hc_response_send_simple(hc_basic_request_channel(r), 400, "text/plain", "resolve path failed", 1);
    } else if (stat(path, &sbuf) != 0) {
        snprintf(msg, sizeof(msg), "File %s not found", path);
        send_error_page(r, 404, msg);
    } else if (!S_ISREG(sbuf.st_mode) || (fd = open(path, O_RDONLY)) == -1) {
        snprintf(msg, sizeof(msg), "Cannot read file %s", path);
        send_error_page(r, 403, msg);
    } else {
        const char *content_type = content_type_for(path);
        file_xfer  *xfer = calloc(1, sizeof(*xfer));
        hc_headers *headers = hc_headers_new();

        if (xfer == NULL || headers == NULL ||
            hc_headers_add(headers, "Content-type", content_type) != 0) {
            hc_headers_free(headers);
            free(xfer);
            close(fd);
            send_internal_error(r, "out of memory (200 response)");
            return;
        }

        /* Attached before sending: from here on file_release() owns the
         * fd and xfer, whatever happens next. */
        xfer->fd = fd;
        hc_basic_request_set_data(r, xfer);

        hc_response_head head = {
            .headers = headers,
            .status = 200,
            .version = 11
        };
        hc_entity entity = {
            .content_length = sbuf.st_size,
            .content_type = content_type
        };
        println("%s | serving file %s", remote_address(r, msg, sizeof(msg)), path);
        if (hc_response_send(hc_basic_request_channel(r), &head, &entity) == NULL)
            println("cannot send the response for %s", path);  /* ->failed and ->release follow */
        hc_headers_free(headers);
    }
}

/* ------------------------------------------------------------------------ */
/* TODO 5: sending the file                                                 */
/* ------------------------------------------------------------------------ */

/* Java: what the FileEntityProducer does: called whenever the connection
 * can take more of the body.
 *
 * Loop while hc_data_capacity(ch) > 0: read() a chunk (16 KiB or so) from
 * the file and hc_data_write() it. At end of file (read() returns 0):
 * hc_data_end(ch, NULL) and return. When the capacity reaches 0, just
 * return: hcore calls this again once the client has taken some.
 *
 * Note: only the 200 path reaches this (error pages from
 * hc_response_send_simple() have no body channel), but a NULL
 * hc_basic_request_data(r) costs nothing to check. */
static void
file_produce(void *arg, hc_basic_request *r, hc_data_channel *ch)
{
    (void)arg;
    file_xfer *xfer = hc_basic_request_data(r);
    if (xfer == NULL)
        return;
    while (hc_data_capacity(ch) > 0) {
        char buf[16 * 1024];
        ssize_t n = read(xfer->fd, buf, sizeof(buf));
        if (n < 0 && errno == EINTR)
            continue;
        if (n < 0) {
            /* Content-Length is already promised: there is no way to
             * answer differently now. Abort the exchange; the client sees
             * the connection (or HTTP/2 stream) close short of the length. */
            println("read failed: %s", strerror(errno));
            hc_server_abort(hc_basic_request_exchange(r));
            return;
        }
        if (n == 0) {
            hc_data_end(ch, NULL);
            return;
        }
        if (hc_data_write(ch, buf, n) < 0) {
            return;
        }
    }
}

/* Java: the exchange's failed(): the client went away mid-file, a
 * timeout... Only logged; file_release() follows and cleans up. */
static void
file_failed(void *arg, hc_basic_request *r, const char *reason)
{
    (void)arg;
    (void)r;
    println("exchange failed: %s", reason);
}

/* Java: releaseResources(). The last call for r: close the file and free
 * the file_xfer, if file_request() made one. */
static void
file_release(void *arg, hc_basic_request *r)
{
    (void)arg;
    file_xfer *xfer = hc_basic_request_data(r);
    if (xfer) {
        close(xfer->fd);
        free(xfer);
    }
}

/* ------------------------------------------------------------------------ */
/* Done: main                                                               */
/* ------------------------------------------------------------------------ */

int
main(int argc, char **argv)
{
    int port = 8080;
    /* Java: AsyncServerBootstrap.bootstrap().register("*", handler). The
     * basic factory gives the handler whole requests (BasicRequestConsumer). */
    hc_basic_server_config config = {
        .request = file_request,
        .produce = file_produce,
        .failed  = file_failed,
        .release = file_release,
    };
    hc_reactor *r;
    hc_server  *s;

    if (argc < 2) {
        fprintf(stderr, "Please specify document root directory\n");
        return 1;
    }
    doc_root = argv[1];
    if (argc >= 3) {
        port = atoi(argv[2]);
    }

    r = hc_reactor_new();
    s = r ? hc_server_new(r, hc_basic_server_factory, &config) : NULL;
    if (s == NULL) {
        return 1;
    }

    /* Java: IOReactorConfig.setSoTimeout(15, SECONDS). */
    hc_server_set_timeouts(s, 15000, 15000);

    /* Java: server.listen(new InetSocketAddress(port), URIScheme.HTTP). */
    if (hc_server_listen(s, "0.0.0.0", (uint16_t)port) != 0) {
        fprintf(stderr, "cannot listen on port %d\n", port);
        return 1;
    }

    /* Java: the shutdown hook - Ctrl-C (SIGINT) or SIGTERM ends
     * hc_reactor_run(). */
    hc_reactor_stop_on_signals(r);
    println("Listening on 0.0.0.0:%d", port);

    hc_reactor_run(r);      /* Java: server.awaitShutdown() */

    println("HTTP server shutting down");
    hc_server_free(s);
    hc_reactor_free(r);

    return 0;
}
