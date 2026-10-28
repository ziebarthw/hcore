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
    (void)name;
    /* TODO */
    return "application/octet-stream";
}

/* ------------------------------------------------------------------------ */
/* TODO 2: an error page                                                    */
/* ------------------------------------------------------------------------ */

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
static void
send_error_page(hc_basic_request *r, int status, const char *msg)
{
    (void)status;
    (void)msg;
    /* TODO: replace this line */
    hc_response_send_simple(hc_basic_request_channel(r), 501, "text/plain",
                            "send_error_page: not written yet\n", 0);
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
    (void)target;
    (void)out;
    (void)size;
    /* TODO */
    return -1;
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
 *        - ch = hc_response_send(hc_basic_request_channel(r), &head, &entity);
 *        - hc_data_request_output(ch): ->produce() then sends the bytes.
 *      Don't read the file here: file_produce() does, as the client takes it.
 */
static void
file_request(void *arg, hc_basic_request *r)
{
    (void)arg;
    /* Not used yet (keeps the compiler quiet): delete once they are. */
    (void)resolve_path;
    (void)send_error_page;
    (void)content_type_for;
    (void)remote_address;
    /* TODO: replace this line */
    hc_response_send_simple(hc_basic_request_channel(r), 501, "text/plain",
                            "file_request: not written yet\n", 0);
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
    (void)r;
    (void)ch;
    /* TODO */
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
    (void)r;
    /* TODO */
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
