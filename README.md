# hcore: HttpCore-style async exchange handlers in C, on libevhtp

A C99 port of the *shape* of Apache HttpCore 5's async API: exchange
handlers, a route-based connection pool, a requester, request producers and
response consumers, URI-pattern routing, interim (1xx) responses and TLS.
It comes with ports of three HttpCore examples:
[`AsyncReverseProxyExample`](https://github.com/apache/httpcomponents-core/blob/rel/v5.1/httpcore5/src/test/java/org/apache/hc/core5/http/examples/AsyncReverseProxyExample.java),
[`AsyncFullDuplexClientExample`](https://github.com/apache/httpcomponents-core/blob/rel/v5.1/httpcore5/src/test/java/org/apache/hc/core5/http/examples/AsyncFullDuplexClientExample.java)
and
[`AsyncFullDuplexServerExample`](https://github.com/apache/httpcomponents-core/blob/rel/v5.1/httpcore5/src/test/java/org/apache/hc/core5/http/examples/AsyncFullDuplexServerExample.java).

```
include/hc.h            the "interfaces" an app implements, channels it calls, hc_host (no libevhtp types)
include/hc_pool.h       generic route-based connection pool (StrictConnPool model)
include/hc_support.h    request producers, response consumers, basic client handler (nio.support)
include/hc_processor.h  request/response interceptors, the HTTP processor, the exchange context (http.protocol)
src/hc_evhtp.c          engine: drives the interfaces from libevhtp/libevent/OpenSSL
src/hc_pool.c           the pool              } engine-independent:
src/hc_host.c           hc_host / URI parsing } hc.h only
src/hc_support.c        producers/consumers   }
src/hc_processor.c      interceptors/context  }
examples/reverse_proxy.c       port of AsyncReverseProxyExample (pooled, http/https origins, relays 1xx)
examples/full_duplex_client.c  port of AsyncFullDuplexClientExample (producers/consumers, http/https)
examples/full_duplex_server.c  port of AsyncFullDuplexServerExample (routing, stream listener, http/https)
examples/echo_server.c         minimal server handler: streams the request body back (http/https)
tests/pool_test.c       pool + hc_host unit tests with fake connections
tests/support_test.c    in-process integration tests: support API, TLS, 1xx, routing (HTTP + HTTPS servers)
tests/embed_test.c      hcore in the application's event loop: reuse_port, pause/resume accept, graceful drain
tests/shutdown_test.c   freeing a requester (and server) while the application's loop keeps running
tests/tls_name_test.c   https to an address, presenting and verifying a host name (hc_host.tls_name)
tests/h2_test.c         the server over HTTP/2 (libevhtp with HTTP/2 only), checked with curl
tests/processor_test.c  interceptors and processors on both sides of real exchanges
tests/run.sh            end-to-end tests (curl / client -> proxy -> tests/origin.py, echo_server)
tests/tls/              self-signed test certificates (localhost, other.example)
third_party/htstring/   copy of htstring: htstring_t string views, used by the engine's routing
scripts/update-htstring.sh  refreshes that copy from GitHub
```

**htstring.** `third_party/htstring` is a copy of
[ziebarthw/htstring](https://github.com/ziebarthw/htstring), the non-owning
string views (`htstring_t`) that the ziebarthw/libevhtp fork's parser is built
on. hcore uses only its header-only `htstring.h`, and only internally:
- it is not part of hcore's API;
- nothing of it is linked into hcore;
- `make install` does not install it.

Because the copy is in the repository, a fresh clone builds with nothing extra
to install. To update it, run `scripts/update-htstring.sh`, which takes the
latest `main`, or pass a tag or branch name. Then review with `git diff`, run
`make check` and commit. `third_party/htstring/UPSTREAM` records which commit
the copy came from.

## The pattern: interfaces as vtables

C has no interfaces, so each one is a struct whose first member points at a
table of function pointers. The app embeds that struct as the **first member**
of its own handler, so a pointer to the interface is also a pointer to the
app's struct:

```c
/* hc.h */
typedef struct hc_server_handler_vtbl {
    void (*handle_request)(hc_server_handler *self, hc_server_exchange *ex,
                           const hc_request_head *head, const hc_entity *entity,
                           hc_response_channel *rc);
    void (*consume)(hc_server_handler *self, const void *data, size_t len);
    void (*stream_end)(hc_server_handler *self, const hc_headers *trailers);
    void (*produce)(hc_server_handler *self, hc_data_channel *ch);
    void (*failed)(hc_server_handler *self, const char *reason);
    void (*release)(hc_server_handler *self);
} hc_server_handler_vtbl;
struct hc_server_handler { const hc_server_handler_vtbl *vtbl; };

/* app */
typedef struct incoming_handler {
    hc_server_handler base;        /* must be first */
    exchange_state   *state;
} incoming_handler;

static const hc_server_handler_vtbl incoming_vtbl = {
    .handle_request = in_handle_request, .consume = in_consume, ...
};
```

The engine only ever sees `hc_server_handler *` and calls `h->vtbl->consume(h, ...)`.
A factory function (`hc_server_handler *(*)(void *arg)`) plays the role of
Java's `Supplier<AsyncServerExchangeHandler>`: one handler per exchange.

| httpcore5 (Java)                              | hc.h (C)                                   |
|-----------------------------------------------|--------------------------------------------|
| `AsyncServerExchangeHandler.handleRequest`    | `hc_server_handler_vtbl.handle_request`    |
| `AsyncClientExchangeHandler.produceRequest`   | `hc_client_handler_vtbl.produce_request`   |
| `AsyncClientExchangeHandler.consumeResponse`  | `hc_client_handler_vtbl.consume_response`  |
| `AsyncClientExchangeHandler.consumeInformation` | `hc_client_handler_vtbl.consume_information` |
| `ResponseChannel.sendInformation`             | `hc_response_send_information()`           |
| `AsyncServerBootstrap.register(uriPattern, supplier)` | `hc_server_register()`             |
| `Http1StreamListener` (server)                | `hc_server_listener` / `hc_server_set_listener()` |
| `EndpointDetails.getRemoteAddress/getLocalAddress` | `hc_server_exchange_remote_address()` / `_local_address()` |
| `HttpHost`                                    | `hc_host` (scheme, name, port)             |
| `consume` / `streamEnd` / `produce`           | `consume` / `stream_end` / `produce`       |
| `failed` / `releaseResources`                 | `failed` / `release`                       |
| `ResponseChannel.sendResponse`                | `hc_response_send()`                       |
| `RequestChannel.sendRequest`                  | `hc_request_send()`                        |
| `DataStreamChannel.write / endStream`         | `hc_data_write()` / `hc_data_end()`        |
| `DataStreamChannel.requestOutput`             | `hc_data_request_output()`                 |
| `CapacityChannel.update`                      | `hc_server_/hc_client_ suspend_input / resume_input` |
| `EntityDetails` (null = no body)              | `const hc_entity *` (NULL = no body)       |
| `HttpAsyncRequester.execute` (no pool)        | `hc_client_execute()`                      |
| `HttpAsyncRequester.connect` -> `AsyncClientEndpoint` | `hc_requester_connect()` -> `hc_endpoint` |
| `AsyncClientEndpoint.execute`                 | `hc_endpoint_execute()`                    |
| `HttpAsyncRequester.execute(handler, timeout, ctx)` (pooled) | `hc_requester_execute()`    |
| `HttpAsyncRequester.execute(producer, consumer, timeout, callback)` | `hc_requester_execute_request()` |
| `releaseAndReuse` / `releaseAndDiscard`       | `hc_endpoint_release_and_reuse/_discard()` |

### Contract

- Every callback is dispatched from the event loop, never from inside an `hc_*`
  call. So any `hc_*` function may be called from any callback, including ones
  that tear things down: `hc_client_cancel`, `hc_server_abort`.
- `release()` is the last call a handler receives, exactly once. Channels and
  the exchange pointer are invalid after it, so null your copies there.
- Head structs and entity strings passed to callbacks are valid only during
  the call. Copy what you keep.
- `hc_data_write()` always queues. `hc_data_capacity()` says how much fits
  before the channel is "full"; once full, the engine calls `produce()` when
  it drains. `hc_data_request_output()` asks for a `produce()` call.

## Connection pool

Built in three layers, like httpcore5:

```
reverse_proxy.c   hc_requester_connect() -> hc_endpoint_execute() -> release_and_reuse/discard
      |
hc_evhtp.c        requester + endpoints: connects, decides reusability, parks idle
      |           connections, evicts dead ones          (knows libevhtp)
      |
hc_pool.c         routes, limits, queuing, expiry, LRU    (knows nothing about libevhtp)
```

### The pool (`hc_pool.h`): StrictConnPool, in C

The pool manages *entries*; a connection is an opaque `void *` that it can
only close, through `hc_pool_conn_ops.close` (Java:
`ManagedConnPool<T, C extends ModalCloseable>`). A lease completes with an
entry that either carries an idle connection or is empty, and the caller
connects and calls `hc_pool_entry_assign()`, exactly as `PoolEntry` works in
Java. That split is what lets `tests/pool_test.c` check the pool with fake
connections and no sockets.

| StrictConnPool (Java)                          | hc_pool (C)                                  |
|------------------------------------------------|----------------------------------------------|
| `lease(route, state, timeout, callback)`       | `hc_pool_lease()` (route: `hc_host`; callback from the loop) |
| `Future.cancel()`                              | `hc_pool_lease_cancel()`                     |
| `release(entry, reusable)`                     | `hc_pool_release()`                          |
| `PoolEntry.assignConnection / discardConnection` | `hc_pool_entry_assign / _discard()`        |
| `PoolEntry.updateExpiry(keepAlive)`            | `hc_pool_entry_update_expiry()`              |
| `setMaxTotal / setDefaultMaxPerRoute / setMaxPerRoute` | `hc_pool_set_max_*()`                |
| `getTotalStats / getStats(route)`              | `hc_pool_get_total_stats / _route_stats()`   |
| `closeIdle / closeExpired`                     | `hc_pool_close_idle / _expired()`            |
| `PoolReusePolicy.LIFO / FIFO`                  | `HC_POOL_REUSE_LIFO / _FIFO`                 |
| `ConnPoolListener.onLease / onRelease`         | `hc_pool_listener`                           |
| `timeToLive`                                   | `hc_pool_config.time_to_live_ms`             |

Lease processing follows `StrictConnPool.processPendingRequest`:
1. Hand out an idle entry of the route whose state matches, falling back to a
   stateless one. If it has expired, its connection is closed and the caller
   gets the empty slot to reconnect.
2. Otherwise close idle entries of this route that push it over its per-route
   limit.
3. Otherwise, if the route is under its limit and the pool under `max_total`,
   create a new slot. When the pool is full of idle connections, it first
   closes the least recently used idle connection of any route.
4. Otherwise the request stays queued, in arrival order, until a release,
   close or limit change lets it through, or its timeout fails it.

Releases go to the front of the route's idle list for LIFO (fewer, hotter
connections) or the back for FIFO. Each release or close re-runs the queue. A
connection's expiry is `min(now + keep-alive, created + time_to_live)`.

There is no `validateAfterInactivity`, because the engine does better: idle
connections keep `EV_READ` armed, so an origin closing an idle connection, or
sending stray bytes on it, evicts it immediately instead of being found stale
at the next lease.

### The engine side: what makes a connection reusable

An `hc_pconn` wraps one `evhtp_connection_t` and outlives individual
exchanges. When an exchange ends, the connection goes back to the pool only
if all of these hold:

- the response completed normally (not cancelled or failed, and not a
  read-until-close body);
- the request body was fully sent;
- nothing is left in the input or output buffers;
- the response was persistent: HTTP/1.1 without `Connection: close`, or
  HTTP/1.0 with `Connection: keep-alive`.

`release_and_reuse()` is a request; the engine still applies those rules. The
keep-alive period comes from `Keep-Alive: timeout=N` or the configured default,
which is 3 minutes, as in httpcore5's `DefaultConnectionKeepAliveStrategy`.

Reusing a libevhtp client connection takes three things:
- **A fresh request.** Each exchange gets its own `evhtp_request_t`. The
  previous one is freed, which also clears `conn->request`.
- **A clean parser.** `htparser_init()` resets it for the next response. The
  HEAD workaround leaves the parser expecting a body, so this reset is
  required, not optional.
- **Idle callbacks.** Whenever no exchange is running, the bufferevent's
  callbacks are swapped for the engine's own (saved with
  `bufferevent_getcb()`, restored on the next execute). libevhtp's parser
  callbacks dereference `conn->request`, which is NULL between exchanges.

Routes are `hc_host` values, so `http://h:8443` and `https://h:8443` never
share a connection (Java: `HttpHost` includes the scheme).

## Producers and consumers (`hc_support.h`)

The C version of httpcore5's `nio.support` and `nio.entity` packages, built
only on `hc.h`:

| httpcore5 (Java)                                   | hc_support.h (C)                         |
|----------------------------------------------------|------------------------------------------|
| `AsyncEntityProducer`                              | `hc_entity_producer` (+ `_vtbl`)         |
| `BasicAsyncEntityProducer` / `StringAsyncEntityProducer` | `hc_bytes_/hc_string_entity_producer_new()` |
| `AsyncRequestBuilder` / `BasicRequestProducer`     | `hc_request_producer_new(method, uri, entity)` |
| `AsyncEntityConsumer<T>`                           | `hc_entity_consumer` (+ `_vtbl`)         |
| `BasicAsyncEntityConsumer` / `StringAsyncEntityConsumer` | `hc_buffer_entity_consumer_new(max)` |
| `BasicResponseConsumer<T>` + `BasicClientExchangeHandler` | `hc_basic_client_handler_new()`   |
| `Message<HttpResponse, T>`                         | `hc_message` (head + body consumer)      |
| `FutureCallback<T>`                                | `hc_result_callback` (completed / failed, plus optional information and released) |

```c
hc_request_producer *req  = hc_request_producer_new("POST", "https://example.org/post",
                                hc_string_entity_producer_new("stuff", NULL));
hc_entity_consumer  *body = hc_buffer_entity_consumer_new(0);   /* 16 MiB cap */
hc_result_callback   cb   = { on_completed, on_failed, on_information, on_released, arg };

hc_requester_execute_request(requester, req, body, 30000, &cb);
/* on_completed(arg, msg): msg->head->status, hc_buffer_entity_consumer_data(msg->body, &len) */
```

Producers and consumers are interfaces, so a streaming body is your own
`hc_entity_producer` (write while `hc_data_capacity()` allows, `hc_data_end()`
when done) and a streaming sink is your own `hc_entity_consumer`. A consumer
can refuse a body by returning -1 from `consume()`, as the buffer consumer
does past its limit. The exchange is then cancelled, the connection is not
reused, and the callback fails with the reason. The full-duplex client's
duplex mode uses exactly this: a pattern-generating producer and a verifying
consumer, plugged into the same basic handler.

## Interceptors and the HTTP processor (`hc_processor.h`)

The lightweight way to shape every exchange's headers, as httpcore5's
`HttpProcessor` with its `HttpRequestInterceptor`s and
`HttpResponseInterceptor`s. A processor is built once, is immutable and
reference counted (atomically), so one can serve every requester and server
on every thread:

```c
hc_processor_builder *b = hc_processor_builder_new();
hc_processor_builder_add_target_host(b);                  /* RequestTargetHost */
hc_processor_builder_add_user_agent(b, "htm8/0.6");       /* RequestUserAgent  */
hc_processor_builder_add_request_last(b, add_auth, token, free);   /* your own */
hc_processor *p = hc_processor_builder_build(b);

hc_requester_config cfg = { .processor = p };             /* client side */
hc_requester *rq = hc_requester_new(r, &cfg);
hc_processor_unref(p);                                    /* rq keeps its own ref */

hc_processor *sp = hc_processor_server_default("htm8");   /* server side */
hc_server_set_processor(server, sp);
hc_processor_unref(sp);
```

Where they run: client request interceptors just before the request head is
sent, client response interceptors when the final response head arrives
(before `consume_response()`); server request interceptors after the head is
parsed (before `handle_request()`), server response interceptors in
`hc_response_send()`. They change headers (and a response's status); framing
headers stay the engine's. Returning a status (400..599) stops the exchange:
a client request is not sent and fails, a client response fails the
exchange, a server request is answered with that status instead of reaching
the handler, and a refused server response drops the connection.

Each exchange has an `hc_context` (`HttpCoreContext`): attributes the
interceptors and the handler share (`hc_server_exchange_context()`,
`hc_client_exchange_context()`), the protocol version, TLS, and the target
(client) or the client's address (server). Standard interceptors: target
host, user agent, request date, default headers, response date, server;
`hc_processor_client_default()` / `_server_default()` as `HttpProcessors`.

## Interim (1xx) responses

- **Client:** `consume_information` (optional in the vtbl) receives each
  interim response: `100 Continue`, `103 Early Hints` with its headers, and so
  on. `101 Switching Protocols` is treated as final; upgrades aren't
  supported, and such a connection is never reused.
- **Server:** `hc_response_send_information()` sends 1xx responses (102-199,
  HTTP/1.1 clients only) before the final one. The engine itself sends
  `100 Continue` for `Expect: 100-continue`, before `handle_request()` runs.
- **Proxy:** relays the origin's 1xx responses to the client. The exception is
  100, which the proxy's own server side has already sent.

## TLS

- **Client:** `hc_requester_config.tls_ca_file` (PEM; the system trust store
  when unset) and `tls_no_verify` (testing only). Every `https` connection
  sends SNI and verifies the certificate chain and the host name, or the IP
  address for an IP literal. Failures arrive as specific reasons:
  `TLS certificate verification failed: hostname mismatch`, `... self-signed
  certificate`, `TLS handshake failed: ...`, `cannot resolve <name>: ...`,
  `cannot connect to origin: Connection refused`.
- **Server:** `hc_server_enable_tls(server, cert.pem, key.pem)`. It returns
  -1 if the pair doesn't load; libevhtp itself ignores that error, so the
  engine checks.
- **Examples:** `echo_server --tls CERT KEY`;
  `reverse_proxy [--cafile PEM] https://origin`;
  `full_duplex_client [--cafile PEM | --insecure] https://...`.

TLS connections are pooled and reused exactly like plain ones, and full
duplex works the same over TLS.

## Routing and the full-duplex server

`hc_server_register(server, pattern, factory, arg)` routes requests by path,
like `AsyncServerBootstrap.register()`, and matches like HttpCore's
`UriPatternMatcher`:
- a pattern is an exact path (`/echo`), a prefix (`/api*`), a suffix
  (`*.html`) or `*`;
- an exact match wins; otherwise the longest matching pattern wins, and
  between two of equal length the one ending in `*`;
- the query string isn't part of the path.

Requests that match nothing go to the factory passed to `hc_server_new()`. If
that's `NULL`, they get `404 Resource not found`, as from HttpCore's
bootstrap. Servers that register nothing behave as before.

`hc_server_set_listener()` is the server side of `Http1StreamListener`. It
reports the request head, the response head, and each exchange that completes
normally, with whether the connection stays open. Every callback gets the
client's `address:port`.

`examples/full_duplex_server.c` ports `AsyncFullDuplexServerExample`.
`/echo` is the only registered route. The response head is sent as soon as
the request head arrives, carrying the request's entity details, so a
`Content-Length` request gets a `Content-Length` response with the same
`Content-Type`. Body bytes are relayed as they arrive, through a buffer that
starts at 2048 bytes like the Java one; client input is suspended while the
buffer is full and resumed once `produce()` drains it. The listener prints
what Java's prints:

```
Listening on 0.0.0.0:8080
127.0.0.1:42950 POST /echo HTTP/1.1
127.0.0.1:42950 HTTP/1.1 200
127.0.0.1:42950 exchange completed (connection kept alive)
```

Run it as `full_duplex_server [port] [--tls CERT KEY]`; `--tls` is an
addition, since the Java example is plain HTTP. With the full-duplex client
against it, both directions stream at once: 32 MB echoes back byte for
byte, over HTTP and HTTPS.

## Full-duplex client

`hc_requester_execute(rq, &target, timeout_ms, handler)` is
`HttpAsyncRequester.execute()`. It leases a pooled connection, runs the
handler on it, and releases it afterwards: back to the pool after a complete
exchange, discarded after a failure. Internally it wraps the app's handler in
one of its own that forwards every call and manages the endpoint. If no
connection can be had, the app's handler gets `failed()` and `release()`
without `produce_request()`. A handler that needs its exchange, for
`hc_client_suspend_input` or `hc_client_cancel`, gets it from
`hc_request_channel_exchange(rc)` in `produce_request()`.

`examples/full_duplex_client.c` ports `AsyncFullDuplexClientExample`:

```
full_duplex_client http://host[:port]/path                 # Java mode: POST "stuff", print the response
full_duplex_client --duplex 8 http://127.0.0.1:8081/echo   # stream 8 MB both ways via echo_server
full_duplex_client -n 3 --duplex 8 http://...              # 3 exchanges, one pooled connection
full_duplex_client --expect http://...                     # shows the 100 Continue (interim)
full_duplex_client --cafile tests/tls/localhost.pem --duplex 8 https://localhost:9443/echo
```

It is built on `hc_support.h`. Java mode is a request producer with a string
entity plus a buffer consumer. Duplex mode plugs a custom producer and
consumer into the same basic handler.

The Java example sends only `"stuff"`, too little to show full duplex. With
`--duplex`, the client streams generated data as a chunked body, verifies
every echoed byte, and reports how much of the upload had been sent when the
first response byte arrived:

```
>> POST http://127.0.0.1:8081/echo
http://127.0.0.1:8081 opening new connection [leased: 1][available: 0]
http://127.0.0.1:8081/echo->200
  sent 8388608 bytes, received 8388608 bytes, echo verified
  first response byte after 1 ms, with 130903 of 8388608 request bytes sent (1.6%)
  upload finished after 54 ms, response after 83 ms -> full duplex: both directions streamed at the same time
http://127.0.0.1:8081 connection kept alive [leased: 0][available: 1]
```

Against a server that reads the whole request before answering, it reports
half duplex instead.

For `-n`, each exchange starts the next from the callback's `released()`. The
engine returns the finished connection to the pool synchronously, before
that `release()` returns, so the next lease always finds it idle and reuse
doesn't depend on event ordering.

## Embedding: your event loop, several threads, graceful drain

An application that already runs libevent loops, one per worker thread for
example, can run hcore inside them, with every thread listening on the same
port:

```c
hc_reactor *r = hc_reactor_new_on_base(thread_event_base);   /* not owned */
hc_server  *s = hc_server_new(r, factory, arg);
hc_listen_options o = { .backlog = 1024, .reuse_port = 1, .defer_accept = 1 };
hc_server_listen_opts(s, "0.0.0.0", 8080, &o);               /* in each thread */
```

- **Who runs the loop:** the application does. Don't call `hc_reactor_run()`.
  `hc_reactor_free()` leaves the base alone. Everything hcore does for that
  reactor happens in the thread running that loop.
- **`reuse_port`:** sets `SO_REUSEPORT`. Every thread's server binds the same
  address, and the kernel spreads new connections between them.
- **Accepting:** `hc_server_pause_accept()` and `hc_server_resume_accept()`
  stop and restart accepting; the socket stays bound. `hc_server_unlisten()`
  closes it.
- **Graceful drain, for a config reload or shutdown:** call
  `hc_server_drain()`. It stops accepting, closes idle keep-alive connections
  immediately, and closes every other connection once its current exchange
  is complete. A response that hasn't started yet carries
  `Connection: close`, and nothing is cut off.
- **When the drain is done:** `hc_server_connection_count()` reaches 0, and
  the server can then be freed. `hc_server_resume_accept()` cancels a drain,
  for example when a reload is rolled back.

A connection whose request head is still arriving counts as idle. Like any
server closing an idle keep-alive connection, it is closed, and the client
retries.

## How the engine maps onto libevhtp

| hc.h event                    | libevhtp mechanism                                                   |
|-------------------------------|----------------------------------------------------------------------|
| server `handle_request`       | `evhtp_hook_on_headers` on a `"*"` glob callback (the gencb can't carry hooks) |
| server `consume`              | per-request `evhtp_hook_on_read`, draining the buffer                 |
| server `stream_end`           | the request callback (fires when the body is complete)               |
| response head / body          | `evhtp_send_reply_start` / `evhtp_send_reply_chunk_start`, then direct writes to the bufferevent with our own chunk framing |
| `produce` after drain         | an `evbuffer_add_cb` on the bufferevent's output buffer               |
| client connect / request      | `evhtp_connection_new_dns` (http); own `evdns_getaddrinfo` + `evhtp_connection_ssl_new`, then SNI/verification on `conn->ssl` (https); `evhtp_make_request` |
| client 1xx                    | the engine's read callback in front of libevhtp's until the final status line |
| server `Expect: 100-continue` | written by the engine before `handle_request()`; libevhtp's flag cleared |
| server TLS                    | `evhtp_ssl_init`, then a certificate/key check                       |
| server shutdown               | connections tracked via post-accept + connection-fini hooks, closed by `hc_server_free()` |
| client `consume_response`     | request `evhtp_hook_on_headers` + `evhtp_request_status`              |
| client `consume` / `stream_end` | request `evhtp_hook_on_read` / request callback                     |
| failures                      | `evhtp_hook_on_event`, `evhtp_hook_on_error`, `evhtp_hook_on_connection_fini`, `evhtp_hook_on_request_fini` |
| suspend / resume input        | `bufferevent_disable/enable(EV_READ)`                                |

libevhtp behaviours the engine works around (each is commented at the site in
`hc_evhtp.c`):

1. **Async replies need a pause.** If the request callback returns without
   finishing the reply, the request is paused, or pipelined input would be
   parsed into a new request.
2. **`evhtp_request_resume()` races the write callback.** Resume defers its work
   to an event. A write callback running in between frees the finished request,
   then the deferred resume re-arms `EV_WRITE` and libevent calls the write
   callback again with no request. The result is "no request associated with
   connection" and a dropped keep-alive connection. The engine un-pauses
   synchronously instead (clears `PAUSED`, sets `WAITING`).
3. **No write callback while paused,** so drain notifications use an
   `evbuffer` callback rather than `evhtp_hook_on_write`.
4. **`evhtp_send_reply_end()` only sets a flag.** If the output buffer is
   already empty, nothing looks at it again. The engine triggers the write
   callback itself.
5. **Freeing a connection inside its own callback is a use-after-free.**
   Everything destructive goes through a per-exchange task event that runs on
   the next loop iteration.
6. **The client parser can't do read-until-close bodies.** A response with
   neither `Content-Length` nor chunked encoding is reported complete after the
   head, and its body is then misparsed. The engine pauses the parser there,
   takes over the bufferevent callbacks, and reads to EOF itself.
7. **The client parser doesn't know about HEAD.** It waits for the
   `Content-Length` bytes of a HEAD response forever, leaving both connections
   open (the client sees nothing wrong, because it stops after the headers).
   The engine treats a HEAD response as complete at the head (and resets the
   parser before reusing the connection).
8. **Client parser callbacks assume a request is attached.** Between exchanges
   on a pooled connection there is none, so the engine swaps in its own
   bufferevent callbacks while the connection is idle (see above).
9. **htparser can't parse interim responses.** On `100 Continue` with no
   header lines it advances one byte per call without reporting the message.
   libevhtp then "resumes" a connection that isn't paused (a no-op), and the
   exchange hangs. A 1xx with header lines is rejected as an invalid request
   line. So the engine parses 1xx messages itself, in a read callback placed
   in front of libevhtp's, and hands over at the final status line.
10. **The automatic `100 Continue` comes after the on-headers hook.** A handler
   that starts its response from `handle_request()` would put `200 OK` on the
   wire before `100 Continue`, which the client then reads as body. The
   engine disables libevhtp's 100 and writes its own first.
11. **`evhtp_connection_ssl_new()` takes only a numeric IPv4 address** and
   sets neither SNI nor host-name checks, so the engine resolves the name
   itself and configures the SSL object before the handshake starts.
12. **`evhtp_ssl_init()` ignores certificate and key loading errors**, so
   `hc_server_enable_tls()` checks the result.
13. **`evhtp_free()` leaves accepted connections open**, so the engine tracks
   them and closes them in `hc_server_free()`. Exchanges on them still get
   `failed()` and `release()`.

## Build

```sh
# libevhtp (criticalstack/libevhtp 1.2.x), with TLS (tested with OpenSSL 3.0)
git clone https://github.com/criticalstack/libevhtp && cd libevhtp/build
cmake .. -DEVHTP_DISABLE_REGEX=ON -DEVHTP_DISABLE_EVTHR=ON \
         -DCMAKE_INSTALL_PREFIX=$HOME/local && make && make install
# (or the ziebarthw/libevhtp fork: see "With the ziebarthw/libevhtp fork")

# this project (needs the libevent 2.1 and OpenSSL dev packages)
make EVHTP_PREFIX=$HOME/local
./reverse_proxy httpbin.org 8080          # or 127.0.0.1:9000, https://example.org
./echo_server 8081                        # --tls CERT KEY for https
make EVHTP_PREFIX=$HOME/local check     # unit + integration + end-to-end tests
```

**Local settings.** Rather than editing the Makefile, copy
`config.mk.example` to `config.mk` and set `EVHTP_PREFIX`, `EXTRA_LIBS` and
`CFLAGS` there. `config.mk` is read when it exists and is ignored by git, so
machine-specific paths stay out of the repository. A plain `make` or
`make check` then uses those values, and anything given on the command line
still overrides them. For example, for the ziebarthw/libevhtp fork built with
HTTP/2 and regex support, with it and htstring installed under `/opt/evhtp`:

```make
EVHTP_PREFIX = /opt/evhtp
EXTRA_LIBS   = /opt/evhtp/lib/libhtstring.a -lnghttp2 -lonig
```

**Installing for another project.** `make install` copies `hc.h`,
`hc_pool.h` and `hc_support.h` to `$(PREFIX)/include`, `libhcore.a` to
`$(PREFIX)/lib`, and a pkg-config file, `libhcore.pc`, to
`$(PREFIX)/lib/pkgconfig` (or `PKGCONFIGDIR`). `PREFIX` defaults to
`/usr/local`; set it in `config.mk` to install next to your application's
other dependencies:

```make
PREFIX = /home/me/projects/myapp-deps
```

With that directory on `PKG_CONFIG_PATH`, meson finds it with
`dependency('libhcore')`, and `pkg-config --cflags --libs libhcore` gives
`-I$(PREFIX)/include -L$(PREFIX)/lib -lhcore`. The application also links
libevhtp, libevent and OpenSSL; `pkg-config --static --libs libhcore` (or
meson's `static: true`) adds them for applications that do not already.
Rerun `make install` after changing hcore; nothing updates the installed copy
otherwise. `DESTDIR` is prepended for staged installs
(`make install DESTDIR=/tmp/stage`).

**Editors (clangd, e.g. VS Code's clangd extension).** Run
`make compile_commands.json` once in the top directory. It writes the exact
compile command for every source file, using your `config.mk` settings, so
clangd finds `include/` and libevhtp's headers. Rerun it after changing
`config.mk` or adding source files. The file holds this machine's paths and
is git-ignored. In VS Code, reload the window, or run "clangd: Restart
language server" from the command palette, to pick it up.

If libevhtp was built with `-DEVHTP_DISABLE_SSL=ON`, the engine still builds
(drop `-levent_openssl -lssl -lcrypto` from `EVHTP_LIBS`). `https` targets then
fail with "https is not supported", and `hc_server_enable_tls()` returns -1.

`reverse_proxy [--cafile PEM] [--insecure] [--quiet] <[http|https://]hostname[:port]> [listener port]` logs like the Java
example: `<date> | [client->proxy] 0000000001 GET /path`, `[proxy->origin]`,
`[proxy<-origin]`, `[client<-proxy]`. Pool events are logged with totals:
`[proxy->origin] connection leased 127.0.0.1:9000 [leased: 1][available: 0][pending: 0][max: 100]`.
Like the Java example, it pools up to 100 connections, 20 per route.

### With the ziebarthw/libevhtp fork

All three test suites pass, valgrind-clean, against both upstream
criticalstack/libevhtp 1.2.18 and the fork's `develop` branch. To build
against the fork:

```sh
# nghttp2 >= 1.60 is required (the fork uses nghttp2_ssize and
# nghttp2_data_provider2); Ubuntu 24.04 ships 1.59, so build one if needed:
git clone -b v1.64.0 https://github.com/nghttp2/nghttp2 && cd nghttp2 && mkdir b && cd b
cmake .. -DENABLE_LIB_ONLY=ON -DBUILD_TESTING=OFF -DBUILD_SHARED_LIBS=OFF \
         -DBUILD_STATIC_LIBS=ON -DCMAKE_INSTALL_PREFIX=$HOME/local-ng2 && make install

# htstring (https://github.com/ziebarthw/htstring): the fork's URI parser
# and header tokenizer live there now
git clone https://github.com/ziebarthw/htstring && cd htstring
make check && make install PREFIX=$HOME/local-fork

# the fork
cd libevhtp && mkdir build && cd build
PKG_CONFIG_PATH=$HOME/local-ng2/lib/pkgconfig:$HOME/local-fork/lib/pkgconfig \
    cmake .. -DEVHTP_DISABLE_REGEX=ON -DCMAKE_INSTALL_PREFIX=$HOME/local-fork && make && make install

# hcore: libevhtp.a needs libhtstring.a, listed first in EXTRA_LIBS
make EVHTP_PREFIX=$HOME/local-fork \
     EXTRA_LIBS="$HOME/local-fork/lib/libhtstring.a $HOME/local-ng2/lib/libnghttp2.a" check
```

(Use `lib64` in those paths if that is where nghttp2 installs.)

Without `libhtstring.a` in `EXTRA_LIBS`, linking fails with undefined
references to `parse_uri_view` and `parsed_uri_get_host_port`. Fork versions
from before htstring was split out need only nghttp2. hcore's own copy of
htstring, in `third_party/htstring`, is used for compiling only, and doesn't
supply these: they live in htstring's library, not in its header.

Differences the engine accounts for:
- **Method enum order.** `htp_method_UNKNOWN` is first in the fork and last
  upstream, so the engine maps method names through the enumerators by name
  and never iterates over the enum.
- **Header layout.** Only `include/` goes on the include path, not
  `include/evhtp/`, matching libevhtp's own pkg-config file. Fork versions
  before the rename to `include/evhtp/htstring.h` installed
  `include/evhtp/string.h`, which `<string.h>` would otherwise resolve to.
- **HTTP/2 through ALPN.** The fork negotiates h2 when offered. The engine
  frames bodies and writes 1xx responses itself, which is only valid on
  HTTP/1.1, so it pins ALPN to `http/1.1` on both sides: servers answer only
  `http/1.1`, clients offer only `http/1.1`. Both are harmless upstream, which
  does no ALPN. HTTP/2 in the engine itself would build on the fork's nghttp2
  integration.
- **Deferred connection free.** The fork's `evhtp_connection_free()` waits for
  pending output to flush before freeing the bufferevent, without an event
  callback that would notice the flush failing. The engine therefore drops
  unsent output when libevhtp frees a server connection because of an error,
  and when `hc_server_free()` closes connections. That needs
  `evbuffer_unfreeze()` first: libevent freezes the start of a socket
  bufferevent's output buffer. Normal closes still flush.
- **The fork's `msgparser`.** It handles 1xx, pipelined responses and split
  input correctly, so the engine's 1xx interposer is redundant there but
  harmless. When a chunk-size or header line is split across reads, libevhtp's
  read callback logs "ODDITY, resuming when not paused". That's benign: the
  parser waits for the rest of the line.

## Tested

`make check` runs three suites. Each is valgrind-clean (0 errors, nothing in
use at exit).

**Pool unit tests** (`tests/pool_test`, 56 checks) use fake connections. They
cover:
- reuse;
- queuing at the per-route limit;
- `max_total` evicting another route's least recently used idle connection;
- lease timeout;
- LIFO versus FIFO order;
- keep-alive expiry and time-to-live;
- `close_idle`, `close_expired` and `connection_lost`;
- cancellation;
- state matching;
- the listener;
- shutdown failing waiting leases;
- http and https to the same host:port staying separate routes;
- `hc_host` and URI parsing (defaults, IPv6 literals, authority, bad input).

They found one real bug: an expired slot that got a new connection kept its
old expiry, so the new connection was dropped on its first release. Java's
`assignConnection` resets the timestamps, and now so does this.

**Integration tests** (`tests/support_test`, 68 checks) run HTTP, HTTPS and
routing servers and a requester in one process. They cover:
- a string producer and a 3 MB bytes producer round trip;
- a GET without a body;
- a buffer consumer over its limit: the callback fails and the connection
  isn't reused;
- `103 Early Hints` from `hc_response_send_information()` reaching the
  client's information callback;
- 100-continue;
- HTTPS round trips with verification and TLS connection reuse;
- IP-address mismatch;
- connection refused;
- bad URIs;
- routing: exact beats wildcard, prefix and suffix patterns, longest pattern
  wins, a tie goes to the `*`-ending pattern, the query string is ignored, and
  unmatched paths get 404;
- the server listener: one request head, response head and keep-alive
  completion per request, with the client's address.
- the exchange's addresses: the client's matches what the listener reports,
  the local one is the server's, and a short buffer is handled like
  `getpeername()`.

**Embedding** (`tests/embed_test`, 21 checks) runs hcore on an event base the
test owns, with a client thread using plain sockets so it sees exactly what
goes over the wire. It covers:
- a client's `Connection: close` getting its response and then an
  immediate close;
- a second server binding the same port with `reuse_port` (and failing
  without it);
- a paused server holding a new connection until it resumes;
- a drain: an idle keep-alive connection closes at once; a response not yet
  started carries `Connection: close` and the connection then closes; a
  response already under way completes and then closes;
- the connection count reaching 0, and a resume ending the drain;
- the base still working after `hc_reactor_free()`.

Breaking any of the three drain rules fails the matching check, against
both libevhtps.

**HTTP/2 server** (`tests/h2_test`, 41 checks) runs only when libevhtp has
HTTP/2 (the ziebarthw fork built with nghttp2; `make check` says "skipped"
otherwise) and curl does. An HTTPS hcore server answers curl over HTTP/2 and,
for comparison, the same requests over HTTP/1.1:
- a small reply; an error reply asking to close the connection (no
  `Connection` header may reach an HTTP/2 client); HEAD;
- a 3 MB upload, counted (past the 64 KB HTTP/2 flow-control windows);
- an 8 MB download of unknown length produced with backpressure, every byte
  checked;
- six streams at once on one connection;
- a drain with a stream under way: it completes, then the connection closes;
- `hc_server_set_http2(s, 0)`: a client asking for h2 gets HTTP/1.1.

**Interceptors** (`tests/processor_test`, 71 checks): an hcore client and
server, each with a processor, on one reactor. Server side: request
interceptors run in builder order (first/last), change and hide headers,
pass a request id through the context to the handler and to the response
interceptors; Date/Server are added; a response interceptor rewrites the
status; a refused request gets 403 without the handler running; a refused
response drops the connection. Client side: Host from the target,
User-Agent, default headers, Date only with a body, a custom interceptor
whose context reaches the response side; a refused request sends nothing; a
refused response fails the exchange. The client and server keep working
afterwards; the defaults and a shared, multiply referenced processor are
checked directly; valgrind: no errors, no leaks.

**TLS server name** (`tests/tls_name_test`, 19 checks) connects over https
to 127.0.0.1, where a plain OpenSSL server answers with the SNI it received.
With `hc_host_set_tls_name(&target, "localhost")` the origin sees SNI
"localhost" and the certificate verifies against that name; without one, no
SNI is sent (an IP is never sent as SNI) and verification checks the IP; a
name the certificate does not cover fails verification, and with
verification off reaches the origin as given. Different TLS names to one
address are separate pool routes (`tests/pool_test`).

**Shutdown while running** (`tests/shutdown_test`, 34 checks) frees a
requester while the event loop it runs on carries on, the way an embedding
replaces its proxy on a config reload. The origin reads requests but never
answers. `hc_requester_free()` must finish, before it returns and exactly
once each:
- an exchange in flight, and one whose cancel is still queued on the loop;
- requests waiting for a pool slot, served or not yet;
- an endpoint released but not yet handed back, one the application still
  holds, and a connect not yet answered;
- a reverse proxy's request: the server is freed first (its handler cancels
  the upstream exchange), then the requester.

Retrying from `failed()` is refused, the loop runs on with nothing firing,
and a new requester on the same loop works. Under valgrind (no errors, no
leaks) it shows the use-after-free a running loop would otherwise hit.

**End to end:** `tests/run.sh` (after `make`) runs curl and the full-duplex
client against the proxy, `echo_server` (HTTP and HTTPS) and a Python origin
(`tests/origin.py`), 59 checks. Pool behaviour is measured
with a connection counter in the origin:
- 10 sequential requests share 1 origin connection;
- the connection is reused after a HEAD and after a streamed 3 MB upload;
- `Keep-Alive: timeout=1` is honoured;
- an idle connection the origin closes is evicted, not reused;
- a `Connection: close` response is not reused;
- 30 concurrent downloads over the 20-per-route limit all complete, with
  requests queued and never more than 20 leased;
- at the end, every pooled connection still open is healthy (ESTABLISHED,
  none in CLOSE_WAIT).

The full-duplex client checks cover:
- Java mode (POST `"stuff"`);
- 8 MB full duplex direct to `echo_server`;
- three 32 MB full-duplex exchanges through the proxy over one pooled
  connection (1 new, 2 reused, every byte verified);
- half-duplex detection against the buffering Python origin;
- an unreachable origin reaching the handler's `failed()`.

The TLS and 1xx checks cover:
- 3 MB over HTTPS direct, and through the proxy to an HTTPS origin, which
  reuses a single TLS connection (counted at the socket level);
- 2 x 8 MB full duplex over HTTPS on one TLS connection;
- self-signed, IP-mismatch and host-name-mismatch failures, `--insecure`, a
  handshake against a plain-HTTP port, and an unresolvable name;
- an untrusted HTTPS origin turning into a 503 that names the reason;
- the client seeing `100` and `103` (with its `Link` header);
- the proxy relaying `103` to curl;
- through the proxy: the proxy's own `100`, then the origin's `103`, then `200`.

The full-duplex server checks cover:
- 2 x 8 MB full duplex with the ported client on one connection;
- 8 MB full duplex over HTTPS;
- curl's 3 MB echo keeping `Content-Length` and `Content-Type`;
- 404 for unregistered paths;
- the listener's request, response and completion lines.

Two server regression checks cover bugs that the TLS work exposed:
- a 2 MB upload with `Expect` to a handler that responds from
  `handle_request()`;
- keep-alive after a GET answered from `handle_request()`. Before the fix,
  `srv_try_finalize()` never ran once the request completed after the
  response, so the connection stayed paused forever.

It also covers: GET, keep-alive reuse, a 5 MiB body (checksummed), chunked,
read-until-close, HEAD, 204, 3 MB uploads (Content-Length, chunked,
`Expect: 100-continue`), the origin dying mid-body (client sees a truncated
transfer), echo streaming both ways through the proxy, an unreachable origin
(503), HTTP/1.0 clients, a throttled slow reader, client aborts, and 40
concurrent 5 MiB downloads. It also checks that no client connections remain
open once the tests finish. Proxy memory stays flat under slow readers (about
1 MB extra for five throttled 5 MiB downloads), because input is suspended at
64 KiB buffered. The suite also passes with the proxies under valgrind, with 0 errors and
no leaks:
`WRAP="valgrind --leak-check=full" T=60 tests/run.sh`.

## Not done / differences from the Java example

- **Pool differences.** There is no `LaxConnPool` variant: one thread, so
  strict is free. Lookup is a linear list of routes, which is fine for a proxy
  with a handful of origins but wants a hash table for many. A failed request
  on a connection that turned out to be stale is not retried, the same as the
  Java example.
- **Single-threaded.** One reactor, one thread. libevhtp's `evhtp_use_threads`
  would need a reactor per thread and handlers kept thread-local.
- **Incoming trailers aren't surfaced** (`stream_end` gets NULL); outgoing
  trailers on chunked bodies are written.
- **libevhtp chooses some response details.** It writes the reason phrase from
  the status code, adds `Content-Type: text/plain` when none is given, and adds
  `Content-Length: 0` to bodiless HTTP/1.1 responses, including 204.
- **HTTP/1.1 only.** No HTTP/2, no ALPN, no protocol upgrades (101).
- **TLS limits.** Client TLS connects over IPv4 only, because
  `evhtp_connection_ssl_new()` takes an IPv4 address; plain HTTP can use IPv6.
  There are no client certificates (mutual TLS) and no session resumption on
  the client.
- **The client doesn't wait for `100 Continue`.** With `Expect:
  100-continue` it sends the body straight away, which RFC 9110 allows. Java
  waits up to a timeout.
- **Cancellation before a connection exists.** An `hc_requester_execute()` in
  flight can't be cancelled before it has a connection (Java's
  `Future.cancel`). Once it is running, `hc_request_channel_exchange()` +
  `hc_client_cancel()` works.
- **No consumer back-pressure.** Entity consumers have no capacity channel
  (Java's `updateCapacity`). A slow consumer has to buffer, or the handler can
  use `hc_client_suspend_input()` directly.
