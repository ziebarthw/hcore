#!/bin/bash
# End-to-end tests: origin.py on 9000, echo_server on 9001 (http), 9443/9444 (https,
# certs in tests/tls), proxies on 8080/8082/8083/8085/8086.
# Usage: make && tests/run.sh      (under valgrind: WRAP="valgrind --leak-check=full" T=60 tests/run.sh)
HERE=$(cd "$(dirname "$0")" && pwd); S=${TEST_TMP:-/tmp/hcore-test}; mkdir -p $S
B=$HERE/..
WRAP=${WRAP:-}
pass=0; fail=0
check() { if [ "$2" == "$3" ]; then echo "PASS $1"; pass=$((pass+1)); else echo "FAIL $1: got [$2] want [$3]"; fail=$((fail+1)); fi; }

pkill -x reverse_proxy; pkill -x echo_server; pkill -f "[f]ull_duplex_server 92"; sleep 0.3
# Always a fresh origin: one left running from an earlier run may be an older
# origin.py that lacks routes the checks below rely on.
pkill -f "^python3 .*origin.py 9000"; sleep 0.3
python3 $HERE/origin.py 9000 </dev/null >/dev/null 2>&1 &
$B/echo_server 9001 </dev/null >/dev/null 2>$S/echo.err &
$B/echo_server --tls $HERE/tls/localhost.pem $HERE/tls/localhost.key 9443 </dev/null >/dev/null 2>$S/echo_tls.err &
$B/echo_server --tls $HERE/tls/other.pem $HERE/tls/other.key 9444 </dev/null >/dev/null 2>>$S/echo_tls.err &
$WRAP $B/full_duplex_server 9200 </dev/null >$S/fds.log 2>$S/fds.err &
$WRAP $B/full_duplex_server 9201 --tls $HERE/tls/localhost.pem $HERE/tls/localhost.key </dev/null >$S/fds_tls.log 2>$S/fds_tls.err &
$WRAP $B/reverse_proxy --cafile $HERE/tls/localhost.pem https://localhost:9443 8086 </dev/null >$S/p8086.log 2>$S/p8086.err &
$WRAP $B/reverse_proxy https://localhost:9443 8087 --quiet </dev/null >/dev/null 2>$S/p8087.err &
$WRAP $B/reverse_proxy 127.0.0.1:9000 8080 </dev/null >$S/p8080.log 2>$S/p8080.err &
$WRAP $B/reverse_proxy 127.0.0.1:9001 8082 --quiet </dev/null >/dev/null 2>$S/p8082.err &
$WRAP $B/reverse_proxy 127.0.0.1:9999 8083 --quiet </dev/null >/dev/null 2>$S/p8083.err &
# wait until each port accepts connections (valgrind startup can be slow)
wait_ports() { local p i; for p in "$@"; do for i in $(seq 1 600); do
  (exec 3<>/dev/tcp/127.0.0.1/$p) 2>/dev/null && break; sleep 0.1; done; done; }
wait_ports 9000 9001 9443 9444 8080 8082 8083 8086 8087 9200 9201
[ -f $S/up.bin ] || head -c 3000000 /dev/urandom > $S/up.bin
UPSUM="$(stat -c%s $S/up.bin) $(sha256sum < $S/up.bin | cut -d' ' -f1)"
T=${T:-10}

check "GET + headers" "$(curl -s -m$T http://127.0.0.1:8080/hello?x=1 -H 'X-Test: abc' | grep -c 'X-Test: abc')" "1"
check "keep-alive reuse" "$(curl -s -m$T -o /dev/null -o /dev/null -w '%{num_connects}' http://127.0.0.1:8080/a http://127.0.0.1:8080/b)" "10"
check "5MiB CL response" "$(curl -s -m$T http://127.0.0.1:8080/big | sha256sum)" "$(curl -s http://127.0.0.1:9000/big | sha256sum)"
check "chunked response" "$(curl -s -m$T http://127.0.0.1:8080/chunked | wc -l)" "5"
check "read-until-close response" "$(curl -s -m$T http://127.0.0.1:8080/close | md5sum)" "$(curl -s http://127.0.0.1:9000/close | md5sum)"
check "HEAD keeps Content-Length" "$(curl -s -m$T -I http://127.0.0.1:8080/big | tr -d '\r' | grep -i '^content-length')" "Content-Length: 5242880"
check "204" "$(curl -s -m$T -o /dev/null -w '%{http_code}' http://127.0.0.1:8080/nocontent)" "204"
check "POST Content-Length 3MB" "$(curl -s -m$T --data-binary @$S/up.bin http://127.0.0.1:8080/upload)" "$UPSUM"
check "POST chunked 3MB" "$(curl -s -m$T -H 'Transfer-Encoding: chunked' --data-binary @$S/up.bin http://127.0.0.1:8080/upload)" "$UPSUM"
check "POST + Expect: 100-continue" "$(curl -s -m$T -H 'Expect: 100-continue' --data-binary @$S/up.bin http://127.0.0.1:8080/upload)" "$UPSUM"
check "origin dies mid-body -> truncated" "$(curl -s -m$T -o /dev/null -w '%{http_code} %{size_download}' http://127.0.0.1:8080/die)" "200 1000"
check "echo, streaming both ways (CL)" "$(curl -s -m$T --data-binary @$S/up.bin http://127.0.0.1:8082/e | sha256sum | cut -d' ' -f1)" "${UPSUM#* }"
check "echo, streaming both ways (chunked)" "$(curl -s -m$T -H 'Transfer-Encoding: chunked' --data-binary @$S/up.bin http://127.0.0.1:8082/e | sha256sum | cut -d' ' -f1)" "${UPSUM#* }"
check "echo, keep-alive x3" "$(curl -s -m$T -o /dev/null -o /dev/null -o /dev/null -w '%{num_connects}' --data-binary @$S/up.bin http://127.0.0.1:8082/1 http://127.0.0.1:8082/2 http://127.0.0.1:8082/3)" "100"
check "origin down -> 503" "$(curl -s -m$T -o /dev/null -w '%{http_code}' http://127.0.0.1:8083/x)" "503"
check "HTTP/1.0 client" "$(curl -s -m$T -0 http://127.0.0.1:8080/chunked | wc -l)" "5"

# slow reader: 5 MiB through a client reading at ~1 MB/s must stay bounded and complete
check "slow reader (backpressure)" "$(curl -s -m$((T*3)) --limit-rate 1500k http://127.0.0.1:8080/big | wc -c)" "5242880"

# client aborts mid-response, then the proxy must still serve
curl -s -m$T --limit-rate 100k http://127.0.0.1:8080/big -o /dev/null & CP=$!; sleep 1; kill $CP 2>/dev/null; wait $CP 2>/dev/null
curl -s -m$T http://127.0.0.1:8080/slow -o /dev/null & sleep 0.5; pkill -f "curl -s -m$T http://127.0.0.1:8080/slow"; sleep 0.3
check "proxy healthy after client aborts" "$(curl -s -m$T -o /dev/null -w '%{http_code}' http://127.0.0.1:8080/ok)" "200"

# concurrency
pids=""; for i in $(seq 1 40); do (curl -s -m$((T*3)) http://127.0.0.1:8080/big | sha256sum > $S/c$i.sum) & pids="$pids $!"; done; wait $pids
check "40 concurrent 5MiB downloads" "$(cat $S/c*.sum | sort -u | wc -l) $(cat $S/c*.sum | wc -l)" "1 40"
rm -f $S/c*.sum

sleep 2
# --- connection pool (fresh proxy on 8085, so its pool starts empty) -------
$WRAP $B/reverse_proxy 127.0.0.1:9000 8085 </dev/null >$S/p8085.log 2>$S/p8085.err &
wait_ports 8085
conns() { curl -s -m$T http://127.0.0.1:9000/stats; }   # counts its own connection too
new_conns() { echo $(( $(conns) - $1 - 1 )); }
c0=$(conns); for i in $(seq 1 10); do curl -s -m$T -o /dev/null http://127.0.0.1:8085/seq$i; done
check "pool: 10 sequential requests share 1 origin connection" "$(new_conns $c0)" "1"
c0=$(conns)
h=$(curl -s -m$T -I http://127.0.0.1:8085/big | tr -d '\r' | grep -i '^content-length')
g=$(curl -s -m$T http://127.0.0.1:8085/after-head | head -1)
check "pool: connection reused after HEAD (parser reset)" "$h|$g|$(new_conns $c0)" "Content-Length: 5242880|GET /after-head|0"
c0=$(conns); curl -s -m$T -o /dev/null http://127.0.0.1:8085/ka; sleep 2.5
check "pool: Keep-Alive timeout=1 honoured (reconnects after expiry)" "$(curl -s -m$T -o /dev/null -w '%{http_code}' http://127.0.0.1:8085/x) $(new_conns $c0)" "200 1"
c0=$(conns); curl -s -m$T -o /dev/null http://127.0.0.1:8085/drop; sleep 0.3
check "pool: idle connection closed by origin is evicted" "$(curl -s -m$T -o /dev/null -w '%{http_code}' http://127.0.0.1:8085/x) $(new_conns $c0)" "200 1"
c0=$(conns); curl -s -m$T -o /dev/null http://127.0.0.1:8085/close
check "pool: 'Connection: close' response is not reused" "$(curl -s -m$T -o /dev/null -w '%{http_code}' http://127.0.0.1:8085/x) $(new_conns $c0)" "200 1"
c0=$(conns); curl -s -m$T -o /dev/null --data-binary @$S/up.bin http://127.0.0.1:8085/upload; curl -s -m$T -o /dev/null http://127.0.0.1:8085/x
check "pool: reused after a streamed 3MB upload" "$(new_conns $c0)" "0"
pids=""; for i in $(seq 1 30); do (curl -s -m$((T*3)) http://127.0.0.1:8085/big | wc -c > $S/q$i.n) & pids="$pids $!"; done; wait $pids
check "pool: 30 concurrent over 20-per-route limit all complete" "$(cat $S/q*.n | sort -u)" "5242880"
rm -f $S/q*.n
check "pool: requests queued at the route limit" "$(grep -c 'pending: [1-9]' $S/p8085.log | awk '{print ($1>0)}')" "1"
check "pool: never more than 20 leased" "$(grep -o 'leased: [0-9]*' $S/p8085.log | awk '{if($2>m)m=$2} END{print (m<=20)}')" "1"

# --- server regressions --------------------------------------------------
# A handler answering from handle_request() must not be overtaken by the 100.
check "server: Expect 100-continue with an immediate response head" "$(curl -s -m$T --data-binary @$S/up.bin http://127.0.0.1:9001/e | sha256sum | cut -d' ' -f1)" "${UPSUM#* }"
# A GET answered from handle_request() must leave the connection usable.
check "server: keep-alive after an immediately answered GET" "$(curl -s -m$T -o /dev/null -o /dev/null -o /dev/null -w '%{num_connects}' http://127.0.0.1:9001/a http://127.0.0.1:9001/b http://127.0.0.1:9001/c)" "100"

# --- TLS -------------------------------------------------------------------
CA=$HERE/tls/localhost.pem
check "tls: curl -> https echo_server, 3MB" "$(curl -s -m$T --cacert $CA --data-binary @$S/up.bin https://localhost:9443/e | sha256sum | cut -d' ' -f1)" "${UPSUM#* }"
check "tls: curl -> proxy -> https origin, 3MB" "$(curl -s -m$T --data-binary @$S/up.bin http://127.0.0.1:8086/e | sha256sum | cut -d' ' -f1)" "${UPSUM#* }"
check "tls: proxy reuses its pooled TLS connection" "$(grep -c 'connection leased https://localhost:9443' $S/p8086.log) $(curl -s -m$T -o /dev/null http://127.0.0.1:8086/x; grep -c 'connection leased https://localhost:9443' $S/p8086.log)" "1 2"
# established sockets of the proxy listening on port $1, to remote port $2
conns_to() { local P n=0 ino rem; P=$(pgrep -f "reverse_proxy .* $1( --quiet)?$" | head -1)
  for ino in $(ls -l /proc/$P/fd 2>/dev/null | grep -o 'socket:\[[0-9]*\]' | grep -o '[0-9]*'); do
    rem=$(awk -v i=$ino '$10==i && $4=="01" {print $3}' /proc/net/tcp)
    [ -n "$rem" ] && [ $((16#${rem##*:})) -eq $2 ] && n=$((n+1)); done; echo $n; }
check "tls: ... over a single TLS connection to the origin" "$(conns_to 8086 9443)" "1"

# --- full-duplex client (examples/full_duplex_client.c) --------------------
FD="$WRAP $B/full_duplex_client"
out=$($FD http://127.0.0.1:9000/post 2>&1); rc=$?
check "client: Java mode POSTs \"stuff\"" "$rc $(echo "$out" | grep -c '^5 35bafb1ce99aef3ab068afbaabae8f21fd9b9f02d3a9442e364fa92c0b3eeef0')" "0 1"
check "client: stream listener logs request, response, keep-alive completion" "$(echo "$out" | grep -c '^127.0.0.1:9000 POST /post HTTP/1.1$') $(echo "$out" | grep -c '^127.0.0.1:9000 HTTP/1.1 200$') $(echo "$out" | grep -c '^127.0.0.1:9000 exchange completed (connection kept alive)$')" "1 1 1"
out=$($FD --expect http://127.0.0.1:9000/early 2>&1)
check "client: stream listener sees interim heads before the final one" "$(echo "$out" | grep -E '^127.0.0.1:9000 HTTP/1.1 [0-9]+$' | awk '{print $3}' | tr '\n' ' ')" "100 103 200 "
out=$($FD http://127.0.0.1:9000/close 2>&1)
check "client: read-until-close response -> exchange completed (connection closed)" "$(echo "$out" | grep -c 'exchange completed (connection closed)')" "1"
out=$($FD http://127.0.0.1:9000/die 2>&1)
check "client: response cut off -> no exchange completed line" "$(echo "$out" | grep -c 'HTTP/1.1 200$') $(echo "$out" | grep -c 'exchange completed')" "1 0"
out=$($FD --duplex 8 http://127.0.0.1:9001/echo 2>&1); rc=$?
check "client: 8MB full duplex direct to echo_server" "$rc $(echo "$out" | grep -c 'echo verified') $(echo "$out" | grep -c 'full duplex: both')" "0 1 1"
out=$($FD -n 3 --duplex 32 http://127.0.0.1:8082/echo 2>&1); rc=$?
check "client: 3 x 32MB full duplex through proxy, 1 pooled connection" "$rc $(echo "$out" | grep -c 'echo verified') $(echo "$out" | grep -c 'opening new connection') $(echo "$out" | grep -c 'reusing pooled connection')" "0 3 1 2"
out=$($FD --duplex 2 http://127.0.0.1:9000/upload 2>&1)
check "client: half duplex detected against a buffering server" "$(echo "$out" | grep -c 'half duplex')" "1"
out=$($FD http://127.0.0.1:9999/post 2>&1); rc=$?
check "client: unreachable origin -> failed()" "$rc $(echo "$out" | grep -c 'post->cannot connect')" "1 1"

out=$($FD -n 2 --duplex 8 --cafile $CA https://localhost:9443/echo 2>&1); rc=$?
check "client: 2 x 8MB full duplex over https, 1 TLS connection" "$rc $(echo "$out" | grep -c 'echo verified') $(echo "$out" | grep -c 'full duplex: both') $(echo "$out" | grep -c 'opening new') $(echo "$out" | grep -c 'reusing pooled')" "0 2 2 1 1"
check "client: https without the CA -> verification fails" "$($FD https://localhost:9443/x 2>&1 | grep -c 'verification failed: self-signed certificate')" "1"
check "client: https by IP -> IP address mismatch" "$($FD --cafile $CA https://127.0.0.1:9443/x 2>&1 | grep -c 'verification failed: IP address mismatch')" "1"
check "client: cert for another name -> hostname mismatch" "$($FD --cafile $HERE/tls/other.pem https://localhost:9444/x 2>&1 | grep -c 'verification failed: hostname mismatch')" "1"
check "client: --insecure skips verification" "$($FD --insecure https://127.0.0.1:9443/x 2>&1 | grep -c -- '->200')" "1"
check "client: https to a plain-http port -> handshake error" "$($FD --insecure https://127.0.0.1:9001/x 2>&1 | grep -c 'TLS handshake failed')" "1"
check "client: unresolvable name -> resolver error" "$($FD http://no-such-host.invalid/x 2>&1 | grep -c 'cannot resolve no-such-host.invalid')" "1"
check "proxy: untrusted https origin -> 503 with the reason" "$(curl -s -m$T -w ' %{http_code}' http://127.0.0.1:8087/x)" "TLS certificate verification failed: self-signed certificate 503"

# --- interim (1xx) responses -----------------------------------------------
out=$($FD --expect http://127.0.0.1:9000/post 2>&1); rc=$?
check "1xx: client sees 100 Continue, then the response" "$rc $(echo "$out" | grep -c -- '->100 (interim)') $(echo "$out" | grep -c -- '->200')" "0 1 1"
out=$($FD http://127.0.0.1:9000/early 2>&1)
check "1xx: client sees 103 Early Hints with its Link header" "$(echo "$out" | grep -c -- '->103 (interim) \[Link: </style.css>; rel=preload\]') $(echo "$out" | grep -c -- '->200')" "1 1"
check "1xx: proxy relays 103 to curl" "$(curl -s -m$T -i http://127.0.0.1:8080/early | tr -d '\r' | grep -E '^HTTP/1.1 [0-9]+|^Link' | tr '\n' '|')" "HTTP/1.1 103 Early Hints|Link: </style.css>; rel=preload|HTTP/1.1 200 OK|"
out=$($FD --expect http://127.0.0.1:8080/early 2>&1)
check "1xx: via proxy: proxy's 100, origin's 103, then 200" "$(echo "$out" | grep -o -- '->[0-9]*' | tr '\n' ' ')" "->100 ->103 ->200 "

# --- full-duplex server (examples/full_duplex_server.c) --------------------
out=$($FD -n 2 --duplex 8 http://127.0.0.1:9200/echo 2>&1); rc=$?
check "server: 2 x 8MB full duplex with the ported client, one connection" "$rc $(echo "$out" | grep -c 'echo verified') $(echo "$out" | grep -c 'full duplex: both') $(echo "$out" | grep -c 'reusing pooled')" "0 2 2 1"
out=$($FD --duplex 8 --cafile $CA https://localhost:9201/echo 2>&1); rc=$?
check "server: 8MB full duplex over https" "$rc $(echo "$out" | grep -c 'echo verified') $(echo "$out" | grep -c 'full duplex: both')" "0 1 1"
check "server: curl 3MB echo keeps Content-Length and Content-Type" "$(curl -s -m$T -D $S/fds.hdr -H 'Content-Type: application/x-test' --data-binary @$S/up.bin http://127.0.0.1:9200/echo | sha256sum | cut -d' ' -f1) $(tr -d '\r' < $S/fds.hdr | grep -ciE '^content-length: 3000000|^content-type: application/x-test')" "${UPSUM#* } 2"
check "server: unregistered path -> 404 Resource not found" "$(curl -s -m$T -w ' %{http_code}' http://127.0.0.1:9200/other)" "Resource not found 404"
check "server: listener logs request, response, keep-alive completion" "$(grep -c ' POST /echo HTTP/1.1$' $S/fds.log) $(grep -c ' HTTP/1.1 200$' $S/fds.log) $(grep -c 'exchange completed (connection kept alive)' $S/fds.log) $(grep -c ' GET /other HTTP/1.1$' $S/fds.log) $(grep -c ' HTTP/1.1 404$' $S/fds.log)" "3 3 4 1 1"

# Client-side connections must all be gone. Pooled origin connections may
# stay open, but only healthy (ESTABLISHED): CLOSE_WAIT would mean the pool
# failed to notice the origin closing an idle connection.
down_open=0; up_bad=0; up_idle=0
for P in $(pgrep -f "reverse_proxy .* 808[0-9]( --quiet)?$"); do
  for ino in $(ls -l /proc/$P/fd 2>/dev/null | grep -o 'socket:\[[0-9]*\]' | grep -o '[0-9]*'); do
    line=$(awk -v i=$ino '$10==i {print $2, $4}' /proc/net/tcp); [ -z "$line" ] && continue
    lport=$((16#${line:9:4})); st=${line##* }
    [ "$st" == "0A" ] && continue
    if [ $lport -ge 8080 ] && [ $lport -le 8089 ]; then down_open=$((down_open+1))
    elif [ "$st" == "01" ]; then up_idle=$((up_idle+1)); else up_bad=$((up_bad+1)); fi
  done
done
check "no client connections left open" "$down_open" "0"
check "pooled origin connections all healthy ($up_idle idle)" "$up_bad" "0"
# "ODDITY, resuming when not paused" is excluded: with ziebarthw/libevhtp's
# msgparser it is logged whenever a chunk-size or header line is split
# across reads (the parser waits for the rest; libevhtp's read callback
# takes that for pipelining and tries to resume an unpaused connection,
# which is a no-op). Harmless there; stalls would fail the checks above.
check "no libevhtp errors" "$(cat $S/p8080.err $S/p8082.err $S/p8083.err $S/p8085.err $S/p8086.err $S/p8087.err $S/echo.err $S/echo_tls.err $S/fds.err $S/fds_tls.err | grep -v 'ODDITY, resuming when not paused' | grep -c 'ERROR\|shutting down')" "0"
echo "passed=$pass failed=$fail"
