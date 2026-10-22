import hashlib, sys, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BIG = bytes(range(256)) * (5 * 1024 * 1024 // 256)  # 5 MiB

class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    connections = 0                 # TCP connections accepted so far
    def log_message(self, *a): pass

    def setup(self):
        H.connections += 1
        super().setup()

    def body(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            out = b""
            while True:
                n = int(self.rfile.readline().strip(), 16)
                if n == 0:
                    self.rfile.readline(); return out
                out += self.rfile.read(n); self.rfile.readline()
        n = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(n)

    def send(self, code, data, ctype="text/plain", extra=()):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        for k, v in extra: self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD": self.wfile.write(data)

    def do_GET(self):
        p = self.path
        if p.startswith("/big"):
            return self.send(200, BIG, "application/octet-stream")
        if p.startswith("/chunked"):
            self.send_response(200)
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for i in range(5):
                c = f"chunk {i}\n".encode()
                self.wfile.write(b"%x\r\n%s\r\n" % (len(c), c)); self.wfile.flush()
                time.sleep(0.05)
            self.wfile.write(b"0\r\n\r\n"); return
        if p.startswith("/close"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(b"read until close\n" * 1000)
            self.close_connection = True; return
        if p.startswith("/nocontent"):
            self.send_response(204); self.end_headers(); return
        if p.startswith("/die"):
            self.send_response(200)
            self.send_header("Content-Length", "100000")
            self.end_headers()
            self.wfile.write(b"x" * 1000); self.wfile.flush()
            self.connection.shutdown(2); self.close_connection = True; return
        if p.startswith("/early"):    # 103 Early Hints, then the response
            self.send_response_only(103, "Early Hints")
            self.send_header("Link", "</style.css>; rel=preload")
            self.end_headers()
            self.wfile.flush()
            return self.send(200, b"after hints\n")
        if p.startswith("/stats"):
            return self.send(200, str(H.connections).encode())
        if p.startswith("/ka"):     # short keep-alive advertised to clients
            return self.send(200, b"ka\n", extra=[("Keep-Alive", "timeout=1")])
        if p.startswith("/drop"):   # keep-alive response, then close anyway
            self.send(200, b"drop\n")
            self.close_connection = True
            return
        if p.startswith("/slow"):
            time.sleep(2)
        hdrs = "".join(f"{k}: {v}\n" for k, v in self.headers.items())
        self.send(200, f"{self.command} {self.path}\n{hdrs}".encode())

    do_HEAD = do_GET

    def do_POST(self):
        data = self.body()
        if self.path.startswith("/early"):
            return self.do_GET()
        self.send(200, f"{len(data)} {hashlib.sha256(data).hexdigest()}\n".encode())
    do_PUT = do_POST

ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
