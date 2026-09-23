#!/usr/bin/env python3
"""Mock HTTP service for HTTP_TASK demos and tests (prompt 05, spec section 7.2).

Endpoints (all GET):
  /delay?ms=N    sleep N ms, then 200 with a fixed body (b"predisched-ok").
  /fail?rate=R   500 with probability R, else 200. rate=0.0 always succeeds,
                 rate=1.0 always fails; pass seed= for a reproducible draw.
  /healthz       200 "ok", for readiness checks.

Only stdlib (http.server), so there is nothing to install:

  python scripts/mock-http.py --port 8100
"""

import argparse
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

BODY = b"predisched-ok"


class Handler(BaseHTTPRequestHandler):
    server_version = "MockPrediSched/1"

    def _send(self, code, body):
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        if parsed.path == "/healthz":
            self._send(200, b"ok")
        elif parsed.path == "/delay":
            try:
                ms = int(query.get("ms", ["0"])[0])
            except ValueError:
                self._send(400, b"ms must be an integer")
                return
            if ms < 0 or ms > 120_000:
                self._send(400, b"ms must be between 0 and 120000")
                return
            time.sleep(ms / 1000.0)
            self._send(200, BODY)
        elif parsed.path == "/fail":
            try:
                rate = float(query.get("rate", ["0"])[0])
            except ValueError:
                self._send(400, b"rate must be a number")
                return
            if not 0.0 <= rate <= 1.0:
                self._send(400, b"rate must be between 0 and 1")
                return
            rng = random.Random(query["seed"][0]) if "seed" in query else random
            if rng.random() < rate:
                self._send(500, b"injected failure")
            else:
                self._send(200, BODY)
        else:
            self._send(404, b"unknown path: use /delay, /fail or /healthz")

    def log_message(self, fmt, *args):  # keep demo output clean
        pass


def main():
    parser = argparse.ArgumentParser(description="Mock HTTP service for HTTP_TASK.")
    parser.add_argument("--port", type=int, default=8100)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"mock-http listening on {args.host}:{args.port} "
          f"(/delay?ms=, /fail?rate=, /healthz)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
