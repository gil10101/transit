"""Dev server for site/: plain http.server plus Cache-Control: no-store.

Without the header, browsers heuristically cache index.html and styles.css and
keep showing a stale page across plain reloads — this cost two debugging rounds
on 2026-08-28. Vercel serves proper ETags in production; this is local-only.

Usage: uv run python scripts/serve_site.py [port]  (default 8899)
"""

import sys
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

SITE = Path(__file__).resolve().parent.parent / "site"


class NoStoreHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, *args):
        pass  # quiet


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8899
    server = ThreadingHTTPServer(("", port), partial(NoStoreHandler, directory=str(SITE)))
    print(f"serving {SITE} on http://localhost:{port} (no-store)", flush=True)
    server.serve_forever()
