from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote


DATA_DIR = Path("/data")
MARKER_FILE = DATA_DIR / "sleepypods-marker"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/write/"):
            marker = unquote(self.path.removeprefix("/write/"))
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            MARKER_FILE.write_text(marker, encoding="utf-8")
            self.respond(200, f"wrote:{marker}\n")
            return

        if self.path.startswith("/read/"):
            marker = unquote(self.path.removeprefix("/read/"))
            if not MARKER_FILE.exists():
                self.respond(404, "missing marker\n")
                return
            actual = MARKER_FILE.read_text(encoding="utf-8")
            if actual != marker:
                self.respond(409, f"expected:{marker}\nactual:{actual}\n")
                return
            self.respond(200, f"read:{actual}\n")
            return

        self.respond(200, "sleepypods-stateful-app\n")

    def log_message(self, format, *args):
        return

    def respond(self, status, body):
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
