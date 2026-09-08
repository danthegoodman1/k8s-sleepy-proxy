from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os


class Handler(BaseHTTPRequestHandler):
    server_version = "SleepypodsRoutingE2E/1.0"

    def do_GET(self):
        instance = os.environ.get("SLEEPYPODS_E2E_INSTANCE", "unknown")
        body = (
            "sleepypods-routing-app\n"
            f"instance={instance}\n"
            f"path={self.path}\n"
            f"host={self.headers.get('Host', '')}\n"
        ).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print("%s - %s" % (self.address_string(), fmt % args), flush=True)


def main():
    port = int(os.environ.get("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
