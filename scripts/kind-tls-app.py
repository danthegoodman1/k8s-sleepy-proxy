from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
import ssl


class Handler(BaseHTTPRequestHandler):
    server_version = "SleepypodsTlsE2E/1.0"

    def do_GET(self):
        instance = os.environ.get("SLEEPYPODS_E2E_INSTANCE", "unknown")
        mode = os.environ.get("SLEEPYPODS_E2E_TLS_APP_MODE", "http")
        body = (
            "sleepypods-tls-app\n"
            f"instance={instance}\n"
            f"mode={mode}\n"
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
    mode = os.environ.get("SLEEPYPODS_E2E_TLS_APP_MODE", "http")
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    if mode == "tls":
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain("/app/tls/cert.pem", "/app/tls/key.pem")
        server.socket = context.wrap_socket(server.socket, server_side=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
