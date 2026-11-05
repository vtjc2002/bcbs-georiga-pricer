"""Local-only CORS server for generated lookup outputs."""
import argparse
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class Handler(SimpleHTTPRequestHandler):
    gzip_response = False

    def translate_path(self, path):
        result = super().translate_path(path)
        self.gzip_response = result.endswith(".json") and Path(result + ".gz").is_file()
        return result + ".gz" if self.gzip_response else result

    def guess_type(self, path):
        return "application/json" if path.endswith(".json.gz") else super().guess_type(path)

    def end_headers(self):
        if self.gzip_response:
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Access-Control-Allow-Origin", "http://127.0.0.1:5173")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", required=True)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    from functools import partial
    ThreadingHTTPServer(("127.0.0.1", args.port), partial(Handler, directory=args.directory)).serve_forever()
