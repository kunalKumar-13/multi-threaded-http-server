#!/usr/bin/env python3
"""
Robust multi-threaded HTTP server.
- Serves files from ./resources
- Accepts JSON POST uploads and stores them under ./resources/uploads
- Supports persistent connections (Keep-Alive) and simple queuing when the thread pool is saturated
Notes:
- Uses RFC-1123 formatted Date header produced from a timezone-aware datetime (no utcnow()).
- Does not implement chunked transfer encoding.
"""

import argparse
import socket
import threading
import os
import json
import datetime
import random
import string
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote
import mimetypes
import signal
import sys

# ---------------------------
# Configuration
# ---------------------------
BASE_DIR = os.getcwd()
RESOURCE_DIR = os.path.join(BASE_DIR, "resources")
UPLOAD_DIR = os.path.join(RESOURCE_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

MAX_PERSISTENT_REQUESTS = 100
KEEP_ALIVE_TIMEOUT = 30  # seconds (socket timeout for idle connections)
RECV_CHUNK = 8192  # bytes to recv at a time

# ---------------------------
# Utilities
# ---------------------------

def ts_now_local():
    """
    Return a human-readable timestamp (local time) for logs.
    Uses timezone-aware datetime (local time). Avoids utcnow().
    """
    return datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def rfc1123_date(dt=None):
    """
    Return a RFC-1123 formatted date string (e.g. 'Tue, 15 Nov 1994 08:12:31 GMT').
    Uses timezone-aware datetime and converts to UTC for the header text 'GMT'.
    """
    if dt is None:
        dt = datetime.datetime.now(datetime.timezone.utc)
    else:
        dt = dt.astimezone(datetime.timezone.utc)
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")


def random_id(n=6):
    """Return a random alphanumeric id of length n."""
    alphabet = string.ascii_lowercase + string.digits
    return ''.join(random.choices(alphabet, k=n))


def safe_join(base, path):
    """
    Safely join base and user-supplied path and prevent directory traversal.
    Returns absolute path if it is inside base; otherwise returns None.
    """
    # Normalize URL-encoded path, strip leading slashes
    path = unquote(path).lstrip("/")
    candidate = os.path.abspath(os.path.join(base, path))
    base_abs = os.path.abspath(base)
    return candidate if candidate.startswith(base_abs + os.sep) or candidate == base_abs else None


def guess_mime(filepath):
    """
    Return a MIME type for a file. Falls back to 'application/octet-stream'.
    """
    mtype, _ = mimetypes.guess_type(filepath)
    return mtype or "application/octet-stream"


def log(msg):
    """Thread-aware log helper."""
    tname = threading.current_thread().name
    print(f"{ts_now_local()} [{tname}] {msg}")


# ---------------------------
# HTTP Response Construction
# ---------------------------

def make_response(status_code: int, reason: str, headers: dict = None, body: bytes = b"") -> bytes:
    """
    Build a complete HTTP/1.1 response bytes object from components.
    Ensures Date header is present and Content-Length is correct.
    """
    headers = dict(headers or {})
    headers.setdefault("Date", rfc1123_date())
    headers.setdefault("Server", "MiniPyHTTP/1.0")
    headers["Content-Length"] = str(len(body))

    status_line = f"HTTP/1.1 {status_code} {reason}"
    header_lines = [f"{k}: {v}" for k, v in headers.items()]
    raw = ("\r\n".join([status_line] + header_lines + ["", ""])).encode("utf-8") + body
    return raw


# ---------------------------
# File handling utilities
# ---------------------------

def read_resource(url_path):
    """
    Read a path under RESOURCE_DIR. Returns tuple (status_code, reason, headers, body).
    Handles index.html for '/', validates path doesn't escape RESOURCE_DIR.
    """
    if url_path == "/":
        url_path = "/index.html"

    filepath = safe_join(RESOURCE_DIR, url_path)
    if filepath is None:
        return 403, "Forbidden", {"Connection": "close"}, b""

    if not os.path.exists(filepath) or not os.path.isfile(filepath):
        return 404, "Not Found", {"Connection": "close"}, b""

    # Determine MIME type
    mime = guess_mime(filepath)
    # For HTML files, serve inline. For others (images, txt) keep as octet-stream by original behavior.
    _, ext = os.path.splitext(filepath)
    ext = ext.lower()
    if ext == ".html":
        headers = {"Content-Type": f"{mime}; charset=utf-8"}
    else:
        # force download-like disposition for non-html (matching original behavior)
        headers = {
            "Content-Type": mime,
            "Content-Disposition": f'attachment; filename="{os.path.basename(filepath)}"'
        }

    with open(filepath, "rb") as f:
        body = f.read()
    return 200, "OK", headers, body


def save_json_upload(json_obj):
    """
    Persist a JSON object to UPLOAD_DIR and return a relative path for the saved file.
    """
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"upload_{ts}_{random_id(6)}.json"
    fpath = os.path.join(UPLOAD_DIR, fname)
    with open(fpath, "w", encoding="utf-8") as f:
        json.dump(json_obj, f, ensure_ascii=False, indent=2)
    # Return web-accessible path relative to resource root
    return f"/uploads/{fname}"


# ---------------------------
# Request parsing (buffered)
# ---------------------------

def parse_header_block(header_bytes):
    """
    Parse request-line and headers from header_bytes (bytes up to \r\n\r\n).
    Returns (method, path, version, headers_dict).
    """
    text = header_bytes.decode("utf-8", errors="ignore")
    lines = text.split("\r\n")
    request_line = lines[0].strip()
    parts = request_line.split()
    if len(parts) != 3:
        return None, None, None, {}
    method, path, version = parts
    headers = {}
    for ln in lines[1:]:
        if not ln:
            continue
        if ": " in ln:
            k, v = ln.split(": ", 1)
            headers[k.strip()] = v.strip()
    return method, path, version, headers


# ---------------------------
# Connection handler class
# ---------------------------

class ConnectionHandler:
    """
    Handles a single client socket connection with request buffering.
    Allows multiple requests per connection (keep-alive) while respecting
    Content-Length and closing when requested.
    """
    def __init__(self, conn: socket.socket, addr, server_host, server_port):
        self.conn = conn
        self.addr = addr
        self.server_host = server_host
        self.server_port = server_port
        self.buffer = b""
        self.conn.settimeout(KEEP_ALIVE_TIMEOUT)
        self.requests_served = 0

    def validate_host(self, host_header: str) -> bool:
        """
        Validate Host header. Accept either 'host' or 'host:port'.
        """
        if not host_header:
            return False
        # Accept "127.0.0.1" or "127.0.0.1:8080"
        if host_header == f"{self.server_host}:{self.server_port}" or host_header == self.server_host:
            return True
        return False

    def read_more(self):
        """
        Try to recv more data into the buffer. Returns False if connection has been closed.
        """
        try:
            data = self.conn.recv(RECV_CHUNK)
            if not data:
                return False
            self.buffer += data
            return True
        except socket.timeout:
            return False
        except OSError:
            return False

    def handle_single_request(self, method, path, version, headers, body_bytes):
        """
        Process a fully-received request and send a response.
        Returns True if the connection should remain open for more requests.
        """
        # Validate Host header
        host_header = headers.get("Host", "")
        if not self.validate_host(host_header):
            log(f"Host validation failed ({host_header}) from {self.addr}")
            resp = make_response(403, "Forbidden", {"Connection": "close"})
            self.conn.sendall(resp)
            return False

        # Decide keep-alive: HTTP/1.1 defaults to keep-alive unless Connection: close
        conn_hdr = headers.get("Connection", "").lower()
        keep_alive = (version == "HTTP/1.1" and conn_hdr != "close") or (version == "HTTP/1.0" and conn_hdr == "keep-alive")

        if method.upper() == "GET":
            status, reason, h, body = read_resource(path)
            # Add keep-alive headers if staying alive
            if keep_alive:
                h["Connection"] = "keep-alive"
                h["Keep-Alive"] = f"timeout={KEEP_ALIVE_TIMEOUT}, max={MAX_PERSISTENT_REQUESTS}"
            else:
                h["Connection"] = "close"
            response = make_response(status, reason, h, body)
            self.conn.sendall(response)
            log(f"GET {path} -> {status} {reason} ({len(body)} bytes)")

        elif method.upper() == "POST":
            # Only accept application/json for POST in this server
            if headers.get("Content-Type", "").split(";")[0].lower() != "application/json":
                response = make_response(415, "Unsupported Media Type", {"Connection": "close"}, b"")
                self.conn.sendall(response)
                log(f"Rejected POST {path}: unsupported content-type")
                return False

            # Attempt to decode body as UTF-8 JSON text
            try:
                text = body_bytes.decode("utf-8")
                payload = json.loads(text)
            except Exception:
                response = make_response(400, "Bad Request", {"Connection": "close"}, b"")
                self.conn.sendall(response)
                log(f"Bad JSON POST from {self.addr}")
                return False

            # Save and reply
            try:
                saved_rel = save_json_upload(payload)
                resp_body = json.dumps({"status": "success", "message": "File created successfully", "filepath": saved_rel}).encode("utf-8")
                headers_out = {"Content-Type": "application/json; charset=utf-8"}
                if keep_alive:
                    headers_out["Connection"] = "keep-alive"
                    headers_out["Keep-Alive"] = f"timeout={KEEP_ALIVE_TIMEOUT}, max={MAX_PERSISTENT_REQUESTS}"
                else:
                    headers_out["Connection"] = "close"
                response = make_response(201, "Created", headers_out, resp_body)
                self.conn.sendall(response)
                log(f"POST saved -> {saved_rel}")
            except Exception as e:
                log(f"Error saving upload: {e}")
                response = make_response(500, "Internal Server Error", {"Connection": "close"}, b"")
                self.conn.sendall(response)
                return False
        else:
            # Method not allowed
            self.conn.sendall(make_response(405, "Method Not Allowed", {"Connection": "close"}, b""))
            log(f"Method not allowed: {method}")
            return False

        self.requests_served += 1
        # Close if we've hit persistent request limit
        if self.requests_served >= MAX_PERSISTENT_REQUESTS:
            return False
        return keep_alive

    def serve(self):
        """
        Main loop for the connection: buffer incoming bytes, split requests at \r\n\r\n,
        ensure full body is present per Content-Length before dispatching to handler.
        """
        log(f"Connection from {self.addr} started")
        alive = True
        try:
            while alive:
                # Ensure we have at least the headers
                if b"\r\n\r\n" not in self.buffer:
                    if not self.read_more():
                        break

                # Find header boundary
                idx = self.buffer.find(b"\r\n\r\n")
                if idx < 0:
                    # Not a complete headers block
                    if not self.read_more():
                        break
                    continue

                header_block = self.buffer[:idx]
                method, path, version, headers = parse_header_block(header_block)
                if method is None:
                    # Malformed request line
                    self.conn.sendall(make_response(400, "Bad Request", {"Connection": "close"}, b""))
                    break

                # Determine content length
                content_length = int(headers.get("Content-Length", "0"))
                total_len = idx + 4 + content_length
                # If body not fully received yet, try to read more
                while len(self.buffer) < total_len:
                    if not self.read_more():
                        break
                if len(self.buffer) < total_len:
                    # Connection closed before body complete
                    break

                body_bytes = self.buffer[idx + 4: total_len]
                # Advance buffer past this request
                self.buffer = self.buffer[total_len:]

                # Handle the request
                keep_open = self.handle_single_request(method, path, version, headers, body_bytes)
                if not keep_open:
                    break
                # otherwise continue to next request (if buffer already contains next one, loop continues)
        finally:
            try:
                self.conn.close()
            except Exception:
                pass
            log(f"Connection from {self.addr} closed")


# ---------------------------
# Server loop
# ---------------------------

class ThreadedHTTPServer:
    """
    Accept incoming connections and hand them to a thread pool.
    When pool is saturated connections are queued and dequeued as threads free up.
    """
    def __init__(self, host="127.0.0.1", port=8080, max_workers=10):
        self.host = host
        self.port = port
        self.max_workers = max_workers
        self._running = False
        self._server_socket = None
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._queue = []
        self._lock = threading.Lock()
        self._active = 0

    def _handle_client_wrapper(self, conn, addr):
        """
        Wrapper that maintains active count and dequeues queued connections if any.
        """
        with self._lock:
            self._active += 1
            log(f"Active workers: {self._active}/{self.max_workers}")

        try:
            handler = ConnectionHandler(conn, addr, self.host, self.port)
            handler.serve()
        finally:
            with self._lock:
                self._active -= 1
                log(f"Active workers: {self._active}/{self.max_workers}")
                # If there are queued connections, schedule the next one
                if self._queue:
                    queued_conn, queued_addr = self._queue.pop(0)
                    log(f"Dequeued connection {queued_addr} -> scheduling")
                    self._executor.submit(self._handle_client_wrapper, queued_conn, queued_addr)

    def start(self):
        """Create listening socket and enter accept loop until interrupted."""
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(100)
        self._server_socket.settimeout(1.0)
        self._running = True

        log(f"Server listening on http://{self.host}:{self.port} (workers={self.max_workers})")
        try:
            while self._running:
                try:
                    conn, addr = self._server_socket.accept()
                except socket.timeout:
                    continue
                with self._lock:
                    # If active workers at capacity, queue connection
                    if self._active >= self.max_workers:
                        log(f"Thread pool saturated, queuing connection from {addr}")
                        self._queue.append((conn, addr))
                        continue
                # Otherwise schedule immediately
                self._executor.submit(self._handle_client_wrapper, conn, addr)
        except KeyboardInterrupt:
            log("Keyboard interrupt received, shutting down")
        finally:
            self.stop()

    def stop(self):
        """Shutdown server and threadpool."""
        self._running = False
        try:
            if self._server_socket:
                self._server_socket.close()
        except Exception:
            pass
        self._executor.shutdown(wait=True)
        log("Server shutdown complete")


# ---------------------------
# CLI and bootstrap
# ---------------------------

def parse_args():
    """Parse command line arguments for the server host, port, and worker count."""
    p = argparse.ArgumentParser(description="Simple threaded HTTP server")
    p.add_argument("--host", type=str, default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--max-threads", type=int, default=10)
    return p.parse_args()


def main():
    """Main entrypoint: parse args, start server, and handle graceful exit."""
    args = parse_args()
    server = ThreadedHTTPServer(host=args.host, port=args.port, max_workers=args.max_threads)

    # Trap SIGINT for nicer shutdown (on Unix)
    def on_sigint(signum, frame):
        log("SIGINT received, stopping server...")
        server.stop()
        sys.exit(0)

    try:
        signal.signal(signal.SIGINT, on_sigint)
    except Exception:
        # signal might not be available on some platforms (e.g. Windows in certain contexts)
        pass

    server.start()


if __name__ == "__main__":
    main()
