#!/usr/bin/env python3
"""Thin HTTP→IRC bridge for reactor.

Listens on HTTP, relays messages to ergochat via IRC protocol.
Maintains a persistent IRC connection and rejoins on disconnect.

Usage:
    python3 irc_bridge.py [--irc-host 127.0.0.1] [--irc-port 6667]
                          [--http-port 8076] [--nick reactor-bridge]
"""

import argparse
import json
import logging
import socket
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

log = logging.getLogger("irc-bridge")

# ── IRC Client ──────────────────────────────────────────────────────

class IRCClient:
    def __init__(self, host: str, port: int, nick: str, channels: list[str]):
        self.host = host
        self.port = port
        self.nick = nick
        self.channels = channels
        self._sock: socket.socket | None = None
        self._lock = threading.Lock()
        self._connected = False

    def connect(self):
        with self._lock:
            try:
                self._sock = socket.create_connection((self.host, self.port), timeout=10)
                self._sock.settimeout(300)
                self._send_raw(f"NICK {self.nick}")
                self._send_raw(f"USER {self.nick} 0 * :Reactor IRC Bridge")
                # Read until registered
                self._read_until_registered()
                for ch in self.channels:
                    self._send_raw(f"JOIN {ch}")
                self._connected = True
                log.info("Connected to IRC %s:%d as %s, joined %s",
                         self.host, self.port, self.nick, self.channels)
            except Exception as e:
                log.error("IRC connect failed: %s", e)
                self._connected = False
                raise

    def _send_raw(self, line: str):
        if self._sock:
            self._sock.sendall((line + "\r\n").encode("utf-8"))

    def _read_until_registered(self):
        buf = b""
        while True:
            data = self._sock.recv(4096)
            if not data:
                raise ConnectionError("IRC server closed connection during registration")
            buf += data
            lines = buf.split(b"\r\n")
            buf = lines.pop()
            for line in lines:
                text = line.decode("utf-8", errors="replace")
                if text.startswith("PING"):
                    pong = text.replace("PING", "PONG", 1)
                    self._send_raw(pong)
                # 001 = RPL_WELCOME
                if " 001 " in text:
                    return

    def send_message(self, channel: str, message: str):
        with self._lock:
            if not self._connected:
                self.connect()
            try:
                for line in message.split("\n"):
                    line = line.strip()
                    if line:
                        self._send_raw(f"PRIVMSG {channel} :{line}")
            except Exception as e:
                log.error("Send failed, reconnecting: %s", e)
                self._connected = False
                self.connect()
                for line in message.split("\n"):
                    line = line.strip()
                    if line:
                        self._send_raw(f"PRIVMSG {channel} :{line}")

    @property
    def is_connected(self) -> bool:
        return self._connected


# ── HTTP Handler ────────────────────────────────────────────────────

class BridgeHandler(BaseHTTPRequestHandler):
    irc_client: IRCClient = None  # set by main

    def do_POST(self):
        if self.path == "/send":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body)
                channel = data.get("channel", "#crew")
                message = data.get("message", "")
                if not channel.startswith("#"):
                    channel = f"#{channel}"
                self.server.irc_client.send_message(channel, message)
                self._respond(200, {"status": "sent", "channel": channel})
            except Exception as e:
                log.error("POST /send error: %s", e)
                self._respond(500, {"error": str(e)})
        else:
            self._respond(404, {"error": "not found"})

    def do_GET(self):
        if self.path == "/health":
            connected = self.server.irc_client.is_connected
            self._respond(200, {"status": "ok", "irc_connected": connected})
        else:
            self._respond(404, {"error": "not found"})

    def _respond(self, code: int, body: dict):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def log_message(self, format, *args):
        log.debug(format, *args)


# ── Main ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="HTTP→IRC bridge for reactor")
    parser.add_argument("--irc-host", default="127.0.0.1")
    parser.add_argument("--irc-port", type=int, default=6667)
    parser.add_argument("--http-port", type=int, default=8076)
    parser.add_argument("--nick", default="reactor-bridge")
    parser.add_argument("--channels", default="#crew,#bridge,#alerts")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    channels = [c.strip() for c in args.channels.split(",")]
    irc = IRCClient(args.irc_host, args.irc_port, args.nick, channels)

    # Connect to IRC with retry
    for attempt in range(5):
        try:
            irc.connect()
            break
        except Exception as e:
            log.warning("IRC connect attempt %d failed: %s", attempt + 1, e)
            time.sleep(2 ** attempt)
    else:
        log.error("Could not connect to IRC after 5 attempts")
        raise SystemExit(1)

    # Start HTTP server
    server = HTTPServer(("127.0.0.1", args.http_port), BridgeHandler)
    server.irc_client = irc
    log.info("IRC bridge HTTP server listening on 127.0.0.1:%d", args.http_port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down")
        server.shutdown()


if __name__ == "__main__":
    main()
