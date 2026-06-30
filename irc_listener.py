#!/usr/bin/env python3
"""IRC inbound listener — translates Overseer IRC messages to gt mail.

Joins ergochat as a bot, listens for messages from the Overseer,
and translates them to gt mail send / bd / gt commands.

Channel mapping (D4):
  #m          → quarterdeck/crew/m
  #moneypenny → quarterdeck/crew/moneypenny
  #bond       → quarterdeck/crew/bond
  #q          → quarterdeck/crew/q
  #r          → quarterdeck/crew/r
  #felix      → quarterdeck/crew/felix
  #crew       → broadcast (gt mail send to all)
  #bridge     → command channel (!commands) or mail to last-addressed agent
  #alerts     → read-only (ignored)

Usage:
    python3 irc_listener.py [--host 127.0.0.1] [--port 6667] [--nick gt-listener]
"""

import argparse
import logging
import os
import re
import shlex
import socket
import subprocess
import threading
import time

log = logging.getLogger("irc-listener")

# ── Channel → Agent Mapping ────────────────────────────────────────

CHANNEL_TO_AGENT = {
    "#m": "quarterdeck/crew/m",
    "#moneypenny": "quarterdeck/crew/moneypenny",
    "#bond": "quarterdeck/crew/bond",
    "#q": "quarterdeck/crew/q",
    "#r": "quarterdeck/crew/r",
    "#felix": "quarterdeck/crew/felix",
}

BROADCAST_CHANNELS = {"#crew"}
READ_ONLY_CHANNELS = {"#alerts"}
COMMAND_CHANNEL = "#bridge"

ALL_CHANNELS = list(CHANNEL_TO_AGENT.keys()) + [COMMAND_CHANNEL, "#crew", "#alerts"]

# Allowlisted commands for #bridge
ALLOWED_COMMANDS = {
    "mail":   lambda args: f"gt mail {args}",
    "bd":     lambda args: f"bd {args}",
    "nudge":  lambda args: f"gt nudge {args}",
    "status": lambda _: "gt hook 2>&1; echo '---'; bd list --status=in_progress 2>&1",
    "hook":   lambda args: f"gt hook {args}",
    "ready":  lambda _: "bd ready 2>&1",
    "help":   lambda _: "echo '!mail inbox | !bd ready | !nudge <agent> <msg> | !status | !hook'",
}

# ── IRC Client ──────────────────────────────────────────────────────

class IRCListener:
    def __init__(self, host: str, port: int, nick: str):
        self.host = host
        self.port = port
        self.nick = nick
        self._sock: socket.socket | None = None
        self._last_addressed: str = "quarterdeck/crew/moneypenny"  # default target — admin handles comms routing
        self._running = True

    def connect(self):
        self._sock = socket.create_connection((self.host, self.port), timeout=10)
        self._sock.settimeout(300)
        self._send(f"NICK {self.nick}")
        self._send(f"USER {self.nick} 0 * :Gas Town IRC Listener")
        self._read_until_registered()
        for ch in ALL_CHANNELS:
            self._send(f"JOIN {ch}")
        log.info("Connected to %s:%d as %s, joined %s", self.host, self.port, self.nick, ALL_CHANNELS)

    def _send(self, line: str):
        if self._sock:
            self._sock.sendall((line + "\r\n").encode("utf-8"))

    def _read_until_registered(self):
        buf = b""
        while True:
            data = self._sock.recv(4096)
            if not data:
                raise ConnectionError("Server closed during registration")
            buf += data
            lines = buf.split(b"\r\n")
            buf = lines.pop()
            for line in lines:
                text = line.decode("utf-8", errors="replace")
                if text.startswith("PING"):
                    self._send(text.replace("PING", "PONG", 1))
                if " 001 " in text:
                    return

    def _send_to_channel(self, channel: str, message: str):
        for line in message.split("\n"):
            line = line.strip()
            if line:
                # IRC max line ~512 bytes, truncate long lines
                if len(line) > 400:
                    line = line[:397] + "..."
                self._send(f"PRIVMSG {channel} :{line}")

    def run(self):
        buf = b""
        while self._running:
            try:
                data = self._sock.recv(4096)
                if not data:
                    raise ConnectionError("Server closed connection")
                buf += data
                lines = buf.split(b"\r\n")
                buf = lines.pop()
                for line in lines:
                    self._handle_line(line.decode("utf-8", errors="replace"))
            except socket.timeout:
                # Send a ping to keep alive
                self._send("PING :keepalive")
            except (ConnectionError, OSError) as e:
                log.warning("Disconnected: %s — reconnecting in 5s", e)
                time.sleep(5)
                try:
                    self.connect()
                    buf = b""
                except Exception as e2:
                    log.error("Reconnect failed: %s", e2)

    def _handle_line(self, line: str):
        if line.startswith("PING"):
            self._send(line.replace("PING", "PONG", 1))
            return

        # Parse PRIVMSG: :nick!user@host PRIVMSG #channel :message
        match = re.match(r'^:(\S+?)!\S+ PRIVMSG (\S+) :(.+)$', line)
        if not match:
            return

        sender_nick = match.group(1)
        target = match.group(2)
        message = match.group(3).strip()

        # Ignore messages from bots (ourselves and reactor-bridge)
        if sender_nick in (self.nick, "reactor-bridge"):
            return

        log.info("[%s] <%s> %s", target, sender_nick, message)

        if target in READ_ONLY_CHANNELS:
            return

        if target == COMMAND_CHANNEL:
            self._handle_bridge_message(sender_nick, message)
        elif target in CHANNEL_TO_AGENT:
            self._handle_agent_channel(target, sender_nick, message)
        elif target in BROADCAST_CHANNELS:
            self._handle_broadcast(target, sender_nick, message)

    def _handle_bridge_message(self, sender: str, message: str):
        """Handle messages in #bridge — commands or mail to last-addressed agent."""
        if message.startswith("!"):
            self._handle_command(sender, message[1:])
        else:
            # Plain text → mail to last-addressed agent
            agent = self._last_addressed
            log.info("Bridge message → %s: %s", agent, message)
            self._exec_gt_mail(agent, f"From {sender} via IRC", message)
            self._send_to_channel(COMMAND_CHANNEL, f"→ Sent to {agent}")

    def _handle_command(self, sender: str, raw: str):
        """Parse and execute !commands in #bridge."""
        parts = raw.split(None, 1)
        cmd = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ""

        builder = ALLOWED_COMMANDS.get(cmd)
        if not builder:
            self._send_to_channel(COMMAND_CHANNEL, f"Unknown command: !{cmd} — try !help")
            return

        shell_cmd = builder(args)
        log.info("Command from %s: !%s → %s", sender, cmd, shell_cmd)

        # Execute in a thread to avoid blocking IRC
        threading.Thread(
            target=self._run_command,
            args=(shell_cmd,),
            daemon=True,
        ).start()

    def _run_command(self, shell_cmd: str):
        """Run a shell command and post output to #bridge."""
        try:
            env = os.environ.copy()
            env["GT_ROLE"] = "quarterdeck/crew/m"  # commands run as overseer context
            result = subprocess.run(
                shell_cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30,
                cwd="/home/ubuntu/workspace/quarterdeck",
                env=env,
            )
            output = (result.stdout + result.stderr).strip()
            if not output:
                output = "(no output)"
            # Limit output to 10 lines for IRC
            lines = output.split("\n")
            if len(lines) > 10:
                lines = lines[:10] + [f"... ({len(lines) - 10} more lines)"]
            for line in lines:
                self._send_to_channel(COMMAND_CHANNEL, line)
        except subprocess.TimeoutExpired:
            self._send_to_channel(COMMAND_CHANNEL, "⏱ Command timed out (30s)")
        except Exception as e:
            self._send_to_channel(COMMAND_CHANNEL, f"Error: {e}")

    def _handle_agent_channel(self, channel: str, sender: str, message: str):
        """Message in #agent channel → gt mail send to that agent."""
        agent = CHANNEL_TO_AGENT[channel]
        self._last_addressed = agent
        self._exec_gt_mail(agent, f"From {sender} via IRC", message)

    def _handle_broadcast(self, channel: str, sender: str, message: str):
        """Message in #crew → gt mail send to all agents."""
        for agent in CHANNEL_TO_AGENT.values():
            self._exec_gt_mail(agent, f"[crew] From {sender} via IRC", message)

    def _exec_gt_mail(self, recipient: str, subject: str, body: str):
        """Send a gt mail message."""
        try:
            result = subprocess.run(
                ["gt", "mail", "send", recipient, "-s", subject, "--stdin"],
                input=body,
                capture_output=True,
                text=True,
                timeout=15,
                cwd="/home/ubuntu/workspace/quarterdeck",
            )
            if result.returncode != 0:
                log.error("gt mail send failed: %s", result.stderr.strip())
            else:
                log.info("Mail sent to %s: %s", recipient, subject)
        except Exception as e:
            log.error("gt mail send error: %s", e)


# ── Main ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="IRC → gt mail listener")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6667)
    parser.add_argument("--nick", default="gt-listener")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    listener = IRCListener(args.host, args.port, args.nick)

    for attempt in range(5):
        try:
            listener.connect()
            break
        except Exception as e:
            log.warning("Connect attempt %d failed: %s", attempt + 1, e)
            time.sleep(2 ** attempt)
    else:
        log.error("Could not connect after 5 attempts")
        raise SystemExit(1)

    listener.run()


if __name__ == "__main__":
    main()
