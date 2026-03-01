#!/usr/bin/env python3
"""
Reactor — Real-time event processing for Dolt databases.

Polls Dolt databases via dolt_log/dolt_diff for new commits and dispatches
configurable reactions when data changes. Designed for use with bd (beads)
which writes locally and pushes to the Dolt server.

Usage:
    reactor.py --config /path/to/config.json
    reactor.py --config /path/to/config.json --dry-run
"""

import argparse
import datetime
import json
import logging
import os
import random
import signal
import socket
import string
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None

try:
    from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.row_event import (
        DeleteRowsEvent,
        UpdateRowsEvent,
        WriteRowsEvent,
    )
except ImportError:
    BinLogStreamReader = None  # Optional — poll mode doesn't need it

try:
    import pymysql
except ImportError:
    print("ERROR: pymysql required. Install with: pip install pymysql", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# ULID generation (stdlib-only, no external dependency)
# ---------------------------------------------------------------------------

_ULID_ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def ulid() -> str:
    """Generate a ULID (Universally Unique Lexicographically Sortable Identifier)."""
    t = int(time.time() * 1000)
    # 10-char timestamp (48 bits)
    ts = ""
    for _ in range(10):
        ts = _ULID_ENCODING[t & 0x1F] + ts
        t >>= 5
    # 16-char randomness (80 bits)
    rand_bytes = os.urandom(10)
    r = int.from_bytes(rand_bytes, "big")
    rs = ""
    for _ in range(16):
        rs = _ULID_ENCODING[r & 0x1F] + rs
        r >>= 5
    return ts + rs


class DefaultDict(dict):
    """Dict subclass that returns '{key}' for missing keys.

    Used with str.format_map() so that unresolvable template placeholders
    are left as-is instead of raising KeyError.
    """
    def __missing__(self, key):
        return "{" + key + "}"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_CONFIG = {
    "reactor": {
        "dolt_host": "127.0.0.1",
        "dolt_port": 3306,
        "dolt_user": "root",
        "dolt_password": "",
        "event_db": "reactor",
        "event_table": "reactor_events",
        "state_table": "reactor_state",
        "timer_table": "reactor_timers",
        "health_port": 8075,
        "poll_interval": 5,
        "log_level": "INFO",
        "watched_databases": [],
    },
    "classify": [],
    "reactions": [],
}


def load_config(path: str) -> dict:
    """Load config file (JSON or TOML based on extension)."""
    with open(path, "rb") as f:
        if path.endswith(".json"):
            return json.load(f)
        elif tomllib:
            return tomllib.load(f)
        else:
            f.seek(0)
            return json.load(f)


# ---------------------------------------------------------------------------
# Schema mapper — resolves column indices to names for binlog events
# ---------------------------------------------------------------------------

class SchemaMapper:
    """Maps table column indices to column names by querying INFORMATION_SCHEMA."""

    def __init__(self, conn_settings: dict):
        self._conn_settings = conn_settings
        self._cache: dict[tuple[str, str], list[str]] = {}

    def get_columns(self, database: str, table: str) -> list[str]:
        """Return ordered list of column names for a table."""
        key = (database, table)
        if key not in self._cache:
            self._cache[key] = self._fetch_columns(database, table)
        return self._cache[key]

    def invalidate(self, database: str = None, table: str = None):
        """Clear cached schema. Call after DDL changes."""
        if database and table:
            self._cache.pop((database, table), None)
        else:
            self._cache.clear()

    def _fetch_columns(self, database: str, table: str) -> list[str]:
        conn = pymysql.connect(**self._conn_settings, database=database)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
                    "ORDER BY ORDINAL_POSITION",
                    (database, table),
                )
                return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Event classifier — turns raw binlog row changes into semantic events
# ---------------------------------------------------------------------------

class EventClassifier:
    """Classifies binlog row changes into semantic event types."""

    def __init__(self, rules: list[dict], schema: SchemaMapper):
        self._rules = self._index_rules(rules)
        self._schema = schema

    def _index_rules(self, rules: list[dict]) -> dict:
        """Index classification rules by (database, table)."""
        idx = {}
        for rule in rules:
            key = (rule["database"], rule["table"])
            idx[key] = rule
        return idx

    def classify_insert(self, database: str, table: str, row: dict) -> dict | None:
        """Classify an INSERT event."""
        rule = self._rules.get((database, table))
        if not rule:
            return None

        event_type = rule.get("insert")
        if not event_type:
            return None

        # Check for sub-classifications based on row data
        for sub in rule.get("sub_classify", []):
            if self._matches_condition(sub.get("condition", ""), row):
                event_type = sub["event_type"]
                break

        return {
            "event_type": event_type,
            "source_db": database,
            "source_table": table,
            "subject_id": self._extract_id(row, rule),
            "actor": row.get("actor", row.get("created_by", "")),
            "summary": self._build_summary(event_type, row, rule),
            "payload": row,
        }

    def classify_update(self, database: str, table: str, before: dict, after: dict) -> dict | None:
        """Classify an UPDATE event."""
        rule = self._rules.get((database, table))
        if not rule:
            return None

        event_type = rule.get("update", "")
        if not event_type:
            return None

        # Detect specific field changes for more precise classification
        for field_rule in rule.get("update_fields", []):
            field = field_rule["field"]
            old_val = before.get(field)
            new_val = after.get(field)
            if old_val != new_val:
                if "transitions" in field_rule:
                    for trans in field_rule["transitions"]:
                        if self._matches_transition(trans, old_val, new_val):
                            event_type = trans["event_type"]
                            break
                    else:
                        if "default_event" in field_rule:
                            event_type = field_rule["default_event"]
                elif "event_type" in field_rule:
                    event_type = field_rule["event_type"]

        return {
            "event_type": event_type,
            "source_db": database,
            "source_table": table,
            "subject_id": self._extract_id(after, rule),
            "actor": after.get("actor", after.get("created_by", before.get("actor", ""))),
            "summary": self._build_summary(event_type, after, rule, before),
            "payload": {"before": before, "after": after},
        }

    def classify_delete(self, database: str, table: str, row: dict) -> dict | None:
        """Classify a DELETE event."""
        rule = self._rules.get((database, table))
        if not rule:
            return None

        event_type = rule.get("delete")
        if not event_type:
            return None

        return {
            "event_type": event_type,
            "source_db": database,
            "source_table": table,
            "subject_id": self._extract_id(row, rule),
            "actor": row.get("actor", row.get("created_by", "")),
            "summary": self._build_summary(event_type, row, rule),
            "payload": row,
        }

    def _extract_id(self, row: dict, rule: dict) -> str:
        """Extract the subject ID from a row."""
        id_field = rule.get("id_field", "id")
        return str(row.get(id_field, ""))

    def _build_summary(self, event_type: str, row: dict, rule: dict, before: dict = None) -> str:
        """Build a human-readable summary."""
        template = rule.get("summary_template", "{event_type}: {id}")
        try:
            ctx = {"event_type": event_type, **row}
            if before:
                ctx.update({f"old_{k}": v for k, v in before.items()})
            return template.format(**ctx)
        except (KeyError, IndexError):
            title = row.get("title", row.get("id", "?"))
            return f"{event_type}: {title}"

    def _matches_condition(self, condition: str, row: dict) -> bool:
        """Evaluate a simple condition against row data."""
        if not condition:
            return True
        # Simple evaluator for conditions like "issue_type == 'decision'"
        try:
            # Safe subset: only allow comparisons on row fields
            return eval(condition, {"__builtins__": {}}, row)
        except Exception:
            return False

    def _matches_transition(self, trans: dict, old_val, new_val) -> bool:
        """Check if a value transition matches a rule."""
        from_val = trans.get("from", None)
        to_val = trans.get("to", None)
        if from_val is not None and str(old_val) != str(from_val):
            return False
        if to_val is not None and str(new_val) != str(to_val):
            return False
        if from_val is None and to_val is None:
            return False
        return True


# ---------------------------------------------------------------------------
# Reaction dispatcher — executes actions when events match rules
# ---------------------------------------------------------------------------

class ReactionDispatcher:
    """Dispatches reactions based on event rules.

    Supported action types:
    - log: Log the event (debugging)
    - webhook: Generic HTTP POST
    - shell: Execute a shell command
    - message-router: POST to message-router (bot.lan:8070/send)
    - telegram: Send plain Telegram message via Bot API
    - telegram-decision: Send Telegram inline keyboard for decision beads
    - irc: Send to IRC channel via aegis-irc HTTP bridge
    """

    def __init__(self, rules: list[dict], config: dict, dry_run: bool = False):
        self._rules = rules
        self._config = config
        self._dry_run = dry_run
        self._log = logging.getLogger("reactor.dispatch")
        # Pending decisions: keyed by callback_data → {bead_id, options, message_id}
        self._pending_decisions: dict[str, dict] = {}
        self._timer_mgr = None  # Set via set_timer_manager()

    def set_timer_manager(self, timer_mgr):
        """Wire in the TimerManager for create-timer/cancel-timer actions."""
        self._timer_mgr = timer_mgr

    def dispatch(self, event: dict):
        """Find matching reaction rules and execute their actions."""
        for rule in self._rules:
            if rule["event"] != event["event_type"]:
                continue
            if not self._check_condition(rule.get("condition", ""), event):
                continue
            for action in rule.get("actions", []):
                self._execute_action(action, event)

    def _check_condition(self, condition: str, event: dict) -> bool:
        """Evaluate a condition against event data."""
        if not condition:
            return True
        try:
            ctx = dict(event.get("payload", {}))
            ctx.update(event)
            return eval(condition, {"__builtins__": {}}, ctx)
        except Exception:
            return False

    def _execute_action(self, action: dict, event: dict):
        """Execute a single reaction action."""
        action_type = action.get("type", "")
        try:
            if self._dry_run:
                self._log.info("[DRY RUN] Would execute %s for %s (%s)",
                               action_type, event["event_type"], event.get("subject_id", "?"))
                return

            handler = {
                "webhook": self._action_webhook,
                "shell": self._action_shell,
                "log": self._action_log,
                "message-router": self._action_message_router,
                "telegram": self._action_telegram,
                "telegram-decision": self._action_telegram_decision,
                "irc": self._action_irc,
                "create-timer": self._action_create_timer,
                "cancel-timer": self._action_cancel_timer,
                "gt-nudge": self._action_gt_nudge,
                "alert-escalate": self._action_alert_escalate,
            }.get(action_type)

            if handler:
                handler(action, event)
            else:
                self._log.warning("Unknown action type: %s", action_type)
        except Exception as e:
            self._log.error("Action %s failed for event %s: %s",
                            action_type, event.get("subject_id", "?"), e)

    # --- Action: message-router ---

    def _action_message_router(self, action: dict, event: dict):
        """POST to message-router /send endpoint."""
        router_url = self._config.get("dispatch", {}).get(
            "message_router_url", "http://bot.lan:8070/send"
        )
        group = self._interpolate(action.get("group", "aegis"), event)
        text = self._interpolate(action.get("template", "{summary}"), event)
        sender = action.get("sender", "reactor")
        priority = self._interpolate(action.get("priority", "info"), event)

        body = json.dumps({
            "group": group,
            "text": text,
            "sender": sender,
            "priority": priority,
        }).encode("utf-8")

        req = urllib.request.Request(
            router_url, data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                self._log.info("message-router → %s group=%s (%d)", text[:60], group, resp.status)
        except urllib.error.URLError as e:
            self._log.error("message-router failed (group=%s): %s", group, e)

    # --- Action: telegram (plain message) ---

    def _action_telegram(self, action: dict, event: dict):
        """Send a plain Telegram message via Bot API."""
        token = self._config.get("dispatch", {}).get("telegram_bot_token", "")
        if not token:
            self._log.error("telegram action: no bot token configured")
            return
        chat_id = action.get("chat_id", self._config.get("dispatch", {}).get("telegram_chat_id", ""))
        text = self._interpolate(action.get("template", "{summary}"), event)

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        body = json.dumps({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }).encode("utf-8")

        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                self._log.info("telegram → chat %s (%d)", chat_id, resp.status)
        except urllib.error.URLError as e:
            self._log.error("telegram failed (chat=%s): %s", chat_id, e)

    # --- Action: telegram-decision (inline keyboard) ---

    def _action_telegram_decision(self, action: dict, event: dict):
        """Send Telegram inline keyboard for a decision bead."""
        token = self._config.get("dispatch", {}).get("telegram_bot_token", "")
        if not token:
            self._log.error("telegram-decision: no bot token configured")
            return
        chat_id = action.get("chat_id", self._config.get("dispatch", {}).get("telegram_chat_id", ""))
        bead_id = event.get("subject_id", "?")
        payload = event.get("payload", {})
        title = payload.get("title", "") if isinstance(payload, dict) else ""
        description = payload.get("description", "") if isinstance(payload, dict) else ""

        # Parse options from bead labels or description
        options = self._extract_decision_options(payload)
        if not options:
            options = [{"label": "Approve", "value": "approve"}, {"label": "Reject", "value": "reject"}]

        # Build inline keyboard
        callback_prefix = f"reactor_decide:{bead_id}"
        keyboard = []
        for opt in options:
            cb_data = f"{callback_prefix}:{opt['value']}"
            keyboard.append([{"text": opt["label"], "callback_data": cb_data}])

        text = f"<b>Decision needed</b>: {title}\n{description[:500]}" if description else f"<b>Decision needed</b>: {title}"
        text += f"\n\n<i>Bead: {bead_id}</i>"

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        body = json.dumps({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "reply_markup": {"inline_keyboard": keyboard},
        }).encode("utf-8")

        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                resp_data = json.loads(resp.read().decode())
                msg_id = resp_data.get("result", {}).get("message_id")
                self._log.info("telegram-decision → chat %s msg %s for %s", chat_id, msg_id, bead_id)
                # Track pending decision for callback handling
                for opt in options:
                    cb_data = f"{callback_prefix}:{opt['value']}"
                    self._pending_decisions[cb_data] = {
                        "bead_id": bead_id,
                        "option": opt["value"],
                        "message_id": msg_id,
                        "chat_id": chat_id,
                    }
        except urllib.error.URLError as e:
            self._log.error("telegram-decision failed (chat=%s): %s", chat_id, e)

    def _extract_decision_options(self, payload: dict) -> list[dict]:
        """Extract decision options from bead payload.

        Looks for labels like 'option:A:Description' or a JSON options field.
        """
        if not isinstance(payload, dict):
            return []
        # Check for explicit options in description (format: [A] Label | [B] Label)
        description = payload.get("description", "")
        import re
        option_pattern = re.compile(r'\[([A-Z0-9])\]\s*([^|\n]+)')
        matches = option_pattern.findall(description)
        if matches:
            return [{"label": label.strip(), "value": key.lower()} for key, label in matches]
        # Check labels for option: prefix
        labels = payload.get("labels", "")
        if isinstance(labels, str) and "option:" in labels:
            opts = []
            for part in labels.split(","):
                part = part.strip()
                if part.startswith("option:"):
                    val = part[7:]
                    opts.append({"label": val.capitalize(), "value": val})
            if opts:
                return opts
        return []

    def handle_telegram_callback(self, callback_data: str, from_user: str) -> dict:
        """Handle a Telegram inline keyboard callback for a decision.

        Returns {ok, bead_id, answer} or {ok: false, error}.
        """
        decision = self._pending_decisions.get(callback_data)
        if not decision:
            return {"ok": False, "error": "Unknown callback (expired or already answered)"}

        bead_id = decision["bead_id"]
        answer = decision["option"]
        self._log.info("Decision callback: %s answered '%s' by %s", bead_id, answer, from_user)

        # Remove all pending callbacks for this bead
        prefix = f"reactor_decide:{bead_id}:"
        to_remove = [k for k in self._pending_decisions if k.startswith(prefix)]
        for k in to_remove:
            del self._pending_decisions[k]

        return {"ok": True, "bead_id": bead_id, "answer": answer, "answered_by": from_user}

    # --- Action: irc ---

    def _action_irc(self, action: dict, event: dict):
        """Send to IRC channel via aegis-irc HTTP bridge."""
        bridge_url = self._config.get("dispatch", {}).get(
            "irc_bridge_url", "http://bot.lan:8099/send"
        )
        channel = self._interpolate(action.get("channel", "#aegis"), event)
        text = self._interpolate(action.get("template", "{summary}"), event)

        body = json.dumps({
            "channel": channel,
            "message": text,
        }).encode("utf-8")

        req = urllib.request.Request(
            bridge_url, data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                self._log.info("irc → %s %s (%d)", channel, text[:60], resp.status)
        except urllib.error.URLError as e:
            self._log.error("irc failed (channel=%s): %s", channel, e)

    # --- Action: create-timer ---

    def _action_create_timer(self, action: dict, event: dict):
        """Create a durable timer that fires a synthetic event after a delay."""
        if not self._timer_mgr:
            self._log.error("create-timer: no TimerManager wired")
            return
        subject_id = event.get("subject_id", "")
        if not subject_id:
            self._log.warning("create-timer: no subject_id in event")
            return
        timer_type = action.get("timer_type", "generic")
        delay = action.get("delay_seconds", 86400)

        # Build reaction payload — store key event fields for template interpolation
        # when the timer fires. Only copy JSON-safe scalar fields to avoid
        # datetime serialization errors from Dolt row data.
        reaction = dict(action.get("reaction", {}))
        payload = event.get("payload", {})
        if isinstance(payload, dict):
            # For update events, flatten "after" (current state) into reaction
            after = payload.get("after")
            source = after if isinstance(after, dict) else payload
            for k, v in source.items():
                if k in ("before", "after"):
                    continue
                if isinstance(v, (str, int, float, bool, type(None))):
                    reaction.setdefault(k, v)
                elif isinstance(v, datetime.datetime):
                    reaction.setdefault(k, v.isoformat())
        reaction.setdefault("subject_id", subject_id)
        reaction.setdefault("original_event_type", event["event_type"])
        # Copy top-level event fields that are useful for templates
        for field in ("summary", "source_db", "source_table", "actor"):
            if field in event and isinstance(event[field], str):
                reaction.setdefault(field, event[field])

        self._timer_mgr.create_timer(subject_id, timer_type, delay, reaction)
        self._log.info("Timer created: %s/%s fires in %ds for %s",
                       timer_type, subject_id, delay, event.get("summary", "")[:60])

    # --- Action: cancel-timer ---

    def _action_cancel_timer(self, action: dict, event: dict):
        """Cancel pending timers for the event's subject."""
        if not self._timer_mgr:
            self._log.error("cancel-timer: no TimerManager wired")
            return
        subject_id = event.get("subject_id", "")
        if not subject_id:
            self._log.warning("cancel-timer: no subject_id in event")
            return
        timer_type = action.get("timer_type")
        self._timer_mgr.cancel_timer(subject_id, timer_type)
        self._log.info("Timer cancelled: %s for %s", timer_type or "all", subject_id)

    # --- Action: gt-nudge (via gt-relay on luvu) ---

    def _action_gt_nudge(self, action: dict, event: dict):
        """Nudge an agent via gt-relay HTTP endpoint."""
        relay_url = self._config.get("dispatch", {}).get(
            "gt_relay_url", "http://192.168.4.187:8076"
        )
        target = self._interpolate(action.get("target", "{assignee}"), event)
        message = self._interpolate(action.get("template", "{summary}"), event)
        mode = action.get("mode", "queue")

        if not target or target.startswith("{"):
            self._log.debug("gt-nudge: no target resolved, skipping")
            return

        body = json.dumps({
            "target": target,
            "message": message,
            "mode": mode,
        }).encode("utf-8")

        req = urllib.request.Request(
            f"{relay_url}/nudge", data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read())
                if result.get("ok"):
                    self._log.info("gt-nudge → %s: %s", target, message[:60])
                else:
                    self._log.error("gt-nudge %s failed: %s", target, result.get("stderr", "")[:100])
        except urllib.error.URLError as e:
            self._log.error("gt-nudge failed (target=%s): %s", target, e)

    # --- Action: alert-escalate (cascading escalation timers) ---

    def _action_alert_escalate(self, action: dict, event: dict):
        """Create the next escalation timer in the chain.

        When triggered from alert.fired: creates first timer (level 0).
        When triggered from alert.escalation: creates next timer (level + 1).
        Stops when escalation chain is exhausted.
        """
        if not self._timer_mgr:
            self._log.error("alert-escalate: no TimerManager")
            return

        payload = event.get("payload", {})
        chain = payload.get("escalation_chain", [])
        current_level = payload.get("escalation_level", 0)
        fingerprint = event.get("subject_id", "")

        if not chain or not fingerprint:
            self._log.warning("alert-escalate: missing chain or fingerprint")
            return

        # For alert.fired: create timer for level 0
        # For alert.escalation: create timer for level + 1
        if event["event_type"] == "alert.fired":
            next_level = 0
        else:
            next_level = current_level + 1

        if next_level >= len(chain):
            self._log.info("alert-escalate: chain exhausted for %s (level %d/%d)",
                           fingerprint, next_level, len(chain))
            return

        target = chain[next_level]
        delay = action.get("delay_seconds",
                           self._config.get("dispatch", {}).get(
                               "escalation_timer_seconds", 600))

        # Build timer reaction payload with full context
        reaction = {
            "event_type": "alert.escalation",
            "escalation_target": target,
            "escalation_level": next_level,
            "escalation_chain": chain,
            "alertname": payload.get("alertname", ""),
            "hostname": payload.get("hostname", ""),
            "severity": payload.get("severity", ""),
            "fingerprint": fingerprint,
            "first_seen": payload.get("first_seen", ""),
        }

        self._timer_mgr.create_timer(fingerprint, "alert_escalation", delay, reaction)
        self._log.info("Escalation timer: %s level %d→%s in %ds",
                       fingerprint, next_level, target, delay)

    # --- Action: webhook (generic) ---

    def _action_webhook(self, action: dict, event: dict):
        """Send an HTTP webhook."""
        url = self._interpolate(action["url"], event)
        method = action.get("method", "POST")
        headers = action.get("headers", {})
        headers.setdefault("Content-Type", "application/json")

        body = json.dumps({
            "event_type": event["event_type"],
            "subject_id": event.get("subject_id"),
            "summary": event.get("summary"),
            "timestamp": event.get("timestamp", datetime.datetime.utcnow().isoformat()),
            "payload": event.get("payload"),
        }).encode("utf-8")

        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                self._log.info("Webhook %s → %s %s", url, resp.status, resp.reason)
        except urllib.error.URLError as e:
            self._log.error("Webhook %s failed: %s", url, e)

    # --- Action: shell ---

    def _action_shell(self, action: dict, event: dict):
        """Execute a shell command."""
        cmd = self._interpolate(action["command"], event)
        timeout = action.get("timeout", 30)
        self._log.info("Executing shell: %s", cmd)
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=timeout
            )
            if result.returncode != 0:
                self._log.error("Shell command failed (rc=%d): %s", result.returncode, result.stderr)
            else:
                self._log.debug("Shell output: %s", result.stdout[:500])
        except subprocess.TimeoutExpired:
            self._log.error("Shell command timed out after %ds: %s", timeout, cmd)

    # --- Action: log ---

    def _action_log(self, action: dict, event: dict):
        """Log the event (useful for debugging)."""
        level = action.get("level", "info").upper()
        message = self._interpolate(action.get("template", "{event_type}: {summary}"), event)
        self._log.log(getattr(logging, level, logging.INFO), message)

    def _interpolate(self, template: str, event: dict) -> str:
        """Interpolate event fields into a template string.

        Handles both insert events (payload is a flat row dict) and update
        events (payload is {"before": {...}, "after": {...}}) by flattening
        the "after" dict into the context so {title}, {assignee} etc. resolve.
        """
        try:
            ctx = dict(event)
            payload = event.get("payload", {})
            if isinstance(payload, dict):
                # For update events, flatten "after" values first (the current state),
                # then overlay any top-level payload keys
                after = payload.get("after")
                if isinstance(after, dict):
                    ctx.update(after)
                before = payload.get("before")
                if isinstance(before, dict):
                    # Make before-values available as old_<field>
                    ctx.update({f"old_{k}": v for k, v in before.items()})
                # Also merge top-level payload keys (for insert events where
                # payload IS the row dict, or for extra metadata fields)
                for k, v in payload.items():
                    if k not in ("before", "after"):
                        ctx.setdefault(k, v)
            return template.format_map(DefaultDict(ctx))
        except Exception:
            return template


# ---------------------------------------------------------------------------
# Event writer — persists classified events to the reactor_events table
# ---------------------------------------------------------------------------

class EventWriter:
    """Writes classified events to the reactor_events Dolt table."""

    def __init__(self, conn_settings: dict, database: str, table: str = "reactor_events"):
        self._conn_settings = conn_settings
        self._database = database
        self._table = table
        self._log = logging.getLogger("reactor.writer")

    def write(self, event: dict):
        """Write a classified event to the events table."""
        event_id = ulid()
        now = datetime.datetime.utcnow()

        payload = event.get("payload", {})
        # Sanitize payload for JSON serialization
        payload_json = json.dumps(payload, default=str)

        conn = pymysql.connect(**self._conn_settings, database=self._database)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {self._table} "
                    "(id, timestamp, event_type, source_db, source_table, "
                    "subject_id, actor, summary, payload) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        event_id,
                        now,
                        event["event_type"],
                        event.get("source_db", ""),
                        event.get("source_table", ""),
                        event.get("subject_id", ""),
                        event.get("actor", ""),
                        event.get("summary", ""),
                        payload_json,
                    ),
                )
            conn.commit()
            self._log.debug("Wrote event %s: %s", event_id, event["event_type"])
        except Exception as e:
            self._log.error("Failed to write event: %s", e)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# State tracker — persists GTID position for crash recovery
# ---------------------------------------------------------------------------

class StateTracker:
    """Persists reactor state (GTID position, etc.) to Dolt."""

    def __init__(self, conn_settings: dict, database: str, table: str = "reactor_state"):
        self._conn_settings = conn_settings
        self._database = database
        self._table = table
        self._log = logging.getLogger("reactor.state")

    def get(self, key: str) -> str | None:
        """Retrieve a state value."""
        conn = pymysql.connect(**self._conn_settings, database=self._database)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT value FROM {self._table} WHERE key_name = %s", (key,)
                )
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            conn.close()

    def set(self, key: str, value: str):
        """Store a state value (upsert)."""
        conn = pymysql.connect(**self._conn_settings, database=self._database)
        try:
            with conn.cursor() as cur:
                # Dolt supports REPLACE INTO
                cur.execute(
                    f"REPLACE INTO {self._table} (key_name, value, updated_at) "
                    "VALUES (%s, %s, %s)",
                    (key, value, datetime.datetime.utcnow()),
                )
            conn.commit()
        except Exception as e:
            self._log.error("Failed to persist state %s: %s", key, e)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Health server — lightweight HTTP health check endpoint
# ---------------------------------------------------------------------------

class HealthServer:
    """HTTP server for health checks, status, and decision callbacks."""

    def __init__(self, port: int, reactor_core):
        self._port = port
        self._reactor = reactor_core
        self._server = None
        self._thread = None

    def start(self):
        """Start the health server in a background thread."""
        from http.server import HTTPServer, BaseHTTPRequestHandler

        reactor = self._reactor

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/health":
                    self._respond(200, {"status": "ok", "uptime": reactor.uptime()})
                elif self.path == "/status":
                    self._respond(200, reactor.status())
                elif self.path == "/events":
                    self._respond(200, reactor.recent_events(limit=50))
                elif self.path == "/metrics":
                    self._respond_text(200, reactor.prometheus_metrics())
                elif self.path == "/decisions":
                    pending = reactor.pending_decisions()
                    self._respond(200, {"pending": pending})
                else:
                    self._respond(404, {"error": "not found"})

            def do_POST(self):
                content_len = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_len) if content_len else b""
                try:
                    data = json.loads(body) if body else {}
                except json.JSONDecodeError:
                    self._respond(400, {"error": "invalid JSON"})
                    return

                if self.path == "/callback/telegram":
                    self._handle_telegram_callback(data)
                elif self.path == "/callback/irc":
                    self._handle_irc_callback(data)
                elif self.path == "/event":
                    result = reactor.inject_external_event(data)
                    self._respond(200 if result.get("ok") else 400, result)
                else:
                    self._respond(404, {"error": "not found"})

            def _handle_telegram_callback(self, data):
                """Handle Telegram callback_query forwarded from aegis-tg."""
                callback_query = data.get("callback_query", data)
                cb_data = callback_query.get("data", "")
                from_user = callback_query.get("from", {}).get("first_name", "unknown")

                if not cb_data.startswith("reactor_decide:"):
                    self._respond(200, {"ok": False, "error": "not a reactor callback"})
                    return

                result = reactor.handle_decision_callback(cb_data, from_user)

                # Answer the callback query (remove "loading" spinner)
                callback_id = callback_query.get("id")
                if callback_id and result.get("ok"):
                    reactor.answer_telegram_callback(callback_id, f"Decided: {result['answer']}")

                self._respond(200, result)

            def _handle_irc_callback(self, data):
                """Handle IRC !decide command forwarded from aegis-irc."""
                bead_id = data.get("bead_id", "")
                answer = data.get("answer", "")
                user = data.get("user", "unknown")

                if not bead_id or not answer:
                    self._respond(400, {"error": "bead_id and answer required"})
                    return

                cb_data = f"reactor_decide:{bead_id}:{answer}"
                result = reactor.handle_decision_callback(cb_data, user)
                self._respond(200, result)

            def _respond(self, code, data):
                body = json.dumps(data, default=str).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _respond_text(self, code, text):
                body = text.encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                pass  # Suppress access logs

        self._server = HTTPServer(("0.0.0.0", self._port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self):
        if self._server:
            self._server.shutdown()


# ---------------------------------------------------------------------------
# Timer manager — handles delayed/scheduled reactions
# ---------------------------------------------------------------------------

class TimerManager:
    """Manages durable timers persisted to Dolt."""

    def __init__(self, conn_settings: dict, database: str,
                 table: str = "reactor_timers", dispatcher=None):
        self._conn_settings = conn_settings
        self._database = database
        self._table = table
        self._dispatcher = dispatcher
        self._log = logging.getLogger("reactor.timers")
        self._running = False
        self._thread = None

    def start(self):
        """Start the timer check loop."""
        self._running = True
        self._thread = threading.Thread(target=self._check_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False

    def create_timer(self, subject_id: str, timer_type: str,
                     delay_seconds: int, reaction: dict):
        """Create a new durable timer."""
        timer_id = ulid()
        fire_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=delay_seconds)
        now = datetime.datetime.utcnow()

        conn = pymysql.connect(**self._conn_settings, database=self._database)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {self._table} "
                    "(id, subject_id, timer_type, fire_at, reaction, created_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (timer_id, subject_id, timer_type, fire_at,
                     json.dumps(reaction), now),
                )
            conn.commit()
            self._log.info("Timer %s created: %s fires at %s", timer_id, timer_type, fire_at)
        finally:
            conn.close()

    def cancel_timer(self, subject_id: str, timer_type: str = None):
        """Cancel timers for a subject."""
        conn = pymysql.connect(**self._conn_settings, database=self._database)
        try:
            with conn.cursor() as cur:
                if timer_type:
                    cur.execute(
                        f"DELETE FROM {self._table} WHERE subject_id = %s AND timer_type = %s",
                        (subject_id, timer_type),
                    )
                else:
                    cur.execute(
                        f"DELETE FROM {self._table} WHERE subject_id = %s",
                        (subject_id,),
                    )
            conn.commit()
        finally:
            conn.close()

    def _check_loop(self):
        """Periodically check for fired timers."""
        while self._running:
            try:
                self._fire_due_timers()
            except Exception as e:
                self._log.error("Timer check error: %s", e)
            time.sleep(5)

    def _fire_due_timers(self):
        """Fire any timers that are past their fire_at time."""
        now = datetime.datetime.utcnow()
        conn = pymysql.connect(**self._conn_settings, database=self._database)
        try:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    f"SELECT * FROM {self._table} WHERE fire_at <= %s", (now,)
                )
                timers = cur.fetchall()

            for timer in timers:
                self._log.info("Timer fired: %s (%s) for %s",
                               timer["id"], timer["timer_type"], timer["subject_id"])
                reaction = json.loads(timer["reaction"]) if isinstance(timer["reaction"], str) else timer["reaction"]

                if self._dispatcher:
                    event = {
                        "event_type": reaction.get("event_type", f"timer.{timer['timer_type']}"),
                        "subject_id": timer["subject_id"],
                        "summary": f"Timer fired: {timer['timer_type']} for {timer['subject_id']}",
                        "payload": reaction,
                    }
                    self._dispatcher.dispatch(event)

                # Delete fired timer
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {self._table} WHERE id = %s", (timer["id"],))
                conn.commit()
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Reactor core — the main event loop
# ---------------------------------------------------------------------------

class Reactor:
    """Core reactor: binlog consumer → classifier → dispatcher pipeline."""

    def __init__(self, config: dict, dry_run: bool = False):
        self._config = config
        self._dry_run = dry_run
        self._log = logging.getLogger("reactor")
        self._running = False
        self._start_time = time.time()
        self._events_processed = 0
        self._events_dispatched = 0
        self._last_event_time = None
        self._recent_events: list[dict] = []

        rc = config.get("reactor", {})
        self._conn_settings = {
            "host": rc.get("dolt_host", "127.0.0.1"),
            "port": rc.get("dolt_port", 3306),
            "user": rc.get("dolt_user", "root"),
            "password": rc.get("dolt_password", ""),
        }
        self._event_db = rc.get("event_db", "reactor")
        self._watched_dbs = set(rc.get("watched_databases", []))

        # Components
        self._schema = SchemaMapper(self._conn_settings)
        self._classifier = EventClassifier(config.get("classify", []), self._schema)
        self._dispatcher = ReactionDispatcher(config.get("reactions", []), config, dry_run)
        self._writer = EventWriter(self._conn_settings, self._event_db)
        self._state = StateTracker(self._conn_settings, self._event_db)
        self._timer_mgr = TimerManager(
            self._conn_settings, self._event_db, dispatcher=self._dispatcher
        )
        self._dispatcher.set_timer_manager(self._timer_mgr)
        self._health = HealthServer(rc.get("health_port", 8075), self)

    def run(self):
        """Main event loop — poll dolt_log for new commits and process diffs.

        Uses Dolt's dolt_log + dolt_diff system tables instead of binlog.
        This is more reliable than binlog streaming which can crash Dolt on
        large databases.
        """
        self._running = True
        self._health.start()
        self._timer_mgr.start()

        poll_interval = self._config.get("reactor", {}).get("poll_interval", 3)
        self._log.info("Starting reactor (poll mode, interval=%ds, dry_run=%s)",
                       poll_interval, self._dry_run)

        # Get watched tables from classify config (database -> set of tables)
        self._watched_tables = {}
        for rule in self._config.get("classify", []):
            db = rule["database"]
            tbl = rule["table"]
            self._watched_tables.setdefault(db, set()).add(tbl)

        # Wait for Dolt to be ready before initializing commit tracking
        max_retries = 12
        for attempt in range(1, max_retries + 1):
            try:
                conn = pymysql.connect(**self._conn_settings, database=self._event_db)
                conn.close()
                break
            except Exception as e:
                if attempt == max_retries:
                    self._log.error("Dolt not ready after %d attempts, giving up: %s",
                                    max_retries, e)
                    raise
                self._log.warning("Dolt not ready (attempt %d/%d): %s — retrying in 5s",
                                  attempt, max_retries, e)
                time.sleep(5)

        # Initialize last-seen commit per database
        for db in self._watched_dbs:
            state_key = f"last_commit_{db}"
            saved = self._state.get(state_key)
            if saved:
                self._log.info("Resuming %s from commit %s", db, saved[:12])
            else:
                # Start from current HEAD (skip historical)
                head = self._get_head_commit(db)
                if head:
                    self._state.set(state_key, head)
                    self._log.info("Initialized %s at HEAD commit %s", db, head[:12])

        while self._running:
            try:
                changes_found = self._poll_all_databases()
                if not changes_found:
                    time.sleep(poll_interval)
            except KeyboardInterrupt:
                self._log.info("Shutting down (keyboard interrupt)")
                break
            except Exception as e:
                self._log.error("Poll error: %s — retrying in %ds", e, poll_interval)
                time.sleep(poll_interval)

        self._shutdown()

    def _get_head_commit(self, database: str) -> str | None:
        """Get the latest commit hash for a database."""
        try:
            conn = pymysql.connect(**self._conn_settings, database=database)
            with conn.cursor() as cur:
                cur.execute("SELECT commit_hash FROM dolt_log LIMIT 1")
                row = cur.fetchone()
                return row[0] if row else None
        except Exception as e:
            self._log.error("Failed to get HEAD for %s: %s", database, e)
            return None
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _poll_all_databases(self) -> bool:
        """Poll all watched databases for new commits. Returns True if changes found."""
        changes = False
        for db in self._watched_dbs:
            try:
                if self._poll_database(db):
                    changes = True
            except Exception as e:
                self._log.error("Error polling %s: %s", db, e)
        return changes

    def _poll_database(self, database: str) -> bool:
        """Check a database for new commits and process diffs. Returns True if changes found."""
        state_key = f"last_commit_{database}"
        last_commit = self._state.get(state_key)
        if not last_commit:
            head = self._get_head_commit(database)
            if head:
                self._state.set(state_key, head)
            return False

        # Check current HEAD
        head = self._get_head_commit(database)
        if not head or head == last_commit:
            return False  # No new commits

        # Get new commits using dolt_log range syntax (from..to)
        try:
            conn = pymysql.connect(**self._conn_settings, database=database)
            with conn.cursor() as cur:
                range_spec = f"{last_commit}..{head}"
                cur.execute(
                    f"SELECT commit_hash, message, committer, date "
                    f"FROM dolt_log(%s) ORDER BY date ASC",
                    (range_spec,)
                )
                new_commits = cur.fetchall()
        except Exception as e:
            err_str = str(e)
            if "invalid ref spec" in err_str.lower() or "not found" in err_str.lower():
                # Last commit no longer in history (database rebased/reset)
                self._log.warning("Last commit %s not found in %s, skipping to HEAD %s",
                                  last_commit[:12], database, head[:12])
                self._state.set(state_key, head)
                return False
            self._log.error("Failed to query dolt_log for %s: %s", database, e)
            return False
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if not new_commits:
            # HEAD changed but no commits in range — skip to HEAD
            self._state.set(state_key, head)
            return False

        self._log.info("Found %d new commit(s) in %s", len(new_commits), database)

        # Process each new commit's diffs
        tables = self._watched_tables.get(database, set())
        prev_commit = last_commit
        for commit_hash, message, committer, date in new_commits:
            self._log.info("Processing commit %s: %s (by %s)",
                           commit_hash[:12], message, committer)
            for table in tables:
                try:
                    self._process_commit_diff(database, table, prev_commit, commit_hash)
                except Exception as e:
                    self._log.error("Error processing diff %s.%s@%s: %s",
                                    database, table, commit_hash[:12], e)
            prev_commit = commit_hash

        # Persist position after processing all new commits
        self._state.set(state_key, prev_commit)
        return True

    def _process_commit_diff(self, database: str, table: str,
                             from_commit: str, to_commit: str):
        """Query dolt_diff for a table between two commits and process changes."""
        diff_table = f"dolt_diff_{table}"
        try:
            conn = pymysql.connect(**self._conn_settings, database=database)
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    f"SELECT * FROM `{diff_table}` "
                    "WHERE from_commit = %s AND to_commit = %s",
                    (from_commit, to_commit)
                )
                diffs = cur.fetchall()
        except Exception as e:
            # Table might not exist in dolt_diff (e.g., new table)
            if "table not found" in str(e).lower():
                return
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass

        for diff_row in diffs:
            diff_type = diff_row.get("diff_type", "")
            # Build row dicts from to_* and from_* prefixed columns
            to_row = {}
            from_row = {}
            for col, val in diff_row.items():
                if col.startswith("to_") and col not in ("to_commit", "to_commit_date"):
                    to_row[col[3:]] = val
                elif col.startswith("from_") and col not in ("from_commit", "from_commit_date"):
                    from_row[col[5:]] = val

            if diff_type == "added":
                classified = self._classifier.classify_insert(database, table, to_row)
                if classified:
                    self._handle_classified_event(classified)

            elif diff_type == "modified":
                classified = self._classifier.classify_update(database, table, from_row, to_row)
                if classified:
                    self._handle_classified_event(classified)

            elif diff_type == "removed":
                classified = self._classifier.classify_delete(database, table, from_row)
                if classified:
                    self._handle_classified_event(classified)

            self._events_processed += 1

    def inject_external_event(self, data: dict) -> dict:
        """Inject an externally-sourced event into the reactor pipeline.

        Used by POST /event to accept events from alert-comms-bridge, etc.
        Returns a result dict with ok status.
        """
        event_type = data.get("event_type")
        subject_id = data.get("subject_id", data.get("fingerprint", ""))
        if not event_type:
            return {"ok": False, "error": "event_type required"}
        if not subject_id:
            return {"ok": False, "error": "subject_id required"}

        event = {
            "event_type": event_type,
            "subject_id": subject_id,
            "summary": data.get("summary", f"{event_type}: {subject_id}"),
            "source_db": "external",
            "source_table": data.get("source", "alert-comms-bridge"),
            "actor": data.get("actor", "alertmanager"),
            "payload": data,
        }
        self._handle_classified_event(event)
        return {"ok": True, "event_type": event_type, "subject_id": subject_id}

    def stop(self):
        """Signal the reactor to stop."""
        self._running = False

    def _row_to_dict(self, values, columns: list[str]) -> dict:
        """Convert binlog row values to a named dict using schema columns."""
        if isinstance(values, dict):
            return values
        # If values is a list/tuple, zip with column names
        if isinstance(values, (list, tuple)):
            return dict(zip(columns, values))
        return values

    def _handle_classified_event(self, event: dict):
        """Write event to log, dispatch reactions."""
        self._events_dispatched += 1
        self._last_event_time = time.time()

        # Store in recent events buffer
        event["timestamp"] = datetime.datetime.utcnow().isoformat()
        self._recent_events.append(event)
        if len(self._recent_events) > 100:
            self._recent_events = self._recent_events[-100:]

        self._log.info("Event: %s [%s] %s",
                       event["event_type"],
                       event.get("subject_id", "?"),
                       event.get("summary", ""))

        # Write to event log table
        self._writer.write(event)

        # Dispatch reactions
        self._dispatcher.dispatch(event)

    # _persist_position removed — poll mode tracks commit hashes via StateTracker

    def _shutdown(self):
        """Clean shutdown."""
        self._log.info("Shutting down reactor...")
        self._timer_mgr.stop()
        self._health.stop()
        self._log.info("Reactor stopped. Processed %d events, dispatched %d.",
                       self._events_processed, self._events_dispatched)

    # --- Status / metrics ---

    def uptime(self) -> float:
        return time.time() - self._start_time

    def status(self) -> dict:
        # Collect last-seen commit per database
        commits = {}
        for db in self._watched_dbs:
            commits[db] = self._state.get(f"last_commit_{db}") or "unknown"
        return {
            "uptime_seconds": self.uptime(),
            "events_processed": self._events_processed,
            "events_dispatched": self._events_dispatched,
            "last_event_time": self._last_event_time,
            "last_commits": commits,
            "dry_run": self._dry_run,
            "watched_databases": list(self._watched_dbs),
            "mode": "poll",
        }

    def recent_events(self, limit: int = 50) -> list[dict]:
        return self._recent_events[-limit:]

    def pending_decisions(self) -> list[dict]:
        """Return pending decisions for the /decisions endpoint."""
        seen = set()
        result = []
        for cb_data, info in self._dispatcher._pending_decisions.items():
            bid = info["bead_id"]
            if bid not in seen:
                seen.add(bid)
                result.append({"bead_id": bid, "options": [], "message_id": info.get("message_id")})
            for r in result:
                if r["bead_id"] == bid:
                    r["options"].append(info["option"])
        return result

    def handle_decision_callback(self, callback_data: str, from_user: str) -> dict:
        """Delegate to dispatcher's callback handler."""
        return self._dispatcher.handle_telegram_callback(callback_data, from_user)

    def answer_telegram_callback(self, callback_id: str, text: str):
        """Send answerCallbackQuery to Telegram to dismiss loading spinner."""
        token = self._config.get("dispatch", {}).get("telegram_bot_token", "")
        if not token:
            return
        url = f"https://api.telegram.org/bot{token}/answerCallbackQuery"
        body = json.dumps({"callback_query_id": callback_id, "text": text}).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            self._log.debug("answerCallbackQuery failed: %s", e)

    def prometheus_metrics(self) -> str:
        lines = [
            f"reactor_uptime_seconds {self.uptime():.1f}",
            f"reactor_events_processed_total {self._events_processed}",
            f"reactor_events_dispatched_total {self._events_dispatched}",
            f"reactor_pending_decisions {len(self._dispatcher._pending_decisions)}",
        ]
        if self._last_event_time:
            lines.append(f"reactor_last_event_timestamp {self._last_event_time:.3f}")
        return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Reactor — Real-time event processing for Dolt")
    parser.add_argument("--config", required=True, help="Path to config file (TOML or JSON)")
    parser.add_argument("--dry-run", action="store_true", help="Log events but don't execute reactions")
    parser.add_argument("--log-level", default=None, help="Override log level (DEBUG, INFO, WARNING, ERROR)")
    args = parser.parse_args()

    config = load_config(args.config)
    rc = config.get("reactor", {})

    log_level = args.log_level or rc.get("log_level", "INFO")
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(name)-20s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    reactor = Reactor(config, dry_run=args.dry_run)

    # Graceful shutdown on SIGTERM/SIGINT
    def handle_signal(signum, frame):
        reactor.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    reactor.run()


if __name__ == "__main__":
    main()
