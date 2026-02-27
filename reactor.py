#!/usr/bin/env python3
"""
Reactor — Real-time event processing for Dolt databases.

Watches Dolt databases via MySQL binlog streaming and dispatches
configurable reactions when data changes.

Usage:
    reactor.py --config /path/to/config.toml
    reactor.py --config /path/to/config.toml --dry-run
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
    print("ERROR: pymysqlreplication required. Install with: pip install mysql-replication", file=sys.stderr)
    sys.exit(1)

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


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_CONFIG = {
    "reactor": {
        "dolt_host": "127.0.0.1",
        "dolt_port": 3306,
        "dolt_user": "root",
        "dolt_password": "",
        "server_id": 100,
        "event_db": "reactor",
        "event_table": "reactor_events",
        "state_table": "reactor_state",
        "timer_table": "reactor_timers",
        "health_port": 8075,
        "log_level": "INFO",
        "watched_databases": [],
    },
    "classify": [],
    "reactions": [],
}


def load_config(path: str) -> dict:
    """Load TOML config file, falling back to JSON if tomllib unavailable."""
    with open(path, "rb") as f:
        if tomllib:
            return tomllib.load(f)
        else:
            # Fallback: try JSON
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
    """Dispatches reactions based on event rules."""

    def __init__(self, rules: list[dict], dry_run: bool = False):
        self._rules = rules
        self._dry_run = dry_run
        self._log = logging.getLogger("reactor.dispatch")

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

            if action_type == "webhook":
                self._action_webhook(action, event)
            elif action_type == "shell":
                self._action_shell(action, event)
            elif action_type == "log":
                self._action_log(action, event)
            else:
                self._log.warning("Unknown action type: %s", action_type)
        except Exception as e:
            self._log.error("Action %s failed for event %s: %s",
                            action_type, event.get("subject_id", "?"), e)

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

    def _action_log(self, action: dict, event: dict):
        """Log the event (useful for debugging)."""
        level = action.get("level", "info").upper()
        message = self._interpolate(action.get("template", "{event_type}: {summary}"), event)
        self._log.log(getattr(logging, level, logging.INFO), message)

    def _interpolate(self, template: str, event: dict) -> str:
        """Interpolate event fields into a template string."""
        try:
            ctx = dict(event)
            payload = event.get("payload", {})
            if isinstance(payload, dict):
                ctx.update(payload)
            return template.format(**ctx)
        except (KeyError, IndexError):
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
    """Minimal HTTP server for health checks and status."""

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
                else:
                    self._respond(404, {"error": "not found"})

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
        self._last_gtid = None
        self._recent_events: list[dict] = []

        rc = config.get("reactor", {})
        self._conn_settings = {
            "host": rc.get("dolt_host", "127.0.0.1"),
            "port": rc.get("dolt_port", 3306),
            "user": rc.get("dolt_user", "root"),
            "password": rc.get("dolt_password", ""),
        }
        self._server_id = rc.get("server_id", 100)
        self._event_db = rc.get("event_db", "reactor")
        self._watched_dbs = set(rc.get("watched_databases", []))

        # Components
        self._schema = SchemaMapper(self._conn_settings)
        self._classifier = EventClassifier(config.get("classify", []), self._schema)
        self._dispatcher = ReactionDispatcher(config.get("reactions", []), dry_run)
        self._writer = EventWriter(self._conn_settings, self._event_db)
        self._state = StateTracker(self._conn_settings, self._event_db)
        self._timer_mgr = TimerManager(
            self._conn_settings, self._event_db, dispatcher=self._dispatcher
        )
        self._health = HealthServer(rc.get("health_port", 8075), self)

    def run(self):
        """Main event loop — connect to binlog and process events."""
        self._running = True
        self._health.start()
        self._timer_mgr.start()

        # Resume from last known position (file + position based, not GTID)
        last_file = self._state.get("last_binlog_file")
        last_pos = self._state.get("last_binlog_pos")
        self._log.info("Starting reactor (server_id=%d, dry_run=%s)", self._server_id, self._dry_run)
        if last_file and last_pos:
            self._log.info("Resuming from %s:%s", last_file, last_pos)
        else:
            self._log.info("Starting from current binlog position (no saved state)")

        while self._running:
            try:
                self._stream_loop(last_file, int(last_pos) if last_pos else None)
            except KeyboardInterrupt:
                self._log.info("Shutting down (keyboard interrupt)")
                break
            except Exception as e:
                self._log.error("Stream error: %s — reconnecting in 5s", e)
                time.sleep(5)

        self._shutdown()

    def stop(self):
        """Signal the reactor to stop."""
        self._running = False

    def _stream_loop(self, log_file: str = None, log_pos: int = None):
        """Connect to binlog and process events in a loop.

        Uses COM_BINLOG_DUMP (file+position) instead of COM_BINLOG_DUMP_GTID
        because Dolt does not support the GTID-based dump protocol.
        """
        stream_kwargs = {
            "connection_settings": self._conn_settings,
            "server_id": self._server_id,
            "blocking": True,
            "resume_stream": True,
            "only_events": [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        }
        if log_file and log_pos:
            stream_kwargs["log_file"] = log_file
            stream_kwargs["log_pos"] = log_pos

        stream = BinLogStreamReader(**stream_kwargs)
        self._log.info("Connected to binlog stream")

        try:
            for event in stream:
                if not self._running:
                    break
                self._process_binlog_event(event)
        finally:
            stream.close()

    def _process_binlog_event(self, event):
        """Process a single binlog event."""
        database = event.schema
        table = event.table

        # Skip events from databases we don't watch
        if self._watched_dbs and database not in self._watched_dbs:
            return
        # Skip our own writes to reactor tables
        if database == self._event_db:
            return

        columns = self._schema.get_columns(database, table)

        if isinstance(event, WriteRowsEvent):
            for row in event.rows:
                row_dict = self._row_to_dict(row["values"], columns)
                classified = self._classifier.classify_insert(database, table, row_dict)
                if classified:
                    self._handle_classified_event(classified)

        elif isinstance(event, UpdateRowsEvent):
            for row in event.rows:
                before = self._row_to_dict(row["before_values"], columns)
                after = self._row_to_dict(row["after_values"], columns)
                classified = self._classifier.classify_update(database, table, before, after)
                if classified:
                    self._handle_classified_event(classified)

        elif isinstance(event, DeleteRowsEvent):
            for row in event.rows:
                row_dict = self._row_to_dict(row["values"], columns)
                classified = self._classifier.classify_delete(database, table, row_dict)
                if classified:
                    self._handle_classified_event(classified)

        # Persist GTID position periodically
        self._events_processed += 1
        if self._events_processed % 10 == 0:
            self._persist_position(event)

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

    def _persist_position(self, event):
        """Save current binlog file+position for crash recovery.

        Uses SHOW MASTER STATUS to get the current position, since Dolt
        uses file+position based binlog (not GTID dump protocol).
        """
        try:
            conn = pymysql.connect(**self._conn_settings)
            with conn.cursor() as cur:
                cur.execute("SHOW MASTER STATUS")
                row = cur.fetchone()
                if row and len(row) >= 2:
                    log_file = row[0]
                    log_pos = str(row[1])
                    self._state.set("last_binlog_file", log_file)
                    self._state.set("last_binlog_pos", log_pos)
                    self._last_gtid = f"{log_file}:{log_pos}"
            conn.close()
        except Exception as e:
            self._log.debug("Could not persist binlog position: %s", e)

    def _shutdown(self):
        """Clean shutdown."""
        self._log.info("Shutting down reactor...")
        self._timer_mgr.stop()
        self._health.stop()
        # Final position save
        try:
            conn = pymysql.connect(**self._conn_settings)
            with conn.cursor() as cur:
                cur.execute("SHOW MASTER STATUS")
                row = cur.fetchone()
                if row and len(row) >= 2:
                    self._state.set("last_binlog_file", row[0])
                    self._state.set("last_binlog_pos", str(row[1]))
            conn.close()
        except Exception:
            pass
        self._log.info("Reactor stopped. Processed %d events, dispatched %d.",
                       self._events_processed, self._events_dispatched)

    # --- Status / metrics ---

    def uptime(self) -> float:
        return time.time() - self._start_time

    def status(self) -> dict:
        return {
            "uptime_seconds": self.uptime(),
            "events_processed": self._events_processed,
            "events_dispatched": self._events_dispatched,
            "last_event_time": self._last_event_time,
            "last_gtid": self._last_gtid,
            "dry_run": self._dry_run,
            "watched_databases": list(self._watched_dbs),
        }

    def recent_events(self, limit: int = 50) -> list[dict]:
        return self._recent_events[-limit:]

    def prometheus_metrics(self) -> str:
        lines = [
            f"reactor_uptime_seconds {self.uptime():.1f}",
            f"reactor_events_processed_total {self._events_processed}",
            f"reactor_events_dispatched_total {self._events_dispatched}",
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
