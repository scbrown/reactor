#!/usr/bin/env python3
"""source_db capability gate (ss-u72jx L1 / ss-bjhyb H3).

Privileged sinks (shell / gt-nudge / webhook) MUST NOT fire on externally-sourced
events (source_db == "external": POST /event and /webhook/*). Binlog-sourced events
(source_db == real DB name) still fire. Non-privileged sinks (telegram/irc/log) fire
regardless of source. `source_db` is the server-set, unforgeable discriminator —
`source_table` is caller-forgeable via /event's `source` field.

Run: python3 -m pytest test_source_db_gate.py -q
"""

import importlib.util
import os
from unittest import mock

HERE = os.path.dirname(__file__)


def _load(mod_name, filename):
    spec = importlib.util.spec_from_file_location(
        mod_name, os.path.join(HERE, filename)
    )
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


reactor = _load("reactor", "reactor.py")


def _dispatcher_with(action_type):
    """A dispatcher with a single rule firing `action_type` on evt 'x'."""
    rule = {"event": "x", "actions": [{"type": action_type, "command": "echo hi"}]}
    return reactor.ReactionDispatcher(rules=[rule], config={}, dry_run=False)


def _evt(source_db):
    return {
        "event_type": "x",
        "subject_id": "s1",
        "source_db": source_db,
        "payload": {},
    }


def _run(action_type, source_db):
    """Dispatch; return the mock for the sink handler so callers assert call/no-call."""
    d = _dispatcher_with(action_type)
    method = "_action_" + action_type.replace("-", "_")
    with mock.patch.object(d, method) as h:
        d.dispatch(_evt(source_db))
    return h


# ── privileged sinks: refused when source_db == "external" ────────────────────


def test_shell_refused_on_external():
    assert _run("shell", "external").call_count == 0


def test_gt_nudge_refused_on_external():
    assert _run("gt-nudge", "external").call_count == 0


def test_webhook_refused_on_external():
    assert _run("webhook", "external").call_count == 0


# ── privileged sinks: allowed on binlog-sourced (trusted) events ──────────────


def test_shell_allowed_on_binlog():
    assert _run("shell", "steve_scratchpad").call_count == 1


def test_gt_nudge_allowed_on_binlog():
    assert _run("gt-nudge", "steve_scratchpad").call_count == 1


def test_gt_mail_refused_on_external():
    # gt-mail is the Deck-action wake sink — privileged, must not fire on forged /event
    assert _run("gt-mail", "external").call_count == 0


def test_gt_mail_allowed_on_binlog():
    # binlog-sourced hitl-decision bead (trusted) → wake MP
    assert _run("gt-mail", "steve_scratchpad").call_count == 1


# ── non-privileged sinks: fire regardless of source ───────────────────────────


def test_telegram_fires_even_on_external():
    assert _run("telegram", "external").call_count == 1


def test_log_fires_even_on_external():
    assert _run("log", "external").call_count == 1


# ── the forgery the gate defeats: source_table is NOT the discriminator ───────


def test_gate_keys_on_source_db_not_source_table():
    """A forged /event can set source_table to anything, but source_db stays 'external'
    (server-set). The gate must key on source_db, so the forgery does not bypass it."""
    d = _dispatcher_with("shell")
    forged = {
        "event_type": "x",
        "subject_id": "s1",
        "source_db": "external",  # server-set, unforgeable
        "source_table": "issues",  # caller-forged to look trusted
        "payload": {},
    }
    with mock.patch.object(d, "_action_shell") as h:
        d.dispatch(forged)
    assert h.call_count == 0
