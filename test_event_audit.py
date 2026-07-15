"""Tests for the POST /event access/audit log (ss-81rhd).

The reactor host is tailnet-only and all tailnet clients are trusted, so /event
is unauthenticated by design — observability, not auth, is the control. These
tests lock the audit line's fields and that it fires on every outcome.
"""

import logging

import reactor


def test_audit_logs_all_fields(caplog):
    with caplog.at_level(logging.INFO, logger="reactor.event_audit"):
        reactor._audit_event_request(
            "100.116.254.20",
            "/event",
            {"event_type": "host.alert.warn", "subject_id": "abc"},
            "ok",
        )
    rec = caplog.records[-1]
    assert rec.name == "reactor.event_audit"
    msg = rec.getMessage()
    assert "source=100.116.254.20" in msg
    assert "path=/event" in msg
    assert "event_type=host.alert.warn" in msg
    assert "subject_id=abc" in msg
    assert "outcome=ok" in msg


def test_audit_falls_back_to_fingerprint_and_placeholders(caplog):
    with caplog.at_level(logging.INFO, logger="reactor.event_audit"):
        reactor._audit_event_request("", "/event", {"fingerprint": "fp1"}, "ok")
    msg = caplog.records[-1].getMessage()
    assert "source=?" in msg  # empty client IP → placeholder
    assert "subject_id=fp1" in msg  # falls back to fingerprint
    assert "event_type=?" in msg  # missing event_type → placeholder


def test_audit_records_error_outcome_with_none_data(caplog):
    with caplog.at_level(logging.INFO, logger="reactor.event_audit"):
        reactor._audit_event_request("100.64.0.1", "/event", None, "error:invalid-json")
    msg = caplog.records[-1].getMessage()
    assert "outcome=error:invalid-json" in msg
    assert "event_type=?" in msg
