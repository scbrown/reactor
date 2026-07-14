#!/usr/bin/env python3
"""Unit tests for the pure logic of gh-notif-poller.py.

The poller's network/loop code is integration-tested by running it against a
live reactor; these cover the pure transforms (reason mapping, dedup keying,
subject-URL parsing, event shaping) that decide correctness of every forwarded
event.

Run: python3 -m pytest test_gh_notif_poller.py -q
"""

import importlib.util
import os

import pytest

# Load the hyphenated module by path.
_SPEC = importlib.util.spec_from_file_location(
    "gh_notif_poller",
    os.path.join(os.path.dirname(__file__), "gh-event-poller.py").replace(
        "gh-event-poller.py", "gh-notif-poller.py"
    ),
)
poller = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(poller)


# ── reason → event_type ────────────────────────────────────────────────────


def test_review_requested_maps_to_canonical_type():
    # Same taxonomy as the webhook classifier's github.review_requested.
    assert poller.event_type_for_reason("review_requested") == "github.review_requested"


def test_mention_and_team_mention_collapse():
    assert poller.event_type_for_reason("mention") == "github.mention"
    assert poller.event_type_for_reason("team_mention") == "github.mention"


def test_unknown_reason_gets_namespaced_fallback():
    assert (
        poller.event_type_for_reason("some_new_reason")
        == "github.notif.some_new_reason"
    )


# ── dedup key ──────────────────────────────────────────────────────────────


def test_dedup_key_combines_id_and_updated_at():
    thread = {"id": "42", "updated_at": "2026-07-14T10:00:00Z"}
    assert poller.dedup_key(thread) == "42:2026-07-14T10:00:00Z"


def test_dedup_key_changes_when_thread_updates():
    # A re-review request on the same thread must NOT be suppressed.
    t1 = {"id": "42", "updated_at": "2026-07-14T10:00:00Z"}
    t2 = {"id": "42", "updated_at": "2026-07-14T12:30:00Z"}
    assert poller.dedup_key(t1) != poller.dedup_key(t2)


# ── subject URL parsing ──────────────────────────────────────────────────────


def test_parse_pull_request_url():
    repo, num = poller.parse_subject_url(
        "https://api.github.com/repos/Sema4AI/koodivaja/pulls/123"
    )
    assert repo == "Sema4AI/koodivaja"
    assert num == 123


def test_parse_issue_url():
    repo, num = poller.parse_subject_url(
        "https://api.github.com/repos/Sema4AI/moonraker/issues/45"
    )
    assert repo == "Sema4AI/moonraker"
    assert num == 45


def test_parse_garbage_url_is_safe():
    assert poller.parse_subject_url("") == ("", 0)
    assert poller.parse_subject_url("not-a-url") == ("", 0)


def test_html_url_from_pull_api_url():
    assert (
        poller.html_url_from_api_url(
            "https://api.github.com/repos/Sema4AI/koodivaja/pulls/123"
        )
        == "https://github.com/Sema4AI/koodivaja/pull/123"
    )


def test_html_url_from_issue_api_url():
    assert (
        poller.html_url_from_api_url(
            "https://api.github.com/repos/Sema4AI/moonraker/issues/45"
        )
        == "https://github.com/Sema4AI/moonraker/issues/45"
    )


# ── event shaping ─────────────────────────────────────────────────────────────


def _thread(reason="review_requested"):
    return {
        "id": "9001",
        "reason": reason,
        "updated_at": "2026-07-14T10:00:00Z",
        "repository": {"full_name": "Sema4AI/koodivaja"},
        "subject": {
            "title": "Fix the flux capacitor",
            "url": "https://api.github.com/repos/Sema4AI/koodivaja/pulls/123",
            "type": "PullRequest",
        },
    }


def test_build_event_minimum_contract():
    # inject_external_event requires event_type + subject_id; everything else
    # is template/dashboard sugar.
    ev = poller.build_event(_thread(), pr_details=None)
    assert ev["event_type"] == "github.review_requested"
    assert ev["subject_id"] == "koodivaja#123"  # repo_short, matches webhook classifier
    assert ev["summary"]  # non-empty
    assert ev["reason"] == "review_requested"
    assert ev["source"] == "gh-notif-poller"


def test_build_event_uses_subject_title_without_enrichment():
    ev = poller.build_event(_thread(), pr_details=None)
    assert "Fix the flux capacitor" in ev["summary"]
    # html_url derived from the api url when enrichment is unavailable
    assert ev["url"] == "https://github.com/Sema4AI/koodivaja/pull/123"


def test_build_event_enrichment_overrides_author_and_url():
    pr = {
        "number": 123,
        "title": "Fix the flux capacitor",
        "user": {"login": "octocat"},
        "html_url": "https://github.com/Sema4AI/koodivaja/pull/123",
        "draft": False,
    }
    ev = poller.build_event(_thread(), pr_details=pr)
    assert ev["author"] == "octocat"
    assert "by octocat" in ev["summary"]
    assert ev["actor"] == "octocat"
    assert ev["draft"] is False


def test_build_event_thread_fallback_subject_id_when_no_number():
    t = _thread()
    t["subject"]["url"] = ""  # e.g. a Discussion with no numeric subject
    t["repository"]["full_name"] = ""
    ev = poller.build_event(t, pr_details=None)
    # Must still produce a non-empty subject_id so inject_external_event accepts it.
    assert ev["subject_id"] == "thread:9001"


# ── enabled-reason filtering ─────────────────────────────────────────────────


def test_should_forward_respects_enabled_set():
    enabled = ["review_requested", "mention"]
    assert poller.should_forward("review_requested", enabled) is True
    assert poller.should_forward("mention", enabled) is True
    assert poller.should_forward("subscribed", enabled) is False
    assert poller.should_forward("ci_activity", enabled) is False


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
