#!/usr/bin/env python3
"""Reactor-side tests for the review_requested capture leg (ss-n0icm).

Covers the three reactor-side pieces the Notifications poller depends on:
  1. the webhook classifier fix (action == "review_requested" → github.review_requested),
  2. the config.toml reaction rule renders a clean Telegram line (no leftover {braces}),
  3. cross-transport subject_id agreement so poller- and webhook-sourced events collapse.

Run: python3 -m pytest test_reactor_review_requested.py -q
"""

import importlib.util
import os
import tomllib
import types
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
poller = _load("gh_notif_poller", "gh-notif-poller.py")


def _stub_reactor():
    """A stand-in `self` for the unbound classifier — only _log is touched."""
    return types.SimpleNamespace(_log=mock.MagicMock())


# ── 1. classifier fix ─────────────────────────────────────────────────────────


def test_classifier_emits_review_requested():
    data = {
        "pull_request": {
            "number": 123,
            "title": "Fix the flux capacitor",
            "user": {"login": "octocat"},
        },
        "requested_reviewer": {"login": "scbrown"},
    }
    et, subj, summary, pr_num = reactor.Reactor._classify_github_event(
        _stub_reactor(),
        "pull_request",
        "review_requested",
        data,
        "koodivaja",
        "octocat",
    )
    assert et == "github.review_requested"
    assert subj == "koodivaja#123"
    assert pr_num == 123
    assert "by octocat" in summary


def test_classifier_still_drops_unhandled_pr_actions():
    # Regression guard: synchronize / edited must remain no-ops (no Telegram spam).
    et, *_ = reactor.Reactor._classify_github_event(
        _stub_reactor(),
        "pull_request",
        "synchronize",
        {"pull_request": {"number": 1}},
        "koodivaja",
        "octocat",
    )
    assert et is None


# ── 2. reaction rule renders cleanly ──────────────────────────────────────────


def test_review_requested_telegram_template_renders_without_leftover_braces():
    cfg = tomllib.load(open(os.path.join(HERE, "config.toml"), "rb"))
    rules = [r for r in cfg["reactions"] if r["event"] == "github.review_requested"]
    assert rules, "github.review_requested reaction rule missing from config.toml"
    tg = [a for a in rules[0]["actions"] if a["type"] == "telegram"]
    assert tg, "no telegram action on github.review_requested"

    # An event shaped exactly as inject_external_event would build it from the
    # poller's POST body (payload = the whole posted dict).
    body = poller.build_event(
        {
            "id": "1",
            "reason": "review_requested",
            "updated_at": "t",
            "repository": {"full_name": "Sema4AI/koodivaja"},
            "subject": {
                "title": "Fix the flux capacitor",
                "url": "https://api.github.com/repos/Sema4AI/koodivaja/pulls/123",
            },
        },
        pr_details={
            "user": {"login": "octocat"},
            "html_url": "https://github.com/Sema4AI/koodivaja/pull/123",
        },
    )
    event = {
        "event_type": body["event_type"],
        "subject_id": body["subject_id"],
        "summary": body["summary"],
        "payload": body,
    }
    rendered = reactor.ReactionDispatcher._interpolate(None, tg[0]["template"], event)
    assert rendered.startswith("👀 ")
    assert "Review requested" in rendered
    assert "octocat" in rendered
    assert "{" not in rendered and "}" not in rendered  # every placeholder resolved


# ── 3. cross-transport subject_id agreement ──────────────────────────────────


def test_poller_and_webhook_subject_ids_match_for_same_pr():
    # If these disagree, reactor keys the same PR under two subject_ids and a
    # poller+webhook double-delivery would NOT collapse.
    poller_event = poller.build_event(
        {
            "id": "1",
            "reason": "review_requested",
            "updated_at": "t",
            "repository": {"full_name": "Sema4AI/koodivaja"},
            "subject": {
                "url": "https://api.github.com/repos/Sema4AI/koodivaja/pulls/123"
            },
        },
        pr_details=None,
    )
    _, webhook_subj, _, _ = reactor.Reactor._classify_github_event(
        _stub_reactor(),
        "pull_request",
        "review_requested",
        {"pull_request": {"number": 123}},
        "koodivaja",
        "octocat",
    )
    assert poller_event["subject_id"] == webhook_subj == "koodivaja#123"


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-q"]))
