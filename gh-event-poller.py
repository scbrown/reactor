#!/usr/bin/env python3
"""
GitHub Event Poller — polls GitHub Events API and feeds into reactor.

Polls /repos/{owner}/{repo}/events for configured repos, deduplicates
by event ID, and POSTs new events to reactor's /webhook/github endpoint
with valid HMAC signatures so the existing handler processes them.

Uses ETags for efficient conditional requests (304s don't count against
the GitHub API rate limit).

Usage:
    gh-event-poller.py [--interval 30] [--config /path/to/config.toml]
"""

import hashlib
import hmac
import json
import os
import sys
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

REPOS = [
    "sema4ai/moonraker",
    "sema4ai/koodivaja",
    "sema4ai/blockparty",
]

EVENT_TYPE_MAP = {
    "PullRequestEvent": "pull_request",
    "PullRequestReviewEvent": "pull_request_review",
    "PullRequestReviewCommentEvent": "pull_request_review_comment",
    "IssueCommentEvent": "issue_comment",
    "CheckSuiteEvent": "check_suite",
    "WorkflowRunEvent": "workflow_run",
    "StatusEvent": "status",
    "IssuesEvent": "issues",
    "LabelEvent": "label",
    "CreateEvent": "create",
    "DeleteEvent": "delete",
    "PushEvent": "push",
}

STATE_FILE = os.path.expanduser("~/.cache/moneypenny/gh-poller-state.json")
REACTOR_URL = "http://127.0.0.1:8075/webhook/github"


def get_gh_token():
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        return token
    try:
        import subprocess
        result = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return None


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {"seen_ids": [], "etags": {}}


def save_state(state):
    seen = state.get("seen_ids", [])
    if len(seen) > 2000:
        state["seen_ids"] = seen[-1000:]
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def poll_repo(repo, token, etags):
    url = f"https://api.github.com/repos/{repo}/events?per_page=30"
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    etag = etags.get(repo, "")
    if etag:
        headers["If-None-Match"] = etag

    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            new_etag = resp.headers.get("ETag", "")
            if new_etag:
                etags[repo] = new_etag
            body = resp.read()
            return json.loads(body)
    except urllib.error.HTTPError as e:
        if e.code == 304:
            return []
        print(f"[poll] {repo}: HTTP {e.code}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"[poll] {repo}: {e}", file=sys.stderr)
        return []


def forward_to_reactor(gh_event_type, payload, repo_full, secret):
    payload.setdefault("repository", {})["full_name"] = repo_full
    body = json.dumps(payload).encode()

    headers = {
        "Content-Type": "application/json",
        "X-GitHub-Event": gh_event_type,
    }
    if secret:
        sig = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        headers["X-Hub-Signature-256"] = sig

    req = urllib.request.Request(REACTOR_URL, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"[forward] failed: {e}", file=sys.stderr)
        return False


def load_config(path):
    if not path or not os.path.exists(path):
        return {}
    with open(path, "rb") as f:
        return tomllib.load(f)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="GitHub event poller for reactor")
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument("--config", default="/home/ubuntu/workspace/reactor/config.toml")
    args = parser.parse_args()

    config = load_config(args.config)
    # S1 (ss-xskg): the webhook secret lives in the environment (reactor.secrets),
    # not tracked config.toml. Reactor validates with the env value, so the poller
    # must sign with the same one — env first, config only as a legacy fallback.
    secret = os.environ.get("REACTOR_GITHUB_WEBHOOK_SECRET") or config.get(
        "dispatch", {}).get("github_webhook_secret", "")
    token = get_gh_token()
    if not token:
        print("WARNING: no GitHub token found, rate limit will be 60/hour", file=sys.stderr)

    state = load_state()
    seen = set(state.get("seen_ids", []))
    etags = state.get("etags", {})

    print(f"GitHub event poller started: {len(REPOS)} repos, {args.interval}s interval, "
          f"{len(seen)} seen events in state")

    while True:
        new_count = 0
        for repo in REPOS:
            events = poll_repo(repo, token, etags)
            for event in reversed(events):
                eid = event.get("id", "")
                if not eid or eid in seen:
                    continue

                etype = event.get("type", "")
                gh_event = EVENT_TYPE_MAP.get(etype)
                if not gh_event:
                    seen.add(eid)
                    continue

                payload = event.get("payload", {})
                actor = event.get("actor", {})
                if actor and "sender" not in payload:
                    payload["sender"] = {"login": actor.get("login", "unknown")}
                if forward_to_reactor(gh_event, payload, repo, secret):
                    new_count += 1

                seen.add(eid)

        if new_count:
            print(f"[poll] forwarded {new_count} new events")

        state["seen_ids"] = list(seen)
        state["etags"] = etags
        save_state(state)

        time.sleep(args.interval)


if __name__ == "__main__":
    main()
