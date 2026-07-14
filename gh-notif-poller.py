#!/usr/bin/env python3
"""
GitHub Notifications Poller — polls the GitHub Notifications API and feeds
review-request / mention events into reactor.

This is the capture leg for Steve's primary ask (ss-s1bm / ss-n0icm): surface
PRs where he is requested as a reviewer. The public repo **Events** API (which
gh-event-poller.py consumes) never emits `review_requested`; the **Notifications**
API does, via each thread's `reason` field. See the design:
  steve/docs/artifacts/gh-event-notifications/design.md (Lane 1).

Unlike gh-event-poller.py (which posts webhook-shaped payloads to
/webhook/github with an HMAC signature), this poller self-classifies and posts
to reactor's /event inject endpoint — notification threads are not webhook
payloads, and /event needs no signature.

Read-only: it NEVER marks threads read (Steve's unread state stays his).

Usage:
    gh-notif-poller.py [--interval 60] [--config /path/to/config.toml] [--once] [--dry-run]
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

try:
    import tomllib
except ImportError:  # pragma: no cover - py<3.11 fallback
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None

# ── constants ────────────────────────────────────────────────────────────────

NOTIFICATIONS_URL = "https://api.github.com/notifications"
REACTOR_URL = "http://127.0.0.1:8075/event"
STATE_FILE = os.path.expanduser("~/.cache/moneypenny/gh-notif-poller-state.json")

# GitHub notification `reason` → reactor event_type. review_requested shares the
# canonical `github.review_requested` type with the webhook classifier so one
# taxonomy holds regardless of transport (design §Lane-1.3).
REASON_EVENT_MAP = {
    "review_requested": "github.review_requested",
    "mention": "github.mention",
    "team_mention": "github.mention",
    "assign": "github.assigned",
    "ci_activity": "github.ci_activity",
    "state_change": "github.state_change",
}

# MVP default (design Open-Q #3): review_requested + mention forward (→ Telegram
# via reaction rules); everything else stays off until enabled in config.toml.
DEFAULT_ENABLED_REASONS = ["review_requested", "mention"]

# Human labels for summary lines.
REASON_LABEL = {
    "review_requested": "Review requested",
    "mention": "Mention",
    "team_mention": "Team mention",
    "assign": "Assigned",
    "ci_activity": "CI activity",
    "state_change": "State change",
}

_SUBJECT_URL_RE = re.compile(
    r"api\.github\.com/repos/([^/]+/[^/]+)/(pulls|issues)/(\d+)"
)


# ── pure transforms (unit-tested) ────────────────────────────────────────────


def event_type_for_reason(reason: str) -> str:
    return REASON_EVENT_MAP.get(reason, f"github.notif.{reason}")


def should_forward(reason: str, enabled: list) -> bool:
    return reason in enabled


def dedup_key(thread: dict) -> str:
    """Key on (thread id, updated_at) so a re-review on the same thread — which
    bumps updated_at — is treated as a fresh event, not suppressed."""
    return f"{thread.get('id', '')}:{thread.get('updated_at', '')}"


def parse_subject_url(url: str):
    """('Owner/Repo', number) from a notification subject.url, or ('', 0)."""
    if not url:
        return ("", 0)
    m = _SUBJECT_URL_RE.search(url)
    if not m:
        return ("", 0)
    return (m.group(1), int(m.group(3)))


def html_url_from_api_url(api_url: str, fallback: str = "") -> str:
    """Best-effort browser URL from a subject api url (pulls→pull)."""
    m = _SUBJECT_URL_RE.search(api_url or "")
    if not m:
        return fallback
    repo, kind, num = m.group(1), m.group(2), m.group(3)
    path = "pull" if kind == "pulls" else "issues"
    return f"https://github.com/{repo}/{path}/{num}"


def build_event(
    thread: dict, pr_details: dict = None, source: str = "gh-notif-poller"
) -> dict:
    """Shape a notification thread into the reactor /event inject body.

    Requires (per inject_external_event) event_type + subject_id; the remaining
    fields feed reaction templates and the dashboard feed.
    """
    reason = thread.get("reason", "")
    subject = thread.get("subject", {}) or {}
    subject_url = subject.get("url", "")

    parsed_repo, number = parse_subject_url(subject_url)
    repo_full = (thread.get("repository", {}) or {}).get("full_name", "") or parsed_repo
    repo_short = repo_full.split("/")[-1] if repo_full else ""
    title = subject.get("title", "")

    author = ""
    html_url = ""
    draft = None
    if pr_details:
        author = (pr_details.get("user", {}) or {}).get("login", "")
        html_url = pr_details.get("html_url", "")
        number = pr_details.get("number", number) or number
        title = pr_details.get("title", title) or title
        draft = pr_details.get("draft")
    if not html_url:
        html_url = html_url_from_api_url(subject_url)

    # subject_id: repo_short#num matches the webhook classifier so poller- and
    # webhook-sourced review_requested for the same PR collapse (design §Lane-2.4).
    if repo_short and number:
        subject_id = f"{repo_short}#{number}"
    elif thread.get("id"):
        subject_id = f"thread:{thread['id']}"
    else:
        subject_id = ""

    label = REASON_LABEL.get(reason, reason or "Notification")
    repo_ref = f"{repo_full}#{number}" if number else (repo_full or "?")
    who = f" by {author}" if author else ""
    summary = f"{label}: {repo_ref} {title}{who}".strip()

    body = {
        "event_type": event_type_for_reason(reason),
        "subject_id": subject_id,
        "summary": summary,
        "actor": author or "github",
        "source": source,
        "reason": reason,
        "repo": repo_full,
        "repo_short": repo_short,
        "pr": number,
        "title": title,
        "author": author,
        "url": html_url,
        "thread_id": thread.get("id", ""),
        "updated_at": thread.get("updated_at", ""),
    }
    if draft is not None:
        body["draft"] = draft
    return body


# ── io ────────────────────────────────────────────────────────────────────────


def get_gh_token() -> str:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        return token
    try:
        result = subprocess.run(
            ["gh", "auth", "token"], capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return ""


def load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {"seen": [], "last_modified": "", "etag": ""}


def save_state(state: dict):
    seen = state.get("seen", [])
    if len(seen) > 4000:
        state["seen"] = seen[-2000:]
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_FILE)


def load_config(path: str) -> dict:
    if not path or not os.path.exists(path) or tomllib is None:
        return {}
    with open(path, "rb") as f:
        return tomllib.load(f)


def _gh_headers(token: str) -> dict:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def poll_notifications(token: str, state: dict):
    """Return (threads, poll_interval_seconds_or_None). Updates state etag /
    last_modified in place. On 304 returns ([], interval)."""
    headers = _gh_headers(token)
    if state.get("etag"):
        headers["If-None-Match"] = state["etag"]
    elif state.get("last_modified"):
        headers["If-Modified-Since"] = state["last_modified"]

    req = urllib.request.Request(NOTIFICATIONS_URL, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            poll_interval = _int_or_none(resp.headers.get("X-Poll-Interval"))
            etag = resp.headers.get("ETag")
            last_mod = resp.headers.get("Last-Modified")
            if etag:
                state["etag"] = etag
            if last_mod:
                state["last_modified"] = last_mod
            body = resp.read()
            return (json.loads(body), poll_interval)
    except urllib.error.HTTPError as e:
        if e.code == 304:
            return ([], _int_or_none(e.headers.get("X-Poll-Interval")))
        print(f"[notif] HTTP {e.code}: {e.reason}", file=sys.stderr)
        return ([], None)
    except Exception as e:
        print(f"[notif] poll error: {e}", file=sys.stderr)
        return ([], None)


def enrich_pr(subject_url: str, token: str):
    """One GET on the subject url for author / html_url / draft. None on failure
    — build_event degrades to subject.title + derived html_url."""
    if not subject_url:
        return None
    req = urllib.request.Request(subject_url, headers=_gh_headers(token))
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"[notif] enrich failed for {subject_url}: {e}", file=sys.stderr)
        return None


def forward_to_reactor(body: dict) -> bool:
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        REACTOR_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"[forward] failed: {e}", file=sys.stderr)
        return False


def _int_or_none(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# ── main loop ────────────────────────────────────────────────────────────────


def process_once(token, enabled_reasons, state, seen, dry_run=False) -> int:
    threads, poll_interval = poll_notifications(token, state)
    forwarded = 0
    for thread in threads:
        reason = thread.get("reason", "")
        if not should_forward(reason, enabled_reasons):
            continue
        key = dedup_key(thread)
        if key in seen:
            continue
        subject = thread.get("subject", {}) or {}
        pr_details = enrich_pr(subject.get("url", ""), token)
        body = build_event(thread, pr_details)
        if not body.get("subject_id"):
            seen.add(key)
            continue
        if dry_run:
            print(
                f"[dry-run] would forward: {body['event_type']} {body['subject_id']} — {body['summary']}"
            )
            forwarded += 1
            seen.add(key)
        elif forward_to_reactor(body):
            print(f"[notif] forwarded {body['event_type']} {body['subject_id']}")
            forwarded += 1
            seen.add(key)
        else:
            # leave unseen so a transient reactor failure retries next cycle
            pass
    state["seen"] = list(seen)
    return forwarded, poll_interval


def main():
    parser = argparse.ArgumentParser(
        description="GitHub Notifications poller for reactor"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="floor poll interval (s); GitHub's X-Poll-Interval overrides upward",
    )
    parser.add_argument(
        "--config", default="/home/ubuntu/workspace/reactor/config.toml"
    )
    parser.add_argument("--once", action="store_true", help="poll once and exit")
    parser.add_argument(
        "--dry-run", action="store_true", help="log what would forward; do not POST"
    )
    args = parser.parse_args()

    config = load_config(args.config)
    notif_cfg = config.get("notifications", {}) if isinstance(config, dict) else {}
    enabled_reasons = notif_cfg.get("reasons", DEFAULT_ENABLED_REASONS)

    token = get_gh_token()
    if not token:
        print(
            "WARNING: no GitHub token — Notifications API needs auth; nothing will be seen",
            file=sys.stderr,
        )

    state = load_state()
    seen = set(state.get("seen", []))
    print(
        f"gh-notif-poller started: reasons={enabled_reasons} floor={args.interval}s "
        f"seen={len(seen)} dry_run={args.dry_run}"
    )

    while True:
        forwarded, poll_interval = process_once(
            token, enabled_reasons, state, seen, args.dry_run
        )
        if forwarded:
            print(f"[notif] forwarded {forwarded} new event(s)")
        save_state(state)
        if args.once:
            break
        sleep_for = max(args.interval, poll_interval or 0)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
