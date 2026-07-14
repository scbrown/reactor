# Reactor GitHub capture — deploy units

Systemd **user** units for the two GitHub capture pollers that feed reactor's
event stream (design:
[gh-event-notifications/design.md](https://github.com/Sema4AI/koodivaja/blob/master/steve/docs/artifacts/gh-event-notifications/design.md),
bead `ss-n0icm`). These files are the canonical, reviewable source; the live
copies are installed to `~/.config/systemd/user/`.

| Unit | Script | Sink | Secret |
|---|---|---|---|
| `gh-notif-poller.service` | `gh-notif-poller.py` | `POST /event` (self-classified) | none — `/event` is unsigned |
| `gh-event-poller.service` | `gh-event-poller.py` | `POST /webhook/github` (HMAC) | `REACTOR_GITHUB_WEBHOOK_SECRET` from `reactor.secrets` |

- **notif poller** = Steve's primary ask: polls the Notifications API for
  `review_requested` / `mention`, enriches, forwards as `github.review_requested`
  / `github.mention`. The Events API can't see review requests (design Finding 1),
  which is why this poller exists.
- **event poller** = the pre-existing repo-events poller (PR lifecycle / CI),
  resurrected under systemd (it died on the 2026-06-22 host restart for lack of a
  unit). Now reads the webhook secret from the environment so its signature matches
  what reactor validates (the S1 secrets-in-env fix had left it signing with an
  empty config value → 403).

## Install (already done by the build)

```bash
cp deploy/gh-notif-poller.service deploy/gh-event-poller.service ~/.config/systemd/user/
systemctl --user daemon-reload
```

## Activate (the gated cutover — hold until Steve's reactor push-auth + go)

Activation makes the notif poller start paging Steve's Telegram on real review
requests, so it is a deliberate step, not part of the local build:

```bash
systemctl --user enable --now gh-notif-poller.service gh-event-poller.service
systemctl --user status gh-notif-poller.service gh-event-poller.service
tail -f /home/ubuntu/workspace/daemon/gh-notif-poller.log
```

**First-run backfill:** on first start the notif poller's seen-set is empty, so it
forwards every *currently-unread* `review_requested`/`mention` at once (one Telegram
each). That is intended — it surfaces what is already waiting on Steve — but expect
an initial burst proportional to the unread backlog.

## Verify after activation

- `curl -s http://127.0.0.1:8075/health` → `{"status":"ok",...}`.
- A live `review_requested` on any repo Steve can see → `👀 Review requested: …`
  Telegram within ≤60s (GitHub's `X-Poll-Interval` floor).
- `github.review_requested` rows appear in `~/.cache/moneypenny/gh-events.jsonl`
  is written by the webhook path only; notif-path events land in the reactor
  `reactor_events` table via `/event` → dashboard feed.

## Tuning

Enable more notification reasons without a redeploy — add to `config.toml`:

```toml
[notifications]
reasons = ["review_requested", "mention", "assign", "ci_activity"]
```

Restart: `systemctl --user restart gh-notif-poller.service`.
