#!/usr/bin/env bash
# Register GitHub webhooks for reactor on all Steve-relevant repos.
# Requires: gh auth with admin:repo_hook scope
# Run: gh auth refresh -h github.com -s admin:repo_hook
#      then: bash register-gh-webhooks.sh

set -euo pipefail

WEBHOOK_URL="https://steve-dev.tailb1072f.ts.net/webhook/github"
SECRET="c707895022a14311bb73c2e959e80e0f8a54dead03ce710d45fe9181e6855a0d"
REPOS=("sema4ai/moonraker" "sema4ai/koodivaja" "sema4ai/blockparty")

EVENTS='["pull_request","pull_request_review","pull_request_review_comment","issue_comment","check_suite","workflow_run","status"]'

for repo in "${REPOS[@]}"; do
    echo ">>> Registering webhook on $repo ..."
    gh api "repos/$repo/hooks" \
        -X POST \
        --input - <<EOF
{
  "name": "web",
  "config": {
    "url": "$WEBHOOK_URL",
    "content_type": "json",
    "secret": "$SECRET",
    "insecure_ssl": "0"
  },
  "events": $EVENTS,
  "active": true
}
EOF
    echo "    ✓ $repo done"
done

echo ""
echo "All webhooks registered. Test with:"
echo "  curl -s https://steve-dev.tailb1072f.ts.net/webhook/github"
echo "  (should get 404 on GET, webhooks fire on POST from GitHub)"
