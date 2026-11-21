#!/usr/bin/env bash
# S0/S1: register a host as a self-hosted GitHub Actions runner (label lab-<role>, as in
# contracts/runtime.yaml), so deploy workflows can run there with the host's instance profile.
#
#   scripts/register-runner.sh [app|platform|llm]      (default: app)
#
# Decision (decisions/): the runner uses the host's instance profile (LabInstanceProfile), so no AWS
# credentials are stored in GitHub. Jobs on it get short-lived credentials from the instance metadata.
#
# How: a short-lived GitHub registration token is fetched with `gh`, then the runner is downloaded,
# checksum-verified, configured and installed as a systemd service on the host through SSM (no SSH in
# this lab). Safe to re-run: a host that is already registered is left alone.
#
# The token is never printed. It sits briefly in an owner-only temp file (removed on exit) and travels
# in the SSM command, which AWS keeps in its command history: it expires within an hour and can only
# register a runner.
# The repo is public, so: fork-PR workflows must wait for your approval before the runner exists
# (this script refuses to continue otherwise), and no workflow that runs on a pull_request may use
# this runner (tools/check_workflows.py enforces it).
set -euo pipefail
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""

here="$(cd "$(dirname "$0")" && pwd)"
role=${1:-app}
case "$role" in app | platform | llm) ;; *) echo "usage: $0 [app|platform|llm]" >&2; exit 2 ;; esac
label="lab-$role"

repo=$(gh repo view --json nameWithOwner -q .nameWithOwner)
policy=$(gh api "repos/$repo/actions/permissions/fork-pr-contributor-approval" -q .approval_policy)
[[ "$policy" == all_external_contributors ]] || {
  echo "refusing: fork-PR approval is '$policy'. Settings > Actions > General > Fork pull request workflows:" >&2
  echo "choose 'Require approval for all external contributors', then re-run." >&2
  exit 1
}

instance=$(terraform -chdir="$here/../infra/live" output -raw "${role}_instance_id")
aws ssm describe-instance-information --filters "Key=InstanceIds,Values=$instance" \
  --query 'InstanceInformationList[0].PingStatus' --output text | grep -qx Online ||
  { echo "$instance is not Online in SSM (is the lab session running?)" >&2; exit 1; }

# Latest runner release, and the SHA-256 GitHub publishes for the arm64 archive.
release=$(gh api repos/actions/runner/releases/latest)
tag=$(jq -r .tag_name <<<"$release")
version=${tag#v}
archive="actions-runner-linux-arm64-$version.tar.gz"
sha=$(jq -r .body <<<"$release" | sed -n "s/.*$archive.*BEGIN SHA linux-arm64 -->\([0-9a-f]\{64\}\)<!--.*/\1/p" | head -1)
[[ -n "$sha" ]] || { echo "could not read the checksum for $archive from the release notes" >&2; exit 1; }

token=$(gh api -X POST "repos/$repo/actions/runners/registration-token" -q .token)

# Runs as root on the host. config.sh refuses root, so it runs as `runner` (created by user-data).
read -r -d '' script <<EOF || true
set -euo pipefail
cd /opt/actions-runner
if [ -f .runner ]; then echo "already configured; nothing to do"; exit 0; fi
curl -fsSL -o runner.tar.gz "https://github.com/actions/runner/releases/download/$tag/$archive"
echo "$sha  runner.tar.gz" | sha256sum -c -
tar xzf runner.tar.gz && rm runner.tar.gz
chown -R runner:runner /opt/actions-runner
sudo -u runner ./config.sh --unattended --replace --disableupdate \
  --url "https://github.com/$repo" --token "$token" --name "$label" --labels "$label"
./svc.sh install runner
./svc.sh start
echo "runner service started"
EOF

# The aws CLI can't read JSON from a pipe, and a command-line argument would show the token in the
# process list. So the request goes through a file only the owner can read, deleted when this exits.
umask 077
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
jq -n --arg id "$instance" --arg s "$script" \
  '{InstanceIds: [$id], DocumentName: "AWS-RunShellScript", Parameters: {commands: [$s]}}' >"$tmp/request.json"
cmd=$(aws ssm send-command --cli-input-json "file://$tmp/request.json" --query Command.CommandId --output text)
echo "sent to $instance (command $cmd); waiting..."

status=InProgress
for _ in $(seq 1 60); do
  status=$(aws ssm get-command-invocation --command-id "$cmd" --instance-id "$instance" --query Status --output text 2>/dev/null || echo Pending)
  case "$status" in Success | Failed | Cancelled | TimedOut) break ;; esac
  sleep 3
done
out=$(aws ssm get-command-invocation --command-id "$cmd" --instance-id "$instance" \
  --query '[StandardOutputContent, StandardErrorContent]' --output text | sed "s/$token/<token>/g")
echo "$out" | tail -15
[[ "$status" == Success ]] || { echo "registration command ended as $status" >&2; exit 1; }

echo "GitHub's view of the runner:"
for _ in $(seq 1 10); do
  line=$(gh api "repos/$repo/actions/runners" -q ".runners[] | select(.name==\"$label\") | \"\(.name) \(.status) labels=\([.labels[].name] | join(\",\"))\"" || true)
  [[ "$line" == *" online "* ]] && { echo "$line"; exit 0; }
  sleep 3
done
echo "${line:-no runner named $label yet}" >&2
exit 1
