#!/usr/bin/env python3
"""
Provision the MCP stream-abuse alert: Cloud Logging metric + Cloud
Monitoring alert policy that fires when one identity repeatedly exceeds
its standalone GET /mcp stream budget -- the signal that produced the
end-of-May Cloud Run cost spike.

Why this exists:
    mcp_stream_guard.py budgets standalone GET /mcp SSE streams per
    identity and emits `MCP_STREAM_THRESHOLD` whenever an identity crosses
    the concurrent-stream cap (BOOMI_MCP_MAX_GET_STREAMS_PER_IDENTITY).
    A buggy or orphaned client (e.g. leaked mcp-remote bridge processes)
    produces a storm of these events as it repeatedly reconnects. Without
    an alert, operators only learn about it from the next month's bill.

    The alert provides a human-in-the-loop circuit-breaker: when the
    threshold-event rate spikes, an operator gets paged and can flip the
    kill switch `BOOMI_MCP_GET_MODE=post_only` on the Cloud Run service to
    stop GET SSE streams entirely while investigating.

What it does:
    1. Creates (or skips) a Cloud Logging log-based counter metric
       `boomi-mcp-stream-threshold` whose filter matches the
       `MCP_STREAM_THRESHOLD` line emitted by mcp_stream_guard.py.
    2. Creates (or skips) a Cloud Monitoring alert policy
       `boomi-mcp-stream-threshold-rate` that fires when the metric
       exceeds --threshold (default 5) in --duration-seconds (default
       300) summed across all instances.
    3. Attaches one or more notification channels passed via
       --notification-channel (repeatable). If none, the policy is
       created without channels (operator attaches later).

When to run:
    Once, by an operator with `gcloud auth` against project boomimcp.
    Re-runs are idempotent: existing metric/policy are skipped unless
    --update is passed (which deletes and recreates them).

Usage:
    # Review what would be created without touching GCP:
    python scripts/setup_mcp_stream_alert.py --dry-run

    # Run for real with a notification channel:
    python scripts/setup_mcp_stream_alert.py \\
        --notification-channel projects/boomimcp/notificationChannels/12345

Separately, create a GCP billing budget alert for the billing account
(Billing > Budgets & alerts in the console, or `gcloud billing budgets
create`) so a future cost regression pages independently of the logs.

Exit code 0 on clean success or all-skip; 1 on any subprocess failure.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Sequence

# Optional .env autoload for parity with other scripts/. The script
# itself does not read MONGODB_URI etc. -- all values come from CLI
# flags -- but keeping the autoload preserves the project convention.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_ENV = _REPO_ROOT / ".env"
if _ENV.exists():
    try:
        from dotenv import load_dotenv

        load_dotenv(_ENV)
    except ImportError:
        pass


# Exact event token emitted by mcp_stream_guard.py when an identity crosses
# the GET stream cap. Do NOT change without updating the guard's log event
# too -- the metric filter depends on this substring.
THRESHOLD_EVENT = "MCP_STREAM_THRESHOLD"

DEFAULT_PROJECT = "boomimcp"
DEFAULT_SERVICE = "boomi-mcp-server"
DEFAULT_REGION = "us-central1"
DEFAULT_METRIC_NAME = "boomi-mcp-stream-threshold"
DEFAULT_POLICY_NAME = "boomi-mcp-stream-threshold-rate"
DEFAULT_THRESHOLD = 5
DEFAULT_DURATION_SECONDS = 300


def build_metric_filter(service: str) -> str:
    """Return the Cloud Logging filter that scopes the metric to the
    MCP_STREAM_THRESHOLD line emitted by mcp_stream_guard.py.

    Filter components:
      - Cloud Run service (so dev/staging deployments don't bleed in).
      - Exact event token substring of the emitted message.

    Note: unlike the corruption alert, this filter does NOT pin
    severity. The guard logs MCP_STREAM_THRESHOLD at WARNING, and matching
    on the event token alone keeps the filter robust if the log level is
    ever tuned.
    """
    return (
        'resource.type="cloud_run_revision"\n'
        f'resource.labels.service_name="{service}"\n'
        f'textPayload:"{THRESHOLD_EVENT}"'
    )


def build_policy_json(
    *,
    policy_name: str,
    metric_name: str,
    project: str,
    service: str,
    region: str,
    threshold: int,
    duration_seconds: int,
    notification_channels: Sequence[str],
) -> dict:
    """Return the Cloud Monitoring alert policy as a dict that can be
    serialized to JSON and passed to `gcloud alpha monitoring policies
    create --policy-from-file=...`.

    Alignment choice mirrors the corruption alert (ALIGN_DELTA on a
    counter, REDUCE_SUM across instances, duration='0s'): "more than
    `threshold` MCP_STREAM_THRESHOLD events in any `duration_seconds`
    window across all Cloud Run instances", firing on the first breach.

    The embedded documentation shows the responder the EXACT kill-switch
    command for the deployment that produced this policy. It uses
    `--update-env-vars` (additive) rather than `--set-env-vars`
    (destructive) so the emergency rollout cannot drop required OIDC /
    Mongo / session settings while flipping the GET mode.
    """
    metric_type = f"logging.googleapis.com/user/{metric_name}"
    return {
        "displayName": policy_name,
        "documentation": {
            "content": (
                "mcp_stream_guard.py budgets standalone GET /mcp SSE streams per "
                "identity and emits MCP_STREAM_THRESHOLD when an identity crosses the "
                "concurrent-stream cap. This policy fires when more than "
                f"{threshold} such events occur within any {duration_seconds}s window -- "
                "a circuit-breaker against a buggy/orphaned client holding GET SSE "
                "streams open on request-billed Cloud Run (the end-of-May cost spike).\n\n"
                "**Immediate kill switch**: "
                f"`gcloud run services update {service} "
                f"--region {region} --project {project} "
                "--update-env-vars BOOMI_MCP_GET_MODE=post_only` "
                "to stop all GET /mcp SSE streams (GET returns 405; POST/DELETE "
                "unaffected) while investigating. Revert with BOOMI_MCP_GET_MODE=bounded.\n\n"
                "Also confirm a GCP billing budget alert is configured for the billing "
                "account so cost regressions page independently."
            ),
            "mimeType": "text/markdown",
        },
        "conditions": [
            {
                "displayName": (
                    f"More than {threshold} MCP_STREAM_THRESHOLD events in {duration_seconds}s"
                ),
                "conditionThreshold": {
                    "filter": (
                        f'metric.type="{metric_type}" '
                        'AND resource.type="cloud_run_revision"'
                    ),
                    "comparison": "COMPARISON_GT",
                    "thresholdValue": threshold,
                    "duration": "0s",
                    "aggregations": [
                        {
                            "alignmentPeriod": f"{duration_seconds}s",
                            "perSeriesAligner": "ALIGN_DELTA",
                            "crossSeriesReducer": "REDUCE_SUM",
                        }
                    ],
                },
            }
        ],
        "combiner": "OR",
        "notificationChannels": list(notification_channels),
        "enabled": True,
    }


def gcloud_run(
    args: Sequence[str],
    *,
    dry_run: bool,
    capture: bool = False,
) -> subprocess.CompletedProcess:
    """Run a gcloud command. In --dry-run mode, print and skip."""
    cmd = ["gcloud", *args]
    if dry_run:
        print("[DRY-RUN]", " ".join(_quote(a) for a in cmd))
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")
    return subprocess.run(
        cmd,
        check=False,
        capture_output=capture,
        text=True,
    )


def _quote(token: str) -> str:
    """Shell-quote a token for human-readable dry-run output."""
    if not token or any(c in token for c in " \t\n\"'$`\\(){}[]<>|&;*?"):
        return "'" + token.replace("'", "'\"'\"'") + "'"
    return token


def gcloud_authed() -> str | None:
    """Return the currently authenticated gcloud account, or None."""
    result = subprocess.run(
        ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    account = result.stdout.strip()
    return account or None


def metric_exists(*, project: str, metric_name: str) -> bool:
    result = subprocess.run(
        [
            "gcloud",
            "logging",
            "metrics",
            "describe",
            metric_name,
            f"--project={project}",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def policy_exists(*, project: str, policy_name: str) -> bool:
    result = subprocess.run(
        [
            "gcloud",
            "alpha",
            "monitoring",
            "policies",
            "list",
            f"--project={project}",
            f"--filter=displayName=\"{policy_name}\"",
            "--format=value(name)",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return False
    return bool(result.stdout.strip())


def find_policy_id(*, project: str, policy_name: str) -> str | None:
    result = subprocess.run(
        [
            "gcloud",
            "alpha",
            "monitoring",
            "policies",
            "list",
            f"--project={project}",
            f"--filter=displayName=\"{policy_name}\"",
            "--format=value(name)",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    name = result.stdout.strip().splitlines()
    return name[0] if name else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n", 1)[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--service", default=DEFAULT_SERVICE)
    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help=(
            "Cloud Run region for the service. Used in the alert "
            "documentation's kill-switch command so a non-default-region "
            "install points the responder at the right service."
        ),
    )
    parser.add_argument("--metric-name", default=DEFAULT_METRIC_NAME)
    parser.add_argument("--policy-name", default=DEFAULT_POLICY_NAME)
    parser.add_argument("--threshold", type=int, default=DEFAULT_THRESHOLD)
    parser.add_argument("--duration-seconds", type=int, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument(
        "--notification-channel",
        action="append",
        default=[],
        metavar="CHANNEL_ID",
        help=(
            "Full notification channel resource name, "
            "e.g. projects/boomimcp/notificationChannels/12345. Repeatable."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the gcloud commands and the policy JSON without executing.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help=(
            "If the metric or policy already exists, delete and recreate. "
            "Default behavior is to skip existing resources."
        ),
    )
    args = parser.parse_args(argv)

    # Verify gcloud auth (skip in dry-run -- caller is reviewing, not executing)
    if not args.dry_run:
        account = gcloud_authed()
        if account is None:
            print(
                "ERROR: no active gcloud account. Run `gcloud auth login` first.",
                file=sys.stderr,
            )
            return 1
        print(f"Using gcloud account: {account}")

    metric_filter = build_metric_filter(args.service)
    print(f"Project: {args.project}")
    print(f"Service: {args.service}")
    print(f"Region:  {args.region}")
    print(f"Metric:  {args.metric_name}")
    print(f"Policy:  {args.policy_name}")
    print(f"Threshold: more than {args.threshold} events in {args.duration_seconds}s")
    print(f"Notification channels: {args.notification_channel or '(none -- attach later in console)'}")
    print()
    print("Metric filter:")
    for line in metric_filter.splitlines():
        print(f"  {line}")
    print()

    # --- 1. Create or skip metric ---
    if not args.dry_run and metric_exists(project=args.project, metric_name=args.metric_name):
        if args.update:
            print(f"Metric {args.metric_name} exists; deleting (--update).")
            del_metric = gcloud_run(
                [
                    "logging",
                    "metrics",
                    "delete",
                    args.metric_name,
                    f"--project={args.project}",
                    "--quiet",
                ],
                dry_run=False,
            )
            if del_metric.returncode != 0:
                print(f"ERROR: failed to delete existing metric: {del_metric.stderr}",
                      file=sys.stderr)
                return 1
        else:
            print(f"Metric {args.metric_name} already exists; skipping.")

    if args.dry_run or args.update or not metric_exists(
        project=args.project, metric_name=args.metric_name
    ):
        create_metric = gcloud_run(
            [
                "logging",
                "metrics",
                "create",
                args.metric_name,
                f"--project={args.project}",
                "--description=Counts MCP_STREAM_THRESHOLD lines from mcp_stream_guard.py",
                f"--log-filter={metric_filter}",
            ],
            dry_run=args.dry_run,
        )
        if create_metric.returncode != 0:
            print(f"ERROR: failed to create metric: {create_metric.stderr}",
                  file=sys.stderr)
            return 1

    # --- 2. Build policy JSON ---
    policy = build_policy_json(
        policy_name=args.policy_name,
        metric_name=args.metric_name,
        project=args.project,
        service=args.service,
        region=args.region,
        threshold=args.threshold,
        duration_seconds=args.duration_seconds,
        notification_channels=args.notification_channel,
    )
    policy_json = json.dumps(policy, indent=2)

    if args.dry_run:
        print("Policy JSON that would be created:")
        for line in policy_json.splitlines():
            print(f"  {line}")
        print()

    # --- 3. Create or skip policy ---
    if not args.dry_run and policy_exists(project=args.project, policy_name=args.policy_name):
        if args.update:
            policy_id = find_policy_id(project=args.project, policy_name=args.policy_name)
            if policy_id:
                print(f"Policy {args.policy_name} exists; deleting (--update).")
                del_policy = gcloud_run(
                    [
                        "alpha",
                        "monitoring",
                        "policies",
                        "delete",
                        policy_id,
                        f"--project={args.project}",
                        "--quiet",
                    ],
                    dry_run=False,
                )
                if del_policy.returncode != 0:
                    print(f"ERROR: failed to delete existing policy: {del_policy.stderr}",
                          file=sys.stderr)
                    return 1
        else:
            print(f"Policy {args.policy_name} already exists; skipping.")
            print("Done. (No changes -- pass --update to recreate.)")
            return 0

    if args.dry_run:
        # Skip the tempfile entirely -- the JSON is already printed
        # above. Show a placeholder path so the operator sees the
        # shape of the actual command they would run.
        gcloud_run(
            [
                "alpha",
                "monitoring",
                "policies",
                "create",
                f"--project={args.project}",
                "--policy-from-file=<policy-json-shown-above>",
            ],
            dry_run=True,
        )
    else:
        with tempfile.NamedTemporaryFile(
            "w", suffix=".json", delete=False, prefix="mcp-stream-policy-"
        ) as fh:
            fh.write(policy_json)
            policy_path = fh.name
        try:
            create_policy = gcloud_run(
                [
                    "alpha",
                    "monitoring",
                    "policies",
                    "create",
                    f"--project={args.project}",
                    f"--policy-from-file={policy_path}",
                ],
                dry_run=False,
            )
            if create_policy.returncode != 0:
                print(f"ERROR: failed to create policy: {create_policy.stderr}",
                      file=sys.stderr)
                return 1
        finally:
            try:
                os.unlink(policy_path)
            except OSError:
                pass

    print()
    if args.dry_run:
        print("DRY-RUN complete. No GCP changes were made.")
    else:
        print(f"Done. Metric {args.metric_name} and policy {args.policy_name} ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
