#!/usr/bin/env python3
"""Slurm Job Status Notification Daemon.

Watches Slurm job status changes and sends email notifications
when job statuses change. Aggregates changes over a configurable
time window before sending.
"""

import re
import configparser
import shutil
import subprocess
import smtplib
import time
import logging
import getpass
import sys
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, Callable

logger = logging.getLogger("slurm-mailer")

JOB_STATE_LABELS = {
    "PD": "Pending",
    "R": "Running",
    "CG": "Completing",
    "SF": "Suspended",
    "ST": "Stopped",
    "TO": "Timeout",
    "CD": "Completed",
    "CA": "Cancelled",
    "NF": "Node Failure",
    "BO": "Boot Fail",
    "OOM": "Out of Memory",
    "DL": "Deadline",
    "RF": "Refuse",
    "RS": "Resv/Node",
    "SD": "Signaling",
    "SE": "Signal",
}

STATE_COLORS = {
    "PD": "#f59e0b",
    "R": "#10b981",
    "CG": "#3b82f6",
    "SF": "#8b5cf6",
    "ST": "#64748b",
    "TO": "#ef4444",
    "CD": "#22c55e",
    "CA": "#ef4444",
    "NF": "#ef4444",
    "BO": "#ef4444",
    "OOM": "#ef4444",
}

SACCT_STATE_MAP = {
    "COMPLETED": "CD",
    "FAILED": "F",
    "CANCELLED": "CA",
    "TIMEOUT": "TO",
    "NODE_FAIL": "NF",
    "BOOT_FAIL": "BO",
    "OUT_OF_MEMORY": "OOM",
    "DEADLINE": "DL",
    "PREEMPTED": "PR",
    "NODENAME": "NF",
    "RUNNING": "R",
    "PENDING": "PD",
    "SUSPENDED": "S",
    "CONFIGURING": "CF",
    "COMPLETING": "CG",
}

SACCT_STATE_LABELS = {
    "CD": "Completed",
    "F": "Failed",
    "CA": "Cancelled",
    "TO": "Timeout",
    "NF": "Node Failure",
    "BO": "Boot Fail",
    "OOM": "Out of Memory",
    "DL": "Deadline",
    "PR": "Preempted",
    "R": "Running",
    "PD": "Pending",
    "S": "Suspended",
    "CF": "Configuring",
    "CG": "Completing",
}


@dataclass
class SlurmJob:
    job_id: str
    partition: str
    name: str
    state: str
    user: str
    nodes: str
    time_running: str
    reason: str = ""
    from_sacct: bool = False


@dataclass
class EmailConfig:
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    smtp_tls: bool
    from_addr: str
    to_addrs: list[str]
    smtp_ssl: bool = False


@dataclass
class AppConfig:
    email: EmailConfig
    watch_user: Optional[str]
    watch_all_users: bool
    aggregation_interval: int
    poll_interval: int
    squeue_args: list[str]
    sacct_args: list[str]


def _parse_tls_mode(value: str) -> tuple:
    """Map a tls config value to (starttls, ssl) flags."""
    mode = (value or "").strip().lower()
    if mode in ("ssl", "smtps"):
        return False, True
    if mode in ("none", "off", "no", "false", "0", "disabled"):
        return False, False
    return True, False


def parse_config(config_path: str) -> AppConfig:
    cp = configparser.ConfigParser()
    cp.read(config_path)

    smtp_host = cp.get("smtp", "host", fallback="localhost")
    smtp_port = cp.getint("smtp", "port", fallback=587)
    smtp_user = cp.get("smtp", "user", fallback="")
    smtp_password = cp.get("smtp", "password", fallback="")
    smtp_tls, smtp_ssl = _parse_tls_mode(cp.get("smtp", "tls", fallback="starttls"))
    from_addr = cp.get("email", "from", fallback="slurm-mailer@example.com")
    to_addrs = [a.strip() for a in cp.get("email", "to", fallback="").split(",") if a.strip()]
    watch_user = cp.get("slurm", "watch_user", fallback=None)
    watch_all_users = cp.getboolean("slurm", "watch_all_users", fallback=False)
    aggregation_interval = cp.getint("slurm", "aggregation_minutes", fallback=10)
    poll_interval = cp.getint("slurm", "poll_interval_seconds", fallback=30)

    squeue_args = []
    if watch_all_users:
        squeue_args = ["squeue", "--noheader", "-o", "%.2i|%.9P|%.16j|%.8T|%.8u|%.4D|%.10M|%.10l"]
    elif watch_user:
        squeue_args = ["squeue", "--noheader", "-u", watch_user, "-o", "%.2i|%.9P|%.16j|%.8T|%.8u|%.4D|%.10M|%.10l"]
    else:
        squeue_args = ["squeue", "--noheader", "-o", "%.2i|%.9P|%.16j|%.8T|%.8u|%.4D|%.10M|%.10l"]

    sacct_args = [
        "sacct",
        "-S", f"now-{aggregation_interval}minutes",
        "-E", "now",
        "--noheader",
        "-o", "JobID,JobName,State,User,AllocCPUS,Elapsed,Partition",
    ]
    if watch_user:
        sacct_args.extend(["-u", watch_user])

    return AppConfig(
        email=EmailConfig(
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            smtp_user=smtp_user,
            smtp_password=smtp_password,
            smtp_tls=smtp_tls,
            smtp_ssl=smtp_ssl,
            from_addr=from_addr,
            to_addrs=to_addrs,
        ),
        watch_user=watch_user,
        watch_all_users=watch_all_users,
        aggregation_interval=aggregation_interval,
        poll_interval=poll_interval,
        squeue_args=squeue_args,
        sacct_args=sacct_args,
    )


class SlurmMailer:
    def __init__(
        self,
        config: AppConfig,
        subprocess_fn: Optional[Callable] = None,
        smtp_class: Optional[Callable] = None,
    ):
        self.config = config
        self.subprocess_fn = subprocess_fn or subprocess.run
        if smtp_class is not None:
            self.smtp_class = smtp_class
        else:
            self.smtp_class = smtplib.SMTP_SSL if config.email.smtp_ssl else smtplib.SMTP
        self.previous_jobs: dict[str, SlurmJob] = {}
        self.pending_changes: dict[str, dict] = {}
        self.last_aggregation = time.time()

    def _call_subprocess(self, cmd: list[str]) -> subprocess.CompletedProcess:
        return self.subprocess_fn(cmd, capture_output=True, text=True, timeout=30)

    def _fetch_sacct_jobs(self) -> list[SlurmJob]:
        try:
            result = self._call_subprocess(self.config.sacct_args)
        except FileNotFoundError:
            print("Error: sacct command disappeared. Is Slurm installed?", file=sys.stderr)
            sys.exit(1)
        except subprocess.TimeoutExpired:
            logger.error("sacct command timed out")
            return []

        if result.returncode != 0:
            logger.error(f"sacct failed: {result.stderr.strip()}")
            return []

        jobs = []
        for line in result.stdout.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 7:
                continue
            raw_job_id = parts[0].strip()
            base_job_id = raw_job_id.split(".")[0]
            state_full = parts[2].strip().upper()
            short_state = SACCT_STATE_MAP.get(state_full, state_full[:2].upper())
            elapsed = parts[5].strip()
            try:
                time_str = f"{int(elapsed.split(':')[0]):02d}:{int(elapsed.split(':')[1]):02d}" if ':' in elapsed else elapsed
            except (ValueError, IndexError):
                time_str = elapsed
            jobs.append(SlurmJob(
                job_id=base_job_id,
                partition=parts[6].strip(),
                name=parts[1].strip(),
                state=short_state,
                user=parts[3].strip(),
                nodes=parts[4].strip(),
                time_running=time_str,
                from_sacct=True,
            ))
        return jobs

    def _fetch_slurm_jobs(self) -> list[SlurmJob]:
        try:
            result = self._call_subprocess(self.config.squeue_args)
        except FileNotFoundError:
            print("Error: squeue command disappeared. Is Slurm installed?", file=sys.stderr)
            sys.exit(1)
        except subprocess.TimeoutExpired:
            logger.error("squeue command timed out")
            return []

        active_jobs = []
        if result.returncode == 0:
            for line in result.stdout.strip().splitlines():
                line = line.strip()
                if not line:
                    continue
                parts = line.split("|")
                if len(parts) < 8:
                    continue
                active_jobs.append(SlurmJob(
                    job_id=parts[0].strip(),
                    partition=parts[1].strip(),
                    name=parts[2].strip(),
                    state=parts[3].strip().upper(),
                    user=parts[4].strip(),
                    nodes=parts[5].strip(),
                    time_running=parts[6].strip(),
                    reason=parts[7].strip() if len(parts) > 7 else "",
                ))

        historical_jobs = self._fetch_sacct_jobs()
        active_ids = {j.job_id for j in active_jobs}
        merged = list(active_jobs)
        for job in historical_jobs:
            if job.job_id not in active_ids:
                merged.append(job)
                active_ids.add(job.job_id)
        return merged

    def _fetch_job_reason(self, job_id: str) -> str:
        try:
            result = self._call_subprocess(["scontrol", "show", "job", job_id])
            if result.returncode == 0:
                for line in result.stdout.splitlines():
                    if "Reason=" in line:
                        match = re.search(r'Reason=(\S+)', line)
                        if match:
                            return match.group(1)
        except Exception:
            print("Error: Failed to query job reason.", file=sys.stderr)
            sys.exit(1)
        return ""

    def _detect_changes(self, old_jobs: dict[str, SlurmJob], new_jobs: list[SlurmJob]) -> dict[str, dict]:
        new_job_map = {j.job_id: j for j in new_jobs}
        changes = {}
        for job_id, new_job in new_job_map.items():
            old_job = old_jobs.get(job_id)
            if old_job is None:
                changes[job_id] = {
                    "type": "new",
                    "job": new_job,
                    "old_state": None,
                    "new_state": new_job.state,
                }
            elif old_job.state != new_job.state:
                changes[job_id] = {
                    "type": "change",
                    "job": new_job,
                    "old_state": old_job.state,
                    "new_state": new_job.state,
                }
        for job_id in old_jobs:
            if job_id not in new_job_map:
                changes[job_id] = {
                    "type": "gone",
                    "job": old_jobs[job_id],
                    "old_state": old_jobs[job_id].state,
                    "new_state": None,
                }
        return changes

    def _state_badge(self, state: str) -> str:
        color = STATE_COLORS.get(state, "#6b7280")
        label = JOB_STATE_LABELS.get(state, state)
        return f'<span style="display:inline-block;padding:3px 12px;border-radius:4px;font-size:12px;font-weight:600;color:#ffffff;background-color:{color}">{label}</span>'

    def _build_html_email(
        self,
        changes: dict[str, dict],
        current_jobs: list[SlurmJob],
        window_start: datetime,
        window_end: datetime,
    ) -> str:
        change_items_html = ""
        for job_id, change in changes.items():
            job = change["job"]
            if change["type"] == "new":
                change_items_html += f"""
        <tr>
          <td style="padding:14px 18px;border-bottom:1px solid #e5e7eb;vertical-align:top;">
            <div style="font-weight:700;font-size:15px;color:#111827;margin-bottom:4px;">New Job Submitted</div>
            <div style="font-size:13px;color:#4b5563;line-height:1.5;">Job <strong>{job.name}</strong> (ID: <code style="background:#f3f4f6;padding:1px 6px;border-radius:3px;font-size:12px;">{job.job_id}</code>) entered the queue with state {self._state_badge(job.state)}.</div>
          </td>
        </tr>"""
            elif change["type"] == "change":
                change_items_html += f"""
        <tr>
          <td style="padding:14px 18px;border-bottom:1px solid #e5e7eb;vertical-align:top;">
            <div style="font-weight:700;font-size:15px;color:#111827;margin-bottom:4px;">Job Status Changed</div>
            <div style="font-size:13px;color:#4b5563;line-height:1.5;">Job <strong>{job.name}</strong> (ID: <code style="background:#f3f4f6;padding:1px 6px;border-radius:3px;font-size:12px;">{job.job_id}</code>) changed from {self._state_badge(change['old_state'])} to <strong>{self._state_badge(change['new_state'])}</strong>.</div>
          </td>
        </tr>"""
            elif change["type"] == "gone":
                change_items_html += f"""
        <tr>
          <td style="padding:14px 18px;border-bottom:1px solid #e5e7eb;vertical-align:top;">
            <div style="font-weight:700;font-size:15px;color:#111827;margin-bottom:4px;">Job Removed</div>
            <div style="font-size:13px;color:#4b5563;line-height:1.5;">Job <strong>{job.name}</strong> (ID: <code style="background:#f3f4f6;padding:1px 6px;border-radius:3px;font-size:12px;">{job.job_id}</code>) with state {self._state_badge(change['old_state'])} is no longer in the queue.</div>
          </td>
        </tr>"""

        queue_rows = ""
        state_counts = defaultdict(int)
        for job in current_jobs:
            state_counts[job.state] += 1
            queue_rows += f"""
        <tr>
          <td style="padding:10px 16px;border-bottom:1px solid #f3f4f6;font-family:'SF Mono',Monaco,Consolas,monospace;font-size:13px;color:#111827;">{job.job_id}</td>
          <td style="padding:10px 16px;border-bottom:1px solid #f3f4f6;font-size:13px;color:#374151;">{job.name[:20]}</td>
          <td style="padding:10px 16px;border-bottom:1px solid #f3f4f6;font-size:13px;color:#374151;">{job.user}</td>
          <td style="padding:10px 16px;border-bottom:1px solid #f3f4f6;text-align:center;">{self._state_badge(job.state)}</td>
          <td style="padding:10px 16px;border-bottom:1px solid #f3f4f6;font-size:13px;text-align:center;color:#374151;">{job.nodes}</td>
          <td style="padding:10px 16px;border-bottom:1px solid #f3f4f6;font-size:13px;text-align:center;color:#9ca3af;">{job.time_running}</td>
        </tr>"""

        state_summary_items = ""
        for state_code, count in sorted(state_counts.items(), key=lambda x: -x[1]):
            color = STATE_COLORS.get(state_code, "#6b7280")
            label = JOB_STATE_LABELS.get(state_code, state_code)
            state_summary_items += f"""
        <div style="display:inline-flex;align-items:center;gap:8px;padding:8px 16px;border-radius:6px;background:#f9fafb;border:1px solid #e5e7eb;margin:4px;">
          <span style="width:8px;height:8px;border-radius:50%;background-color:{color};display:inline-block;"></span>
          <span style="font-size:13px;font-weight:500;color:#374151;">{label}</span>
          <span style="font-size:14px;font-weight:700;color:#111827;">{count}</span>
        </div>"""

        total_jobs = len(current_jobs)
        agg_text = f"{self.config.aggregation_interval} minute{'s' if self.config.aggregation_interval > 1 else ''}"
        watch_text = "All Users" if self.config.watch_all_users else (self.config.watch_user or "Current User")

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Slurm Job Status Notification</title>
</head>
<body style="margin:0;padding:0;background-color:#f4f5f7;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f5f7;padding:40px 20px;">
    <tr>
      <td align="center">
        <table role="presentation" width="660" cellpadding="0" cellspacing="0" style="background-color:#ffffff;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,0.08),0 1px 2px rgba(0,0,0,0.06);overflow:hidden;">
          <tr>
            <td style="background:#1e293b;padding:28px 40px;text-align:center;">
              <h1 style="margin:0;font-size:22px;font-weight:700;color:#ffffff;letter-spacing:-0.3px;">Slurm Job Monitor</h1>
              <p style="margin:6px 0 0;font-size:13px;color:#94a3b8;">Job Status Notification Report</p>
            </td>
          </tr>
          <tr>
            <td style="padding:16px 40px;background-color:#f8fafc;border-bottom:1px solid #e2e8f0;">
              <table role="presentation" width="100%">
                <tr>
                  <td style="font-size:12px;color:#64748b;">
                    <strong>Watch:</strong> {watch_text} &nbsp;|&nbsp; <strong>Window:</strong> {window_start.strftime('%Y-%m-%d %H:%M')} &ndash; {window_end.strftime('%H:%M')}
                  </td>
                </tr>
              </table>
            </td>
          </tr>
          <tr>
            <td style="padding:24px 40px;background-color:#f8fafc;border-bottom:1px solid #e2e8f0;">
              <table role="presentation" width="100%">
                <tr>
                  <td style="text-align:center;border-right:1px solid #e2e8f0;">
                    <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:1px;font-weight:600;">Total Jobs</div>
                    <div style="font-size:36px;font-weight:800;color:#0f172a;margin-top:4px;">{total_jobs}</div>
                  </td>
                  <td style="text-align:center;border-right:1px solid #e2e8f0;">
                    <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:1px;font-weight:600;">Changes</div>
                    <div style="font-size:36px;font-weight:800;color:#0f172a;margin-top:4px;">{len(changes)}</div>
                  </td>
                  <td style="text-align:center;">
                    <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:1px;font-weight:600;">Aggregation</div>
                    <div style="font-size:22px;font-weight:700;color:#0f172a;margin-top:4px;">{agg_text}</div>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
          <tr>
            <td style="padding:0 40px;">
              <h2 style="font-size:16px;font-weight:700;color:#0f172a;margin:24px 0 8px;">Status Changes</h2>
              {"<p style='font-size:14px;color:#94a3b8;padding:20px;background:#f8fafc;border-radius:8px;text-align:center;'>No status changes detected during this period.</p>" if not change_items_html else f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tbody>{change_items_html}</tbody></table>'}
            </td>
          </tr>
          <tr>
            <td style="padding:0 40px;">
              <h2 style="font-size:16px;font-weight:700;color:#0f172a;margin:24px 0 8px;">Queue Summary</h2>
              <div style="display:flex;flex-wrap:wrap;gap:6px;">
                {state_summary_items}
              </div>
            </td>
          </tr>
          <tr>
            <td style="padding:0 40px 40px;">
              <h2 style="font-size:16px;font-weight:700;color:#0f172a;margin:24px 0 8px;">Current Queue</h2>
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;">
                <thead>
                  <tr style="background-color:#f8fafc;">
                    <th style="padding:12px 16px;text-align:left;font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid #e2e8f0;">Job ID</th>
                    <th style="padding:12px 16px;text-align:left;font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid #e2e8f0;">Name</th>
                    <th style="padding:12px 16px;text-align:left;font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid #e2e8f0;">User</th>
                    <th style="padding:12px 16px;text-align:center;font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid #e2e8f0;">State</th>
                    <th style="padding:12px 16px;text-align:center;font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid #e2e8f0;">Nodes</th>
                    <th style="padding:12px 16px;text-align:center;font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid #e2e8f0;">Time</th>
                  </tr>
                </thead>
                <tbody>
                  {queue_rows}
                </tbody>
              </table>
              {"<p style='font-size:14px;color:#94a3b8;padding:24px;text-align:center;background:#f8fafc;margin:0;'>No jobs currently in the queue.</p>" if not queue_rows else ''}
            </td>
          </tr>
          <tr>
            <td style="padding:20px 40px;background-color:#f8fafc;border-top:1px solid #e2e8f0;text-align:center;">
              <p style="margin:0;font-size:11px;color:#94a3b8;">
                Slurm Mailer &nbsp;&bull;&nbsp; Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} &nbsp;&bull;&nbsp; Automated notification
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""
        return html

    def _send_email(self, subject: str, html_body: str) -> bool:
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.config.email.from_addr
            msg["To"] = ", ".join(self.config.email.to_addrs)
            msg.attach(MIMEText(html_body, "html"))

            with self.smtp_class(self.config.email.smtp_host, self.config.email.smtp_port) as server:
                if self.config.email.smtp_tls:
                    if hasattr(server, "starttls"):
                        server.starttls()
                if self.config.email.smtp_user:
                    server.login(self.config.email.smtp_user, self.config.email.smtp_password)
                server.sendmail(self.config.email.from_addr, self.config.email.to_addrs, msg.as_string())

            logger.info(f"Email sent to {', '.join(self.config.email.to_addrs)}")
            return True
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            sys.exit(1)

    def run(self) -> None:
        for tool_name in ["squeue", "sacct"]:
            if shutil.which(tool_name) is None:
                print(f"Error: Slurm client not found: {tool_name}", file=sys.stderr)
                print("Install Slurm client tools or ensure they are in your PATH.", file=sys.stderr)
                sys.exit(1)

        try:
            result = self.subprocess_fn(
                ["squeue", "--noheader", "-o", "%.2i"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode != 0:
                print(f"Error: Cannot connect to Slurm daemon (squeue exited with code {result.returncode})", file=sys.stderr)
                print(f"squeue stderr: {result.stderr.strip()}", file=sys.stderr)
                sys.exit(1)
        except FileNotFoundError:
            print("Error: squeue command not found. Is Slurm installed?", file=sys.stderr)
            sys.exit(1)
        except subprocess.TimeoutExpired:
            print("Error: squeue timed out. Is the Slurm daemon (slurmctld) reachable?", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Error: Failed to query Slurm: {e}", file=sys.stderr)
            sys.exit(1)

        logger.info("Starting Slurm job monitor...")
        logger.info(f"Watch mode: {'all users' if self.config.watch_all_users else self.config.watch_user or 'current user'}")
        logger.info(f"Poll interval: {self.config.poll_interval}s, Aggregation: {self.config.aggregation_interval}min")

        while True:
            try:
                current_jobs = self._fetch_slurm_jobs()
                current_job_map = {j.job_id: j for j in current_jobs}

                changes = self._detect_changes(self.previous_jobs, current_jobs)
                for job_id, change in changes.items():
                    self.pending_changes[job_id] = change

                now = time.time()
                if now - self.last_aggregation >= self.config.aggregation_interval * 60 and self.pending_changes:
                    logger.info(f"Aggregation window reached. Sending notification for {len(self.pending_changes)} change(s).")
                    window_start = datetime.now() - timedelta(minutes=self.config.aggregation_interval)
                    window_end = datetime.now()
                    subject = f"Slurm Job Status Update — {len(self.pending_changes)} change(s) detected"
                    html_body = self._build_html_email(self.pending_changes, current_jobs, window_start, window_end)
                    self._send_email(subject, html_body)
                    self.pending_changes = {}
                    self.last_aggregation = now

                self.previous_jobs = current_job_map
                time.sleep(self.config.poll_interval)

            except KeyboardInterrupt:
                logger.info("Shutting down Slurm mailer.")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                sys.exit(1)


# ---------------------------------------------------------------------------
# Setup wizard (slurm-mailer-setup)
# ---------------------------------------------------------------------------

TLS_MODES = [
    ("starttls", "StartTLS upgrade on the given port (recommended, port 587)"),
    ("ssl", "Implicit TLS from the start (port 465)"),
    ("none", "No encryption (trusted local relay only)"),
]

WATCH_MODES = [
    ("me", "My jobs only"),
    ("user", "A specific user's jobs"),
    ("all", "All jobs visible to me"),
]


def prompt_str(message: str, default: Optional[str] = None, input_fn=input) -> str:
    suffix = f" [{default}]" if default else ""
    while True:
        raw = input_fn(f"{message}{suffix}: ").strip()
        if raw:
            return raw
        if default is not None:
            return default
        print("A value is required.")


def prompt_bool(message: str, default: bool = True, input_fn=input) -> bool:
    hint = "Y/n" if default else "y/N"
    while True:
        raw = input_fn(f"{message} [{hint}]: ").strip().lower()
        if not raw:
            return default
        if raw in ("y", "yes", "true", "1"):
            return True
        if raw in ("n", "no", "false", "0"):
            return False
        print("Please answer yes or no.")


def prompt_int(message: str, default: int, input_fn=input, min_value: Optional[int] = None, max_value: Optional[int] = None) -> int:
    while True:
        raw = input_fn(f"{message} [{default}]: ").strip()
        if not raw:
            value = default
        else:
            try:
                value = int(raw)
            except ValueError:
                print("Please enter a number.")
                continue
        if min_value is not None and value < min_value:
            print(f"Minimum is {min_value}.")
            continue
        if max_value is not None and value > max_value:
            print(f"Maximum is {max_value}.")
            continue
        return value


def prompt_choice(message: str, options: list, default_key: Optional[str] = None, input_fn=input) -> str:
    print(f"{message}:")
    keys = []
    for i, (key, label) in enumerate(options, 1):
        keys.append(key)
        marker = " (default)" if key == default_key else ""
        print(f"  {i}. {label}{marker}")
    valid_numbers = {str(i) for i in range(1, len(options) + 1)}
    while True:
        suffix = f" [{default_key}]" if default_key else ""
        raw = input_fn(f"Choice{suffix}: ").strip().lower()
        if not raw and default_key:
            return default_key
        if raw in valid_numbers:
            return options[int(raw) - 1][0]
        if raw in keys:
            return raw
        print("Invalid choice.")


def prompt_password(message: str, getpass_fn=None) -> str:
    fn = getpass_fn or getpass.getpass
    return fn(f"{message}: ").strip()


@dataclass
class WizardAnswers:
    smtp_host: str
    smtp_port: int
    smtp_tls_mode: str
    smtp_user: str
    smtp_password: str
    from_addr: str
    to_addrs: list[str]
    watch_user: str
    watch_all_users: bool
    poll_interval: int
    aggregation_minutes: int


def render_config(answers: WizardAnswers) -> str:
    return (
        "# Slurm Mailer configuration\n"
        "# Generated by slurm-mailer-setup. Do not commit this file;\n"
        "# it contains your SMTP password.\n"
        "\n"
        "[smtp]\n"
        f"host = {answers.smtp_host}\n"
        f"port = {answers.smtp_port}\n"
        f"tls = {answers.smtp_tls_mode}\n"
        f"user = {answers.smtp_user}\n"
        f"password = {answers.smtp_password}\n"
        "\n"
        "[email]\n"
        f"from = {answers.from_addr}\n"
        f"to = {', '.join(answers.to_addrs)}\n"
        "\n"
        "[slurm]\n"
        f"watch_user = {answers.watch_user}\n"
        f"watch_all_users = {'true' if answers.watch_all_users else 'false'}\n"
        f"poll_interval_seconds = {answers.poll_interval}\n"
        f"aggregation_minutes = {answers.aggregation_minutes}\n"
    )


def render_unit(config_path: str, exec_start: str, path_value: str) -> str:
    return (
        "[Unit]\n"
        "Description=Slurm job status notification daemon\n"
        "After=network-online.target\n"
        "\n"
        "[Service]\n"
        "Type=simple\n"
        f"ExecStart={exec_start}\n"
        f"Environment=SLURM_MAILER_CONFIG={config_path}\n"
        f"Environment=PATH={path_value}\n"
        "Restart=on-failure\n"
        "RestartSec=10\n"
        "\n"
        "[Install]\n"
        "WantedBy=default.target\n"
    )


def resolve_exec_start(which_fn=None) -> str:
    which = which_fn or shutil.which
    path = which("slurm-mailer")
    if path:
        return path
    return f"{sys.executable} {os.path.abspath(__file__)}"


def write_private(path: str, text: str, overwrite: bool = False) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, mode=0o700, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    if not overwrite:
        flags |= os.O_EXCL
    fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write(text)
    finally:
        os.chmod(path, 0o600)


def send_test_email(email_config: EmailConfig, smtp_class: Optional[Callable] = None) -> Optional[str]:
    """Send a test message. Return None on success or an error message on failure."""
    msg = MIMEText(
        "<p>This is a test message from the <strong>slurm-mailer</strong> setup wizard.</p>",
        "html",
    )
    msg["Subject"] = "slurm-mailer test email"
    msg["From"] = email_config.from_addr
    msg["To"] = ", ".join(email_config.to_addrs)
    cls = smtp_class or (smtplib.SMTP_SSL if email_config.smtp_ssl else smtplib.SMTP)
    try:
        with cls(email_config.smtp_host, email_config.smtp_port) as server:
            if email_config.smtp_tls and hasattr(server, "starttls"):
                server.starttls()
            if email_config.smtp_user:
                server.login(email_config.smtp_user, email_config.smtp_password)
            server.sendmail(email_config.from_addr, email_config.to_addrs, msg.as_string())
    except Exception as exc:
        return str(exc) or exc.__class__.__name__
    return None


def run_setup(
    input_fn=input,
    getpass_fn=None,
    smtp_class=None,
    env: Optional[dict] = None,
    which_fn=None,
    username: Optional[str] = None,
) -> int:
    environ = env if env is not None else os.environ
    which = which_fn or shutil.which
    home = environ.get("HOME") or os.path.expanduser("~")
    xdg = environ.get("XDG_CONFIG_HOME") or os.path.join(home, ".config")

    print("slurm-mailer setup wizard")
    print("=" * 60)
    print("Press Enter to accept the value shown in [brackets].\n")

    print("SMTP server")
    print("-" * 60)
    smtp_host = prompt_str("SMTP host", "localhost", input_fn)
    tls_mode = prompt_choice("Encryption", TLS_MODES, default_key="starttls", input_fn=input_fn)
    default_port = 465 if tls_mode == "ssl" else 587
    smtp_port = prompt_int("SMTP port", default_port, input_fn, min_value=1, max_value=65535)
    smtp_user = prompt_str("SMTP username (empty for none)", "", input_fn)
    smtp_password = ""
    if smtp_user:
        smtp_password = prompt_password("SMTP password (empty for none)", getpass_fn)
    else:
        print("No SMTP username given; skipping password.")

    print("\nEmail")
    print("-" * 60)
    from_addr = prompt_str("From address", None, input_fn)
    while True:
        to_raw = prompt_str("Recipient addresses (comma-separated)", None, input_fn)
        to_addrs = [a.strip() for a in to_raw.split(",") if a.strip()]
        if to_addrs and all("@" in a for a in to_addrs):
            break
        print("Invalid address list. Example: alice@example.com, bob@example.com")

    email_cfg = EmailConfig(
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        smtp_tls=tls_mode == "starttls",
        smtp_ssl=tls_mode == "ssl",
        from_addr=from_addr,
        to_addrs=to_addrs,
    )

    print()
    if prompt_bool("Send a test email now?", True, input_fn):
        while True:
            error = send_test_email(email_cfg, smtp_class)
            if error is None:
                print("Test email sent successfully.")
                break
            print(f"Test email failed: {error}")
            if not prompt_bool("Retry?", True, input_fn):
                print("Continuing without a verified connection.")
                break

    print("\nSlurm monitoring")
    print("-" * 60)
    mode = prompt_choice("Watch mode", WATCH_MODES, default_key="me", input_fn=input_fn)
    watch_user = ""
    watch_all_users = False
    if mode == "me":
        watch_user = username if username is not None else getpass.getuser()
    elif mode == "user":
        watch_user = prompt_str("Username to watch", None, input_fn)
    else:
        watch_all_users = True
    poll_interval = prompt_int("Poll interval in seconds", 30, input_fn, min_value=1)
    aggregation_minutes = prompt_int("Aggregation window in minutes", 10, input_fn, min_value=1)

    answers = WizardAnswers(
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_tls_mode=tls_mode,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        from_addr=from_addr,
        to_addrs=to_addrs,
        watch_user=watch_user,
        watch_all_users=watch_all_users,
        poll_interval=poll_interval,
        aggregation_minutes=aggregation_minutes,
    )

    print("\nFiles")
    print("-" * 60)
    default_config = os.path.join(xdg, "slurm-mailer", "config.ini")
    config_path = os.path.abspath(os.path.expanduser(prompt_str("Configuration file path", default_config, input_fn)))
    overwrite = False
    if os.path.exists(config_path):
        if not prompt_bool(f"{config_path} already exists. Overwrite?", False, input_fn):
            print("Aborted: existing configuration was not modified.")
            return 1
        overwrite = True
    write_private(config_path, render_config(answers), overwrite=overwrite)

    default_unit = os.path.join(xdg, "systemd", "user", "slurm-mailer.service")
    unit_path = os.path.abspath(os.path.expanduser(prompt_str("systemd user unit path", default_unit, input_fn)))
    write_unit = True
    unit_overwrite = False
    if os.path.exists(unit_path):
        if prompt_bool(f"{unit_path} already exists. Overwrite?", False, input_fn):
            unit_overwrite = True
        else:
            print("Skipping systemd unit generation.")
            write_unit = False
    if write_unit:
        unit_text = render_unit(
            config_path,
            resolve_exec_start(which),
            environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        )
        write_private(unit_path, unit_text, overwrite=unit_overwrite)

    print()
    print("Setup complete.")
    print("=" * 60)
    print(f"Configuration: {config_path} (mode 0600)")
    if write_unit:
        print(f"Service unit:   {unit_path}")
        print()
        print("Next steps (run these yourself):")
        print("  systemctl --user daemon-reload")
        print("  systemctl --user enable --now slurm-mailer.service")
        print("  journalctl --user -u slurm-mailer -f")
        print()
        print("To keep the daemon running after logout:")
        print("  loginctl enable-linger $USER")
    else:
        print("Service unit:   not written")
        print()
        print("Run the daemon manually with: slurm-mailer")
    return 0


def setup_main() -> int:
    try:
        return run_setup()
    except (KeyboardInterrupt, EOFError):
        print("\nAborted.")
        return 130


def main():
    config_path = os.environ.get("SLURM_MAILER_CONFIG", "config.ini")

    if not os.path.exists(config_path):
        print(f"Error: Config file not found at {config_path}", file=sys.stderr)
        print("Run 'slurm-mailer-setup' to create one, or set SLURM_MAILER_CONFIG.", file=sys.stderr)
        sys.exit(1)

    config = parse_config(config_path)

    if not config.email.to_addrs:
        logger.error("No recipient email addresses configured.")
        sys.exit(1)

    if not config.email.smtp_host:
        print("Error: SMTP host is not configured.", file=sys.stderr)
        sys.exit(1)
    if not config.email.from_addr:
        print("Error: SMTP 'from' address is not configured.", file=sys.stderr)
        sys.exit(1)

    mailer = SlurmMailer(config)
    mailer.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("slurm-mailer")
    main()
