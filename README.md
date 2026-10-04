# slurm-mailer

Slurm Job Status Notification Daemon

A tool for monitoring Slurm job status changes and sending email notifications
when jobs transition between states. Changes are aggregated over a configurable
time window before sending.

---

## ⚠️ Important Notice

**This project is AI-generated.** The code has not been thoroughly reviewed by a
human developer for all edge cases, security implications, or production-readiness.
Use at your own risk.

---

## User-Space Notification

This tool operates entirely on the Slurm client side. **No Slurm administrator
privileges or configuration changes are required.**

- Runs by polling `squeue` and `sacct` like any Slurm user can do
- No modifications to `slurm.conf`, `sacct.conf`, or admin partitions required
- Can monitor only your own jobs, or all visible jobs if permissions allow
- No system-wide impact; runs as a regular daemon

### How It Differs from Admin Notifications

| Admin System Notification | This Tool |
|---|---|
| Requires `slurm.conf` modifications | Runs as standalone daemon |
| Admin sets recipients | User configures their own email |
| Affects all users | Only monitors specified user's jobs |
| Requires restart of daemons | Runs independently |
| Site-wide policy | User-controlled |

---

## Features

- **Dual querying**: Uses both `squeue` (active jobs) and `sacct` (completed/failed jobs)
- **Change detection** with aggregation window
- **Rich HTML email** with inline CSS (emoji-free)
- **Configurable polling interval** and aggregation window
- **Aggregation** of multiple changes before sending
- **Status badge** colors and labels

### User-Space Benefits

- No Slurm admin required
- No system configuration changes
- Works on shared/HPC systems where users cannot modify config
- User controls who receives notifications
- Can monitor any user within permission limits

---

## Installation

```bash
uv sync
# or install the wheel
uv pip install dist/slurm_mailer-0.1.0-py3-none-any.whl
```

## Configuration

Copy `config.ini` and adjust the settings:

```ini
[smtp]
host = smtp.example.com
port = 587
tls = true
user = your_user
password = your_password

[email]
from = slurm-mailer@example.com
to = recipient@example.com

[slurm]
watch_all_users = false
aggregation_minutes = 10
poll_interval_seconds = 30
```

## Usage

```bash
slurm-mailer
```

Or run from source:

```bash
uv run slurm-mailer
```

## License

MIT