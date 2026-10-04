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
uv pip install dist/slurm_mailer-0.2.0-py3-none-any.whl
```

## Quick Setup

Run the interactive wizard:

```bash
slurm-mailer-setup
```

The wizard walks you through:

1. **SMTP server** — host, port, encryption (StartTLS / SSL / none), credentials
2. **Email** — sender and recipient addresses, with an optional test email to
   verify the connection before anything is written
3. **Slurm monitoring** — watch your own jobs, a specific user's jobs, or all
   visible jobs; poll and aggregation intervals
4. **Files** — writes two files (default locations shown):

   | File | Default path |
   |---|---|
   | Configuration | `~/.config/slurm-mailer/config.ini` |
   | systemd user unit | `~/.config/systemd/user/slurm-mailer.service` |

   The configuration file is written with mode `0600` because it contains your
   SMTP password. Existing files are never overwritten without confirmation.

The wizard only **generates** the systemd unit; it does not run `systemctl`.
After it finishes, enable the service yourself:

```bash
systemctl --user daemon-reload
systemctl --user enable --now slurm-mailer.service
journalctl --user -u slurm-mailer -f
```

To keep the daemon running after you log out:

```bash
loginctl enable-linger $USER
```

## Configuration

No sample `config.ini` is shipped; the wizard creates one for you. You can
also write it by hand. The full format:

```ini
# It contains your SMTP password - never commit this file.

[smtp]
host = smtp.example.com
port = 587
# starttls | ssl | none
tls = starttls
user = your_user
password = your_password

[email]
from = slurm-mailer@example.com
# Comma-separated list of recipients
to = recipient@example.com, manager@example.com

[slurm]
# Username to watch; empty means "not restricted by user"
watch_user = alice
# true overrides watch_user and watches every visible job
watch_all_users = false
# How often to poll for job status changes (seconds)
poll_interval_seconds = 30
# How long to aggregate changes before sending an email (minutes)
aggregation_minutes = 10
```

### Options

| Section | Key | Default | Description |
|---|---|---|---|
| `smtp` | `host` | `localhost` | SMTP server hostname |
| `smtp` | `port` | `587` | SMTP port (465 is typical for `tls = ssl`) |
| `smtp` | `tls` | `starttls` | `starttls` (upgrade), `ssl` (implicit TLS), or `none` |
| `smtp` | `user` | *(empty)* | SMTP username; empty disables login |
| `smtp` | `password` | *(empty)* | SMTP password |
| `email` | `from` | `slurm-mailer@example.com` | Sender address |
| `email` | `to` | *(empty)* | Comma-separated recipients; at least one required |
| `slurm` | `watch_user` | *(empty)* | Watch only this user's jobs |
| `slurm` | `watch_all_users` | `false` | Watch all visible jobs (overrides `watch_user`) |
| `slurm` | `poll_interval_seconds` | `30` | Seconds between `squeue`/`sacct` polls |
| `slurm` | `aggregation_minutes` | `10` | Minutes to batch changes before one email |

### Config file location

The daemon looks for the configuration in this order:

1. `SLURM_MAILER_CONFIG` environment variable:

   ```bash
   SLURM_MAILER_CONFIG=/path/to/config.ini slurm-mailer
   ```

2. `./config.ini` in the current working directory

The wizard writes to `~/.config/slurm-mailer/config.ini` by default; either
point `SLURM_MAILER_CONFIG` at it or symlink it to your working directory.

`config.ini` is listed in `.gitignore` so a filled-in configuration (which
contains credentials) is never committed.

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