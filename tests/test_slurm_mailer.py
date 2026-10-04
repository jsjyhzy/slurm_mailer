import unittest
import sys
import os
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from slurm_mailer import (  # noqa: E402
    parse_config,
    SlurmJob,
    SlurmMailer,
)


SQQUEUE_SAMPLE = (
    "12345|debug|job1|R|dave|4|0:21|02:00\n"
    "12346|debug|job2|PD|dave|8|0:00|2-00:00:00\n"
    "12348|debug|job3|PD|ed|4|0:00|1-00:00:00\n"
)

SACCT_SAMPLE = (
    "2|script01|COMPLETED|acct1|1|00:01:30|srun\n"
    "3|script02|RUNNING|acct1|2|00:10:00|debug\n"
    "4|endscript|COMPLETED|acct1|1|00:05:00|srun\n"
    "4.0||COMPLETED|acct1|1|00:05:00|srun\n"
    "5|failing-job|FAILED|acct1|2|00:03:00|debug\n"
    "6|timeout-job|TIMEOUT|acct1|4|01:00:00|debug\n"
    "7|cancelled-job|CANCELLED|acct1|1|00:00:00|debug\n"
    "8|oom-job|OUT_OF_MEMORY|acct1|2|00:01:00|debug\n"
)


def mock_subprocess_run(cmd, **kwargs):
    cmd_str = " ".join(cmd) if isinstance(cmd, list) else str(cmd)
    result = MagicMock()
    if "squeue" in cmd_str:
        result.returncode = 0
        result.stdout = SQQUEUE_SAMPLE
        result.stderr = ""
    else:
        result.returncode = 0
        result.stdout = SACCT_SAMPLE
        result.stderr = ""
    return result


class MockSMTP:

    def __init__(self, *args, **kwargs):
        pass

    def login(self, *args, **kwargs):
        pass

    def sendmail(self, *args, **kwargs):
        pass

    def starttls(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class TestParseConfig(unittest.TestCase):
    def setUp(self):
        self.config_path = "/tmp/test_config.ini"
        content = """[smtp]
host = smtp.example.com
port = 587
tls = true
user = testuser
password = testpass

[email]
from = sender@example.com
to = recv1@example.com, recv2@example.com

[slurm]
watch_user = testuser
watch_all_users = false
aggregation_minutes = 15
poll_interval_seconds = 45
"""
        with open(self.config_path, "w") as f:
            f.write(content)

    def tearDown(self):
        os.remove(self.config_path)

    def test_parse_config_basic(self):
        c = parse_config(self.config_path)
        self.assertEqual(c.email.smtp_host, "smtp.example.com")
        self.assertEqual(c.email.smtp_port, 587)
        self.assertEqual(c.email.smtp_user, "testuser")
        self.assertEqual(c.email.from_addr, "sender@example.com")
        self.assertEqual(c.email.to_addrs, ["recv1@example.com", "recv2@example.com"])

    def test_parse_config_slurm_options(self):
        c = parse_config(self.config_path)
        self.assertEqual(c.watch_user, "testuser")
        self.assertFalse(c.watch_all_users)
        self.assertEqual(c.aggregation_interval, 15)
        self.assertEqual(c.poll_interval, 45)

    def test_parse_config_defaults(self):
        path = "/tmp/test_defaults.ini"
        with open(path, "w") as f:
            f.write("[smtp]\n[email]\n[slurm]\n")
        c = parse_config(path)
        self.assertEqual(c.email.smtp_host, "localhost")
        self.assertEqual(c.email.smtp_port, 587)
        self.assertIsNone(c.watch_user)
        self.assertFalse(c.watch_all_users)
        self.assertEqual(c.aggregation_interval, 10)
        self.assertEqual(c.poll_interval, 30)
        os.remove(path)

    def test_sacct_args_include_aggregation(self):
        c = parse_config(self.config_path)
        self.assertIn("now-15minutes", c.sacct_args)
        self.assertIn("-u", c.sacct_args)
        self.assertIn("testuser", c.sacct_args)


class TestSlurmMailerFetch(unittest.TestCase):
    def test_fetch_slurm_jobs_returns_active_and_historical(self):
        c = parse_config("/tmp/test_config.ini")
        mailer = SlurmMailer(c, subprocess_fn=mock_subprocess_run)
        jobs = mailer._fetch_slurm_jobs()
        squeue_jobs = [j for j in jobs if not j.from_sacct]
        self.assertEqual(len(squeue_jobs), 3)
        self.assertEqual(squeue_jobs[0].job_id, "12345")
        self.assertEqual(squeue_jobs[0].state, "R")
        self.assertEqual(squeue_jobs[0].user, "dave")
        self.assertEqual(squeue_jobs[1].state, "PD")
        self.assertEqual(squeue_jobs[2].state, "PD")
        sacct_jobs = [j for j in jobs if j.from_sacct]
        self.assertGreater(len(sacct_jobs), 0)

    def test_fetch_slurm_jobs_squeue_jobs_not_from_sacct(self):
        c = parse_config("/tmp/test_config.ini")
        mailer = SlurmMailer(c, subprocess_fn=mock_subprocess_run)
        jobs = mailer._fetch_slurm_jobs()
        squeue_jobs = [j for j in jobs if not j.from_sacct]
        self.assertTrue(all(not j.from_sacct for j in squeue_jobs))

    def test_fetch_sacct_jobs_parses_states(self):
        c = parse_config("/tmp/test_config.ini")
        mailer = SlurmMailer(c, subprocess_fn=mock_subprocess_run)
        sacct_jobs = mailer._fetch_sacct_jobs()
        job_map = {j.job_id: j for j in sacct_jobs}
        self.assertEqual(job_map["2"].state, "CD")
        self.assertEqual(job_map["5"].state, "F")
        self.assertEqual(job_map["6"].state, "TO")
        self.assertEqual(job_map["7"].state, "CA")
        self.assertEqual(job_map["8"].state, "OOM")
        self.assertEqual(job_map["3"].state, "R")

    def test_fetch_sacct_jobs_strips_step_suffix(self):
        c = parse_config("/tmp/test_config.ini")
        mailer = SlurmMailer(c, subprocess_fn=mock_subprocess_run)
        sacct_jobs = mailer._fetch_sacct_jobs()
        job_ids = {j.job_id for j in sacct_jobs}
        self.assertNotIn("4.0", job_ids)
        self.assertIn("4", job_ids)
        self.assertTrue(all(j.from_sacct for j in sacct_jobs))

    def test_fetch_sacct_returns_empty_on_failure(self):
        c = parse_config("/tmp/test_config.ini")

        def _fail_fn(cmd, **kwargs):
            return MagicMock(returncode=1, stdout="", stderr="error")
        mailer = SlurmMailer(c, subprocess_fn=_fail_fn)
        jobs = mailer._fetch_sacct_jobs()
        self.assertEqual(jobs, [])

    def test_fetch_sacct_returns_empty_on_not_found(self):
        c = parse_config("/tmp/test_config.ini")
        mailer = SlurmMailer(c, subprocess_fn=lambda cmd, **kwargs: (_ for _ in ()).throw(FileNotFoundError()))
        with self.assertRaises(SystemExit):
            mailer._fetch_sacct_jobs()


class TestSlurmMailerDetectChanges(unittest.TestCase):
    def test_new_job_detected(self):
        mailer = SlurmMailer(parse_config("/tmp/test_config.ini"))
        old = {}
        new = [SlurmJob("100", "debug", "new-job", "R", "user1", "2", "0:05")]
        changes = mailer._detect_changes(old, new)
        self.assertIn("100", changes)
        self.assertEqual(changes["100"]["type"], "new")
        self.assertIsNone(changes["100"]["old_state"])
        self.assertEqual(changes["100"]["new_state"], "R")

    def test_state_change_detected(self):
        mailer = SlurmMailer(parse_config("/tmp/test_config.ini"))
        old = {"100": SlurmJob("100", "debug", "my-job", "PD", "user1", "4", "0:00")}
        new = [SlurmJob("100", "debug", "my-job", "R", "user1", "4", "0:05")]
        changes = mailer._detect_changes(old, new)
        self.assertIn("100", changes)
        self.assertEqual(changes["100"]["type"], "change")
        self.assertEqual(changes["100"]["old_state"], "PD")
        self.assertEqual(changes["100"]["new_state"], "R")

    def test_job_removed_detected(self):
        mailer = SlurmMailer(parse_config("/tmp/test_config.ini"))
        old = {"100": SlurmJob("100", "debug", "old-job", "R", "user1", "4", "1:00")}
        new = []
        changes = mailer._detect_changes(old, new)
        self.assertIn("100", changes)
        self.assertEqual(changes["100"]["type"], "gone")
        self.assertEqual(changes["100"]["old_state"], "R")
        self.assertIsNone(changes["100"]["new_state"])

    def test_no_changes(self):
        mailer = SlurmMailer(parse_config("/tmp/test_config.ini"))
        old = {"100": SlurmJob("100", "debug", "my-job", "R", "user1", "4", "0:05")}
        new = [SlurmJob("100", "debug", "my-job", "R", "user1", "4", "0:10")]
        changes = mailer._detect_changes(old, new)
        self.assertEqual(len(changes), 0)

    def test_multiple_changes(self):
        mailer = SlurmMailer(parse_config("/tmp/test_config.ini"))
        old = {
            "100": SlurmJob("100", "debug", "job1", "PD", "user1", "4", "0:00"),
            "200": SlurmJob("200", "debug", "job2", "R", "user1", "2", "1:00"),
        }
        new = [
            SlurmJob("100", "debug", "job1", "R", "user1", "4", "0:05"),
            SlurmJob("200", "debug", "job2", "CD", "user1", "2", "1:30"),
            SlurmJob("300", "debug", "job3", "PD", "user2", "1", "0:00"),
        ]
        changes = mailer._detect_changes(old, new)
        self.assertEqual(len(changes), 3)
        self.assertEqual(changes["100"]["type"], "change")
        self.assertEqual(changes["200"]["type"], "change")
        self.assertEqual(changes["300"]["type"], "new")


class TestSlurmMailerEmail(unittest.TestCase):
    def test_email_has_proper_structure(self):
        c = parse_config("/tmp/test_config.ini")
        mailer = SlurmMailer(c)
        job = SlurmJob("100", "debug", "test-job", "R", "user1", "4", "0:05")
        html = mailer._build_html_email({}, [job], datetime.now() - timedelta(minutes=10), datetime.now())
        self.assertIn("Slurm Job Monitor", html)
        self.assertIn("Current Queue", html)
        self.assertIn("Job ID", html)
        self.assertIn("Name", html)
        self.assertIn("User", html)
        self.assertIn("State", html)
        self.assertIn("Nodes", html)
        self.assertIn("Time", html)

    def test_email_shows_completed_state(self):
        c = parse_config("/tmp/test_config.ini")
        mailer = SlurmMailer(c)
        job = SlurmJob("99", "debug", "done-job", "CD", "user1", "2", "0:03", from_sacct=True)
        html = mailer._build_html_email({}, [job], datetime.now() - timedelta(minutes=10), datetime.now())
        self.assertIn("Completed", html)

    def test_email_no_changes_message(self):
        c = parse_config("/tmp/test_config.ini")
        mailer = SlurmMailer(c)
        job = SlurmJob("100", "debug", "test-job", "R", "user1", "4", "0:05")
        html = mailer._build_html_email({}, [job], datetime.now() - timedelta(minutes=10), datetime.now())
        self.assertIn("No status changes detected", html)

    def test_email_queue_summary(self):
        c = parse_config("/tmp/test_config.ini")
        mailer = SlurmMailer(c)
        jobs = [
            SlurmJob("100", "debug", "r1", "R", "user1", "4", "0:05"),
            SlurmJob("101", "debug", "p1", "PD", "user2", "2", "0:00"),
            SlurmJob("102", "debug", "c1", "CD", "user1", "1", "1:00", from_sacct=True),
        ]
        html = mailer._build_html_email({}, jobs, datetime.now() - timedelta(minutes=10), datetime.now())
        self.assertIn("Running", html)
        self.assertIn("Pending", html)
        self.assertIn("Completed", html)

    def test_notify_calls_smtp(self):
        mock_smtp_cls = MagicMock()
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)
        mailer = SlurmMailer(parse_config("/tmp/test_config.ini"), smtp_class=mock_smtp_cls)
        mailer.pending_changes = {
            "100": {"type": "new", "job": SlurmJob("100", "debug", "test-job", "R", "user1", "4", "0:05"), "old_state": None, "new_state": "R"}
        }
        current_jobs = [SlurmJob("100", "debug", "test-job", "R", "user1", "4", "0:05")]
        window_start = datetime.now() - timedelta(minutes=mailer.config.aggregation_interval)
        window_end = datetime.now()
        subject = f"Slurm Job Status Update — {len(mailer.pending_changes)} change(s) detected"
        html_body = mailer._build_html_email(mailer.pending_changes, current_jobs, window_start, window_end)
        mailer._send_email(subject, html_body)
        mock_smtp_cls.assert_called()


class TestSlurmMailerClass(unittest.TestCase):
    def setUp(self):
        self.config_path = "/tmp/test_config.ini"
        content = """[smtp]
host = smtp.example.com
port = 587
tls = true
user = testuser
password = testpass

[email]
from = sender@example.com
to = recv1@example.com

[slurm]
watch_user = testuser
watch_all_users = false
aggregation_minutes = 10
poll_interval_seconds = 1
"""
        with open(self.config_path, "w") as f:
            f.write(content)
        self.config = parse_config(self.config_path)

    def tearDown(self):
        os.remove(self.config_path)

    def test_init_defaults(self):
        mailer = SlurmMailer(self.config)
        self.assertEqual(mailer.config, self.config)
        self.assertEqual(mailer.previous_jobs, {})
        self.assertEqual(mailer.pending_changes, {})

    def test_init_with_injected_deps(self):
        mock_subprocess = MagicMock(return_value=MagicMock(returncode=0, stdout="", stderr=""))
        mock_smtp = MagicMock()
        mailer = SlurmMailer(self.config, subprocess_fn=mock_subprocess, smtp_class=mock_smtp)
        self.assertEqual(mailer.subprocess_fn, mock_subprocess)
        self.assertEqual(mailer.smtp_class, mock_smtp)

    def test_run_loop_calls_fetch_and_notify(self):
        mock_subprocess = MagicMock(return_value=MagicMock(returncode=0, stdout=SQQUEUE_SAMPLE, stderr=""))
        mock_smtp_cls = MagicMock()
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)
        mailer = SlurmMailer(
            self.config,
            subprocess_fn=mock_subprocess,
            smtp_class=mock_smtp_cls,
        )
        mailer.config.poll_interval = 999
        mailer.config.aggregation_interval = 0
        mailer.previous_jobs = {}
        mailer.pending_changes = {
            "12345": {"type": "new", "job": SlurmJob("12345", "debug", "job1", "R", "dave", "4", "0:21"), "old_state": None, "new_state": "R"}
        }

        def break_sleep(duration):
            raise KeyboardInterrupt()
        with patch("slurm_mailer.time.sleep", side_effect=break_sleep):
            with patch("shutil.which", return_value="/usr/bin/squeue"):
                mailer.run()
        mock_subprocess.assert_called()


if __name__ == "__main__":
    config_path = "/tmp/test_config.ini"
    config_content = """[smtp]
host = smtp.example.com
port = 587
tls = true
user = testuser
password = testpass

[email]
from = sender@example.com
to = recv1@example.com

[slurm]
watch_user = testuser
watch_all_users = false
aggregation_minutes = 15
poll_interval_seconds = 45
"""
    with open(config_path, "w") as f:
        f.write(config_content)

    unittest.main(verbosity=2)
