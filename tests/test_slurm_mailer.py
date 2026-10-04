import unittest
import sys
import os
import io
import smtplib
import shutil
import tempfile
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from slurm_mailer import (  # noqa: E402
    parse_config,
    SlurmJob,
    SlurmMailer,
    prompt_str,
    prompt_bool,
    prompt_int,
    prompt_choice,
    prompt_password,
    WizardAnswers,
    render_config,
    render_unit,
    write_private,
    send_test_email,
    run_setup,
    EmailConfig,
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


def scripted_input(responses):
    queue = list(responses)

    def _input(prompt=""):
        if not queue:
            raise AssertionError(f"Unexpected prompt: {prompt}")
        return queue.pop(0)
    return _input


def sample_answers(**overrides):
    base = dict(
        smtp_host="smtp.example.com",
        smtp_port=587,
        smtp_tls_mode="starttls",
        smtp_user="mailer@example.com",
        smtp_password="secret",
        from_addr="from@example.com",
        to_addrs=["alice@example.com", "bob@example.com"],
        watch_user="alice",
        watch_all_users=False,
        poll_interval=30,
        aggregation_minutes=10,
    )
    base.update(overrides)
    return WizardAnswers(**base)


class FailingSMTP:
    def __init__(self, *args, **kwargs):
        raise OSError("connection refused")


class TestPrompts(unittest.TestCase):
    def test_prompt_str_default_on_empty(self):
        self.assertEqual(prompt_str("Host", "localhost", scripted_input([""])), "localhost")
        self.assertEqual(prompt_str("Host", "localhost", scripted_input(["mail.example.com"])), "mail.example.com")

    def test_prompt_str_required_reprompt(self):
        fn = scripted_input(["", "someone@example.com"])
        self.assertEqual(prompt_str("From address", None, fn), "someone@example.com")

    def test_prompt_bool_reprompt_on_invalid(self):
        fn = scripted_input(["maybe", "n"])
        self.assertFalse(prompt_bool("Send test?", True, fn))
        self.assertTrue(prompt_bool("Send test?", True, scripted_input([""])))
        self.assertTrue(prompt_bool("Send test?", True, scripted_input(["yes"])))

    def test_prompt_int_reprompt_on_invalid_and_bounds(self):
        fn = scripted_input(["abc", "0", "42"])
        self.assertEqual(prompt_int("Port", 587, fn, min_value=1), 42)
        self.assertEqual(prompt_int("Port", 587, scripted_input([""])), 587)
        fn = scripted_input(["99999", ""])
        self.assertEqual(prompt_int("Port", 587, fn, max_value=65535), 587)

    def test_prompt_choice_number_key_and_default(self):
        options = [("me", "My jobs"), ("user", "Specific user"), ("all", "All jobs")]
        self.assertEqual(prompt_choice("Watch", options, "me", scripted_input(["2"])), "user")
        self.assertEqual(prompt_choice("Watch", options, "me", scripted_input(["all"])), "all")
        self.assertEqual(prompt_choice("Watch", options, "me", scripted_input([""])), "me")
        fn = scripted_input(["bogus", "3"])
        self.assertEqual(prompt_choice("Watch", options, "me", fn), "all")

    def test_prompt_password_uses_getpass_fn(self):
        calls = []

        def fake_getpass(prompt):
            calls.append(prompt)
            return "hunter2"
        self.assertEqual(prompt_password("SMTP password", fake_getpass), "hunter2")
        self.assertEqual(len(calls), 1)


class TestRenderConfig(unittest.TestCase):
    def test_render_config_roundtrip(self):
        answers = sample_answers()
        path = "/tmp/test_render_roundtrip.ini"
        with open(path, "w") as f:
            f.write(render_config(answers))
        try:
            c = parse_config(path)
            self.assertEqual(c.email.smtp_host, "smtp.example.com")
            self.assertEqual(c.email.smtp_port, 587)
            self.assertTrue(c.email.smtp_tls)
            self.assertFalse(c.email.smtp_ssl)
            self.assertEqual(c.email.smtp_user, "mailer@example.com")
            self.assertEqual(c.email.smtp_password, "secret")
            self.assertEqual(c.email.from_addr, "from@example.com")
            self.assertEqual(c.email.to_addrs, ["alice@example.com", "bob@example.com"])
            self.assertEqual(c.watch_user, "alice")
            self.assertFalse(c.watch_all_users)
            self.assertEqual(c.poll_interval, 30)
            self.assertEqual(c.aggregation_interval, 10)
        finally:
            os.remove(path)

    def test_render_config_tls_modes(self):
        for mode, tls, ssl in (("starttls", True, False), ("ssl", False, True), ("none", False, False)):
            answers = sample_answers(smtp_tls_mode=mode)
            path = f"/tmp/test_render_tls_{mode}.ini"
            with open(path, "w") as f:
                f.write(render_config(answers))
            try:
                c = parse_config(path)
                self.assertEqual(c.email.smtp_tls, tls, mode)
                self.assertEqual(c.email.smtp_ssl, ssl, mode)
            finally:
                os.remove(path)

    def test_render_config_all_users(self):
        answers = sample_answers(watch_user="", watch_all_users=True)
        text = render_config(answers)
        self.assertIn("watch_user = \n", text)
        self.assertIn("watch_all_users = true", text)


class TestRenderUnit(unittest.TestCase):
    def test_render_unit_contents(self):
        text = render_unit("/home/alice/.config/slurm-mailer/config.ini", "/usr/local/bin/slurm-mailer", "/usr/local/bin:/usr/bin")
        self.assertIn("[Unit]", text)
        self.assertIn("[Service]", text)
        self.assertIn("[Install]", text)
        self.assertIn("ExecStart=/usr/local/bin/slurm-mailer", text)
        self.assertIn("Environment=SLURM_MAILER_CONFIG=/home/alice/.config/slurm-mailer/config.ini", text)
        self.assertIn("Environment=PATH=/usr/local/bin:/usr/bin", text)
        self.assertIn("Restart=on-failure", text)
        self.assertIn("WantedBy=default.target", text)


class TestWritePrivate(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)

    def test_write_private_creates_with_0600(self):
        path = os.path.join(self.dir, "sub", "config.ini")
        write_private(path, "hello")
        with open(path) as f:
            self.assertEqual(f.read(), "hello")
        self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)

    def test_write_private_refuses_overwrite(self):
        path = os.path.join(self.dir, "config.ini")
        write_private(path, "original")
        with self.assertRaises(FileExistsError):
            write_private(path, "changed", overwrite=False)
        with open(path) as f:
            self.assertEqual(f.read(), "original")

    def test_write_private_overwrite(self):
        path = os.path.join(self.dir, "config.ini")
        write_private(path, "original")
        write_private(path, "replaced", overwrite=True)
        with open(path) as f:
            self.assertEqual(f.read(), "replaced")
        self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)


class TestSendTestEmail(unittest.TestCase):
    def test_success_returns_none(self):
        cfg = EmailConfig(
            smtp_host="smtp.example.com", smtp_port=587, smtp_user="u", smtp_password="p",
            smtp_tls=True, from_addr="from@example.com", to_addrs=["to@example.com"],
        )
        self.assertIsNone(send_test_email(cfg, MockSMTP))

    def test_failure_returns_error(self):
        cfg = EmailConfig(
            smtp_host="smtp.example.com", smtp_port=587, smtp_user="u", smtp_password="p",
            smtp_tls=True, from_addr="from@example.com", to_addrs=["to@example.com"],
        )
        error = send_test_email(cfg, FailingSMTP)
        self.assertIn("connection refused", error)


class TestParseConfigTlsModes(unittest.TestCase):
    def _parse_tls(self, value):
        path = f"/tmp/test_tls_{value.replace(' ', '_')}.ini"
        with open(path, "w") as f:
            f.write(f"[smtp]\ntls = {value}\n[email]\n[slurm]\n")
        try:
            return parse_config(path)
        finally:
            os.remove(path)

    def test_ssl_mode(self):
        c = self._parse_tls("ssl")
        self.assertTrue(c.email.smtp_ssl)
        self.assertFalse(c.email.smtp_tls)

    def test_none_mode(self):
        c = self._parse_tls("none")
        self.assertFalse(c.email.smtp_ssl)
        self.assertFalse(c.email.smtp_tls)

    def test_legacy_true_still_starttls(self):
        c = self._parse_tls("true")
        self.assertTrue(c.email.smtp_tls)
        self.assertFalse(c.email.smtp_ssl)

    def test_legacy_false_still_disabled(self):
        c = self._parse_tls("false")
        self.assertFalse(c.email.smtp_tls)
        self.assertFalse(c.email.smtp_ssl)

    def test_ssl_config_selects_smtp_ssl_class(self):
        path = "/tmp/test_ssl_class.ini"
        with open(path, "w") as f:
            f.write("[smtp]\ntls = ssl\n[email]\n[slurm]\n")
        try:
            mailer = SlurmMailer(parse_config(path))
            self.assertEqual(mailer.smtp_class, smtplib.SMTP_SSL)
        finally:
            os.remove(path)


class TestRunSetup(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.home)
        self.env = {"HOME": self.home, "PATH": "/usr/bin:/bin"}
        self.stdout = io.StringIO()
        self._stdout_patch = patch("sys.stdout", self.stdout)
        self._stdout_patch.start()
        self.addCleanup(self._stdout_patch.stop)

    def base_responses(self, extra=()):
        return [
            "smtp.example.com",           # SMTP host
            "",                           # encryption -> starttls
            "",                           # port -> 587
            "mailer@example.com",         # SMTP user
            "from@example.com",           # from address
            "alice@example.com, bob@example.com",  # recipients
            "n",                          # send test email? no
            "",                           # watch mode -> me
            "",                           # poll interval -> 30
            "",                           # aggregation -> 10
            "",                           # config path default
            "",                           # unit path default
        ] + list(extra)

    def _run(self, responses, getpass_fn=None, smtp_class=None, username="alice"):
        return run_setup(
            input_fn=scripted_input(responses),
            getpass_fn=getpass_fn or (lambda prompt: "secret"),
            smtp_class=smtp_class,
            env=self.env,
            which_fn=lambda name: "/usr/local/bin/slurm-mailer" if name == "slurm-mailer" else None,
            username=username,
        )

    def test_end_to_end_writes_config_and_unit(self):
        rc = self._run(self.base_responses())
        self.assertEqual(rc, 0)
        config_path = os.path.join(self.home, ".config", "slurm-mailer", "config.ini")
        unit_path = os.path.join(self.home, ".config", "systemd", "user", "slurm-mailer.service")
        self.assertTrue(os.path.exists(config_path))
        self.assertTrue(os.path.exists(unit_path))
        self.assertEqual(os.stat(config_path).st_mode & 0o777, 0o600)
        c = parse_config(config_path)
        self.assertEqual(c.email.smtp_host, "smtp.example.com")
        self.assertEqual(c.email.smtp_password, "secret")
        self.assertEqual(c.email.to_addrs, ["alice@example.com", "bob@example.com"])
        self.assertEqual(c.watch_user, "alice")
        with open(unit_path) as f:
            unit = f.read()
        self.assertIn("ExecStart=/usr/local/bin/slurm-mailer", unit)
        self.assertIn(f"Environment=SLURM_MAILER_CONFIG={config_path}", unit)
        self.assertIn("Environment=PATH=/usr/bin:/bin", unit)

    def test_aborts_when_config_exists_and_declined(self):
        config_path = os.path.join(self.home, ".config", "slurm-mailer", "config.ini")
        os.makedirs(os.path.dirname(config_path))
        with open(config_path, "w") as f:
            f.write("original content")
        responses = self.base_responses()
        responses[-2] = config_path  # explicit config path
        responses[-1] = "n"          # decline overwrite
        rc = self._run(responses)
        self.assertEqual(rc, 1)
        with open(config_path) as f:
            self.assertEqual(f.read(), "original content")

    def test_skips_password_when_no_user(self):
        def fail_getpass(prompt):
            raise AssertionError("password should not be prompted")
        responses = [
            "smtp.example.com", "", "", "",   # host, tls, port, user (empty)
            "from@example.com", "alice@example.com",
            "n", "", "", "", "", "",
        ]
        rc = self._run(responses, getpass_fn=fail_getpass)
        self.assertEqual(rc, 0)
        config_path = os.path.join(self.home, ".config", "slurm-mailer", "config.ini")
        c = parse_config(config_path)
        self.assertEqual(c.email.smtp_user, "")
        self.assertEqual(c.email.smtp_password, "")

    def test_test_email_failure_then_skip(self):
        responses = [
            "smtp.example.com", "", "",
            "mailer@example.com",
            "from@example.com", "alice@example.com",
            "y",   # send test email
            "n",   # retry? no
            "", "", "", "", "",
        ]
        rc = self._run(responses, smtp_class=FailingSMTP)
        self.assertEqual(rc, 0)
        self.assertIn("Test email failed", self.stdout.getvalue())


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
