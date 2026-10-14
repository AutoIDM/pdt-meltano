import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from pdt_meltano import extension, main
from pdt_meltano.extension import Pdt, PdtMeltanoError, Schedule, app_name, parse_schedules

LISTING = {"schedules": {
    "job": [
        {"name": "daily-sync", "cron_interval": "@daily", "job": {"name": "sync", "tasks": []}},
        {"name": "every-15", "cron_interval": "*/15 * * * *", "job": {"name": "fast"}},
        {"name": "by-hand", "cron_interval": "@manual", "job": {"name": "sync"}},
    ],
    "elt": [{"name": "old-style", "interval": "@daily", "elt_args": ["tap", "target"]}],
}}


def test_parse_schedules_keeps_repeating_job_schedules():
    found, skipped = parse_schedules(LISTING)
    assert found == [Schedule("daily-sync", "daily", ["sync"]),
                     Schedule("every-15", "*/15 * * * *", ["fast"])]
    assert [reason.split(":")[0] for reason in skipped] == ["by-hand", "old-style"]


def test_app_name_is_the_schedule_name_as_a_folder_name_pdt_accepts():
    assert app_name("daily-sync") == "daily-sync"
    assert app_name("Daily_Sync") == "daily-sync"


@pytest.fixture
def meltano_project(tmp_path, monkeypatch):
    root = tmp_path / "warehouse"
    (root / "extract").mkdir(parents=True)
    (root / "meltano.yml").write_text("project_id: x\n")
    (root / "extract" / "catalog.json").write_text("{}")
    (root / ".env").write_text("TAP_SECRET=abc\nTARGET_PASSWORD=def\n")
    (root / ".meltano" / "plugins").mkdir(parents=True)
    monkeypatch.setenv("MELTANO_PROJECT_ROOT", str(root))
    monkeypatch.setenv("MELTANO_SYS_DIR_ROOT", str(root / ".meltano"))
    monkeypatch.setenv("MELTANO_UTILITY_NAME", "pdt-aws")
    monkeypatch.setenv("MELTANO_UTILITY_NAMESPACE", "pdt_aws")
    monkeypatch.setenv("PDT_AWS_PROVIDER", "aws")
    monkeypatch.setenv("PDT_AWS_REGION", "us-east-2")
    monkeypatch.setenv("MELTANO_ENVIRONMENT", "prod")

    def fake_meltano(*args, cwd):
        return "meltano, version 4.2.0\n" if args == ("--version",) else ""
    monkeypatch.setattr(extension, "meltano", fake_meltano)
    return root


def test_write_project_makes_one_app_per_schedule(meltano_project):
    ext = Pdt()
    names = ext.write_project([Schedule("daily-sync", "daily", ["sync"])])
    assert names == ["daily-sync"]
    assert ext.stage == meltano_project / ".meltano" / "utilities" / "pdt-aws" / "pdt"
    app = ext.stage / "daily-sync"
    assert (app / "meltano.yml").is_file()
    assert (app / "extract" / "catalog.json").is_file()
    assert not (app / ".env").exists()
    assert not (app / ".meltano").exists()
    assert yaml.safe_load((app / "config.yml").read_text()) == {
        "schedule": "daily", "env": {"optional": ["TAP_SECRET", "TARGET_PASSWORD"]}}
    run_py = (app / "run.py").read_text()
    assert '"pdt-cli[apps]==' in run_py and '"meltano==4.2.0"' in run_py
    assert "JOB = ['sync']" in run_py
    assert "ENVIRONMENT = 'prod'" in run_py
    compile(run_py, "run.py", "exec")
    assert "WORKDIR /workspace/daily-sync" in (app / "Dockerfile").read_text()
    assert yaml.safe_load((ext.stage / "pdt.yml").read_text()) == {
        "platform": {"provider": "aws", "region": "us-east-2"}}



def test_a_pdt_project_in_the_run_folder_moves_once(meltano_project):
    ext = Pdt()
    old = meltano_project / ".meltano" / "run" / "pdt-aws"
    (old / ".pdt").mkdir(parents=True)
    (old / ".pdt" / "state").write_text('{"deployed": ["daily-sync"]}')
    (old / "pdt.yml").write_text("platform:\n  provider: aws\n  account: '123456789012'\n")
    (old / "daily-sync").mkdir()
    (old / "daily-sync" / "config.yml").write_text("schedule: daily\npause: true\n")
    ext.write_project([Schedule("daily-sync", "daily", ["sync"])])
    assert not old.exists()
    assert ext.deployed() == {"daily-sync"}
    assert yaml.safe_load((ext.stage / "pdt.yml").read_text())["platform"]["account"] == "123456789012"
    assert yaml.safe_load((ext.stage / "daily-sync" / "config.yml").read_text())["pause"] is True
    old.mkdir(parents=True)
    (old / "pdt.yml").write_text("platform:\n  provider: aws\n")
    ext.write_project([Schedule("daily-sync", "daily", ["sync"])])
    assert (old / "pdt.yml").is_file()
    assert ext.deployed() == {"daily-sync"}

def test_a_system_folder_inside_the_project_is_not_copied(meltano_project, monkeypatch):
    monkeypatch.setenv("MELTANO_SYS_DIR_ROOT", str(meltano_project / "state" / "meltano"))
    ext = Pdt()
    ext.write_project([Schedule("daily-sync", "daily", ["sync"])])
    assert ext.stage == meltano_project / "state" / "meltano" / "utilities" / "pdt-aws" / "pdt"
    assert not (ext.stage / "daily-sync" / "state" / "meltano").exists()


def test_write_project_keeps_what_pdt_wrote_back(meltano_project):
    ext = Pdt()
    ext.stage.mkdir(parents=True)
    (ext.stage / "pdt.yml").write_text(
        'platform:\n  provider: aws\n  account: "123456789012"\n  timezone: Etc/UTC\n')
    ext.write_project([])
    assert yaml.safe_load((ext.stage / "pdt.yml").read_text())["platform"] == {
        "account": "123456789012", "provider": "aws", "region": "us-east-2"}


def test_write_project_keeps_what_pdt_wrote_into_an_app_config(meltano_project):
    ext = Pdt()
    ext.write_project([Schedule("daily-sync", "daily", ["sync"])])
    config = ext.stage / "daily-sync" / "config.yml"
    config.write_text(config.read_text() + "pause: true\nenabled: false\n")
    ext.write_project([Schedule("daily-sync", "hourly", ["sync"])])
    assert yaml.safe_load(config.read_text()) == {
        "schedule": "hourly", "env": {"optional": ["TAP_SECRET", "TARGET_PASSWORD"]},
        "enabled": False, "pause": True}


def test_every_setting_goes_to_platform_for_pdt_to_check(meltano_project, monkeypatch):
    monkeypatch.setenv("PDT_AWS_TIMEZONE", "America/Chicago")
    monkeypatch.setenv("PDT_AWS_BUGABOO", "abcdef")
    monkeypatch.setenv("PDT_AWS__LOG_PARSER", "singer-sdk")
    ext = Pdt()
    ext.write_project([])
    assert yaml.safe_load((ext.stage / "pdt.yml").read_text())["platform"] == {
        "provider": "aws", "region": "us-east-2", "timezone": "America/Chicago", "bugaboo": "abcdef"}


def test_a_missing_provider_names_the_fix(meltano_project, monkeypatch):
    monkeypatch.setenv("PDT_AWS_PROVIDER", "")
    with pytest.raises(PdtMeltanoError, match="Meltano Hub entry"):
        Pdt().write_project([])


def test_write_project_follows_meltano_yml(meltano_project, capsys):
    ext = Pdt()
    ext.write_project([Schedule("daily-sync", "daily", ["sync"]), Schedule("hourly", "hourly", ["sync"])])
    (ext.stage / ".pdt").mkdir()
    (ext.stage / ".pdt" / "state").write_text('{"deployed": ["hourly"]}')
    assert ext.write_project([Schedule("weekly", "weekly", ["sync"])]) == ["weekly"]
    assert sorted(run_py.parent.name for run_py in ext.stage.glob("*/run.py")) == ["hourly", "weekly"]
    assert capsys.readouterr().err == (
        "warning: hourly is deployed but is no longer a job schedule in meltano.yml. Run "
        "`meltano --environment=prod invoke pdt-aws destroy hourly` to remove it.\n")


def test_each_takes_schedule_names(meltano_project, monkeypatch):
    ext = Pdt()
    monkeypatch.setattr(ext, "schedules", lambda: [Schedule("Daily_Sync", "daily", ["sync"])])
    ran = []
    monkeypatch.setattr(ext, "pdt", lambda *args: ran.append(args) or 0)
    assert ext.each("deploy", ["Daily_Sync"]) == 0
    assert ext.each("runs", ["daily-sync"]) == 0
    assert ran == [("deploy", "daily-sync"), ("runs", "daily-sync")]
    with pytest.raises(PdtMeltanoError, match="This project has: daily-sync"):
        ext.each("deploy", ["test-daily-sync"])


def test_only_deploy_leaves_out_a_removed_schedule_that_is_still_deployed(meltano_project, monkeypatch):
    ext = Pdt()
    ext.write_project([Schedule("hourly", "hourly", ["sync"])])
    (ext.stage / ".pdt").mkdir()
    (ext.stage / ".pdt" / "state").write_text('{"deployed": ["hourly"]}')
    monkeypatch.setattr(ext, "schedules", lambda: [])
    monkeypatch.setattr(ext, "pdt", lambda *args: 0)
    assert ext.each("destroy", ["hourly"]) == 0
    with pytest.raises(PdtMeltanoError, match="no job schedule named hourly"):
        ext.each("deploy", ["hourly"])


def test_a_command_that_is_not_ours_goes_to_pdt(monkeypatch):
    sent = []
    monkeypatch.setattr(main.Pdt, "__init__", lambda self: None)
    monkeypatch.setattr(main.Pdt, "invoke", lambda self, *args: sent.append(args) or sys.exit(0))
    monkeypatch.setattr(sys, "argv", ["pdt_meltano", "list", "--names"])
    with pytest.raises(SystemExit):
        main.main()
    assert sent == [("list", "--names")]


def test_help_comes_from_pdt(monkeypatch):
    sent = []
    monkeypatch.setattr(main.Pdt, "__init__", lambda self: None)
    monkeypatch.setattr(main.Pdt, "pdt", lambda self, *args: sent.append(args) or 0)
    for argv in (["--help"], ["-h"], ["runs", "daily-sync", "-h"], ["--version"]):
        monkeypatch.setattr(sys, "argv", ["pdt_meltano", *argv])
        with pytest.raises(SystemExit):
            main.main()
    assert sent == [("--help",), ("-h",), ("runs", "daily-sync", "-h"), ("--version",)]


def test_help_for_our_commands_is_ours(monkeypatch, capsys):
    monkeypatch.setattr(main.Pdt, "pdt", lambda self, *args: pytest.fail("sent to pdt"))
    for argv in (["deploy", "--help"], ["destroy", "-h"], ["describe", "--help"]):
        monkeypatch.setattr(sys, "argv", ["pdt_meltano", *argv])
        with pytest.raises(SystemExit):
            main.main()
        assert "Usage:" in capsys.readouterr().out


def test_pdt_stderr_goes_to_the_terminal_that_stdout_goes_to(meltano_project, monkeypatch):
    runs = []
    monkeypatch.setattr(extension.subprocess, "run",
                        lambda *args, **kwargs: runs.append(kwargs) or SimpleNamespace(returncode=0))
    for tty in (False, True):
        monkeypatch.setattr(sys, "stdout", SimpleNamespace(isatty=lambda: tty))
        Pdt().pdt("list")
    assert [run["stderr"] for run in runs] == [None, sys.stdout]


def test_log_writes_a_level_meltano_reads(monkeypatch, capsys):
    monkeypatch.setenv("MELTANO_UTILITY_NAMESPACE", "pdt_aws")
    monkeypatch.delenv("PDT_AWS__LOG_PARSER", raising=False)
    extension.log("warning", "hourly is deployed")
    assert capsys.readouterr().err == "warning: hourly is deployed\n"
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    extension.log("warning", "hourly is deployed")
    assert capsys.readouterr().out == "warning: hourly is deployed\n"
    monkeypatch.setenv("PDT_AWS__LOG_PARSER", "singer-sdk")
    extension.log("warning", "hourly is deployed")
    line = json.loads(capsys.readouterr().err)
    assert {key: line[key] for key in ("level", "message", "logger_name")} == {
        "level": "warning", "message": "hourly is deployed", "logger_name": "pdt-meltano"}
    assert {"pid", "ts", "thread_name", "app_name", "stream_name"} <= set(line)


def test_describe_text_names_the_commands(meltano_project):
    assert "- deploy" in Pdt().describe_formatted()


def test_deploy_needs_an_environment(meltano_project, monkeypatch):
    monkeypatch.setenv("MELTANO_ENVIRONMENT", "")
    with pytest.raises(PdtMeltanoError, match="no Meltano environment is active"):
        Pdt().each("deploy", [])


def test_the_provider_setting_is_the_plugin_name_and_pdt_gets_its_own_name(meltano_project, monkeypatch):
    monkeypatch.setenv("PDT_AWS_PROVIDER", "gcloud")
    ext = Pdt()
    ext.write_project([])
    assert yaml.safe_load((ext.stage / "pdt.yml").read_text())["platform"]["provider"] == "google-cloud"
    monkeypatch.setenv("PDT_AWS_PROVIDER", "google")
    with pytest.raises(PdtMeltanoError, match="it must be one of aws, azure, gcloud."):
        Pdt().write_project([])


ALERTS = [{"type": "slack", "channel": "alerts", "events": ["job:run-failed"]}]


def use_plugin(monkeypatch, name, provider):
    monkeypatch.setenv("MELTANO_UTILITY_NAME", name)
    monkeypatch.setenv("MELTANO_UTILITY_NAMESPACE", name.replace("-", "_"))
    monkeypatch.setenv(f"{name.replace('-', '_').upper()}_PROVIDER", provider)


def test_alerts_set_up_through_one_plugin_reach_every_plugin(meltano_project, monkeypatch):
    def alerts_setup(args, env, **kwargs):
        """Write what `pdt alerts setup` writes, with pdt's own functions."""
        from dotenv import set_key
        from pdt import config
        monkeypatch.setenv("PDT_PROJECT", env["PDT_PROJECT"])
        config.save_root_alerts(ALERTS)
        set_key(Path(env["PDT_PROJECT"]) / ".env", "SLACK_WEBHOOK_ALERTS", "https://hooks.slack.com/x",
                quote_mode="never")
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(extension.subprocess, "run", alerts_setup)
    aws = Pdt()
    aws.write_project([Schedule("daily-sync", "daily", ["sync"])])
    aws.pdt("alerts", "setup")
    assert yaml.safe_load((meltano_project / "pdt.yml").read_text()) == {"alerts": ALERTS}
    assert "SLACK_WEBHOOK_ALERTS=https://hooks.slack.com/x" in (meltano_project / ".env").read_text()
    assert not (aws.stage / ".env").exists()
    use_plugin(monkeypatch, "pdt-azure", "azure")
    azure = Pdt()
    azure.write_project([Schedule("daily-sync", "daily", ["sync"])])
    assert yaml.safe_load((azure.stage / "pdt.yml").read_text()) == {
        "platform": {"provider": "azure"}, "alerts": ALERTS}
    assert "SLACK_WEBHOOK_ALERTS" in yaml.safe_load(
        (azure.stage / "daily-sync" / "config.yml").read_text())["env"]["optional"]
    assert not (azure.stage / "daily-sync" / "pdt.yml").exists()
    aws.write_project([Schedule("daily-sync", "daily", ["sync"])])
    assert yaml.safe_load((aws.stage / "pdt.yml").read_text())["alerts"] == ALERTS


def test_a_key_removed_from_the_project_pdt_yml_stays_removed(meltano_project, monkeypatch):
    (meltano_project / "pdt.yml").write_text(yaml.safe_dump({"alerts": ALERTS}))
    ext = Pdt()
    ext.write_project([])
    (meltano_project / "pdt.yml").write_text("{}\n")
    ext.write_project([])
    ext.keep_written()
    assert "alerts" not in yaml.safe_load((ext.stage / "pdt.yml").read_text())
    assert yaml.safe_load((meltano_project / "pdt.yml").read_text()) == {}


def test_what_pdt_wrote_into_an_older_run_folder_moves_to_the_project(meltano_project):
    ext = Pdt()
    ext.stage.mkdir(parents=True)
    (ext.stage / "pdt.yml").write_text(
        extension.LEGACY_HEADER + "platform:\n  provider: aws\n  account: '123456789012'\n"
        "alerts:\n  - type: slack\n    channel: alerts\n    events: [job:run-failed]\n")
    (ext.stage / ".env").write_text("SLACK_WEBHOOK_ALERTS=https://hooks.slack.com/x\n")
    ext.write_project([])
    assert yaml.safe_load((meltano_project / "pdt.yml").read_text()) == {"alerts": ALERTS}
    assert "SLACK_WEBHOOK_ALERTS=https://hooks.slack.com/x" in (meltano_project / ".env").read_text()
    assert yaml.safe_load((ext.stage / "pdt.yml").read_text()) == {
        "platform": {"account": "123456789012", "provider": "aws", "region": "us-east-2"},
        "alerts": ALERTS}
