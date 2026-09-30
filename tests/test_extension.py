import sys
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
    assert ext.stage == meltano_project / ".meltano" / "run" / "pdt-aws"
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


def test_a_system_folder_inside_the_project_is_not_copied(meltano_project, monkeypatch):
    monkeypatch.setenv("MELTANO_SYS_DIR_ROOT", str(meltano_project / "state" / "meltano"))
    ext = Pdt()
    ext.write_project([Schedule("daily-sync", "daily", ["sync"])])
    assert ext.stage == meltano_project / "state" / "meltano" / "run" / "pdt-aws"
    assert not (ext.stage / "daily-sync" / "state" / "meltano").exists()


def test_write_project_keeps_what_pdt_wrote_back(meltano_project):
    ext = Pdt()
    ext.stage.mkdir(parents=True)
    (ext.stage / "pdt.yml").write_text(
        'platform:\n  provider: aws\n  account: "123456789012"\n  timezone: Etc/UTC\n')
    ext.write_project([])
    assert yaml.safe_load((ext.stage / "pdt.yml").read_text())["platform"] == {
        "account": "123456789012", "provider": "aws", "region": "us-east-2"}


def test_every_setting_goes_to_platform_for_pdt_to_check(meltano_project, monkeypatch):
    monkeypatch.setenv("PDT_AWS_TIMEZONE", "America/Chicago")
    monkeypatch.setenv("PDT_AWS_BUGABOO", "abcdef")
    ext = Pdt()
    ext.write_project([])
    assert yaml.safe_load((ext.stage / "pdt.yml").read_text())["platform"] == {
        "provider": "aws", "region": "us-east-2", "timezone": "America/Chicago", "bugaboo": "abcdef"}


def test_a_missing_provider_names_the_fix(meltano_project, monkeypatch):
    monkeypatch.setenv("PDT_AWS_PROVIDER", "")
    with pytest.raises(PdtMeltanoError, match="Meltano Hub entry"):
        Pdt().write_project([])


def test_write_project_follows_meltano_yml(meltano_project):
    ext = Pdt()
    ext.write_project([Schedule("daily-sync", "daily", ["sync"]), Schedule("hourly", "hourly", ["sync"])])
    (ext.stage / ".pdt").mkdir()
    (ext.stage / ".pdt" / "state").write_text('{"deployed": ["hourly"]}')
    assert ext.write_project([Schedule("weekly", "weekly", ["sync"])]) == ["weekly"]
    assert sorted(run_py.parent.name for run_py in ext.stage.glob("*/run.py")) == ["hourly", "weekly"]


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


def test_stderr_goes_to_the_terminal_that_stdout_goes_to(monkeypatch):
    dups = []
    monkeypatch.setattr(main.os, "dup2", lambda *fds: dups.append(fds))
    monkeypatch.setattr(main.Pdt, "__init__", lambda self: None)
    monkeypatch.setattr(main.Pdt, "invoke", lambda self, *args: sys.exit(0))
    monkeypatch.setattr(sys, "argv", ["pdt_meltano", "list"])
    monkeypatch.setattr(sys, "stderr", SimpleNamespace(fileno=lambda: 2))
    for tty in (False, True):
        monkeypatch.setattr(sys, "stdout", SimpleNamespace(isatty=lambda: tty, fileno=lambda: 1))
        with pytest.raises(SystemExit):
            main.main()
    assert dups == [(1, 2)]


def test_deploy_needs_an_environment(meltano_project, monkeypatch):
    monkeypatch.setenv("MELTANO_ENVIRONMENT", "")
    with pytest.raises(PdtMeltanoError, match="no Meltano environment is active"):
        Pdt().each("deploy", [])
