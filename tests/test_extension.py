import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from pdt.config import PLATFORM_KEYS

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


def test_only_the_declared_settings_go_to_platform_for_pdt_to_check(meltano_project, monkeypatch):
    monkeypatch.setenv("PDT_AWS_TIMEZONE", "America/Chicago")
    monkeypatch.setenv("PDT_AWS_RESOURCE_GROUP", "jobs")
    monkeypatch.setenv("PDT_AWS_BUGABOO", "abcdef")
    monkeypatch.setenv("PDT_AWS__LOG_PARSER", "singer-sdk")
    ext = Pdt()
    ext.write_project([])
    assert yaml.safe_load((ext.stage / "pdt.yml").read_text())["platform"] == {
        "provider": "aws", "region": "us-east-2", "timezone": "America/Chicago", "resource_group": "jobs"}


def test_the_hub_entries_and_the_readme_name_the_settings_the_extension_reads():
    repo = Path(__file__).parent.parent
    readme = (repo / "README.md").read_text()
    for entry in sorted((repo / "hub").glob("*.yml")):
        settings = yaml.safe_load(entry.read_text())["settings"]
        assert tuple(setting["name"] for setting in settings) == (*extension.SETTINGS, "config_dir"), entry.name
        assert all({"label", "description", "kind"} <= set(setting) for setting in settings), entry.name
    assert all(f"| `{name}`" in readme for name in extension.SETTINGS)
    assert "`config_dir`" in readme and "config_dir: $MELTANO_PROJECT_ROOT/orchestrate/pdt-prod" in readme
    assert set(extension.SETTINGS) == PLATFORM_KEYS


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


def fake_pdt_alerts(monkeypatch, alerts, ran=None):
    """Write what `pdt alerts setup` or a deploy's alerts offer writes, with pdt's own functions."""
    def run(args, env, **kwargs):
        from dotenv import set_key
        from pdt import config
        if ran is not None:
            ran.append(env["PDT_PROJECT"])
        monkeypatch.setenv("PDT_PROJECT", env["PDT_PROJECT"])
        config.save_root_alerts(alerts)
        set_key(Path(env["PDT_PROJECT"]) / ".env", "SLACK_WEBHOOK_ALERTS", "https://hooks.slack.com/x",
                quote_mode="never")
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(extension.subprocess, "run", run)


def test_config_dir_defaults_to_orchestrate_pdt_and_stays_out_of_platform(meltano_project, monkeypatch):
    assert Pdt().config_dir == meltano_project / "orchestrate" / "pdt"
    monkeypatch.setenv("PDT_AWS_CONFIG_DIR", "orchestrate/pdt-prod")
    assert Pdt().config_dir == meltano_project / "orchestrate" / "pdt-prod"
    monkeypatch.setenv("PDT_AWS_CONFIG_DIR", str(meltano_project / "shared"))
    ext = Pdt()
    assert ext.config_dir == meltano_project / "shared"
    ext.write_project([])
    assert "config_dir" not in yaml.safe_load((ext.stage / "pdt.yml").read_text())["platform"]


def test_the_shared_keys_come_from_the_config_dir_only(meltano_project):
    (meltano_project / "orchestrate" / "pdt").mkdir(parents=True)
    (meltano_project / "orchestrate" / "pdt" / "pdt.yml").write_text(
        "# shared\nalerts:\n  - type: slack\n    channel: alerts\n    events: [job:run-failed]\n"
        "platform:\n  provider: azure\n")
    (meltano_project / "pdt.yml").write_text("docker-build: cloud\n")
    ext = Pdt()
    ext.write_project([])
    assert yaml.safe_load((ext.stage / "pdt.yml").read_text()) == {
        "platform": {"provider": "aws", "region": "us-east-2"}, "alerts": ALERTS}


def test_initialize_writes_a_starter_pdt_yml_once(meltano_project):
    ext = Pdt()
    ext.initialize()
    shared = ext.config_dir / "pdt.yml"
    assert yaml.safe_load(shared.read_text()) is None
    assert "meltano invoke pdt-aws alerts setup" in shared.read_text()
    assert "# alerts:\n#   - type: slack\n" in shared.read_text()
    shared.write_text("# mine\n")
    ext.initialize()
    assert shared.read_text() == "# mine\n"


def test_alerts_setup_on_a_missing_folder_writes_the_shared_file_and_every_plugin_reads_it(
        meltano_project, monkeypatch):
    ran = []
    fake_pdt_alerts(monkeypatch, [], ran)
    aws = Pdt()
    aws.write_project([Schedule("daily-sync", "daily", ["sync"])])
    aws.pdt("alerts", "--help")
    assert not aws.config_dir.exists()
    fake_pdt_alerts(monkeypatch, ALERTS, ran)
    aws.pdt("alerts", "setup")
    assert ran == [str(aws.stage), str(aws.config_dir)]
    shared = (aws.config_dir / "pdt.yml").read_text()
    assert shared.startswith(extension.STARTER.format(plugin="pdt-aws"))
    assert yaml.safe_load(shared) == {"alerts": ALERTS}
    assert "SLACK_WEBHOOK_ALERTS=https://hooks.slack.com/x" in (meltano_project / ".env").read_text()
    assert not (aws.config_dir / ".env").exists()
    assert not (meltano_project / "pdt.yml").exists()
    use_plugin(monkeypatch, "pdt-azure", "azure")
    azure = Pdt()
    azure.write_project([Schedule("daily-sync", "daily", ["sync"])])
    assert yaml.safe_load((azure.stage / "pdt.yml").read_text()) == {
        "platform": {"provider": "azure"}, "alerts": ALERTS}
    assert "SLACK_WEBHOOK_ALERTS" in yaml.safe_load(
        (azure.stage / "daily-sync" / "config.yml").read_text())["env"]["optional"]
    assert not (azure.stage / "daily-sync" / "pdt.yml").exists()


def test_alerts_setup_replaces_the_alerts_and_keeps_comments(meltano_project, monkeypatch):
    new = [{"type": "slack", "channel": "it-alerts", "events": ["job:run-failed"]}]
    (meltano_project / "orchestrate" / "pdt").mkdir(parents=True)
    shared = meltano_project / "orchestrate" / "pdt" / "pdt.yml"
    shared.write_text("# our alerts\nalerts:\n  - type: slack\n    channel: alerts\n"
                      "    events: [job:run-failed]  # old\n\n# the build\ndocker-build: cloud\n")
    fake_pdt_alerts(monkeypatch, new)
    ext = Pdt()
    ext.write_project([])
    ext.pdt("alerts", "setup")
    assert yaml.safe_load(shared.read_text()) == {"alerts": new, "docker-build": "cloud"}
    assert "# our alerts\n" in shared.read_text() and "# the build\n" in shared.read_text()


def test_alerts_from_a_deploy_offer_reach_the_shared_file_with_comments_kept(meltano_project, monkeypatch):
    (meltano_project / "orchestrate" / "pdt").mkdir(parents=True)
    shared = meltano_project / "orchestrate" / "pdt" / "pdt.yml"
    shared.write_text("# the build\ndocker-build: cloud\n")
    ran = []
    fake_pdt_alerts(monkeypatch, ALERTS, ran)
    ext = Pdt()
    ext.write_project([Schedule("daily-sync", "daily", ["sync"])])
    ext.pdt("deploy", "daily-sync")
    assert ran == [str(ext.stage)]
    assert yaml.safe_load(shared.read_text()) == {"docker-build": "cloud", "alerts": ALERTS}
    assert shared.read_text().startswith("# the build\n")
    assert "SLACK_WEBHOOK_ALERTS=https://hooks.slack.com/x" in (meltano_project / ".env").read_text()
    assert not (ext.stage / ".env").exists()


def test_alerts_removed_from_the_shared_pdt_yml_stay_removed(meltano_project):
    (meltano_project / "orchestrate" / "pdt").mkdir(parents=True)
    shared = meltano_project / "orchestrate" / "pdt" / "pdt.yml"
    shared.write_text(yaml.safe_dump({"alerts": ALERTS}))
    ext = Pdt()
    ext.write_project([])
    shared.write_text("# none\n")
    ext.write_project([])
    ext.keep_written()
    assert "alerts" not in yaml.safe_load((ext.stage / "pdt.yml").read_text())
    assert shared.read_text() == "# none\n"
