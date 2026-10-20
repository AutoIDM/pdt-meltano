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


def test_parse_schedules_takes_the_plugins_of_each_job():
    listing = {"schedules": {"job": [{"name": "s", "cron_interval": "@daily", "job": {
        "name": "j", "tasks": ["tap-a hide-email target-b", ["tap-c", "target-b"], "dbt:run"]}}]}}
    assert parse_schedules(listing)[0][0].plugins == ("tap-a", "hide-email", "target-b", "tap-c", "dbt")


def test_app_name_is_the_schedule_and_environment_as_a_folder_name_pdt_accepts():
    assert app_name("daily-sync", "prod") == "daily-sync-prod"
    assert app_name("Daily_Sync", "Staging EU") == "daily-sync-staging-eu"


# What `meltano config print <plugin> --format=env` prints, with the values hidden.
PLUGIN_ENV = {
    "tap-github": "TAP_GITHUB_TOKEN='*****'\nMELTANO_EXTRACT_TOKEN='*****'\nTAP_GITHUB_START_DATE=2024-01-01\n",
    "target-postgres": "TARGET_POSTGRES_PASSWORD='*****'\nMELTANO_LOAD_PASSWORD='*****'\n",
    "dbt": "DBT_PROFILES_DIR=/x\n",
    "pdt-aws": "PDT_AWS_PROVIDER=aws\nPDT_AWS_REGION=us-east-2\nPDT_AWS_CONFIG_DIR=/x\n",
}


@pytest.fixture
def meltano_project(tmp_path, monkeypatch):
    root = tmp_path / "warehouse"
    (root / "extract").mkdir(parents=True)
    (root / "meltano.yml").write_text("project_id: x\n")
    (root / "extract" / "catalog.json").write_text("{}")
    (root / ".env").write_text("TAP_GITHUB_TOKEN=abc\nTARGET_POSTGRES_PASSWORD=def\n")
    (root / ".meltano" / "plugins").mkdir(parents=True)
    monkeypatch.setenv("MELTANO_PROJECT_ROOT", str(root))
    monkeypatch.setenv("MELTANO_SYS_DIR_ROOT", str(root / ".meltano"))
    monkeypatch.setenv("MELTANO_UTILITY_NAME", "pdt-aws")
    monkeypatch.setenv("MELTANO_UTILITY_NAMESPACE", "pdt_aws")
    monkeypatch.setenv("PDT_AWS_PROVIDER", "aws")
    monkeypatch.setenv("PDT_AWS_REGION", "us-east-2")
    monkeypatch.setenv("MELTANO_ENVIRONMENT", "prod")

    def fake_meltano(*args, cwd):
        if args == ("--version",):
            return "meltano, version 4.2.0\n"
        if args[2:4] == ("config", "print"):
            if args[4] in ("tap-empty", "tap-broken") and args[5] == "--format=env":
                raise PdtMeltanoError("`meltano config print` failed:\nFileNotFoundError")
            if args[5] == "--format=json":
                return '{"token": "*****"}\n' if args[4] == "tap-broken" else "{}\n"
            return PLUGIN_ENV.get(args[4], "")
        return ""
    monkeypatch.setattr(extension, "meltano", fake_meltano)
    return root


def test_write_project_makes_one_app_per_schedule(meltano_project):
    ext = Pdt()
    names = ext.write_project([Schedule("daily-sync", "daily", ["sync"], ("tap-github", "target-postgres"))])
    assert names == ["daily-sync-prod"]
    assert ext.stage == meltano_project / ".meltano" / "utilities" / "pdt-aws" / "pdt" / "prod" / "warehouse"
    app = ext.stage / "daily-sync-prod"
    assert (app / "meltano.yml").is_file()
    assert (app / "extract" / "catalog.json").is_file()
    assert not (app / ".env").exists()
    assert not (app / ".meltano").exists()
    assert yaml.safe_load((app / "config.yml").read_text()) == {
        "schedule": "daily", "env": {"optional": ["TAP_GITHUB_TOKEN", "TARGET_POSTGRES_PASSWORD"]}}
    run_py = (app / "run.py").read_text()
    assert '"pdt-cli[apps]==' in run_py and '"meltano==4.2.0"' in run_py
    assert "JOB = ['sync']" in run_py
    assert "ENVIRONMENT = 'prod'" in run_py
    compile(run_py, "run.py", "exec")
    dockerfile = (app / "Dockerfile").read_text()
    assert ('COPY [".", "/srv/warehouse"]\nWORKDIR "/srv/warehouse/daily-sync-prod"\n'
            'ENV PDT_PROJECT="/srv/warehouse" ') in dockerfile
    assert yaml.safe_load((ext.stage / "pdt.yml").read_text()) == {
        "platform": {"provider": "aws", "region": "us-east-2"}}


def test_each_app_lists_only_the_env_names_its_job_reads(meltano_project, monkeypatch):
    (meltano_project / "meltano.yml").write_text("project_id: x\nplugins: {extractors: [{name: tap-github, "
                                                 "config: {org: $GITHUB_ORG, out: '${MELTANO_PROJECT_ROOT}/out'}}]}\n")
    (meltano_project / ".env").write_text("TAP_GITHUB_TOKEN=a\nTARGET_POSTGRES_PASSWORD=b\nGITHUB_ORG=c\n"
                                          "AWS_SECRET_ACCESS_KEY=d\nDBT_PROFILES_DIR=\n")
    monkeypatch.setenv("TAP_GITHUB_START_DATE", "2024-01-01")
    calls = []
    fake = extension.meltano
    monkeypatch.setattr(extension, "meltano", lambda *args, cwd: calls.append(args) or fake(*args, cwd=cwd))
    ext = Pdt()
    ext.write_project([Schedule("github", "daily", ["a"], ("tap-github", "target-postgres")),
                       Schedule("models", "daily", ["b"], ("dbt", "target-postgres"))])
    optional = {app: yaml.safe_load((ext.stage / app / "config.yml").read_text())["env"]["optional"]
                for app in ("github-prod", "models-prod")}
    assert optional == {
        "github-prod": ["GITHUB_ORG", "TAP_GITHUB_START_DATE", "TAP_GITHUB_TOKEN", "TARGET_POSTGRES_PASSWORD"],
        "models-prod": ["TARGET_POSTGRES_PASSWORD"]}
    assert sorted(call[4] for call in calls if call[2:4] == ("config", "print")) == [
        "dbt", "tap-github", "target-postgres"]
    assert all(call[:2] == ("--environment", "prod") for call in calls if "config" in call)


MELTANO_YML = """\
project_id: x
include_paths: [./meltano-*.yml]
plugins:
  extractors:
  - {name: tap-base, config: {key: $BASE_KEY}}
  - {name: tap-child, inherit_from: tap-base, config: {org: '${CHILD_ORG}'}}
  - {name: tap-other, config: {key: $OTHER_KEY}}
  mappers:
  - name: mapper-x
    config: {salt: $MAPPER_SALT}
    mappings:
    - {name: hide-email, config: {field: $HIDE_FIELD}}
environments:
- name: prod
  config:
    plugins:
      extractors:
      - {name: tap-child, config: {host: $PROD_HOST}}
- name: dev
  config:
    plugins:
      extractors:
      - {name: tap-child, config: {host: $DEV_HOST}}
"""


def test_a_plugin_with_no_setting_values_adds_no_name(meltano_project):
    ext = Pdt()
    ext.write_project([Schedule("empty", "daily", ["e"], ("tap-empty", "target-postgres"))])
    assert yaml.safe_load((ext.stage / "empty-prod" / "config.yml").read_text())["env"] == {
        "optional": ["TARGET_POSTGRES_PASSWORD"]}
    with pytest.raises(PdtMeltanoError, match="FileNotFoundError"):
        ext.setting_env_names("tap-broken")


def test_a_hand_made_shared_pdt_yml_gets_the_lock_gitignore(meltano_project, monkeypatch):
    fake_pdt_alerts(monkeypatch, ALERTS)
    ext = Pdt()
    ext.config_dir.mkdir(parents=True)
    (ext.config_dir / "pdt.yml").write_text("# mine\n")
    ext.pdt("alerts", "setup")
    assert (ext.config_dir / ".gitignore").read_text() == "pdt.yml.lock\n"


def test_names_come_only_from_the_blocks_of_the_job_plugins(meltano_project):
    (meltano_project / "meltano.yml").write_text(MELTANO_YML)
    (meltano_project / "meltano-loaders.yml").write_text(
        "plugins:\n  loaders:\n  - {name: target-x, config: {password: $TARGET_X_PW}}\n")
    assert Pdt().referenced_names(["tap-child", "hide-email", "target-x", "tap-unknown"]) == {
        "tap-child": {"BASE_KEY", "CHILD_ORG", "PROD_HOST"},
        "hide-email": {"HIDE_FIELD", "MAPPER_SALT"},
        "target-x": {"TARGET_X_PW"},
        "tap-unknown": set()}


def test_the_project_settings_names_go_to_every_job(meltano_project, monkeypatch):
    (meltano_project / "meltano.yml").write_text(
        "project_id: x\nstate_backend: {uri: 's3://${STATE_BUCKET}/state'}\n"
        "send_anonymous_usage_stats: $MELTANO_STATS\njobs: [{name: j, tasks: [$JOB_ONLY]}]\n"
        "plugins: {extractors: [{name: tap-github, config: {org: $GITHUB_ORG}}],\n"
        "  loaders: [{name: target-postgres}]}\n"
        "environments:\n- {name: prod, config: {state_backend: {uri: $PROD_STATE}, plugins: {loaders: "
        "[{name: target-postgres, config: {host: $PROD_HOST}}]}}}\n"
        "- {name: dev, config: {state_backend: {uri: $DEV_STATE}}}\n")
    for name in ("STATE_BUCKET", "MELTANO_STATS", "JOB_ONLY", "GITHUB_ORG", "PROD_STATE", "PROD_HOST",
                 "DEV_STATE"):
        monkeypatch.setenv(name, "x")
    ext = Pdt()
    assert ext.referenced_names(["tap-github", "target-postgres"]) == {
        "tap-github": {"STATE_BUCKET", "MELTANO_STATS", "PROD_STATE", "GITHUB_ORG"},
        "target-postgres": {"STATE_BUCKET", "MELTANO_STATS", "PROD_STATE", "PROD_HOST"}}
    ext.write_project([Schedule("sync", "daily", ["j"], ("target-postgres",))])
    assert yaml.safe_load((ext.stage / "sync-prod" / "config.yml").read_text())["env"]["optional"] == [
        "PROD_HOST", "PROD_STATE", "STATE_BUCKET", "TARGET_POSTGRES_PASSWORD"]


def test_the_names_of_this_pdt_plugin_stay_out(meltano_project, monkeypatch):
    monkeypatch.setenv("PDT_AWS_CONFIG_DIR", "/x")
    ext = Pdt()
    ext.write_project([Schedule("describe", "daily", ["d"], ("pdt-aws", "tap-github"))])
    assert yaml.safe_load((ext.stage / "describe-prod" / "config.yml").read_text())["env"] == {
        "optional": ["TAP_GITHUB_TOKEN"]}


def test_only_deploy_and_secrets_find_the_env_names(meltano_project, monkeypatch):
    calls = []
    fake = extension.meltano
    monkeypatch.setattr(extension, "meltano", lambda *args, cwd: calls.append(args) or fake(*args, cwd=cwd))
    ext = Pdt()
    monkeypatch.setattr(ext, "schedules", lambda: [Schedule("daily-sync", "daily", ["sync"], ("tap-github",))])
    monkeypatch.setattr(ext, "pdt", lambda *args: 0)
    config = ext.stage / "daily-sync-prod" / "config.yml"
    finds = []
    for command in ("list", "deploy", "logs", "secrets", None):
        calls.clear()
        if command == "deploy":
            ext.each(command, [])
        else:
            with pytest.raises(SystemExit):
                ext.invoke(command, "daily-sync")
        finds.append(any(call[2:4] == ("config", "print") for call in calls))
        if command == "deploy":
            written = config.read_text()
        if command == "logs":
            assert config.read_text() == written
    assert finds == [False, True, False, True, False]
    assert yaml.safe_load(written)["env"] == {"optional": ["TAP_GITHUB_TOKEN"]}


def test_the_image_keeps_a_project_folder_with_a_space_under_srv(tmp_path, meltano_project, monkeypatch):
    root = meltano_project.rename(tmp_path / "usr warehouse")
    monkeypatch.setenv("MELTANO_PROJECT_ROOT", str(root))
    monkeypatch.setenv("MELTANO_SYS_DIR_ROOT", str(root / ".meltano"))
    ext = Pdt()
    ext.write_project([Schedule("daily-sync", "daily", ["sync"])])
    assert ext.stage.name == "usr warehouse"
    dockerfile = (ext.stage / "daily-sync-prod" / "Dockerfile").read_text()
    assert ('COPY [".", "/srv/usr warehouse"]\nWORKDIR "/srv/usr warehouse/daily-sync-prod"\n'
            'ENV PDT_PROJECT="/srv/usr warehouse" ') in dockerfile


def test_a_system_folder_inside_the_project_is_not_copied(meltano_project, monkeypatch):
    monkeypatch.setenv("MELTANO_SYS_DIR_ROOT", str(meltano_project / "state" / "meltano"))
    ext = Pdt()
    ext.write_project([Schedule("daily-sync", "daily", ["sync"])])
    assert ext.stage == (meltano_project / "state" / "meltano" / "utilities" / "pdt-aws" / "pdt" / "prod"
                         / "warehouse")
    assert not (ext.stage / "daily-sync-prod" / "state" / "meltano").exists()


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
    config = ext.stage / "daily-sync-prod" / "config.yml"
    config.write_text(config.read_text() + "pause: true\nenabled: false\n")
    ext.write_project([Schedule("daily-sync", "hourly", ["sync"])])
    assert yaml.safe_load(config.read_text()) == {
        "schedule": "hourly", "env": {"optional": []}, "enabled": False, "pause": True}


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
    cloud_settings = {"aws": ["account", "profile"], "azure": ["environment", "resource_group", "subscription"],
                      "gcloud": ["project"]}
    for entry in sorted((repo / "hub").glob("*.yml")):
        settings = yaml.safe_load(entry.read_text())["settings"]
        provider = next(setting["value"] for setting in settings if setting["name"] == "provider")
        names = ["config_dir", "provider", "region", "timezone", *cloud_settings[provider]]
        assert [setting["name"] for setting in settings] == sorted(names), entry.name
        assert all({"label", "description", "kind"} <= set(setting) for setting in settings), entry.name
    assert sorted(["provider", "region", "timezone", *sum(cloud_settings.values(), [])]) == sorted(extension.SETTINGS)
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
    (ext.stage / ".pdt" / "state").write_text('{"deployed": ["hourly-prod"]}')
    assert ext.write_project([Schedule("weekly", "weekly", ["sync"])]) == ["weekly-prod"]
    assert ext.known_apps() == ["hourly-prod", "weekly-prod"]
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
    assert ext.each("runs", ["daily-sync-prod"]) == 0
    assert ran == [("deploy", "daily-sync-prod"), ("runs", "daily-sync-prod"), ("runs", "daily-sync-prod")]
    with pytest.raises(PdtMeltanoError, match="This project has: daily-sync"):
        ext.each("deploy", ["test-daily-sync"])


def test_only_deploy_leaves_out_a_removed_schedule_that_is_still_deployed(meltano_project, monkeypatch):
    ext = Pdt()
    ext.write_project([Schedule("hourly", "hourly", ["sync"])])
    (ext.stage / ".pdt").mkdir()
    (ext.stage / ".pdt" / "state").write_text('{"deployed": ["hourly-prod"]}')
    monkeypatch.setattr(ext, "schedules", lambda: [])
    monkeypatch.setattr(ext, "pdt", lambda *args: 0)
    assert ext.each("destroy", ["hourly"]) == 0
    with pytest.raises(PdtMeltanoError, match="no job schedule named hourly"):
        ext.each("deploy", ["hourly"])


def test_each_environment_has_its_own_project_and_apps(meltano_project, monkeypatch):
    monkeypatch.setenv("MELTANO_ENVIRONMENT", "dev")
    dev = Pdt()
    dev.write_project([Schedule("daily-sync", "daily", ["sync"])])
    (dev.stage / ".pdt").mkdir()
    (dev.stage / ".pdt" / "state").write_text('{"deployed": ["daily-sync-dev"]}')
    monkeypatch.setenv("MELTANO_ENVIRONMENT", "prod")
    prod = Pdt()
    prod.write_project([Schedule("hourly", "hourly", ["sync"])])
    assert dev.stage.parent.parent == prod.stage.parent.parent
    from pdt import lifecycle
    for ext in (dev, prod):
        monkeypatch.setenv("PDT_PROJECT", str(ext.stage))
        assert lifecycle.app_params(ext.known_apps()[0], "aws")["project"] == "warehouse"
    assert (dev.known_apps(), prod.known_apps()) == (["daily-sync-dev"], ["hourly-prod"])
    assert prod.deployed() == set()


def test_a_pdt_command_gets_the_app_of_the_schedule_it_names(meltano_project, monkeypatch):
    sent = []
    ext = Pdt()
    monkeypatch.setattr(ext, "schedules", lambda: [Schedule("daily-sync", "daily", ["sync"])])
    monkeypatch.setattr(ext, "pdt", lambda *args: sent.append(args) or 0)
    for args in (("logs", "daily-sync", "2"), ("logs", "--since", "3d", "daily-sync"),
                 ("storage", "daily-sync", "get", "daily-sync"), ("runs", "daily-sync-prod"),
                 ("health",), ("list", "daily-sync"), ("pause", "other")):
        with pytest.raises(SystemExit):
            ext.invoke(*args)
    assert sent == [("logs", "daily-sync-prod", "2"), ("logs", "--since", "3d", "daily-sync-prod"),
                    ("storage", "daily-sync-prod", "get", "daily-sync"), ("runs", "daily-sync-prod"),
                    ("health",), ("list", "daily-sync"), ("pause", "other")]


def test_every_command_needs_an_environment(meltano_project, monkeypatch):
    monkeypatch.setenv("MELTANO_ENVIRONMENT", "")
    with pytest.raises(PdtMeltanoError, match="no Meltano environment is active"):
        Pdt().invoke("logs", "daily-sync")


def test_a_command_that_is_not_ours_goes_to_pdt(monkeypatch):
    monkeypatch.setenv("MELTANO_PROJECT_ROOT", "/project")
    sent = []
    monkeypatch.setattr(main.Pdt, "__init__", lambda self: None)
    monkeypatch.setattr(main.Pdt, "invoke", lambda self, *args: sent.append(args) or sys.exit(0))
    monkeypatch.setattr(sys, "argv", ["pdt_meltano", "list", "--names"])
    with pytest.raises(SystemExit):
        main.main()
    assert sent == [("list", "--names")]


def test_help_comes_from_pdt(monkeypatch):
    monkeypatch.setenv("MELTANO_PROJECT_ROOT", "/project")
    sent = []
    monkeypatch.setattr(main.Pdt, "__init__", lambda self: None)
    monkeypatch.setattr(main.Pdt, "pdt", lambda self, *args: sent.append(args) or 0)
    for argv in (["--help"], ["-h"], ["runs", "daily-sync", "-h"], ["--version"]):
        monkeypatch.setattr(sys, "argv", ["pdt_meltano", *argv])
        with pytest.raises(SystemExit):
            main.main()
    assert sent == [("--help",), ("-h",), ("runs", "daily-sync", "-h"), ("--version",)]


def test_outside_meltano_help_runs_pdt_and_other_commands_fail(monkeypatch, capsys):
    monkeypatch.delenv("MELTANO_PROJECT_ROOT", raising=False)
    monkeypatch.setattr(main.Pdt, "__init__", lambda self: pytest.fail("needs Meltano"))
    runs = []
    monkeypatch.setattr(main.subprocess, "run", lambda cmd: runs.append(cmd[3:]) or SimpleNamespace(returncode=3))
    for argv, code in ((["--help"], 3), (["--version"], 3), (["deploy"], 1), (["list"], 1), (["runs", "-h"], 1),
                       (["storage", "x", "destroy", "--yes", "--help"], 1), (["deploy", "--", "--help"], 1)):
        monkeypatch.setattr(sys, "argv", ["pdt_meltano", *argv])
        with pytest.raises(SystemExit) as exit:
            main.main()
        assert exit.value.code == code, argv
    assert runs == [["--help"], ["--version"]]
    assert "runs only through Meltano" in capsys.readouterr().err


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


def test_describe_marks_the_pdt_commands_as_pass_through(meltano_project):
    commands = yaml.safe_load(Pdt().describe_formatted())["commands"]
    assert [(c["name"], c["commands"], c["pass_through_cli"]) for c in commands] == [
        ("pdt_meltano", ["deploy", "destroy", "describe", "initialize"], False), ("pdt_meltano", [":splat"], True)]


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


def test_the_starter_comes_with_a_gitignore_for_the_lock_file_unless_one_exists(meltano_project):
    ext = Pdt()
    ext.initialize()
    assert (ext.config_dir / ".gitignore").read_text() == "pdt.yml.lock\n"
    (ext.config_dir / "pdt.yml").unlink()
    (ext.config_dir / ".gitignore").write_text("# mine\n")
    ext.initialize()
    assert (ext.config_dir / ".gitignore").read_text() == "# mine\n"


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
    assert (aws.config_dir / ".gitignore").read_text() == "pdt.yml.lock\n"
    assert "SLACK_WEBHOOK_ALERTS=https://hooks.slack.com/x" in (meltano_project / ".env").read_text()
    assert not (aws.config_dir / ".env").exists()
    assert not (meltano_project / "pdt.yml").exists()
    use_plugin(monkeypatch, "pdt-azure", "azure")
    azure = Pdt()
    azure.write_project([Schedule("daily-sync", "daily", ["sync"])])
    assert yaml.safe_load((azure.stage / "pdt.yml").read_text()) == {
        "platform": {"provider": "azure"}, "alerts": ALERTS}
    from pdt import config
    monkeypatch.setenv("PDT_PROJECT", str(azure.stage))
    assert "SLACK_WEBHOOK_ALERTS" in config.env_spec(config.merged_app("daily-sync-prod"))["required"]
    assert not (azure.stage / "daily-sync-prod" / "pdt.yml").exists()


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
