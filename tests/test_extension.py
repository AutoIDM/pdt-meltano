import pytest
import yaml

from pdt_meltano import extension
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


def test_app_name_is_a_folder_name_pdt_accepts():
    assert app_name("My_Meltano Project", "Daily.Sync") == "my-meltano-project-daily-sync"


@pytest.fixture
def meltano_project(tmp_path, monkeypatch):
    root = tmp_path / "warehouse"
    (root / "extract").mkdir(parents=True)
    (root / "meltano.yml").write_text("project_id: x\n")
    (root / "extract" / "catalog.json").write_text("{}")
    (root / ".env").write_text("TAP_SECRET=abc\nTARGET_PASSWORD=def\n")
    (root / ".meltano" / "plugins").mkdir(parents=True)
    monkeypatch.setenv("MELTANO_PROJECT_ROOT", str(root))
    monkeypatch.setenv("PDT_MELTANO_PROVIDER", "aws")
    monkeypatch.setenv("PDT_MELTANO_REGION", "us-east-2")

    def fake_meltano(*args, cwd):
        return "meltano, version 4.2.0\n" if args == ("--version",) else ""
    monkeypatch.setattr(extension, "meltano", fake_meltano)
    return root


def test_write_project_makes_one_app_per_schedule(meltano_project):
    ext = Pdt()
    names = ext.write_project([Schedule("daily-sync", "daily", ["sync"])])
    assert names == ["warehouse-daily-sync"]
    app = ext.stage / "warehouse-daily-sync"
    assert (app / "meltano.yml").is_file()
    assert (app / "extract" / "catalog.json").is_file()
    assert not (app / ".env").exists()
    assert not (app / ".meltano").exists()
    assert yaml.safe_load((app / "config.yml").read_text()) == {
        "schedule": "daily", "env": {"optional": ["TAP_SECRET", "TARGET_PASSWORD"]}}
    run_py = (app / "run.py").read_text()
    assert '"pdt-cli[apps]==' in run_py and '"meltano==4.2.0"' in run_py
    assert "JOB = ['sync']" in run_py
    compile(run_py, "run.py", "exec")
    assert "WORKDIR /workspace/warehouse-daily-sync" in (app / "Dockerfile").read_text()
    assert yaml.safe_load((ext.stage / "pdt.yml").read_text()) == {
        "platform": {"provider": "aws", "region": "us-east-2"}}


def test_write_project_keeps_what_pdt_wrote_back(meltano_project):
    ext = Pdt()
    ext.stage.mkdir(parents=True)
    (ext.stage / "pdt.yml").write_text('platform:\n  provider: aws\n  account: "123456789012"\n')
    ext.write_project([])
    assert yaml.safe_load((ext.stage / "pdt.yml").read_text())["platform"] == {
        "provider": "aws", "account": "123456789012", "region": "us-east-2"}


def test_a_missing_provider_names_the_fix(meltano_project, monkeypatch):
    monkeypatch.setenv("PDT_MELTANO_PROVIDER", "")
    with pytest.raises(PdtMeltanoError, match="Meltano Hub entry"):
        Pdt().write_project([])
