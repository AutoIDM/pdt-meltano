"""Deploy the schedules of a Meltano project as pdt jobs.

pdt deploys an app: a folder holding run.py, next to a pdt.yml. This
extension writes one such app for each Meltano schedule into a pdt
project in .meltano/utilities/<plugin name>/pdt/, then runs pdt on it.
`meltano install` empties the run folder Meltano gives the plugin,
.meltano/run/<plugin name>/, but keeps this folder, so what pdt writes
there lasts: the deployed list, and the keys pdt saves on a deploy. Each
app folder is a copy of the Meltano project plus three generated files:
run.py, which runs the schedule's job; config.yml, which holds its cron;
and a Dockerfile, which installs the schedule's plugins when the image is
built.

A cloud job starts from a new container each time, so run.py keeps the
Meltano system database, which holds each extractor's state, in the
app's pdt storage folder: it pulls the database before the run and
pushes it after, under pdt's storage lock.

Every setting of the Hub entries goes under platform: in pdt.yml, so pdt
checks it. Another env var with the plugin's prefix does not. The provider
comes from the Meltano Hub entry the user added (a default setting value);
the user sets the others with `meltano config`.

Every other key of pdt.yml, such as alerts:, is the same for each cloud, so it
lives in <config_dir>/pdt.yml, by default orchestrate/pdt/pdt.yml in the
Meltano project. Every pdt plugin of the project reads it. `alerts setup` and
`alerts test` run pdt on that folder, so pdt edits the file itself and keeps
its comments. When a deploy's alerts offer writes alerts: into the plugin's
pdt project, pdt-meltano copies them to the shared file with pdt's own edit.
A value that pdt writes into a .env in either folder moves to .env in the
Meltano project after pdt exits.
"""

from __future__ import annotations

import dataclasses
import importlib.metadata
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import yaml
from dotenv import dotenv_values, set_key
from meltano.edk import models
from meltano.edk.extension import DescribeFormat, ExtensionBase
from pdt import config as pdt_config
from pdt.deploy_common import BASE_IMAGE

# The provider setting of each Hub entry, and the pdt provider it names.
PROVIDERS = {"aws": "aws", "azure": "azure", "gcloud": "google-cloud"}
LEFT_OUT = (".meltano", ".git", ".env", ".env.*", ".venv", "venv", "__pycache__",
            "output", ".pdt", "pdt.yml")
# The settings of the Meltano Hub entries. Each one that has a value goes under platform: in pdt.yml.
SETTINGS = ("provider", "region", "timezone", "account", "profile", "subscription",
            "resource_group", "environment", "project")
# pdt saves these into pdt.yml on the first deploy.
WRITTEN_BACK = ("account", "profile", "project", "subscription")
# pdt saves these into an app's config.yml with `pdt pause` and when deploy --all disables an app.
APP_WRITTEN_BACK = ("enabled", "pause")
SHORTHAND = {"@hourly": "hourly", "@daily": "daily", "@weekly": "weekly",
             "@monthly": "monthly", "@yearly": "yearly"}

RUN_PY = '''\
# /// script
# requires-python = ">=3.12"
# dependencies = ["pdt-cli[apps]=={pdt}", "meltano=={meltano}"]
# ///
"""Run the Meltano schedule {schedule!r}. pdt-meltano writes this file on every deploy."""

import os
import subprocess
import sys

from pdt.config import load_env_json
from pdt.utils import storage

SCHEDULE = {schedule!r}
JOB = {job!r}
ENVIRONMENT = {environment!r}


def main() -> int:
    if sys.argv[1:] == ["install"]:
        return subprocess.run(
            ["meltano", "--environment", ENVIRONMENT, "install", "--schedule", SCHEDULE]).returncode
    load_env_json()
    with storage.sync(push_state_on_error=True) as run:
        env = dict(os.environ, MELTANO_DATABASE_URI=f"sqlite:///{{run.state / 'meltano.db'}}")
        return subprocess.run(["meltano", "--environment", ENVIRONMENT, "run", *JOB], env=env).returncode


if __name__ == "__main__":
    sys.exit(main())
'''

STARTER = """\
# pdt settings that every pdt plugin of this Meltano project reads, such as alerts:.
# pdt-meltano writes platform: from the plugin settings in meltano.yml, so leave it out.
# Run `meltano invoke {plugin} alerts setup` to add alerts:, or write them here:
#
# alerts:
#   - type: slack
#     channel: it-alerts
#     events: [app:deploy-completed, app:deploy-failed, job:run-failed]
"""

DOCKERFILE = f"FROM {BASE_IMAGE}\n" + """\
RUN apt-get update && apt-get install -y --no-install-recommends git \\
    && rm -rf /var/lib/apt/lists/*
COPY . /workspace
WORKDIR /workspace/{app}
ENV PDT_PROJECT=/workspace NO_COLOR=1 PYTHONUNBUFFERED=1 MELTANO_SEND_ANONYMOUS_USAGE_STATS=false
RUN uv sync --script run.py && uv run --script run.py install
ENTRYPOINT ["sh", "-c", "uv run --script run.py; code=$?; echo \\"pdt: exit $code\\"; exit $code"]
"""


class PdtMeltanoError(Exception):
    pass


def log(level: str, message: str) -> None:
    """Write a message at `level`: warning, error, or info.

    Meltano logs each stderr line of a utility at info, unless the plugin sets
    the structured-logging capability and a _log_parser in meltano.yml. Then
    Meltano reads a Singer SDK JSON line at the level it names. Otherwise the
    message goes to the terminal when there is one, where Meltano does not
    label it.
    """
    namespace = os.environ.get("MELTANO_UTILITY_NAMESPACE", "").upper()
    if namespace and os.environ.get(f"{namespace}__LOG_PARSER") == "singer-sdk":
        message = json.dumps({"level": level, "pid": os.getpid(), "logger_name": "pdt-meltano",
                              "ts": time.time(), "thread_name": "main", "app_name": "pdt-meltano",
                              "stream_name": None, "message": message})
    else:
        message = f"{level}: {message}"
        if sys.stdout.isatty():
            print(message, flush=True)
            return
    print(message, file=sys.stderr, flush=True)


def load_yaml(path: Path) -> dict:
    return (yaml.safe_load(path.read_text()) if path.is_file() else None) or {}


@dataclasses.dataclass(frozen=True)
class Schedule:
    name: str
    cron: str
    job: list[str]


def meltano(*args: str, cwd: Path) -> str:
    proc = subprocess.run(["meltano", *args], cwd=cwd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise PdtMeltanoError(f"`meltano {' '.join(args)}` failed:\n{proc.stderr.strip()}")
    return proc.stdout


def parse_schedules(listing: dict) -> tuple[list[Schedule], list[str]]:
    """The schedules pdt can deploy, and a reason for each one it cannot."""
    found, skipped = [], []
    for item in listing["schedules"].get("job", []):
        interval = item.get("cron_interval") or item.get("interval") or ""
        if interval in ("", "@once", "@manual"):
            skipped.append(f"{item['name']}: it has no repeating interval ({interval or 'none'})")
            continue
        found.append(Schedule(item["name"], SHORTHAND.get(interval, interval), [item["job"]["name"]]))
    for item in listing["schedules"].get("elt", []):
        skipped.append(f"{item['name']}: it runs `meltano elt`; make it a job schedule "
                       "(`meltano job add`, then `meltano schedule add --job`)")
    return found, skipped


def app_name(schedule: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", schedule.lower()).strip("-")


class Pdt(ExtensionBase):
    def __init__(self) -> None:
        self.root = Path(os.environ["MELTANO_PROJECT_ROOT"]).resolve()
        sys_dir = Path(os.environ["MELTANO_SYS_DIR_ROOT"]).resolve()
        self.stage = sys_dir / "utilities" / os.environ["MELTANO_UTILITY_NAME"] / "pdt"
        # Where older versions wrote the pdt project.
        self.legacy_stage = sys_dir / "run" / os.environ["MELTANO_UTILITY_NAME"]
        self.left_out = LEFT_OUT + ((sys_dir.name,) if sys_dir.is_relative_to(self.root) else ())
        namespace = os.environ.get("MELTANO_UTILITY_NAMESPACE", "").upper()
        values = {name: os.environ.get(f"{namespace}_{name.upper()}", "").strip()
                  for name in SETTINGS} if namespace else {}
        self.settings = {name: value for name, value in values.items() if value}
        self.provider = self.settings.get("provider", "")
        config_dir = os.environ.get(f"{namespace}_CONFIG_DIR", "").strip() if namespace else ""
        self.config_dir = (self.root / (config_dir or "orchestrate/pdt")).resolve()
        self.environment = os.environ.get("MELTANO_ENVIRONMENT", "")

    def describe(self) -> models.Describe:
        return models.Describe(commands=[models.ExtensionCommand(
            name="pdt_meltano", description="deploy Meltano schedules with pdt",
            commands=["deploy", "destroy", "describe", "initialize"])])

    def describe_formatted(self, output_format: DescribeFormat = DescribeFormat.text) -> str:
        # The EDK prints text with devtools, which shows no fields of the EDK's slots dataclasses.
        if output_format == DescribeFormat.text:
            output_format = DescribeFormat.yaml
        return super().describe_formatted(output_format)

    def schedules(self) -> list[Schedule]:
        listing = json.loads(meltano("schedule", "list", "--format=json", cwd=self.root))
        found, skipped = parse_schedules(listing)
        for reason in skipped:
            log("info", f"left out {reason}")
        return found

    def deployed(self) -> set[str]:
        state = self.stage / ".pdt" / "state"
        return set(json.loads(state.read_text()).get("deployed") or []) if state.is_file() else set()

    def write_project(self, schedules: list[Schedule]) -> list[str]:
        """Write the pdt project for these schedules and return their app names.

        An app folder whose schedule is gone is removed, unless it is still
        deployed: destroy needs the folder to find what to remove.
        """
        provider = PROVIDERS.get(self.provider)
        if provider is None:
            raise PdtMeltanoError(
                f"the provider setting is {self.provider!r}; it must be one of "
                f"{', '.join(PROVIDERS)}. Add the plugin from its Meltano Hub entry, "
                "which sets it for you.")
        if not self.stage.exists() and (self.legacy_stage / "pdt.yml").is_file():
            self.stage.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(self.legacy_stage, self.stage)
            log("info", f"moved the pdt project from {self.legacy_stage} to {self.stage}")
        self.stage.mkdir(parents=True, exist_ok=True)
        project_file = self.stage / "pdt.yml"
        existing = load_yaml(project_file)
        platform = existing.get("platform") or {}
        if platform.get("provider") != provider:
            platform = {}
        platform = {**{key: platform[key] for key in WRITTEN_BACK if key in platform},
                    **self.settings, "provider": provider}
        shared = {key: value for key, value in load_yaml(self.config_dir / "pdt.yml").items()
                  if key != "platform"}
        project_file.write_text(
            "# Written by pdt-meltano from the plugin settings in meltano.yml and from\n"
            f"# {self.config_dir / 'pdt.yml'}. pdt-meltano copies alerts: that pdt\n"
            "# writes here to that file.\n"
            + yaml.safe_dump({"platform": platform, **shared}, sort_keys=False))
        env_names = sorted(dotenv_values(self.root / ".env")) if (self.root / ".env").is_file() else []
        versions = {"pdt": importlib.metadata.version("pdt-cli"),
                    "meltano": meltano("--version", cwd=self.root).split()[-1]}
        names = []
        for schedule in schedules:
            name = app_name(schedule.name)
            folder = self.stage / name
            config = folder / "config.yml"
            own = load_yaml(config)
            kept = {key: own[key] for key in APP_WRITTEN_BACK if key in own}
            shutil.rmtree(folder, ignore_errors=True)
            shutil.copytree(self.root, folder, ignore=shutil.ignore_patterns(*self.left_out),
                            symlinks=True)
            (folder / "run.py").write_text(RUN_PY.format(
                schedule=schedule.name, job=schedule.job, environment=self.environment, **versions))
            (folder / "Dockerfile").write_text(DOCKERFILE.format(app=name))
            config.write_text(yaml.safe_dump(
                {"schedule": schedule.cron, "env": {"optional": env_names}, **kept}, sort_keys=False))
            names.append(name)
        deployed = self.deployed()
        for folder in self.stage.iterdir():
            if folder.name in names or not (folder / "run.py").is_file():
                continue
            if folder.name in deployed:
                environment = f" --environment={self.environment}" if self.environment else ""
                log("warning", f"{folder.name} is deployed but is no longer a job schedule in "
                    f"meltano.yml. Run `meltano{environment} invoke "
                    f"{os.environ['MELTANO_UTILITY_NAME']} destroy {folder.name}` to remove it.")
            else:
                shutil.rmtree(folder)
        return names

    def initialize(self, force: bool = False) -> None:
        """Write a starter <config_dir>/pdt.yml, unless the file exists."""
        shared_file = self.config_dir / "pdt.yml"
        if shared_file.is_file():
            log("info", f"{shared_file} exists, so it stays as it is")
            return
        self.config_dir.mkdir(parents=True, exist_ok=True)
        shared_file.write_text(STARTER.format(plugin=os.environ.get("MELTANO_UTILITY_NAME", "pdt-aws")))
        log("info", f"wrote {shared_file}")

    def keep_written(self, copy_alerts: bool = True) -> None:
        """Copy the alerts: that pdt wrote into the plugin's pdt project to the shared
        pdt.yml, and move each value pdt wrote into a .env of either folder to .env in
        the Meltano project. Every pdt plugin of the project then reads them."""
        alerts = load_yaml(self.stage / "pdt.yml").get("alerts") if copy_alerts else None
        if alerts is not None and alerts != load_yaml(self.config_dir / "pdt.yml").get("alerts"):
            if not (self.config_dir / "pdt.yml").is_file():
                self.initialize()
            saved = os.environ.get("PDT_PROJECT")
            os.environ["PDT_PROJECT"] = str(self.config_dir)
            try:
                pdt_config.save_root_alerts(alerts)
            finally:
                if saved is None:
                    os.environ.pop("PDT_PROJECT")
                else:
                    os.environ["PDT_PROJECT"] = saved
            log("info", f"saved alerts in {self.config_dir / 'pdt.yml'}")
        for env_file in (self.stage / ".env", self.config_dir / ".env"):
            values = {key: value for key, value in dotenv_values(env_file).items()
                      if value is not None} if env_file.is_file() else {}
            if values:
                (self.root / ".env").touch(mode=0o600)
                for key, value in values.items():
                    set_key(self.root / ".env", key, value, quote_mode="never")
                log("info", f"saved {', '.join(values)} in .env in the Meltano project")
            env_file.unlink(missing_ok=True)

    def pdt(self, *args: str) -> int:
        # The shared pdt.yml holds alerts:, so pdt edits it there and keeps its comments.
        if args[:2] == ("alerts", "setup") and not (self.config_dir / "pdt.yml").is_file():
            self.initialize()
        shared = args[:1] == ("alerts",) and (self.config_dir / "pdt.yml").is_file()
        project = self.config_dir if shared else self.stage
        env = {**os.environ, **{key: value for key, value in dotenv_values(self.root / ".env").items()
                                if value is not None and key not in os.environ},
               "PDT_PROJECT": str(project)}
        # Meltano shows a utility's stderr one line at a time, so a prompt with no
        # newline would not show. pdt's stderr goes to the terminal instead.
        stderr = sys.stdout if sys.stdout.isatty() else None
        try:
            return subprocess.run([sys.executable, "-m", "pdt.cli", *args], env=env,
                                  stderr=stderr).returncode
        finally:
            self.keep_written(copy_alerts=project == self.stage)

    def each(self, command: str, names: list[str], extra: tuple[str, ...] = ()) -> int:
        """Run a pdt command on the named schedules, or on every one.

        Only deploy is limited to the schedules in meltano.yml; the other
        commands also reach a removed schedule that is still deployed.
        """
        if command == "deploy" and self.environment == "":
            raise PdtMeltanoError(
                "no Meltano environment is active, and `meltano run` needs one. Set "
                "`default_environment` in meltano.yml, or deploy with `meltano --environment <name>`.")
        current = self.write_project(self.schedules())
        known = current if command == "deploy" else sorted(
            run_py.parent.name for run_py in self.stage.glob("*/run.py"))
        unknown = [name for name in names if app_name(name) not in known]
        if unknown:
            raise PdtMeltanoError(
                f"no job schedule named {', '.join(unknown)}. This project has: "
                f"{', '.join(known) or 'none'}. Add one with `meltano schedule add`.")
        apps = [app_name(name) for name in names] or known
        if not apps:
            raise PdtMeltanoError("this project has no job schedule with a repeating interval. "
                                  "Add one with `meltano schedule add`.")
        codes = [self.pdt(command, app, *extra) for app in apps]
        return max(codes)

    def invoke(self, command_name: str | None, *command_args: str) -> None:
        self.write_project(self.schedules())
        sys.exit(self.pdt(*([command_name] if command_name else []), *command_args))
