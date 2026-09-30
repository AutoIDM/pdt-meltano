"""Deploy the schedules of a Meltano project as pdt jobs.

pdt deploys an app: a folder holding run.py, next to a pdt.yml. This
extension writes one such app for each Meltano schedule into a pdt
project in the run folder Meltano gives the plugin,
.meltano/run/<plugin name>/, then runs pdt on it. Each app folder is a
copy of the Meltano project plus three generated files: run.py, which
runs the schedule's job; config.yml, which holds its cron; and a
Dockerfile, which installs the schedule's plugins when the image is
built.

A cloud job starts from a new container each time, so run.py keeps the
Meltano system database, which holds each extractor's state, in the
app's pdt storage folder: it pulls the database before the run and
pushes it after, under pdt's storage lock.

Every setting of the plugin goes under platform: in pdt.yml, so pdt
checks it. The provider comes from the Meltano Hub entry the user added (a
default setting value); the user sets the others with `meltano config`.
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
from pathlib import Path

import yaml
from dotenv import dotenv_values
from meltano.edk import models
from meltano.edk.extension import ExtensionBase

PROVIDERS = ("aws", "azure", "google-cloud")
LEFT_OUT = (".meltano", ".git", ".env", ".env.*", ".venv", "venv", "__pycache__",
            "output", ".pdt", ".pdt-state")
# pdt saves these into pdt.yml on the first deploy.
WRITTEN_BACK = ("account", "profile", "project", "subscription")
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
from pathlib import Path

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
    store = storage.store()
    state = Path(".pdt-state").resolve()
    lease = store.pull("state/", state)
    env = dict(os.environ, MELTANO_DATABASE_URI=f"sqlite:///{{state / 'meltano.db'}}")
    code = subprocess.run(["meltano", "--environment", ENVIRONMENT, "run", *JOB], env=env).returncode
    store.push(state, "state/", lease)
    return code


if __name__ == "__main__":
    sys.exit(main())
'''

DOCKERFILE = """\
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends git \\
    && rm -rf /var/lib/apt/lists/*
COPY . /workspace
WORKDIR /workspace/{app}
ENV PDT_PROJECT=/workspace NO_COLOR=1 MELTANO_SEND_ANONYMOUS_USAGE_STATS=false
RUN uv sync --script run.py && uv run --script run.py install
ENTRYPOINT ["sh", "-c", "uv run --script run.py; code=$?; echo \\"pdt: exit $code\\"; exit $code"]
"""


class PdtMeltanoError(Exception):
    pass


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
        self.stage = sys_dir / "run" / os.environ["MELTANO_UTILITY_NAME"]
        self.left_out = LEFT_OUT + ((sys_dir.name,) if sys_dir.is_relative_to(self.root) else ())
        prefix = os.environ.get("MELTANO_UTILITY_NAMESPACE", "").upper() + "_"
        self.settings = {key[len(prefix):].lower(): value.strip()
                         for key, value in os.environ.items()
                         if prefix != "_" and key.startswith(prefix) and value.strip() != ""}
        self.provider = self.settings.get("provider", "")
        self.environment = os.environ.get("MELTANO_ENVIRONMENT", "")

    def describe(self) -> models.Describe:
        return models.Describe(commands=[models.ExtensionCommand(
            name="pdt_meltano", description="deploy Meltano schedules with pdt",
            commands=["deploy", "destroy", "describe", "initialize"])])

    def schedules(self) -> list[Schedule]:
        listing = json.loads(meltano("schedule", "list", "--format=json", cwd=self.root))
        found, skipped = parse_schedules(listing)
        for reason in skipped:
            print(f"pdt-meltano: left out {reason}", file=sys.stderr)
        return found

    def deployed(self) -> set[str]:
        state = self.stage / ".pdt" / "state"
        return set(json.loads(state.read_text()).get("deployed") or []) if state.is_file() else set()

    def write_project(self, schedules: list[Schedule]) -> list[str]:
        """Write the pdt project for these schedules and return their app names.

        An app folder whose schedule is gone is removed, unless it is still
        deployed: destroy needs the folder to find what to remove.
        """
        if self.provider not in PROVIDERS:
            raise PdtMeltanoError(
                f"the provider setting is {self.provider!r}; it must be one of "
                f"{', '.join(PROVIDERS)}. Add the plugin from its Meltano Hub entry, "
                "which sets it for you.")
        self.stage.mkdir(parents=True, exist_ok=True)
        project_file = self.stage / "pdt.yml"
        existing = yaml.safe_load(project_file.read_text()) if project_file.is_file() else {}
        platform = (existing or {}).get("platform") or {}
        if platform.get("provider") != self.provider:
            platform = {}
        platform = {**{key: platform[key] for key in WRITTEN_BACK if key in platform},
                    **self.settings}
        project_file.write_text(
            "# Written by pdt-meltano from the plugin settings in meltano.yml. Changes here\n"
            "# are kept only for keys pdt writes back, such as the cloud account.\n"
            + yaml.safe_dump({"platform": platform}, sort_keys=False))
        env_names = sorted(dotenv_values(self.root / ".env")) if (self.root / ".env").is_file() else []
        versions = {"pdt": importlib.metadata.version("pdt-cli"),
                    "meltano": meltano("--version", cwd=self.root).split()[-1]}
        names = []
        for schedule in schedules:
            name = app_name(schedule.name)
            folder = self.stage / name
            shutil.rmtree(folder, ignore_errors=True)
            shutil.copytree(self.root, folder, ignore=shutil.ignore_patterns(*self.left_out),
                            symlinks=True)
            (folder / "run.py").write_text(RUN_PY.format(
                schedule=schedule.name, job=schedule.job, environment=self.environment, **versions))
            (folder / "Dockerfile").write_text(DOCKERFILE.format(app=name))
            (folder / "config.yml").write_text(yaml.safe_dump(
                {"schedule": schedule.cron, "env": {"optional": env_names}}, sort_keys=False))
            names.append(name)
        deployed = self.deployed()
        for folder in self.stage.iterdir():
            if folder.name in names or not (folder / "run.py").is_file():
                continue
            if folder.name in deployed:
                print(f"pdt-meltano: {folder.name} is deployed but is no longer a job schedule "
                      f"in meltano.yml. Run `destroy {folder.name}` to remove it.", file=sys.stderr)
            else:
                shutil.rmtree(folder)
        return names

    def pdt(self, *args: str) -> int:
        env = {**os.environ, **{key: value for key, value in dotenv_values(self.root / ".env").items()
                                if value is not None and key not in os.environ},
               "PDT_PROJECT": str(self.stage)}
        return subprocess.run([sys.executable, "-m", "pdt.cli", *args], env=env).returncode

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
