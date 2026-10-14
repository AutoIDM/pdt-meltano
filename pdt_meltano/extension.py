"""Deploy the schedules of a Meltano project as pdt jobs.

pdt deploys an app: a folder holding run.py, next to a pdt.yml. This
extension writes one such app for each Meltano schedule into a pdt
project in .meltano/utilities/<plugin name>/pdt/<environment>/<Meltano
project folder name>/, then runs pdt on it. pdt names the project after
that folder in events and alerts, and the image keeps the project in
/<Meltano project folder name>, so a deployed run shows the same name.
Each Meltano environment has its own project, and each app is
named <schedule>-<environment>, so dev and prod deploy as separate jobs
with separate state. The user names schedules in every command, and
pdt-meltano gives pdt the app of the active environment. `meltano
install` empties the run folder Meltano gives the plugin,
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

The image holds no .env. A deploy sends the job the env values it reads as
the app's cloud secret: the setting env names of the job's plugins, from
`meltano config print --format=env`, and each $NAME in those plugins'
blocks and in the project settings of meltano.yml, when .env or the shell
gives it a value. config.yml lists them under env: optional:. Only deploy
and secrets find them again; the other commands keep the list. pdt adds
the alert webhooks itself.

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
from concurrent.futures import ThreadPoolExecutor
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
# The pdt commands that take an app name. pdt-meltano gives them the app of a schedule.
APP_COMMANDS = ("run", "deploy", "login", "destroy", "pause", "unpause", "secrets", "storage",
                "runs", "logs", "health")
# The pdt commands that send or compare an app's env values, so they need its env names.
SECRET_COMMANDS = ("deploy", "secrets")
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
COPY . /{project}
WORKDIR /{project}/{app}
ENV PDT_PROJECT=/{project} NO_COLOR=1 PYTHONUNBUFFERED=1 MELTANO_SEND_ANONYMOUS_USAGE_STATS=false
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
    plugins: tuple[str, ...] = ()


def task_plugins(tasks: list) -> tuple[str, ...]:
    """The plugins a job's tasks run: each name of a `tap mapping target` block, without
    the `:command` of a `plugin:command` task."""
    blocks = [block for task in tasks
              for block in (task_plugins(task) if isinstance(task, list) else task.split())]
    return tuple(dict.fromkeys(block.split(":")[0] for block in blocks))


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
        found.append(Schedule(item["name"], SHORTHAND.get(interval, interval), [item["job"]["name"]],
                              task_plugins(item["job"].get("tasks") or [])))
    for item in listing["schedules"].get("elt", []):
        skipped.append(f"{item['name']}: it runs `meltano elt`; make it a job schedule "
                       "(`meltano job add`, then `meltano schedule add --job`)")
    return found, skipped


def folder_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def app_name(schedule: str, environment: str) -> str:
    """The pdt app of a schedule in a Meltano environment, so dev and prod deploy apart."""
    return f"{folder_name(schedule)}-{folder_name(environment)}"


class Pdt(ExtensionBase):
    def __init__(self) -> None:
        self.root = Path(os.environ["MELTANO_PROJECT_ROOT"]).resolve()
        sys_dir = Path(os.environ["MELTANO_SYS_DIR_ROOT"]).resolve()
        self.left_out = LEFT_OUT + ((sys_dir.name,) if sys_dir.is_relative_to(self.root) else ())
        namespace = os.environ.get("MELTANO_UTILITY_NAMESPACE", "").upper()
        values = {name: os.environ.get(f"{namespace}_{name.upper()}", "").strip()
                  for name in SETTINGS} if namespace else {}
        self.settings = {name: value for name, value in values.items() if value}
        self.provider = self.settings.get("provider", "")
        config_dir = os.environ.get(f"{namespace}_CONFIG_DIR", "").strip() if namespace else ""
        self.config_dir = (self.root / (config_dir or "orchestrate/pdt")).resolve()
        self.environment = os.environ.get("MELTANO_ENVIRONMENT", "")
        # One pdt project for each Meltano environment, so each keeps its own deployed list.
        # pdt names the project after its folder in events and alerts, so the folder has
        # the Meltano project's name; the image copies it to the same name.
        self.stage = (sys_dir / "utilities" / os.environ["MELTANO_UTILITY_NAME"] / "pdt"
                      / folder_name(self.environment) / self.root.name)

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

    def write_project(self, schedules: list[Schedule], find_env: bool = True) -> list[str]:
        """Write the pdt project for these schedules and return their app names.

        An app folder whose schedule is gone is removed, unless it is still
        deployed: destroy needs the folder to find what to remove. Without
        `find_env`, each app keeps the env names its config.yml lists, because
        finding them takes a Meltano call for each plugin.
        """
        provider = PROVIDERS.get(self.provider)
        if provider is None:
            raise PdtMeltanoError(
                f"the provider setting is {self.provider!r}; it must be one of "
                f"{', '.join(PROVIDERS)}. Add the plugin from its Meltano Hub entry, "
                "which sets it for you.")
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
        env_names = self.env_names(schedules) if find_env else {}
        versions = {"pdt": importlib.metadata.version("pdt-cli"),
                    "meltano": meltano("--version", cwd=self.root).split()[-1]}
        names = []
        for schedule in schedules:
            name = app_name(schedule.name, self.environment)
            folder = self.stage / name
            config = folder / "config.yml"
            own = load_yaml(config)
            kept = {key: own[key] for key in APP_WRITTEN_BACK if key in own}
            optional = env_names.get(schedule.name, (own.get("env") or {}).get("optional") or [])
            shutil.rmtree(folder, ignore_errors=True)
            shutil.copytree(self.root, folder, ignore=shutil.ignore_patterns(*self.left_out),
                            symlinks=True)
            (folder / "run.py").write_text(RUN_PY.format(
                schedule=schedule.name, job=schedule.job, environment=self.environment, **versions))
            (folder / "Dockerfile").write_text(DOCKERFILE.format(app=name, project=self.root.name))
            config.write_text(yaml.safe_dump(
                {"schedule": schedule.cron, "env": {"optional": optional}, **kept},
                sort_keys=False))
            names.append(name)
        deployed = self.deployed()
        for folder in self.stage.iterdir():
            if folder.name in names or not (folder / "run.py").is_file():
                continue
            if folder.name in deployed:
                log("warning", f"{self.schedule_of(folder.name)} is deployed but is no longer a job "
                    f"schedule in meltano.yml. Run `meltano --environment={self.environment} invoke "
                    f"{os.environ['MELTANO_UTILITY_NAME']} destroy {self.schedule_of(folder.name)}` "
                    "to remove it.")
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
        # pdt locks pdt.yml with a pdt.yml.lock file beside it and leaves the file there.
        gitignore = self.config_dir / ".gitignore"
        if not gitignore.exists():
            gitignore.write_text("pdt.yml.lock\n")

    def write_environment_project(self, command: str | None) -> list[str]:
        """Write the pdt project of the active Meltano environment and return its app names."""
        if self.environment == "":
            raise PdtMeltanoError(
                "no Meltano environment is active, and pdt-meltano deploys each environment as "
                "its own jobs. Set `default_environment` in meltano.yml, or run the command with "
                "`meltano --environment <name>`.")
        return self.write_project(self.schedules(), find_env=command in SECRET_COMMANDS)

    def env_names(self, schedules: list[Schedule]) -> dict[str, list[str]]:
        """For each schedule, the env names its job reads that have a value in .env or in the
        shell: the setting env names of the job's plugins, and each $NAME in their blocks of
        meltano.yml. Meltano sets the MELTANO_ names in the job itself, and the names of this
        pdt plugin's own settings are for this computer, so both stay out."""
        plugins = sorted({plugin for schedule in schedules for plugin in schedule.plugins})
        # One Meltano call for each plugin, at the same time, because each takes seconds.
        with ThreadPoolExecutor() as pool:
            settings = dict(zip(plugins, pool.map(self.setting_env_names, plugins)))
        referenced = self.referenced_names(plugins)
        env_file = self.root / ".env"
        values = {**(dotenv_values(env_file) if env_file.is_file() else {}), **os.environ}
        own = tuple(f"{os.environ.get(key, '').upper().replace('-', '_')}_"
                    for key in ("MELTANO_UTILITY_NAME", "MELTANO_UTILITY_NAMESPACE"))
        have = {name for name, value in values.items()
                if value and not name.startswith(("MELTANO_", *own))}
        return {schedule.name: sorted(have & set().union(
                    *(settings[plugin] | referenced[plugin] for plugin in schedule.plugins)))
                for schedule in schedules}

    def referenced_names(self, plugins: list[str]) -> dict[str, set[str]]:
        """Each $NAME in the blocks of each plugin in meltano.yml and its include_paths files:
        the plugin entry, the mapper of a mapping, the active environment's config for the
        plugin, and the same for each inherit_from parent. Every `meltano run` reads the
        project settings, such as state_backend, so their $NAMEs go to every plugin."""
        documents = [load_yaml(self.root / "meltano.yml")]
        for pattern in documents[0].get("include_paths") or []:
            documents += [load_yaml(path) for path in sorted(self.root.glob(pattern))]
        entries, overrides, project = {}, [], []
        for document in documents:
            project.append({key: value for key, value in document.items() if key not in (
                "plugins", "environments", "jobs", "schedules", "include_paths")})
            for group in (document.get("plugins") or {}).values():
                for entry in group or []:
                    entries[entry["name"]] = entry
                    for mapping in entry.get("mappings") or []:
                        entries[mapping["name"]] = {**mapping, "inherit_from": entry["name"]}
            for environment in document.get("environments") or []:
                if environment.get("name") == self.environment:
                    project.append({key: value for key, value in (environment.get("config") or {}).items()
                                    if key != "plugins"})
                    for group in ((environment.get("config") or {}).get("plugins") or {}).values():
                        overrides += group or []
        found = {}
        for plugin in plugins:
            blocks, name = list(project), plugin
            while name in entries and entries[name] not in blocks:
                blocks += [entries[name], *(entry for entry in overrides if entry.get("name") == name)]
                name = entries[name].get("inherit_from")
            found[plugin] = set(re.findall(r"\$\{?([A-Za-z_]\w*)", json.dumps(blocks)))
        return found

    def setting_env_names(self, plugin: str) -> set[str]:
        """The env names of the plugin's settings that have a value. Meltano hides each
        sensitive value, and only the names are kept."""
        out = meltano("--environment", self.environment, "config", "print", plugin, "--format=env",
                      cwd=self.root)
        return set(re.findall(r"^([A-Za-z_]\w*)=", out, re.MULTILINE))

    def schedule_of(self, app: str) -> str:
        return app.removesuffix(f"-{folder_name(self.environment)}")

    def app_of(self, name: str, known: list[str]) -> str | None:
        """The app of the schedule `name` in the active environment, or `name` when it
        is already an app name; None when it is neither."""
        app = app_name(name, self.environment)
        return app if app in known else name if name in known else None

    def known_apps(self) -> list[str]:
        return sorted(run_py.parent.name for run_py in self.stage.glob("*/run.py"))

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
        current = self.write_environment_project(command)
        known = current if command == "deploy" else self.known_apps()
        unknown = [name for name in names if self.app_of(name, known) is None]
        if unknown:
            raise PdtMeltanoError(
                f"no job schedule named {', '.join(unknown)}. This project has: "
                f"{', '.join(map(self.schedule_of, known)) or 'none'}. "
                "Add one with `meltano schedule add`.")
        apps = [self.app_of(name, known) for name in names] or known
        if not apps:
            raise PdtMeltanoError("this project has no job schedule with a repeating interval. "
                                  "Add one with `meltano schedule add`.")
        codes = [self.pdt(command, app, *extra) for app in apps]
        return max(codes)

    def invoke(self, command_name: str | None, *command_args: str) -> None:
        self.write_environment_project(command_name)
        args = list(command_args)
        if command_name in APP_COMMANDS:
            # The user names a schedule; pdt needs the app of the active environment.
            known = self.known_apps()
            for index, arg in enumerate(args):
                app = None if arg.startswith("-") else self.app_of(arg, known)
                if app is not None:
                    args[index] = app
                    break
        sys.exit(self.pdt(*([command_name] if command_name else []), *args))
