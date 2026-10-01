"""The `pdt_meltano` command that Meltano runs for each Hub entry."""

from __future__ import annotations

import os
import sys
from typing import List, Optional

import typer
from meltano.edk.extension import DescribeFormat

from pdt_meltano.extension import Pdt, PdtMeltanoError, log

app = typer.Typer(name="pdt_meltano", pretty_exceptions_enable=False, no_args_is_help=True,
                  context_settings={"help_option_names": ["-h", "--help"]},
                  epilog="Any other command, such as list, validate, or run, goes to pdt.")
YES = typer.Option(False, "--yes", help="skip the confirmation prompt")


def run(command: str, schedules: list[str], *extra: str) -> None:
    try:
        sys.exit(Pdt().each(command, schedules, tuple(extra)))
    except PdtMeltanoError as e:
        log("error", str(e))
        sys.exit(1)


@app.command()
def deploy(schedules: Optional[List[str]] = typer.Argument(None), yes: bool = YES) -> None:
    """Deploy each job schedule, or only the ones named, as a scheduled cloud job."""
    run("deploy", schedules or [], *(["--yes"] if yes else []))


@app.command()
def destroy(schedules: Optional[List[str]] = typer.Argument(None), yes: bool = YES) -> None:
    """Remove everything deploy created for each schedule, or only the ones named."""
    run("destroy", schedules or [], *(["--yes"] if yes else []))


@app.command()
def describe(output_format: DescribeFormat = typer.Option(DescribeFormat.text, "--format")) -> None:
    """Describe the commands of this extension."""
    typer.echo(Pdt().describe_formatted(output_format))


@app.command()
def initialize(force: bool = typer.Option(False, help="ignored")) -> None:
    """Nothing to set up; deploy writes what it needs."""


def main() -> None:
    """Send any command that is not one of ours, and any request for help or
    the version that is not about one of ours, to pdt, and name the command in
    usage and error text as the user typed it: `meltano invoke pdt-aws`."""
    args = sys.argv[1:]
    own = {command.name or command.callback.__name__ for command in app.registered_commands}
    if args[:1] == ["--version"] or ({"-h", "--help"} & set(args) and args[0] not in own):
        sys.exit(Pdt().pdt(*args))
    if len(sys.argv) > 1 and not sys.argv[1].startswith("-") and sys.argv[1] not in own:
        try:
            Pdt().invoke(*sys.argv[1:])
        except PdtMeltanoError as e:
            log("error", str(e))
            sys.exit(1)
    name = os.environ.get("MELTANO_UTILITY_NAME")
    app(prog_name=f"meltano invoke {name}" if name else "pdt_meltano")
