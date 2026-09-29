"""The `pdt_meltano` command that Meltano runs for each Hub entry."""

from __future__ import annotations

import sys
from typing import List, Optional

import typer
from meltano.edk.extension import DescribeFormat

from pdt_meltano.extension import Pdt, PdtMeltanoError

app = typer.Typer(name="pdt_meltano", pretty_exceptions_enable=False, no_args_is_help=True)
YES = typer.Option(False, "--yes", help="skip the confirmation prompt")


def run(command: str, schedules: list[str], *extra: str) -> None:
    try:
        sys.exit(Pdt().each(command, schedules, tuple(extra)))
    except PdtMeltanoError as e:
        typer.echo(f"error: {e}", err=True)
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
def runs(schedule: str) -> None:
    """List the recent runs of one deployed schedule."""
    run("runs", [schedule])


@app.command()
def logs(schedule: str, number: int = typer.Argument(1)) -> None:
    """Read the log of one run of a deployed schedule (1 is the newest)."""
    run("logs", [schedule], str(number))


@app.command()
def health() -> None:
    """Show whether the last run of each deployed schedule succeeded."""
    run("health", [])


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def invoke(ctx: typer.Context) -> None:
    """Run any pdt command on the project pdt-meltano writes, for example `invoke list`."""
    try:
        Pdt().invoke(*(ctx.args[:1] or [None]), *ctx.args[1:])
    except PdtMeltanoError as e:
        typer.echo(f"error: {e}", err=True)
        sys.exit(1)


@app.command()
def describe(output_format: DescribeFormat = typer.Option(DescribeFormat.text, "--format")) -> None:
    """Describe the commands of this extension."""
    typer.echo(Pdt().describe_formatted(output_format))


@app.command()
def initialize(force: bool = typer.Option(False, help="ignored")) -> None:
    """Nothing to set up; deploy writes what it needs."""
