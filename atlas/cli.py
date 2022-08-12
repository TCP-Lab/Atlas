import logging
from pathlib import Path

import colorama as c
import typer

from atlas.abcs import AtlasQuery
from atlas.core import Atlas
from atlas.utils.strings import INFO

log = logging.getLogger(__name__)

cli_root = typer.Typer(no_args_is_help=True)
info = typer.Typer()

cli_root.add_typer(info, name="info")


ATLAS = Atlas()


@cli_root.callback()
def context_handler():
    log.debug(f"Starting Atlas.")


@info.callback(invoke_without_command=True)
def generic_info(ctx: typer.Context):
    """Get information on the status of the tool."""
    log.debug("Invoked info command.")
    if ctx.invoked_subcommand:
        return

    print(INFO)


@info.command("database")
def info_database_command():
    """Get information on how much memory Atlas is using, and other useful
    statistics regarding the database.
    """
    log.debug("Invoked info::database command.")

    raise NotImplementedError()


@cli_root.command("tables")
def tables_command(
    query: str = typer.Argument(
        None, help="Search query to filter the tables before printing. RegEx allowed"
    )
):
    """Get a list of what data can be retrieved by Atlas."""
    log.debug(f"Invoked tables command. Args: query: '{query}'")

    print(ATLAS.supported_paths)


@cli_root.command("genquery")
def genquery_command(
    target: Path = typer.Argument(..., help="Target path to save the query to")
):
    """Generate a query file with a variety of options."""
    target = target.expanduser().resolve()
    log.debug(f"Invoked genquery command. Args: target: '{target}'")

    raise NotImplementedError()


@cli_root.command("retrieve")
def retrieve_command(
    query_file: Path = typer.Argument(..., help="Input query file"),
    target: Path = typer.Argument(..., help="Output file location"),
):
    """Retrieve data following a query file."""
    target = target.expanduser().resolve()
    query_file = query_file.expanduser().resolve()

    log.debug(
        f"Invoked retrieve command. Args: target: '{target}', query_file: '{query_file}'"
    )

    # raise NotImplementedError()

    ATLAS.fulfill_query(
        AtlasQuery(
            {"paths": ["tests::error", "tests::dummy_download2"], "filters": None}
        )
    )


# This is just here to wrap (cli_root) in case we ever need to change its
# behavior, like when we are developing.
def main():
    import inspect

    # This whole block stops NotImplementedErrors that I've littered about
    # for testing purposes while I come up with the structure of the tool.
    # One can remove it when the implementation is finished, or they will
    # interfere with regular NotImplementedErrors in ABCs.

    try:
        cli_root()
    except NotImplementedError:
        fname = inspect.trace()[-1][3]  # Magic from StackOverflow.
        print(
            f"{c.Fore.RED}STOP{c.Fore.RESET} -- Fn '{c.Fore.MAGENTA}{fname}{c.Fore.RESET}' is not implemented yet."
        )
