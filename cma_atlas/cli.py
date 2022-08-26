import logging
from pathlib import Path

import colorama as c
import typer
import yaml
from simple_term_menu import TerminalMenu

from cma_atlas import __version__
from cma_atlas.abcs import AtlasQuery
from cma_atlas.core import Atlas
from cma_atlas.utils.strings import INFO

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


def generate_preview_from_interface_name(interface_name):
    interfaces = {x.name: x for x in ATLAS.interfaces}

    return interfaces[interface_name].paths_description


@cli_root.command("tables")
def tables_command(
    query: str = typer.Argument(
        None, help="Search query to filter the tables before printing. RegEx allowed"
    )
):
    """Get a list of what data can be retrieved by Atlas."""
    log.debug(f"Invoked tables command. Args: query: '{query}'")

    raise NotImplementedError()


@cli_root.command("genquery")
def genquery_command(
    target: Path = typer.Argument(..., help="Target path to save the query to")
):
    """Generate a query file with a variety of options."""
    # Based on the example given in the simple_term_meu GitHub

    target = target.expanduser().resolve()
    log.debug(f"Invoked genquery command. Args: target: '{target}'")

    ## MENU STYLE
    menu_cursor = "> "
    menu_cursor_style = ("fg_green", "bold")
    menu_style = ("standout",)
    ##

    # Make a menu with the interfaces and their supported paths. The menu is
    # clickable.
    interfaces = ATLAS.loaded_interfaces
    interface_types = list(interfaces.keys())
    interface_types.sort()

    type_selection_menu = TerminalMenu(
        interface_types,
        title="Select a data type. Press Q or Esc to abort.",
        menu_cursor=menu_cursor,
        menu_cursor_style=menu_cursor_style,
        menu_highlight_style=menu_style,
        cycle_cursor=True,
        clear_screen=True,
    )

    while True:
        selection = type_selection_menu.show()
        if selection is None:
            raise typer.Abort()

        selected_type = interface_types[selection]
        possible_interfaces_names = list(interfaces[selected_type].keys())
        possible_interfaces_names.sort()

        interface_selection_menu = TerminalMenu(
            possible_interfaces_names,
            preview_command=generate_preview_from_interface_name,
            title="Select items to retrieve. Selecting no items will select all of them, instead. Press Q/Esc to go back.",
            preview_title="Downloaded data preview",
            multi_select=True,
            multi_select_empty_ok=True,
            multi_select_select_on_accept=False,
            show_multi_select_hint=True,
            menu_cursor=menu_cursor,
            menu_cursor_style=menu_cursor_style,
            menu_highlight_style=menu_style,
            cycle_cursor=True,
            clear_screen=True,
        )

        selections = interface_selection_menu.show()

        if selections is not None:
            break

        # If we get here, we have to go back to the main menu

    # If they selected nothing, they select everything
    if len(selections) == 0:
        selected_names = possible_interfaces_names
    else:
        selected_names = [possible_interfaces_names[index] for index in selections]

    # Save a YAML file with the selected options
    if target.is_dir():
        log.warn(f"{target} is a directory. Making a default query file instead.")
        target /= "atlas_query.yaml"
    with target.open("w+") as stream:
        yaml.dump(
            {
                "type": selected_type,
                "interfaces": selected_names,
                "atlas_version": __version__,
            },
            stream,
        )

    log.info(f"Saved query in {target}. Ready to use `atlas retrieve`!")


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

    if not (query_file.exists() and query_file.is_file()):
        log.error(f"{query_file} does not exists, or is not a file.")
        typer.Abort()

    if target.suffix != ".csv":
        log.warning(
            f"The target output file '{target}' does not end in .csv. Note that the output is always in .csv format."
        )

    with query_file.open("r") as stream:
        query = AtlasQuery(yaml.safe_load(stream))

    output_data = ATLAS.fulfill_query(query)

    with target.open("w+") as output_stream:
        output_data.to_csv(output_stream)


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

    log.info("Atlas finished.")
