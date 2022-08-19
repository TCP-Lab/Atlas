"""Contains all ABCs used by Atlas.

ABCs allow us to express what interfaces and processing endpoints are needed
whithout proving a specific implementation.

Everything in here is akin to a dataclass, or a function without implementation.
Instances of these are created and stored just to sort out all the implementation
and data that is needed.

Everything starts with an AtlasQuery. It contains what fields Atlas has to
retrieve. The query has to be validated against what Atlas CAN actually
retrieve, and the depedency tree (e.g. to download and process some data
you might need data from another source. For example, ensembl IDs are
always needed).

Once validated, the interface(s) that is needed by the query is fired up,
and it needs to download and process the data. To do this, we use
Downloaders and Processors. We implement the downloaders and processors
differently so that we can re-use them in similar interfaces.

The interfaces download the data, and give it out in specific formats.

It is up to Atlas to marge all of these outputs in a way that makes sense,
and save it out as the correct output formats.

"""
import logging
from abc import ABC, abstractmethod
from multiprocessing import current_process
from typing import Optional

import pandas as pd
from colorama import Fore
from typer import Abort

log = logging.getLogger(__name__)


class AtlasDownloader(ABC):
    @abstractmethod
    def retrieve(self, name: str):
        """Download from the remote repository what we need to download

        Retrieves the raw data to be processed by Processors.
        """
        pass

    @property
    def worker_id(self):
        process = current_process()
        if process.name == "MainProcess":
            log.warning("A processor asked for the worker ID in the main process.")
            return 0

        return process._identity[0]


class AtlasProcessor(ABC):
    @abstractmethod
    def __call__(self, name: str, melted_data) -> pd.DataFrame:
        pass

    @property
    def worker_id(self):
        process = current_process()
        if process.name == "MainProcess":
            log.warning("A processor asked for the worker ID in the main process.")
            return 0

        return process._identity[0]


def contains_all(x: list, y: list) -> bool:
    """Does list y contain all items in x and vice-versa?"""
    return all([i in y for i in x]) and all([i in x for i in y])


class AtlasInterface(ABC):
    type: str = "Undefined Type"
    """The interface's data type

    The interface type defines what type of data is retrieved, as well
    as what interfaces are required for this interface to work. The interface
    dependencies pivot around a single column, usually. For instance, data
    regarding genes is pivoted on the 'Ensembl gene IDs' column."""
    name: str = "Undefined Interface"
    """An arbitrary name for the interface. Shows up in menus and progress bars."""

    downloader: AtlasDownloader
    processor: AtlasProcessor

    provided_cols: Optional[dict[str]]
    """Description of the columns of data provided by this interface.

    Given as a dictionary of col_name: description. Used to check the output
    of the processor and shown in menus. If None, will print out that no
    specific columns are defined.
    """

    extra_args: Optional[dict] = None
    """Extra arguments to pass to the downloader and processor"""

    def run(self):
        try:
            raw_data = self.downloader.retrieve(name=self.name)
            processed_data = self.processor(name=self.name, melted_data=raw_data)

            if self.provided_cols is not None and not contains_all(
                list(processed_data.columns), list(self.provided_cols.keys())
            ):
                log.warn(
                    "The promised columns are not identical to the retrieved ones. Ignoring this, hoping for the best."
                )

            return processed_data
        except Exception as e:
            # Catch any errors happening in the workers, and give them to the main
            # thread. They will be re-raised there, if needed.
            return e

    @property
    def paths_description(self):
        if self.provided_cols is None:
            return "No columns defined."

        max_col_len = max([len(col) for col in self.provided_cols.keys()])

        # A bit of padding
        max_col_len += 2

        result = []
        for key, value in self.provided_cols.items():
            # This is a bit convoluted as I'm lazy. First, I pad the string.
            # Then I split it up, color it, and patch it back together.
            # I hope noone will even use "¬" in a description or name.
            fmt_key = f"{key}¬".ljust(max_col_len, "-")
            fmt_result = f"{fmt_key}¬{value}"

            fmt_result = fmt_result.split("¬")
            fmt_result = " ".join(
                [
                    Fore.LIGHTGREEN_EX + fmt_result[0] + Fore.RESET,
                    Fore.LIGHTBLACK_EX + fmt_result[1] + Fore.RESET,
                    fmt_result[2],
                ]
            )

            result.append(fmt_result)

        return "\n".join(result)


class AtlasQuery(ABC):
    def __init__(self, query: dict) -> None:
        try:
            self.interfaces = query["interfaces"]
            self.type = query["type"]
            self.version = query["atlas_version"]
        except KeyError as e:
            log.error(f"Could not find required key in query: {e}.")
            raise Abort()
