import concurrent.futures
import logging
from functools import reduce
from multiprocessing import cpu_count

import pandas as pd
from tqdm import tqdm
from typer import Abort

import atlas.abcs as abcs
from atlas import __version__
from atlas.errors import InvalidQuery
from atlas.interfaces import ALL_INTERFACES
from atlas.utils.tools import handler_suppressed

log = logging.getLogger(__name__)

# The ProcessPoolExecutor needs a top-level function as it needs to pickle the
# object to send it to the worker process. So, we can't use a lambda here, we
# need to do it like this.
def run(x):
    return x.run()


class Atlas:
    interfaces: list[abcs.AtlasInterface] = ALL_INTERFACES

    def test_query(self, query: abcs.AtlasQuery) -> True:
        if query.type not in [x.type for x in self.interfaces]:
            log.error("Tested query is invalid: unsupported type.")
            raise InvalidQuery

        possible_names = [x.name for x in self.interfaces]
        if not all([x in possible_names for x in query.interfaces]):
            log.error("Tested query is invalid: unsupported interface(s).")
            raise InvalidQuery

        # Test that all interfaces have the same type, and it is the one specified.
        # Just to be sure...
        query_interfaces = [x for x in self.interfaces if x.name in query.interfaces]
        if not all([x.type == query.type for x in query_interfaces]):
            log.warning(
                "Not all query interfaces have the same type. Merging may fail."
            )

        if len(set([x.merge_col for x in query_interfaces])) != 1:
            log.error("Not all selected interfaces have the same merge col. Aborting.")
            raise InvalidQuery

        if query.version != __version__:
            log.warn(
                f"The query vas generated in version {query.version}, "
                f"which is not the current version ({__version__})."
            )

        return True

    def fulfill_query(self, query: abcs.AtlasQuery) -> pd.DataFrame:
        try:
            self.test_query(query)
        except InvalidQuery:
            raise Abort()

        # We are sure that the query is fulfillable, here.

        query_interfaces = [x for x in self.interfaces if x.name in query.interfaces]
        merge_col = query_interfaces[0].merge_col

        cpus = cpu_count()

        log.info(f"Spawning process pool with {cpus} workers.")
        log.warning(
            f"The processing pool is not CTRL+C friendly. Use it with caution when killing Atlas."
        )
        with concurrent.futures.ProcessPoolExecutor(
            cpus, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),)
        ) as pool:
            # Temporarily disable stream logging
            with handler_suppressed(logging.getLogger("atlas").handlers[1]):
                data: list[pd.DataFrame] = list(
                    tqdm(
                        pool.map(run, query_interfaces),
                        position=0,
                        leave=False,
                        total=len(query_interfaces),
                        desc="Overall Completion",
                        colour="GREEN",
                    )
                )

        # Handle errors coming from child processes.
        for i, item in enumerate(data):
            if isinstance(item, Exception):
                log.error(f"Interface {query_interfaces[i]} errorer with {type(item)}.")
                raise item

        if len(data) == 1:
            log.info("Data needs not be collapsed. Fulfilled query.")
            return data[0]

        log.info("Data was retrived and processed. Attempting to collapse it.")

        log.info("Performing merge...")

        def merge_or_concat(x: pd.DataFrame, y: pd.DataFrame) -> pd.DataFrame:
            """Merge or concatenate two dataframes"""
            try:
                merged = pd.merge(x, y, how="outer", validate="one_to_one", copy=False)
            except ValueError:
                merged = pd.concat(
                    [x, y], ignore_index=True, verify_integrity=True, copy=False
                )

            return merged

        merged = reduce(
            merge_or_concat,
            tqdm(data, desc="Merging progress"),
        )

        log.info("Fulfilled query.")
        return merged

    @property
    def supported_paths(self):
        paths = {}
        for interface in self.interfaces:
            for path in interface.paths:
                paths.update({path: interface})

        return paths

    @property
    def loaded_interfaces(self):
        """Retrieve a structure with the interfaces loaded by Atlas.

        The structure is composed by an outer dictionary, with keys
        equal to the interface types, and values dictionaries. The inner
        dictionaries contain keys with the interface names, and values
        the paths fulfilled by the interfaces.

        This is intended to aid the building of menus.
        """
        # This is probably terribly inefficient, but there should not be many
        # interfaces
        interface_types = set([x.type for x in self.interfaces])
        result = {type: {} for type in interface_types}
        for type in interface_types:
            for interface in self.interfaces:
                if interface.type == type:
                    result[type].update({interface.name: interface.paths_description})

        return result
