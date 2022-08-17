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

        cpus = cpu_count()

        log.info(f"Spawning process pool with {cpus} workers.")
        log.warning(
            f"The processing pool is not CTRL+C friendly. Use it with caution when killing Atlas."
        )
        with concurrent.futures.ProcessPoolExecutor(
            cpus, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),)
        ) as pool:
            data: list[pd.DataFrame] = list(pool.map(run, query_interfaces))

        # Handle errors coming from child processes.
        for i, item in enumerate(data):
            if isinstance(item, Exception):
                log.error(f"Interface {query_interfaces[i]} errorer with {type(item)}.")
                raise item

        if len(data) == 1:
            log.info("Data needs not be collapsed. Fulfilled query.")
            return data[0]

        log.info("Data was retrived and processed. Attempting to collapse it.")

        # I think this is pretty bad, maybe counters would be better? But
        # I am tired and sleepy.
        log.debug("Detecting duplicated columns...")
        all_cols = []
        duplicate_cols = []
        for dataframe in data:
            for col in dataframe.columns:
                if col in all_cols and col not in duplicate_cols:
                    log.debug(f"Detected col '{col}' as duplicated.")
                    duplicate_cols.append(col)
                else:
                    all_cols.append(col)

        if not duplicate_cols:
            log.error(
                f"No pivot columns found. Dumping dataframe cols: {' --- '.join(dataframe.columns)}."
            )
            raise Abort()

        log.debug("Finding pivot column...")
        pivot_col = None
        for duplicate_col in duplicate_cols:
            if not all([duplicate_col in x.columns for x in data]):
                log.warn(
                    f"Found a duplicate col ({duplicate_col}) which is not shared between all frames. This should not happen, and might lead to data loss. Dumping dataframe cols: {' --- '.join(dataframe.columns)}"
                )
            elif pivot_col is None:
                pivot_col = duplicate_col
            else:
                log.error(
                    f"Found two pivot columns! First: {pivot_col}, second: {duplicate_col}. Aborting."
                )
                raise Abort()

        log.info(f"Pivot column detected: '{pivot_col}'.")

        log.info("Performing merge...")
        merged = reduce(
            lambda left, right: pd.merge(left, right, on=pivot_col, how="outer"),
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
