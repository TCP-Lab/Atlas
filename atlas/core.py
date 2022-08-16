import concurrent.futures
import logging
import sys
from multiprocessing import cpu_count

from tqdm import tqdm

import atlas.abcs as abcs
from atlas import interfaces
from atlas.interfaces import ALL_INTERFACES

log = logging.getLogger(__name__)

# The ProcessPoolExecutor needs a top-level function as it needs to pickle the
# object to send it to the worker process. So, we can't use a lambda here, we
# need to do it like this.
def run(x):
    return x.run()


class Atlas:
    interfaces: list[abcs.AtlasInterface] = ALL_INTERFACES

    def fulfill_query(self, query: abcs.AtlasQuery):
        query_interfaces = []
        for path in query.paths:
            try:
                query_interfaces.append(self.supported_paths[path])
            except KeyError:
                log.error(
                    f"Unsupported or invalid path '{path}'. Cannot fulfull query."
                )
                sys.exit(1)

        cpus = cpu_count()

        log.info(f"Spawning process pool with {cpus} workers.")
        log.warning(
            f"The processing pool is not CTRL+C friendly. Use it with caution when killing Atlas."
        )
        with concurrent.futures.ProcessPoolExecutor(
            cpus, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),)
        ) as pool:
            data = list(pool.map(run, query_interfaces))

        print(data)

        # Handle errors coming from child processes.
        for i, item in enumerate(data):
            if isinstance(item, Exception):
                log.error(f"Interface {query_interfaces[i]} errorer with {type(item)}.")
                raise item

        return data

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