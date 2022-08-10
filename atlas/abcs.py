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
import concurrent.futures
from abc import ABC, abstractmethod
from multiprocessing import cpu_count


class AtlasDownloader(ABC):
    @abstractmethod
    def retrieve(self):
        """Download from the remote repository what we need to download

        Retrieves the raw data to be processed by Processors.
        """
        pass


class AtlasProcessor(ABC):
    @abstractmethod
    def __call__(self, melted_data):
        pass


class AtlasInterface(ABC):
    downloader: AtlasDownloader
    processor: AtlasProcessor

    paths: list[tuple[str]]
    # These are the paths that are supported by the interface. Checked
    # by Atlas when fulfilling queries.

    def run(self):
        raw_data = self.downloader.retrieve()
        processed_data = self.processor(raw_data)

        return processed_data


class AtlasQuery(ABC):
    def __init__(self, query: dict) -> None:
        self.paths = query["paths"]
        self.filters = query["filters"]


class Atlas(ABC):
    interfaces: list

    def fulfill_query(query: AtlasQuery):

        query_interfaces = []

        pool = concurrent.futures.ThreadPoolExecutor(cpu_count())
        data = pool.map(lambda x: x.run(), query_interfaces)

        return data
