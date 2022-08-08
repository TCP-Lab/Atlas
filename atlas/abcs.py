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

from abc import ABC, abstractmethod

from atlas.errors import MissingConcreteAttributeError


class AtlasABC(ABC):
    """Extra checks for class attributes to be defined."""

    abstracted_attributes: list[str] = []
    """Put in this list all class attributes that MUST NOT be None."""

    # This is NOT the correct way to check if a class attribute is missing,
    # but the canonical decorator way sucks. You need to specify variables
    # as methods, that will get overwritten when you specify them. You cannot
    # even annotate types. It is super counterintuitive.
    # This fixes it, but you need to add Abstract attributes to
    # "abstracted_attributes".
    def __init_subclass__(cls, /, **kwargs):
        super().__init_subclass__(**kwargs)
        for item in cls.abstracted_attributes:
            if cls.__dict__.get(item, None) is None:
                raise MissingConcreteAttributeError(
                    f"Must specify `{item}` in concrete class."
                )


class AtlasDownloader(AtlasABC):
    @abstractmethod
    def retrieve(self):
        """Download from the remote repository what we need to download

        Retrieves the raw data to be processed by Processors.
        """
        pass


class AtlasProcessor(AtlasABC):
    @abstractmethod
    def __call__(self, melted_data):
        pass


class AtlasInterface(AtlasABC):
    """"""

    downloader: AtlasDownloader = None
    processor: AtlasProcessor = None

    paths: list[tuple[str]] = None
    # These are the paths that are supported by the interface. Checked
    # by Atlas when fulfilling queries.

    abstracted_attributes: list[str] = ["downloader", "processor", "paths"]

    def run(self):
        raw_data = self.downloader.retrieve()
        processed_data = self.processor(raw_data)

        return processed_data


class AtlasQuery(AtlasABC):
    def __init__(self, query: dict) -> None:
        self.paths = None

        self.filters = query["filters"]


class Atlas(AtlasABC):
    interfaces: list = None

    abstracted_attributes: list[str] = ["interfaces"]

    def fulfill_query(query: AtlasQuery):
        pass
