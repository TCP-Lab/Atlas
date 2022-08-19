import logging
import time
from functools import partial
from random import randint, randrange

import pandas as pd
from tqdm import tqdm

from atlas import abcs
from atlas.errors import AtlasTestException

down_tqdm = partial(tqdm, leave=False, colour="BLUE")
process_tqdm = partial(tqdm, leave=False, colour="GREEN")

log = logging.getLogger(__name__)


class TestDownloader(abcs.AtlasDownloader):
    def __init__(
        self, returned_data={"ids": ["a", "b", "c"], "values": [1, 2, 3]}
    ) -> None:
        self.returned_data = returned_data

    def retrieve(self, name):
        test_time = randint(5, 15)

        for _ in down_tqdm(range(test_time), f"{name}", position=self.worker_id):
            time.sleep(randrange(20, 150, 1) / 100)

        log.info("This should be suppressed in the stream log.")

        return self.returned_data


class TestProcessor(abcs.AtlasProcessor):
    def __call__(self, name, melted_data):
        test_time = randint(5, 15)

        for _ in tqdm(
            range(test_time),
            f"{name}",
            position=self.worker_id - 1,
            leave=False,
            colour="GREEN",
        ):
            time.sleep(randrange(20, 150, 1) / 100)

        log.info("This should be suppressed in the stream log.")

        return pd.DataFrame(melted_data)


class TestProcessorThatErrors(abcs.AtlasProcessor):
    def __call__(self, name, melted_data):
        test_time = randint(5, 15)

        for x in tqdm(
            range(test_time),
            f"{name}",
            position=self.worker_id - 1,
            leave=False,
            colour="GREEN",
        ):
            time.sleep(randrange(20, 150, 1) / 100)
            if x == 3:
                raise AtlasTestException()


class BaseTestInterface(abcs.AtlasInterface):
    """A normal interface, but it can be superseeded if needed."""

    pass


class TestInterface0(BaseTestInterface):
    type = "Type 1 tests"
    name = "Test Interface 0"

    downloader: abcs.AtlasDownloader = TestDownloader({"Col 1": [1, 2, 3, 4]})
    processor: abcs.AtlasProcessor = TestProcessor()

    provided_cols = {"Col 1": "A test column, with a nice description."}


class TestInterface1(BaseTestInterface):
    type = "Type 2 tests"
    name = "Test Interface 1"

    downloader: abcs.AtlasDownloader = TestDownloader(
        {"Col 1": [1, 2, 4, 5], "Long Column name, 2": ["a", "b", "c", "d"]}
    )
    processor: abcs.AtlasProcessor = TestProcessor()

    provided_cols = {
        "Col 1": "A test column, with a nice description.",
        "Long Column name, 2": "A test column, with a very long name.",
    }


class TestInterface2(BaseTestInterface):
    type = "Type 2 tests"
    name = "Test Interface 2"

    downloader: abcs.AtlasDownloader = TestDownloader(
        {
            "Col 1": [2, 6, 5],
            "Long Column name, but different": ["f", "h", "d"],
            "A third column": [1.23, 0.2, 0],
        }
    )
    processor: abcs.AtlasProcessor = TestProcessor()

    provided_cols = {
        "Col 1": "A test column, with a nice description.",
        "Long Column name, but different": "A test column, with a very, very long name.",
        "A third column": "That is just a test column, just like the others.",
    }


class TestInterfaceThatErrors(BaseTestInterface):
    type = "Erroring Test interfaces"
    name = "Error Test Interface"

    downloader: abcs.AtlasDownloader = TestDownloader()
    processor: abcs.AtlasProcessor = TestProcessorThatErrors()

    provided_cols = {"No columns": "Since selecting this will make Atlas Error."}


class TestInterfaceWithWrongCols(BaseTestInterface):
    type = "Erroring Test interfaces"
    name = "Wrong columns retrieved Test Interface"

    downloader: abcs.AtlasDownloader = TestDownloader(
        {
            "Col A": [2, 6, 5],
            "Wrong Col": ["f", "h", "d"],
            "Col B": [1.23, 0.2, 0],
        }
    )
    processor: abcs.AtlasProcessor = TestProcessor()

    provided_cols = {
        "Col A": "A test column, with a nice description.",
        "Col B": "That is just a test column, just like the others.",
    }


ALL_TEST_INTERFACES = [
    TestInterface0(),
    TestInterface1(),
    TestInterface2(),
    TestInterfaceThatErrors(),
    TestInterfaceWithWrongCols(),
]
