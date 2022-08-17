"""Interface module, containing all interfaces for Atlas.

###################   >>>  INTERFACE BEST PRACTICES  <<<    ####################
1. Try and reuse old downloaders and processors, if possible.
2. Keep in mind that errors do not get raised immediately. Instead, the
   interface catches them and returns them to the main process. It is after
   all processes complete than Atlas checks to see if any processes have raised
   an error and then re-raises it. This is similar to what `ProcessPoolExecutor`
   already does, but the stack trace is smaller, and we can get what interface
   died with what error, and log it.
3. Keep in mind that the pool is not CTRL+C friendly. The pool catches the
   interrupt, stops making new children, waits until they are all finished
   or dead, and *then* stops, re-raising the KeyboardInterrupt.
   If you really want, use CTRL-C twice to kill the handler. But this might leave
   orphaned processes.
4. Interfaces should all return a pd.DataFrame with the downloaded and
   (optionally) processed data. These dataframes should all contain a single
   "pivot" column that depends on the type of the interface. Atlas will
   automatically detect this common column, and perform a per-row merge
   of the dataframes with this single pivot column. The dataframe is then
   saved to disk.
5. It is YOUR RESPONSIBILITY to ensure the following:
    - The interface names MUST be unique. This is because the queries rely on
      the interface names to be fulfilled.
    - Interfaces with the same type MUST be return ONE, and ONLY ONE, column
      in common. Atlas will trigger a warning (not an error) if there are
      multiple columns in common.
    - Interfaces MUST fulfill their `.provided_cols` promise. Atlas will trigger
      a warning (not an error) if this is not the case.
6. Interfaces not added to the ALL_INTERFACES variable will not be loaded by
   Atlas. This is a good way to remove test interfaces from Atlas while not
   developing the tool.
"""
# TODO: Add some way to test point n. 5 above in automated deployment tests.
import time
from functools import partial
from random import randint, randrange

import pandas as pd
from tqdm import tqdm

from atlas import OPTIONS, abcs
from atlas.errors import AtlasTestException

down_tqdm = partial(tqdm, leave=False, colour="BLUE")
process_tqdm = partial(tqdm, leave=False, colour="GREEN")


class TestDownloader(abcs.AtlasDownloader):
    def __init__(
        self, returned_data={"ids": ["a", "b", "c"], "values": [1, 2, 3]}
    ) -> None:
        self.returned_data = returned_data

    def retrieve(self, name):
        test_time = randint(5, 15)

        for _ in down_tqdm(range(test_time), f"{name}", position=self.worker_id - 1):
            time.sleep(randrange(20, 150, 1) / 100)

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
    """A normal interface, but `run` is much simpler for testing purposes."""

    def run(self):
        try:
            raw_data = self.downloader.retrieve(name=self.name)
            processed_data = self.processor(name=self.name, melted_data=raw_data)

            return processed_data
        except Exception as e:
            # Catch any errors happening in the workers, and give them to the main
            # thread. They will be re-raised there, if needed.
            return e


class TestInterface0(BaseTestInterface):
    type = "Type 1 tests"
    name = "Test Interface 0"

    downloader: abcs.AtlasDownloader = TestDownloader({"Col1": [1, 2, 3, 4]})
    processor: abcs.AtlasProcessor = TestProcessor()

    provided_cols = {"Col1": "A test column, with a nice description."}


class TestInterface1(BaseTestInterface):
    type = "Type 2 tests"
    name = "Test Interface 1"

    downloader: abcs.AtlasDownloader = TestDownloader(
        {"Col 1": [2, 4, 5], "Long Column name, 2": ["a", "b", "c"]}
    )
    processor: abcs.AtlasProcessor = TestProcessor()

    provided_cols = {
        "Col1": "A test column, with a nice description.",
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
        "Col1": "A test column, with a nice description.",
        "Long Column name, but different": "A test column, with a very, very long name.",
        "A third column": "That is just a test column, just like the others.",
    }


class TestInterfaceThatErrors(BaseTestInterface):
    type = "Erroring Test interfaces"
    name = "Error Test Interface"

    downloader: abcs.AtlasDownloader = TestDownloader()
    processor: abcs.AtlasProcessor = TestProcessorThatErrors()

    provided_cols = {"No columns": "Since selecting this will make Atlas Error."}


###############################################################################
# Specify which interfaces are part of Atlas.
ALL_INTERFACES = [
    TestInterface0(),
    TestInterface1(),
    TestInterface2(),
    TestInterfaceThatErrors(),
]

# Remove test interfaces if we are not in debug mode
if not OPTIONS["debugging"]["include_test_interfaces"]:
    ALL_INTERFACES = [
        x for x in ALL_INTERFACES if not issubclass(type(x), BaseTestInterface)
    ]
###############################################################################
