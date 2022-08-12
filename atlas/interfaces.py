import time
from functools import partial
from random import randint, randrange

import pandas as pd
from tqdm import tqdm

from atlas import abcs
from atlas.errors import AtlasTestException

down_tqdm = partial(tqdm, leave=False, colour="BLUE")
process_tqdm = partial(tqdm, leave=False, colour="GREEN")

#################   >>>  INTERFACE BEST PRACTICES  <<<    ######################
# 1. Try and reuse old downloaders and processors, if possible.
# 2. Keep in mind that errors do not get raised immediately. Instead, the
#   interface catches them and returns them to the main process. It is after
#   all processes complete than Atlas checks to see if any processes have raised
#   an error and then re-raises it. This is similar to what `ProcessPoolExecutor`
#   already does, but the stack trace is smaller, and we can get what interface
#   died with what error, and log it.
# 3. Keep in mind that the pool is not CTRL+C friendly. The pool catches the
#   interrupt, stops making new children, waits until they are all finished
#   or dead, and *then* stops, re-raising the KeyboardInterrupt.
#   If you really want, use CTRL-C twice to kill the handler. But this might leave
#   orphaned processes.


class TestDownloader(abcs.AtlasDownloader):
    def retrieve(self, name):
        test_time = randint(5, 15)

        for _ in down_tqdm(range(test_time), f"{name}", position=self.worker_id - 1):
            time.sleep(randrange(20, 150, 1) / 100)

        return {"ids": ["a", "b", "c"], "values": [1, 2, 3]}


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


class TestInterface0(abcs.AtlasInterface):
    downloader: abcs.AtlasDownloader = TestDownloader()
    processor: abcs.AtlasProcessor = TestProcessor()

    paths = ["tests::dummy_download0"]


class TestInterface1(abcs.AtlasInterface):
    downloader: abcs.AtlasDownloader = TestDownloader()
    processor: abcs.AtlasProcessor = TestProcessor()

    paths = ["tests::dummy_download1"]


class TestInterface2(abcs.AtlasInterface):
    downloader: abcs.AtlasDownloader = TestDownloader()
    processor: abcs.AtlasProcessor = TestProcessor()

    paths = ["tests::dummy_download2"]


class TestInterfaceThatErrors(abcs.AtlasInterface):
    downloader: abcs.AtlasDownloader = TestDownloader()
    processor: abcs.AtlasProcessor = TestProcessorThatErrors()

    paths = ["tests::error"]


###############################################################################
# Specify which interfaces are part of Atlas.

ALL_INTERFACES = [
    TestInterface0(),
    TestInterface1(),
    TestInterface2(),
    TestInterfaceThatErrors(),
]

###############################################################################
