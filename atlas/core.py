import concurrent.futures
import logging
import time
from multiprocessing import cpu_count, current_process
from random import randint, randrange

import pandas as pd
from tqdm import tqdm

import atlas.abcs as abcs

log = logging.getLogger(__name__)


class IDFactory:
    def __init__(self) -> None:
        self.x = 0

    def __call__(self) -> int:
        self.x += 1
        return self.x


ids = IDFactory()


class TestDownloader(abcs.AtlasDownloader):
    def retrieve(self):
        test_time = randint(5, 15)

        id = current_process()._identity[0] - 1

        for _ in tqdm(
            range(test_time), f"Test Download {id}", position=id, leave=False
        ):
            time.sleep(randrange(20, 150, 1) / 100)

        return {"ids": ["a", "b", "c"], "values": [1, 2, 3]}


class TestProcessor(abcs.AtlasProcessor):
    def __call__(self, melted_data):
        test_time = randint(5, 15)

        id = current_process()._identity[0] - 1

        for _ in tqdm(
            range(test_time), f"Test Processing {id}", position=id, leave=False
        ):
            time.sleep(randrange(20, 150, 1) / 100)

        return pd.DataFrame(melted_data)


class TestInterface(abcs.AtlasInterface):
    downloader: abcs.AtlasDownloader = TestDownloader()
    processor: abcs.AtlasProcessor = TestProcessor()


def run(x):
    return x.run()


class Atlas(abcs.Atlas):
    interfaces: list = []

    def fulfill_query(query: abcs.AtlasQuery):

        query_interfaces = [TestInterface() for _ in range(6)]

        cpus = cpu_count()

        log.info(f"Spawning process pool with {cpus} workers.")
        with concurrent.futures.ProcessPoolExecutor(
            cpus, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),)
        ) as pool:
            data = list(pool.map(run, query_interfaces))

        return data
