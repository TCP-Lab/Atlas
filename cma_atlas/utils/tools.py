import contextlib
import functools
import io
import logging

import requests
import tqdm


@contextlib.contextmanager
def handler_suppressed(h: logging.Handler):
    original_level = h.level
    try:
        h.setLevel(logging.CRITICAL)
        yield
    finally:
        h.setLevel(original_level)


def download_as_bytes_with_progress(url: str, params, tqdm_class=tqdm.tqdm) -> bytes:
    resp = requests.get(url, params=params, stream=True)
    total = int(resp.headers.get("content-length", 0))
    bio = io.BytesIO()
    with tqdm_class(
        total=total,
        unit="b",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        bar.update(0)
        for chunk in resp.iter_content(chunk_size=65536):
            bar.update(len(chunk))
            bio.write(chunk)
    return bio.getvalue()


def chain(*funcs):
    def wrapper(x):
        return functools.reduce(lambda x, y: y(x), funcs, x)

    return wrapper
