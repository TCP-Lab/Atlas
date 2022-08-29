import contextlib
import functools
import io
import logging

import requests
import tqdm


@contextlib.contextmanager
def handler_suppressed(h: logging.Handler):
    """Suppress the input handler during the context manager's life.

    Args:
        h (logging.Handler): The handler to suppress.
    """
    original_level = h.level
    try:
        h.setLevel(logging.CRITICAL)
        yield
    finally:
        h.setLevel(original_level)


def download_as_bytes_with_progress(url: str, params, tqdm_class=tqdm.tqdm) -> bytes:
    """Download some data as bytes, with a progress bar.

    The download length is taken from the headers, if the server sends any.

    Args:
        url (str): The url to dowlad from.
        params (Any): The parameters passed to requests.get().
        tqdm_class (Callable, optional): The tqdm instance to use to make the progress bar.. Defaults to tqdm.tqdm.

    Returns:
        bytes: The downloaded bytes.
    """
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
    """Chain the functions given as parameters."""

    def wrapper(x):
        return functools.reduce(lambda x, y: y(x), funcs, x)

    return wrapper
