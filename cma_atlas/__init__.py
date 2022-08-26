"""Handles parsing the initial options and sets up the logging facility.

Raises a ValueError if there are keys in the parsed options that are not in the
default options. This is to crash early other that later if the user mistypes
the options.
"""

import collections.abc
import importlib.resources as pkg_resources
import logging
import os
from copy import deepcopy
from functools import reduce
from logging import StreamHandler
from logging.handlers import RotatingFileHandler
from pathlib import Path

import colorama
import yaml
from colorama import Back, Fore, Style

from cma_atlas import resources
from cma_atlas.errors import UnsupportedOptionError

__version__ = "0.1.0a2"
__all__ = ["__version__", "OPTIONS"]

colorama.init(autoreset=True)

DEFAULT_OPTIONS = yaml.safe_load(
    pkg_resources.open_text(resources, "default_options.yml")
)

LEVELS = {
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "CRITICAL": logging.CRITICAL,
}


# Stolen from stackoverflow and modded
def recursive_options_update(d, u):
    for k, v in u.items():
        if k not in d:
            raise UnsupportedOptionError(f"Custom option {k} is not supported.")
        if isinstance(v, collections.abc.Mapping):
            d[k] = recursive_options_update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


_possible_option_paths = [
    Path("~/.atlas/config.yaml"),
    Path("~/.atlas/config.yml"),
    Path("~/.config/Atlas/config.yaml"),
    Path("~/.config/Atlas/config.yml"),
]
_possible_option_paths = map(lambda x: x.expanduser().resolve(), _possible_option_paths)
_all_local_opts = []
for path in _possible_option_paths:
    if not path.exists():
        continue
    with path.open("r") as file:
        _all_local_opts.append(yaml.safe_load(file))
OPTIONS = reduce(recursive_options_update, _all_local_opts, deepcopy(DEFAULT_OPTIONS))


class ColorFormatter(logging.Formatter):
    # Change this dictionary to suit your coloring needs!
    COLORS = {
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "DEBUG": Style.BRIGHT + Fore.MAGENTA,
        "INFO": Fore.GREEN,
        "CRITICAL": Style.BRIGHT + Fore.RED,
    }

    def format(self, record):
        reset = Fore.RESET + Back.RESET + Style.NORMAL
        color = self.COLORS.get(record.levelname, "")
        if color:
            record.name = Style.BRIGHT + Fore.BLACK + record.name + reset
            if record.levelname != "INFO":
                record.msg = color + record.msg + reset
            record.levelname = color + record.levelname + reset
        return logging.Formatter.format(self, record)


# Setup logging
log = logging.getLogger("atlas")  # Keep this at the module level name
log.setLevel(logging.DEBUG)
log.propagate = False
# Keep this at DEBUG - set levels in handlers themselves

format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
file_formatter = logging.Formatter(format)
console_formatter = ColorFormatter(format)

_LOG_PATH = (
    (Path(OPTIONS["logging"]["log_folder"]) / "Atlas.log").expanduser().resolve()
)

if not _LOG_PATH.parent.exists():
    os.makedirs(_LOG_PATH.parent)

file_h = RotatingFileHandler(
    filename=Path(_LOG_PATH),
    encoding="utf-8",
    mode="a+",
    maxBytes=1e5,
    backupCount=5,
)
file_h.setFormatter(file_formatter)

file_level = LEVELS.get(OPTIONS["logging"]["file_log_level"])
if not file_level:
    log.warn(
        "The preferred file log level ({}) set in the options file is invalid. Defaulting to {}".format(
            OPTIONS["logging"]["file_log_level"],
            DEFAULT_OPTIONS["logging"]["file_log_level"],
        )
    )
    file_level = DEFAULT_OPTIONS["logging"]["file_log_level"]

file_h.setLevel(file_level)

stream_h = StreamHandler()
stream_h.setFormatter(console_formatter)

stream_level = LEVELS.get(OPTIONS["logging"]["console_log_level"])
if not stream_level:
    log.warn(
        "The preferred console log level ({}) set in the options file is invalid. Defaulting to {}".format(
            OPTIONS["logging"]["console_log_level"],
            DEFAULT_OPTIONS["logging"]["console_log_level"],
        )
    )
    stream_level = DEFAULT_OPTIONS["logging"]["console_log_level"]
stream_h.setLevel(stream_level)

log.addHandler(file_h)
log.addHandler(stream_h)
