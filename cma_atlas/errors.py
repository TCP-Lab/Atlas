class InvalidQuery(Exception):
    """Raised if an Atlas Query is found to be invalid."""

    pass


class AtlasTestException(Exception):
    """A test exception to test Atlas with"""

    pass


class UnsupportedOptionError(Exception):
    """Raised when an Atlas global option is not supported."""

    pass
