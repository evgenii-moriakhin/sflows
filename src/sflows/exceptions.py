class PausedError(Exception):
    pass


class AbortedError(Exception):
    pass


class UnprocessedError(Exception):
    pass


class CompensatedSuccess(Exception):  # noqa: N818
    pass


class CompensatedError(Exception):
    pass
