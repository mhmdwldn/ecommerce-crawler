"""Custom exceptions for the Tokopedia crawler pipeline."""


class ErrorRequestException(Exception):
    """A request failed permanently (HTTP error, GraphQL error, bad payload)."""


class RateLimitExceeded(Exception):
    """The upstream gateway throttled us (HTTP 429 / Too Many Requests).

    The message intentionally contains "Too Many Requests" so the base
    controller's bury/retry logic can pattern-match it.
    """


class OutputDriverNotRecognizeException(Exception):
    """An unknown ``--destination`` was requested from the output factory."""

    def __str__(self) -> str:
        return super().__str__() or "Destination not recognized"
