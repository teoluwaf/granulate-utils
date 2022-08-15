#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import logging
from datetime import datetime
from logging import Handler, LogRecord
from typing import Any


class BatchingHandler(Handler):
    """
    logging.Handler that accumulates logs in a buffer and flushes them upon explicit request.
    The logs are transformed into dicts.
    """
    capacity = 100 * 1000  # max number of records to buffer

    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self._buffer: list[dict] = []
        # The formatter is needed to format tracebacks
        self.setFormatter(logging.Formatter())

    def emit(self, record: LogRecord) -> None:
        item = self.dictify_record(record)
        self._buffer.append(item)
        # truncate logs to last N entries
        if len(self._buffer) > self.capacity:
            self.on_truncated()
            self._buffer[: -self.capacity] = []

    def on_truncated(self) -> None:
        """
        Called when amount of records in buffer exceeds the capacity.
        Default implementation does nothing.
        """
        pass

    def flush(self) -> None:
        # Snapshot the current num logs because logs list might be extended meanwhile.
        logs_count = len(self._buffer)
        self.flush_logs(self._buffer[:logs_count])
        # If succeeded, remove the flushed logs from the list.
        self._buffer[:logs_count] = []

    def flush_logs(self, logs: list[dict[str, Any]]) -> None:
        raise NotImplementedError("flush_logs must be implemented by subclass")

    def dictify_record(self, record: LogRecord) -> dict[str, Any]:
        formatted_timestamp = datetime.utcfromtimestamp(record.created).isoformat()
        exception_traceback = self._format_exception(record)
        return {
            "message": record.message,
            "level": record.levelname,
            "timestamp": formatted_timestamp,
            "logger_name": record.name,
            "exception": exception_traceback,
        }

    def _format_exception(self, record: LogRecord) -> str:
        assert self.formatter is not None
        if record.exc_info:
            # Use cached exc_text if available.
            return record.exc_text or self.formatter.formatException(record.exc_info)
        else:
            return ""
