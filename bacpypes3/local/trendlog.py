"""Runtime support for local Trend Log objects."""

from __future__ import annotations

import asyncio
import datetime
from typing import Any as _Any, Optional, cast

from ..basetypes import DateTime, LogRecord, LogRecordLogDatum, StatusFlags
from ..constructeddata import Any, ListOf
from ..debugging import ModuleLogger
from ..errors import PropertyError
from ..primitivedata import (
    BitString,
    Boolean,
    Enumerated,
    Integer,
    Null,
    Real,
    Unsigned,
)

_debug = 0
_log = ModuleLogger(globals())


class TrendLogController:
    """Acquire values for one local Trend Log and maintain its buffer."""

    def __init__(self, app, trend_log) -> None:
        self.app = app
        self.trend_log = trend_log
        trend_log._trend_log_controller = self
        self.task: Optional[asyncio.Task] = None
        self._poll_handle: Optional[asyncio.Handle] = None
        self._stopped = False
        self._updating = False

    def recalculate(self) -> None:
        """Recalculate acquisition state from the current Trend Log values."""
        if self._stopped or self._updating:
            return
        self._cancel_poll()
        self.start()

    def start(self) -> None:
        """Start local polling when the Trend Log is configured for polling."""
        if self._poll_handle is not None:
            self._poll_handle.cancel()
            self._poll_handle = None
        if self.task is not None:
            return
        if not self._is_polled():
            return
        try:
            asyncio.get_running_loop().call_soon(self._schedule_poll)
        except RuntimeError:
            return

    def close(self) -> None:
        """Stop acquisition; safe to call more than once."""
        self._stopped = True
        if self._poll_handle is not None:
            self._poll_handle.cancel()
            self._poll_handle = None
        if self.task is not None:
            self.task.cancel()
            self.task = None
        self.trend_log._trend_log_controller = None

    def _is_polled(self) -> bool:
        logging_type = self.trend_log.loggingType
        return logging_type is not None and int(logging_type) == 0

    def _cancel_poll(self) -> None:
        if self._poll_handle is not None:
            self._poll_handle.cancel()
            self._poll_handle = None
        if self.task is not None:
            self.task.cancel()
            self.task = None

    def _schedule_poll(self) -> None:
        if self._stopped or not self._is_polled():
            return
        interval = self._poll_interval_seconds()
        if interval <= 0:
            return
        self._poll_handle = asyncio.get_running_loop().call_later(
            interval, self._start_poll
        )

    def _poll_interval_seconds(self) -> float:
        """Convert BACnet hundredths of a second to asyncio seconds."""
        return int(self.trend_log.logInterval or 0) / 100.0

    def _start_poll(self) -> None:
        self._poll_handle = None
        if self._stopped or self.task is not None:
            return
        self.task = asyncio.create_task(self.poll_once())
        self.task.add_done_callback(self._poll_done)

    def _poll_done(self, task: asyncio.Task) -> None:
        if task is not self.task:
            return
        self.task = None
        if self._stopped:
            return
        if not task.cancelled():
            try:
                task.result()
            except Exception:
                _log.exception(
                    "Trend Log poll failed: %r", self.trend_log.objectIdentifier
                )
        self._schedule_poll()

    async def poll_once(self) -> Optional[LogRecord]:
        """Read the configured local property and append one record."""
        if not self._logging_enabled():
            return None

        target, property_name, array_index = self._resolve_target()
        value = await target.read_property(property_name, array_index)
        record = self.make_record(value)
        self.append_record(record)
        return record

    def _logging_enabled(self) -> bool:
        if not self.trend_log.enable:
            return False
        record_count = self._record_count()
        buffer_size = int(self.trend_log.bufferSize or 0)
        if self.trend_log.stopWhenFull and record_count >= buffer_size:
            return False

        now = datetime.datetime.now()
        start_time = self.trend_log.startTime
        stop_time = self.trend_log.stopTime
        if (
            start_time is not None
            and not start_time.is_special
            and now < start_time.datetime
        ):
            return False
        if (
            stop_time is not None
            and not stop_time.is_special
            and now > stop_time.datetime
        ):
            return False
        return True

    def _record_count(self) -> int:
        """Return a normalized count for nullable BACnet properties."""
        record_count = self.trend_log.recordCount
        if record_count is None:
            log_buffer = self.trend_log.logBuffer
            return 0 if log_buffer is None else len(log_buffer)
        return int(record_count)

    def _resolve_target(self):
        reference = self.trend_log.logDeviceObjectProperty
        if reference is None or reference.objectIdentifier is None:
            raise PropertyError("logDeviceObjectProperty")
        target = self.app.get_object_id(reference.objectIdentifier)
        if target is None:
            raise PropertyError("unknownObject")
        property_name = reference.propertyIdentifier
        if property_name is None:
            raise PropertyError("unknownProperty")
        return target, property_name, reference.propertyArrayIndex

    def make_record(self, value: _Any) -> LogRecord:
        """Convert a BACnet property value into a Trend Log record."""
        datum = LogRecordLogDatum()
        if isinstance(value, Boolean):
            datum.booleanValue = value
        elif isinstance(value, Real):
            datum.realValue = value
        elif isinstance(value, Enumerated):
            datum.enumValue = value
        elif isinstance(value, Unsigned):
            datum.unsignedValue = value
        elif isinstance(value, Integer):
            datum.signedValue = value
        elif isinstance(value, BitString):
            datum.bitstringValue = value
        elif isinstance(value, Null):
            datum.nullValue = value
        elif isinstance(value, Any):
            datum.anyValue = value
        else:
            raise TypeError(f"unsupported Trend Log value: {type(value).__name__}")

        return cast(
            LogRecord,
            LogRecord(
                timestamp=DateTime.now(),
                logDatum=datum,
                statusFlags=StatusFlags([0, 0, 0, 0]),
            ),
        )

    def append_record(self, record: LogRecord) -> None:
        """Append a record, enforcing buffer size and sequence counters."""
        buffer_size = int(self.trend_log.bufferSize or 0)
        if buffer_size <= 0:
            return
        log_buffer = self.trend_log.logBuffer
        if log_buffer is None:
            log_buffer = ListOf(LogRecord)()
            self.trend_log.logBuffer = log_buffer

        record_count = self._record_count()
        if self.trend_log.stopWhenFull and record_count >= buffer_size:
            return

        if record_count >= buffer_size:
            del log_buffer[0]

        self._updating = True
        try:
            log_buffer.append(record)
            self.trend_log.recordCount = len(log_buffer)
            self.trend_log.totalRecordCount = (
                int(self.trend_log.totalRecordCount or 0) + 1
            )
        finally:
            self._updating = False
