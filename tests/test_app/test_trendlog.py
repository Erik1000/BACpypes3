"""Trend Log runtime controller behavior."""

from bacpypes3.basetypes import LogRecord, LogRecordLogDatum, StatusFlags
from bacpypes3.constructeddata import ListOf
from bacpypes3.local.trendlog import TrendLogController
from bacpypes3.primitivedata import Boolean, Unsigned


class TrendLog:
    logInterval = 100
    bufferSize = 2
    stopWhenFull = False
    recordCount = 0
    totalRecordCount = 0
    logBuffer = ListOf(LogRecord)()


class Application:
    pass


def test_trendlog_poll_interval_uses_hundredths_of_a_second():
    controller = TrendLogController(Application(), TrendLog())

    assert controller._poll_interval_seconds() == 1.0


def test_trendlog_controller_converts_boolean_values():
    controller = TrendLogController(Application(), TrendLog())

    record = controller.make_record(Boolean(True))

    assert isinstance(record, LogRecord)
    assert record.logDatum.booleanValue is True
    assert record.statusFlags == StatusFlags([0, 0, 0, 0])


def test_trendlog_controller_rolls_buffer_and_counts_records():
    trend_log = TrendLog()
    controller = TrendLogController(Application(), trend_log)

    for value in (1, 2, 3):
        controller.append_record(controller.make_record(Unsigned(value)))

    assert trend_log.recordCount == 2
    assert trend_log.totalRecordCount == 3
    assert [record.logDatum.unsignedValue for record in trend_log.logBuffer] == [2, 3]


def test_stale_poll_completion_does_not_reschedule():
    controller = TrendLogController(Application(), TrendLog())
    current_task = object()
    stale_task = object()
    controller.task = current_task

    controller._poll_done(stale_task)

    assert controller.task is current_task
    assert controller._poll_handle is None
