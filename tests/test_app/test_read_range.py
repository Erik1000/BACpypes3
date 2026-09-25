"""ReadRange server and client behavior."""

import logging
from datetime import datetime

import pytest

from bacpypes3.apdu import ReadRangeACK, ReadRangeRequest
from bacpypes3.basetypes import (
    DateTime,
    LogRecord,
    LogRecordLogDatum,
    Range,
    RangeByPosition,
    RangeBySequenceNumber,
    RangeByTime,
    RangeByTimeRange,
    StatusFlags,
)
from bacpypes3.constructeddata import ListOf
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import ObjectIdentifier, Unsigned
from bacpypes3.argparse import create_log_handler
from bacpypes3.service.object import ReadRangeServices


class ListObject:
    objectIdentifier = ObjectIdentifier("trend-log,1")
    property_type = ListOf(Unsigned)

    def __init__(self, values):
        self.value = self.property_type(values)

    def get_property_type(self, identifier):
        return self.property_type if identifier == "log-buffer" else None

    async def read_property(self, identifier, index):
        return self.value


class RangeApplication(ReadRangeServices):
    device_object = None

    def __init__(self, obj):
        self.obj = obj
        self.responses = []

    def get_object_id(self, identifier):
        if identifier == self.obj.objectIdentifier:
            return self.obj
        if tuple(identifier) == tuple(self.obj.objectIdentifier):
            return self.obj
        return None

    async def response(self, response):
        self.responses.append(response)


def range_request(reference=5, count=-3):
    return ReadRangeRequest(
        objectIdentifier=ObjectIdentifier("trend-log,1"),
        propertyIdentifier="log-buffer",
        range=Range(byPosition=RangeByPosition(referenceIndex=reference, count=count)),
        destination=Address("1.2.3.4"),
    )


async def test_negative_count_round_trip():
    app = RangeApplication(ListObject([1, 2, 3, 4, 5]))
    request = range_request()

    await app.do_ReadRangeRequest(request)

    assert len(app.responses) == 1
    response = app.responses[0]
    assert response.itemCount == 3
    assert [item.cast_out(Unsigned) for item in response.itemData] == [5, 4, 3]
    assert list(response.resultFlags) == [0, 1, 1]
    assert response.firstSequenceNumber is None


async def test_by_time_round_trip():
    class LogRecordObject:
        objectIdentifier = ObjectIdentifier("trend-log,1")
        property_type = ListOf(LogRecord)

        def __init__(self, values):
            self.value = self.property_type(values)

        def get_property_type(self, identifier):
            return self.property_type if identifier == "log-buffer" else None

        async def read_property(self, identifier, index):
            return self.value

    records = [
        LogRecord(
            timestamp=DateTime(datetime(2024, 1, 1, 8, 0, 0)),
            logDatum=LogRecordLogDatum(booleanValue=True),
            statusFlags=StatusFlags(),
        ),
        LogRecord(
            timestamp=DateTime(datetime(2024, 1, 1, 9, 0, 0)),
            logDatum=LogRecordLogDatum(booleanValue=False),
            statusFlags=StatusFlags(),
        ),
        LogRecord(
            timestamp=DateTime(datetime(2024, 1, 1, 10, 0, 0)),
            logDatum=LogRecordLogDatum(booleanValue=True),
            statusFlags=StatusFlags(),
        ),
        LogRecord(
            timestamp=DateTime(datetime(2024, 1, 1, 11, 0, 0)),
            logDatum=LogRecordLogDatum(booleanValue=False),
            statusFlags=StatusFlags(),
        ),
    ]

    app = RangeApplication(LogRecordObject(records))
    request = ReadRangeRequest(
        objectIdentifier=ObjectIdentifier("trend-log,1"),
        propertyIdentifier="log-buffer",
        range=Range(
            byTime=RangeByTime(
                referenceTime=DateTime(datetime(2024, 1, 1, 9, 30, 0)),
                count=2,
            )
        ),
        destination=Address("1.2.3.4"),
    )

    await app.do_ReadRangeRequest(request)

    assert len(app.responses) == 1
    response = app.responses[0]
    assert response.itemCount == 2
    assert [item.cast_out(LogRecord) for item in response.itemData] == records[2:4]
    assert list(response.resultFlags) == [0, 1, 0]
    assert response.firstSequenceNumber == 3


async def test_by_sequence_number_round_trip():
    app = RangeApplication(ListObject([1, 2, 3, 4, 5]))
    request = ReadRangeRequest(
        objectIdentifier=ObjectIdentifier("trend-log,1"),
        propertyIdentifier="log-buffer",
        range=Range(
            bySequenceNumber=RangeBySequenceNumber(referenceSequenceNumber=3, count=2)
        ),
        destination=Address("1.2.3.4"),
    )

    await app.do_ReadRangeRequest(request)

    assert len(app.responses) == 1
    response = app.responses[0]
    assert response.itemCount == 2
    assert [item.cast_out(Unsigned) for item in response.itemData] == [3, 4]
    assert list(response.resultFlags) == [0, 0, 1]
    assert response.firstSequenceNumber == 3


async def test_negative_sequence_number_round_trip():
    app = RangeApplication(ListObject([1, 2, 3, 4, 5]))
    request = ReadRangeRequest(
        objectIdentifier=ObjectIdentifier("trend-log,1"),
        propertyIdentifier="log-buffer",
        range=Range(
            bySequenceNumber=RangeBySequenceNumber(referenceSequenceNumber=4, count=-2)
        ),
        destination=Address("1.2.3.4"),
    )

    await app.do_ReadRangeRequest(request)

    response = app.responses[0]
    assert [item.cast_out(Unsigned) for item in response.itemData] == [4, 3]
    assert list(response.resultFlags) == [0, 0, 1]
    assert response.firstSequenceNumber == 4


async def test_by_time_range_round_trip():
    class LogRecordObject:
        objectIdentifier = ObjectIdentifier("trend-log,1")
        property_type = ListOf(LogRecord)

        def __init__(self, values):
            self.value = self.property_type(values)

        def get_property_type(self, identifier):
            return self.property_type

        async def read_property(self, identifier, index):
            return self.value

    records = [
        LogRecord(
            timestamp=DateTime(datetime(2024, 1, 1, hour)),
            logDatum=LogRecordLogDatum(booleanValue=bool(hour % 2)),
            statusFlags=StatusFlags(),
        )
        for hour in range(8, 12)
    ]
    app = RangeApplication(LogRecordObject(records))
    request = ReadRangeRequest(
        objectIdentifier=ObjectIdentifier("trend-log,1"),
        propertyIdentifier="log-buffer",
        range=Range(
            byTimeRange=RangeByTimeRange(
                beginningTime=DateTime(datetime(2024, 1, 1, 9)),
                endingTime=DateTime(datetime(2024, 1, 1, 10)),
            )
        ),
        destination=Address("1.2.3.4"),
    )

    await app.do_ReadRangeRequest(request)

    response = app.responses[0]
    assert response.itemCount == 2
    assert [item.cast_out(LogRecord) for item in response.itemData] == records[1:3]
    assert list(response.resultFlags) == [0, 0, 1]
    assert response.firstSequenceNumber == 2


async def test_by_time_uses_log_sequence_numbers_when_buffer_is_cyclic():
    class CyclicLogRecordObject:
        objectIdentifier = ObjectIdentifier("trend-log,1")
        property_type = ListOf(LogRecord)
        recordCount = 3
        totalRecordCount = 10

        def __init__(self, values):
            self.value = self.property_type(values)

        def get_property_type(self, identifier):
            return self.property_type

        async def read_property(self, identifier, index):
            return self.value

    records = [
        LogRecord(
            timestamp=DateTime(datetime(2024, 1, 1, 10)),
            logDatum=LogRecordLogDatum(booleanValue=True),
            statusFlags=StatusFlags(),
        ),
        LogRecord(
            timestamp=DateTime(datetime(2024, 1, 1, 8)),
            logDatum=LogRecordLogDatum(booleanValue=False),
            statusFlags=StatusFlags(),
        ),
        LogRecord(
            timestamp=DateTime(datetime(2024, 1, 1, 9)),
            logDatum=LogRecordLogDatum(booleanValue=True),
            statusFlags=StatusFlags(),
        ),
    ]
    app = RangeApplication(CyclicLogRecordObject(records))
    request = ReadRangeRequest(
        objectIdentifier=ObjectIdentifier("trend-log,1"),
        propertyIdentifier="log-buffer",
        range=Range(
            byTime=RangeByTime(
                referenceTime=DateTime(datetime(2024, 1, 1, 8, 30)), count=1
            )
        ),
        destination=Address("1.2.3.4"),
    )

    await app.do_ReadRangeRequest(request)

    response = app.responses[0]
    assert [item.cast_out(LogRecord) for item in response.itemData] == [records[2]]
    assert list(response.resultFlags) == [0, 0, 1]
    assert response.firstSequenceNumber == 9


async def test_by_time_unspecified_reference_round_trip():
    class LogRecordObject:
        objectIdentifier = ObjectIdentifier("trend-log,1")
        property_type = ListOf(LogRecord)

        def __init__(self, values):
            self.value = self.property_type(values)

        def get_property_type(self, identifier):
            return self.property_type

        async def read_property(self, identifier, index):
            return self.value

    records = [
        LogRecord(
            timestamp=DateTime(datetime(2024, 1, 1, hour)),
            logDatum=LogRecordLogDatum(booleanValue=True),
            statusFlags=StatusFlags(),
        )
        for hour in (8, 9, 10)
    ]
    app = RangeApplication(LogRecordObject(records))
    request = ReadRangeRequest(
        objectIdentifier=ObjectIdentifier("trend-log,1"),
        propertyIdentifier="log-buffer",
        range=Range(
            byTime=RangeByTime(
                referenceTime=DateTime(
                    date=(255, 255, 255, 255),
                    time=(255, 255, 255, 255),
                ),
                count=1,
            )
        ),
        destination=Address("1.2.3.4"),
    )

    await app.do_ReadRangeRequest(request)

    response = app.responses[0]
    assert response.itemCount == 1
    assert [item.cast_out(LogRecord) for item in response.itemData] == [records[0]]
    assert list(response.resultFlags) == [1, 0, 1]
    assert response.firstSequenceNumber == 1


def test_debug_logger_accepts_main_module():
    logger = logging.getLogger("__main__")
    logger.handlers.clear()

    create_log_handler("__main__")

    assert logging.getLogger("__main__").name == "__main__"
