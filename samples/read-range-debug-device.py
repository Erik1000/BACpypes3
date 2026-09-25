#!/usr/bin/env python3
"""
Minimal BACnet/IP device that exposes a Trend Log buffer for debugging
ReadRange requests against a live local BACnet stack.

Usage:
    python samples/read-range-debug-device.py --instance 1000 --address 127.0.0.1/24 --debug
"""

from bacpypes3.primitivedata import Real

import asyncio
from datetime import datetime, timedelta

from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.app import Application
from bacpypes3.basetypes import (
    DateTime,
    DeviceObjectPropertyReference,
    LogRecord,
    LogRecordLogDatum,
    LoggingType,
    StatusFlags,
)
from bacpypes3.constructeddata import ListOf
from bacpypes3.local.analog import AnalogValueObject
from bacpypes3.object import TrendLogObject


def build_debug_log_buffer() -> ListOf(LogRecord):
    now = datetime.now()
    records = [
        LogRecord(
            timestamp=DateTime(now - timedelta(minutes=10)),
            logDatum=LogRecordLogDatum(realValue=Real(2.0)),
            statusFlags=StatusFlags([0, 0, 0, 0]),
        ),
        LogRecord(
            timestamp=DateTime(now - timedelta(minutes=7)),
            logDatum=LogRecordLogDatum(realValue=Real(3.0)),
            statusFlags=StatusFlags([0, 0, 0, 0]),
        ),
        LogRecord(
            timestamp=DateTime(now - timedelta(minutes=5)),
            logDatum=LogRecordLogDatum(realValue=Real(5.0)),
            statusFlags=StatusFlags([0, 0, 0, 0]),
        ),
        LogRecord(
            timestamp=DateTime(now - timedelta(minutes=2)),
            logDatum=LogRecordLogDatum(realValue=Real(-5.0)),
            statusFlags=StatusFlags([0, 0, 0, 0]),
        ),
        LogRecord(
            timestamp=DateTime(now - timedelta(minutes=1)),
            logDatum=LogRecordLogDatum(realValue=(-2.0)),
            statusFlags=StatusFlags([0, 0, 0, 0]),
        ),
    ]
    return ListOf(LogRecord)(records)


async def main() -> None:
    args = SimpleArgumentParser().parse_args()

    app = Application.from_args(args)

    source = AnalogValueObject(
        objectIdentifier=("analog-value", 1),
        objectName="read-range-source",
        presentValue=0.0,
        statusFlags=[0, 0, 0, 0],
        units="noUnits",
    )
    app.add_object(source)

    trend_log = TrendLogObject(
        objectIdentifier=("trend-log", 1),
        objectName="TestTL",
        enable=True,
        stopWhenFull=False,
        bufferSize=20,
        logBuffer=ListOf(LogRecord)(),
        recordCount=0,
        totalRecordCount=0,
        loggingType=LoggingType.polled,
        logInterval=5,
        logDeviceObjectProperty=DeviceObjectPropertyReference(
            objectIdentifier=("analog-value", 1),
            propertyIdentifier="present-value",
        ),
        statusFlags=StatusFlags([0, 0, 0, 0]),
    )
    app.add_object(trend_log)

    print(f"BACnet device started: {app.device_object.objectIdentifier}")
    print(f"Trend log object: {trend_log.objectIdentifier}")
    print("Use a ReadRange request against this device to debug the by-time path.")

    await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
