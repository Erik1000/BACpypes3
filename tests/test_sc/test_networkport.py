#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test BACnet/SC network port object
-----------------------------------
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from bacpypes3.app import Application
from bacpypes3.apdu import APDU
from bacpypes3.basetypes import ErrorClass, ErrorCode, SCHubConnectorState
from bacpypes3.debugging import bacpypes_debugging, ModuleLogger
from bacpypes3.pdu import PDU, SecureConnectAddress
from bacpypes3.npdu import NPDU
from bacpypes3.local.device import DeviceObject
from bacpypes3.local.networkport import NetworkPortObject
from bacpypes3.sc.bvll import ConnectRequest, EncapsulatedNPDU, Result
from bacpypes3.sc.service import SCHubConnector

from .test_hub_connector import LiveFakeConn

# some debugging
_debug = 0
_log = ModuleLogger(globals())


@bacpypes_debugging
class TestSCNetworkPortObject(unittest.IsolatedAsyncioTestCase):
    _debug = None  # type: ignore[assignment]

    async def test_secure_connect_port(self):
        vmac = SecureConnectAddress.random()
        np = NetworkPortObject(
            vmac,
            scPrimaryHubURI="wss://hub.example.org/",
            scFailoverHubURI="wss://failover.example.org/",
        )
        assert int(np.networkType) == 11  # secure-connect
        assert bytes(np.macAddress) == vmac.addrAddr
        assert str(np.protocolLevel) == "bacnet-application"
        assert str(np.scPrimaryHubURI) == "wss://hub.example.org/"
        assert str(np.scFailoverHubURI) == "wss://failover.example.org/"


class TestSCApplicationIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.port = NetworkPortObject(
            SecureConnectAddress.random(),
            objectIdentifier=("network-port", 1),
            objectName="SC Port",
            scPrimaryHubURI="wss://primary/",
            scFailoverHubURI="wss://failover/",
            scMinimumReconnectTime=2,
            scMaximumReconnectTime=8,
            scConnectWaitTimeout=5,
            scDisconnectWaitTimeout=7,
            scHeartbeatTimeout=9,
        )
        device = DeviceObject(
            objectIdentifier=("device", 3456),
            objectName="SC Device",
            vendorIdentifier=999,
        )
        with patch.object(SCHubConnector, "start"):
            self.app = Application.from_object_list([device, self.port])
        self.link = self.app.link_layers[self.port.objectIdentifier]
        self.connector = self.link.connector

    async def asyncTearDown(self):
        await asyncio.wait_for(self.connector.close(), 1)

    async def test_network_port_timers_reach_connector(self):
        assert self.connector.minimum_reconnect_time == 2
        assert self.connector.maximum_reconnect_time == 8
        assert self.connector.connect_wait_timeout == 5
        assert self.connector.disconnect_wait_timeout == 7
        assert self.connector.heartbeat_timeout == 9

    async def test_duplicate_vmac_updates_port_and_adapter(self):
        original = bytes(self.port.macAddress)
        result = Result(
            result_function=ConnectRequest.bvlcFunction,
            result_code=1,
            error_class=ErrorClass.communication,
            error_code=ErrorCode.nodeDuplicateVmac,
        )
        await self.connector._handle_result(result)
        assert bytes(self.port.macAddress) != original
        assert bytes(self.port.macAddress) == self.connector.vmac.addrAddr
        assert self.app.nsap.local_adapter is not None
        assert self.app.nsap.local_adapter.adapterAddr == self.connector.vmac
        assert self.link.local_vmac == self.connector.vmac

    async def test_i_am_sent_after_primary_and_failover_establishment(self):
        primary, failover = LiveFakeConn(), LiveFakeConn()
        self.connector._connect = AsyncMock(
            side_effect=[primary, ConnectionRefusedError(), failover]
        )
        self.connector.start()

        async def next_i_am(conn):
            while True:
                message = await conn.messages_sent.get()
                if isinstance(message, EncapsulatedNPDU):
                    npdu = NPDU.decode(PDU(message.pduData))
                    if npdu.npduNetMessage is None:
                        apdu = APDU.decode(npdu)
                        assert apdu.apduService == 0
                        return message

        await asyncio.wait_for(next_i_am(primary), 1)
        assert self.port.scHubConnectorState == SCHubConnectorState.connectedToPrimary
        self.connector._retry_at[0] = 0
        self.connector._retry_delay[0] = 60
        await primary.close()
        await asyncio.wait_for(next_i_am(failover), 1)
        assert self.port.scHubConnectorState == SCHubConnectorState.connectedToFailover

        remote_vmac = SecureConnectAddress.random()
        who_is = EncapsulatedNPDU(bytes.fromhex("01 00 10 08"))
        who_is.bvlcMessageID = 44
        who_is.bvlcOriginatingVirtualAddress = remote_vmac
        who_is.bvlcDestinationVirtualAddress = SecureConnectAddress(
            SecureConnectAddress.local_broadcast
        )
        failover.incoming.put_nowait(bytes(who_is.encode().pduData))
        response = await asyncio.wait_for(next_i_am(failover), 1)
        assert response.bvlcDestinationVirtualAddress.addrAddr == remote_vmac.addrAddr


if __name__ == "__main__":
    unittest.main()
