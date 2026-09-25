#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test BACnet/SC hub connector state machine
-------------------------------------------

Drives the initiating-peer connection state machine (Clause YY.6.2.2) directly
through its ``_fsm_*`` handlers with a fake WebSocket connection, so no live
TLS socket is required.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock
from uuid import UUID

from bacpypes3.debugging import bacpypes_debugging, ModuleLogger, xtob
from bacpypes3.comm import Client, bind
from bacpypes3.pdu import PDU, SecureConnectAddress, VirtualAddress
from bacpypes3.basetypes import ErrorClass, ErrorCode
from bacpypes3.sc.bvll import (
    ConnectRequest,
    ConnectAccept,
    DisconnectRequest,
    DisconnectACK,
    HeartbeatRequest,
    HeartbeatACK,
    EncapsulatedNPDU,
    Result,
)
from bacpypes3.sc.service import (
    SCHubConnector,
    HubConnectorState,
    HUB_CONNECTOR_CONNECTED_PRIMARY,
    HUB_CONNECTOR_CONNECTED_FAILOVER,
)

# some debugging
_debug = 0
_log = ModuleLogger(globals())

DEVICE_UUID = UUID("f81d4fae-7dec-11d0-a765-00a0c91e6bf6")
HUB_UUID = UUID("12345678-1234-5678-1234-567812345678")


class FakeConn:
    def __init__(self):
        self.sent = []
        self.closed = False

    async def send(self, data):
        self.sent.append(bytes(data))

    async def close(self, *args, **kwargs):
        self.closed = True


class UpstreamCapture(Client):
    def __init__(self):
        super().__init__()
        self.received = []

    async def confirmation(self, pdu):
        self.received.append(pdu)


def decode(data):
    from bacpypes3.sc.bvll import LPCI, pdu_types

    pdu = PDU(data)
    lpci = LPCI.decode(pdu)
    lpdu = pdu_types[lpci.bvlcFunction].decode(pdu)
    LPCI.update(lpdu, lpci)
    return lpdu


def make_connector(**kwargs):
    connector = SCHubConnector(
        SecureConnectAddress(xtob("AABBCCDDEEFF")),
        DEVICE_UUID,
        "wss://hub.example.org/",
        **kwargs,
    )
    capture = UpstreamCapture()
    bind(capture, connector)
    return connector, capture


def connect_accept_bytes(message_id=0):
    lpdu = ConnectAccept(
        vmac_address=VirtualAddress(xtob("010203040506")),
        device_uuid=HUB_UUID,
        maximum_bvlc_length=1497,
        maximum_npdu_length=1476,
    )
    lpdu.bvlcMessageID = message_id
    return lpdu.encode().pduData


@bacpypes_debugging
class TestHubConnectorFSM(unittest.IsolatedAsyncioTestCase):
    _debug = None  # type: ignore[assignment]

    async def asyncTearDown(self):
        # cancel any pending timers created during the test
        if hasattr(self, "connector"):
            self.connector._cancel_all_timers()

    async def _established(self):
        connector, capture = make_connector()
        self.connector = connector
        connector._conn = FakeConn()
        connector._state = HubConnectorState.AWAITING_WEBSOCKET
        await connector._fsm_ws_established()
        return connector, capture

    async def test_connect_request_sent(self):
        connector, capture = await self._established()

        assert connector._state == HubConnectorState.AWAITING_ACCEPT
        assert len(connector._conn.sent) == 1
        lpdu = decode(connector._conn.sent[0])
        assert isinstance(lpdu, ConnectRequest)
        assert lpdu.vmac_address.addrAddr == xtob("AABBCCDDEEFF")
        assert lpdu.device_uuid == DEVICE_UUID

    async def test_connect_accept_establishes(self):
        connector, capture = await self._established()

        await connector._fsm_message(connect_accept_bytes())

        assert connector._state == HubConnectorState.CONNECTED
        assert connector.connected.is_set()
        assert connector.peer_vmac.addrAddr == xtob("010203040506")
        assert connector.peer_uuid == HUB_UUID

    async def test_connect_wait_timeout(self):
        connector, capture = await self._established()

        await connector._fsm_connect_wait_timeout()

        assert connector._state == HubConnectorState.IDLE
        assert connector._conn is None

    async def test_duplicate_vmac_regenerates(self):
        connector, capture = await self._established()
        original = connector.vmac.addrAddr

        nak = Result(
            result_function=ConnectRequest.bvlcFunction,
            result_code=0x01,
            error_class=ErrorClass.communication,
            error_code=ErrorCode.nodeDuplicateVmac,
        )
        nak.bvlcMessageID = 0
        await connector._fsm_message(nak.encode().pduData)

        assert connector._state == HubConnectorState.IDLE
        assert connector.vmac.addrAddr != original
        # Random-48: low nibble of first octet is 0x2
        assert (connector.vmac.addrAddr[0] & 0x0F) == 0x02

    async def test_vmac_change_callback(self):
        changes = []
        connector, capture = make_connector(on_vmac_change=changes.append)
        self.connector = connector
        connector._conn = FakeConn()
        connector._state = HubConnectorState.AWAITING_ACCEPT

        nak = Result(
            result_function=ConnectRequest.bvlcFunction,
            result_code=0x01,
            error_class=ErrorClass.communication,
            error_code=ErrorCode.nodeDuplicateVmac,
        )
        nak.bvlcMessageID = 0
        await connector._fsm_message(nak.encode().pduData)

        assert len(changes) == 1
        assert changes[0] is connector.vmac

    async def test_encapsulated_npdu_forwarded_up(self):
        connector, capture = await self._established()
        await connector._fsm_message(connect_accept_bytes())

        # a forwarded NPDU from the hub
        npdu = EncapsulatedNPDU(xtob("0104cafe"))
        npdu.bvlcMessageID = 1
        npdu.bvlcOriginatingVirtualAddress = SecureConnectAddress(xtob("010203040506"))
        await connector._fsm_message(npdu.encode().pduData)

        assert len(capture.received) == 1
        # the raw BVLC bytes are passed up to the codec unchanged
        assert capture.received[0].pduData == npdu.encode().pduData

    async def test_disconnect_request_received(self):
        connector, capture = await self._established()
        await connector._fsm_message(connect_accept_bytes())

        conn = connector._conn
        conn.sent.clear()

        req = DisconnectRequest()
        req.bvlcMessageID = 7
        await connector._fsm_message(req.encode().pduData)

        # a Disconnect-ACK was returned and the connection closed to IDLE
        assert len(conn.sent) == 1
        ack = decode(conn.sent[0])
        assert isinstance(ack, DisconnectACK)
        assert ack.bvlcMessageID == 7
        assert conn.closed
        assert connector._state == HubConnectorState.IDLE

    async def test_heartbeat_sent_when_idle(self):
        connector, capture = await self._established()
        await connector._fsm_message(connect_accept_bytes())
        connector._conn.sent.clear()

        await connector._fsm_heartbeat()

        assert len(connector._conn.sent) == 1
        assert isinstance(decode(connector._conn.sent[0]), HeartbeatRequest)

    async def test_connector_state_reported(self):
        from bacpypes3.sc.service import (
            HUB_CONNECTOR_NO_CONNECTION,
            HUB_CONNECTOR_CONNECTED_PRIMARY,
        )

        states = []
        connector, capture = make_connector(on_connector_state_change=states.append)
        self.connector = connector
        connector._conn = FakeConn()
        connector._state = HubConnectorState.AWAITING_WEBSOCKET
        await connector._fsm_ws_established()

        # connecting on the primary hub reports connectedToPrimary
        await connector._fsm_message(connect_accept_bytes())
        assert states == [HUB_CONNECTOR_CONNECTED_PRIMARY]

        # closing reports back to noHubConnection
        await connector._close_connection()
        assert states[-1] == HUB_CONNECTOR_NO_CONNECTION

    async def test_heartbeat_ack_must_match(self):
        connector, capture = await self._established()
        await connector._fsm_message(connect_accept_bytes())
        await connector._fsm_heartbeat()
        message_id = connector._heartbeat_message_id
        ack = HeartbeatACK()
        ack.bvlcMessageID = message_id + 1
        await connector._fsm_message(ack.encode().pduData)
        assert connector._heartbeat_message_id == message_id
        ack.bvlcMessageID = message_id
        await connector._fsm_message(ack.encode().pduData)
        assert connector._heartbeat_message_id is None
        assert "heartbeat_ack" not in connector._timers

    async def test_unanswered_heartbeat_disconnects(self):
        connector, capture = await self._established()
        connector.heartbeat_timeout = 0.01
        connector.disconnect_wait_timeout = 0.01
        conn = connector._conn
        await connector._fsm_message(connect_accept_bytes())
        await connector._fsm_heartbeat()
        await asyncio.sleep(0.06)
        assert conn.closed
        assert connector._state == HubConnectorState.IDLE
        assert any(isinstance(decode(data), DisconnectRequest) for data in conn.sent)

    async def test_npdu_before_accept_is_not_forwarded(self):
        connector, capture = await self._established()
        npdu = EncapsulatedNPDU(xtob("0104cafe"))
        npdu.bvlcMessageID = 1
        await connector._fsm_message(npdu.encode().pduData)
        assert capture.received == []

    async def test_concurrent_close_waits_for_socket_cleanup(self):
        connector, capture = await self._established()
        closing = asyncio.Event()
        release = asyncio.Event()
        conn = connector._conn

        async def close():
            closing.set()
            await release.wait()
            conn.closed = True

        conn.close = close
        connector._start_timer("close", 0, connector._close_connection)
        await asyncio.wait_for(closing.wait(), 1)
        cleanup = asyncio.create_task(connector._close_connection())
        await asyncio.sleep(0)
        assert not cleanup.done()
        release.set()
        await asyncio.wait_for(cleanup, 1)
        assert conn.closed
        assert connector._socket_close_task is None

    async def test_indication_requires_connection(self):
        connector, capture = await self._established()

        # not connected yet: dropped
        await connector.indication(PDU(xtob("deadbeef")))
        assert len(connector._conn.sent) == 1  # only the Connect-Request

        # once connected it is sent
        await connector._fsm_message(connect_accept_bytes())
        connector._conn.sent.clear()
        await connector.indication(PDU(xtob("deadbeef")))
        assert connector._conn.sent == [xtob("deadbeef")]


class LiveFakeConn(FakeConn):
    def __init__(self, *, accept=True, reject=False):
        super().__init__()
        self.accept = accept
        self.reject = reject
        self.incoming = asyncio.Queue()
        self.messages_sent = asyncio.Queue()
        self.closed_event = asyncio.Event()

    async def send(self, data):
        await super().send(data)
        message = decode(data)
        self.messages_sent.put_nowait(message)
        if isinstance(message, ConnectRequest):
            if self.reject:
                result = Result(
                    result_function=ConnectRequest.bvlcFunction,
                    result_code=1,
                    error_class=ErrorClass.communication,
                    error_code=ErrorCode.other,
                )
                result.bvlcMessageID = message.bvlcMessageID
                self.incoming.put_nowait(bytes(result.encode().pduData))
            elif self.accept:
                self.incoming.put_nowait(
                    bytes(connect_accept_bytes(message.bvlcMessageID))
                )
        elif isinstance(message, DisconnectRequest):
            ack = DisconnectACK()
            ack.bvlcMessageID = message.bvlcMessageID
            self.incoming.put_nowait(bytes(ack.encode().pduData))

    async def recv(self):
        message = await self.incoming.get()
        if message is None:
            raise ConnectionError("connection closed")
        return message

    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self.incoming.get()
        if message is None:
            raise StopAsyncIteration
        return message

    async def close(self):
        if not self.closed:
            await super().close()
            self.incoming.put_nowait(None)
            self.closed_event.set()


class TestHubConnectorLifecycle(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.states = asyncio.Queue()
        self.connector, self.capture = make_connector(
            failover_hub_uri="wss://failover/",
            minimum_reconnect_time=0.01,
            maximum_reconnect_time=0.04,
            connect_wait_timeout=0.1,
            disconnect_wait_timeout=0.01,
            heartbeat_timeout=100,
            on_connector_state_change=self.states.put_nowait,
        )

    async def asyncTearDown(self):
        await asyncio.wait_for(self.connector.close(), 1)
        assert not self.connector._timers

    async def wait_state(self, expected):
        async def wait():
            while await self.states.get() != expected:
                pass

        await asyncio.wait_for(wait(), 1)

    async def test_failed_primary_does_not_delay_first_failover(self):
        self.connector._retry_delay = [60, 60]
        failover = LiveFakeConn()
        self.connector._connect = AsyncMock(
            side_effect=[ConnectionRefusedError(), failover]
        )
        self.connector.start()
        await self.wait_state(HUB_CONNECTOR_CONNECTED_FAILOVER)
        assert self.connector._connect.call_args_list[1].args == ("wss://failover/",)
        assert self.connector._conn is failover

    async def test_bacnet_rejection_tries_failover(self):
        rejected = LiveFakeConn(reject=True)
        failover = LiveFakeConn()
        self.connector._retry_delay[0] = 60
        self.connector._connect = AsyncMock(side_effect=[rejected, failover])
        self.connector.start()
        await self.wait_state(HUB_CONNECTOR_CONNECTED_FAILOVER)
        assert rejected.closed
        assert self.connector._conn is failover

    async def test_connect_accept_timeout_tries_failover(self):
        stalled = LiveFakeConn(accept=False)
        failover = LiveFakeConn()
        self.connector.connect_wait_timeout = 0.01
        self.connector._retry_delay[0] = 60
        self.connector._connect = AsyncMock(side_effect=[stalled, failover])
        self.connector.start()
        await self.wait_state(HUB_CONNECTOR_CONNECTED_FAILOVER)
        assert stalled.closed

    async def test_lost_primary_retries_primary_before_failover(self):
        primary, failover = LiveFakeConn(), LiveFakeConn()
        self.connector._connect = AsyncMock(
            side_effect=[primary, ConnectionRefusedError(), failover]
        )
        self.connector.start()
        await self.wait_state(HUB_CONNECTOR_CONNECTED_PRIMARY)
        self.connector._retry_at[0] = 0
        self.connector._retry_delay[0] = 60
        await primary.close()
        await self.wait_state(HUB_CONNECTOR_CONNECTED_FAILOVER)
        assert [call.args[0] for call in self.connector._connect.call_args_list] == [
            "wss://hub.example.org/",
            "wss://hub.example.org/",
            "wss://failover/",
        ]

    async def test_primary_recovery_preserves_failover_traffic_until_accept(self):
        failover, primary = LiveFakeConn(), LiveFakeConn(accept=False)
        self.connector._connect = AsyncMock(
            side_effect=[ConnectionRefusedError(), failover, primary]
        )
        self.connector.start()
        await self.wait_state(HUB_CONNECTOR_CONNECTED_FAILOVER)
        request = await asyncio.wait_for(primary.messages_sent.get(), 1)
        assert isinstance(request, ConnectRequest)
        assert self.connector._conn is failover
        npdu = EncapsulatedNPDU(xtob("0104cafe"))
        npdu.bvlcMessageID = 15
        data = bytes(npdu.encode().pduData)
        await self.connector.indication(PDU(data))
        assert failover.sent[-1] == data
        received = asyncio.Event()

        async def capture(pdu):
            self.capture.received.append(pdu)
            received.set()

        self.capture.confirmation = capture
        failover.incoming.put_nowait(data)
        await asyncio.wait_for(received.wait(), 1)
        primary.incoming.put_nowait(bytes(connect_accept_bytes(request.bvlcMessageID)))
        await self.wait_state(HUB_CONNECTOR_CONNECTED_PRIMARY)
        assert failover.closed
        assert any(
            isinstance(decode(data), DisconnectRequest) for data in failover.sent
        )
        assert self.connector._conn is primary
        await self.connector.indication(PDU(data))
        assert primary.sent[-1] == data

    async def test_rejected_primary_probe_keeps_failover(self):
        failover, rejected = LiveFakeConn(), LiveFakeConn(reject=True)
        self.connector._connect = AsyncMock(
            side_effect=[ConnectionRefusedError(), failover, rejected]
        )
        self.connector.start()
        await self.wait_state(HUB_CONNECTOR_CONNECTED_FAILOVER)
        await asyncio.wait_for(rejected.closed_event.wait(), 1)
        assert self.connector._conn is failover
        assert self.connector.connected.is_set()
        assert not failover.closed

    async def test_close_cancels_pending_primary_handshake(self):
        failover, primary = LiveFakeConn(), LiveFakeConn(accept=False)
        self.connector._connect = AsyncMock(
            side_effect=[ConnectionRefusedError(), failover, primary]
        )
        self.connector.start()
        await self.wait_state(HUB_CONNECTOR_CONNECTED_FAILOVER)
        await asyncio.wait_for(primary.messages_sent.get(), 1)
        await self.connector.close()
        assert primary.closed
        assert failover.closed
        assert self.connector._run_task is None

    async def test_close_cancels_pending_websocket_open(self):
        opening = asyncio.Event()
        cancelled = asyncio.Event()

        async def connect(uri):
            opening.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        self.connector._connect = connect
        self.connector.start()
        await asyncio.wait_for(opening.wait(), 1)
        await self.connector.close()
        assert cancelled.is_set()
        assert self.connector._run_task is None


if __name__ == "__main__":
    unittest.main()
