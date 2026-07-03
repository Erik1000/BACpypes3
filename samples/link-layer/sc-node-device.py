#!/usr/bin/env python3
"""
BACnet/SC Node Device Example
=============================

Runs a minimal BACnet device that participates in a BACnet/SC network by
connecting to a hub function over a TLS-secured ("wss") WebSocket, using the
SCNodeLinkLayer wired up through a secure-connect Network Port object.

BACnet/SC requires mutual TLS authentication, so operational credentials must
be provided as PEM files:

  --cert   this node's operational certificate
  --key    the matching private key
  --ca     the accepted issuer (CA) certificate(s)

Example:
--------
    python sc-node-device.py \\
        --name SCNode --instance 3456 \\
        --hub wss://hub.example.org:47808/ \\
        --cert node.pem --key node.key --ca ca.pem

Note:
-----
For initial bring-up the credentials are supplied as file paths (the TLS
context is attached to the Network Port object).  Modelling the credentials as
BACnet File objects (Operational_Certificate_File, Issuer_Certificate_Files,
Certificate_Signing_Request_File) is a later, compliance-oriented step.
"""

import asyncio
import uuid
from argparse import ArgumentParser

from bacpypes3.app import Application
from bacpypes3.local.device import DeviceObject
from bacpypes3.local.networkport import NetworkPortObject
from bacpypes3.local.analog import AnalogValueObject
from bacpypes3.pdu import SecureConnectAddress
from bacpypes3.sc.tls import create_ssl_context
from bacpypes3.debugging import ModuleLogger

# some debugging
_debug = 0
_log = ModuleLogger(globals())


async def main() -> None:
    parser = ArgumentParser(description="BACnet/SC node device")
    parser.add_argument("--name", default="SCNode", help="device name")
    parser.add_argument("--instance", type=int, default=3456, help="device instance")
    parser.add_argument("--vendor", type=int, default=999, help="vendor identifier")
    parser.add_argument(
        "--hub", required=True, help="primary hub URI, e.g. wss://hub.example.org/"
    )
    parser.add_argument("--failover", default=None, help="failover hub URI")
    parser.add_argument("--cert", required=True, help="operational certificate (PEM)")
    parser.add_argument("--key", required=True, help="private key (PEM)")
    parser.add_argument("--ca", required=True, help="issuer/CA certificate(s) (PEM)")
    args = parser.parse_args()

    # every BACnet/SC device requires a stable device UUID (Clause YY.1.5.3)
    device_object = DeviceObject(
        objectIdentifier=("device", args.instance),
        objectName=args.name,
        vendorIdentifier=args.vendor,
        deviceUUID=uuid.uuid4().bytes,
    )

    # a secure-connect network port with a Random-48 VMAC
    network_port = NetworkPortObject(
        SecureConnectAddress.random(),
        objectIdentifier=("network-port", 1),
        objectName="SC Port 1",
        scPrimaryHubURI=args.hub,
        scFailoverHubURI=args.failover or "",
    )

    # attach the mutual-TLS context (v1 file-path credentials) so the link
    # layer can establish the secured WebSocket connection to the hub
    network_port.ssl_context = create_ssl_context(args.cert, args.key, args.ca)

    # a sample point to read
    analog_value = AnalogValueObject(
        objectIdentifier=("analogValue", 1),
        objectName="temperature",
        presentValue=21.5,
    )

    app = Application.from_object_list(
        [device_object, network_port, analog_value]
    )
    _log.info("running as %s on %s", args.name, args.hub)

    try:
        await asyncio.Future()
    finally:
        app.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
