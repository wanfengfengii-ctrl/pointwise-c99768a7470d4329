"""End-to-end demonstration of the limited AMQP ingestion pipeline.

Shows, against a fresh on-disk archive:
  * arbitrarily chunked multi-channel decoding with heartbeats,
  * one ACK per fully persisted delivery,
  * same-id redelivery re-ACKed without duplication,
  * tampered redelivery rejected while the original survives,
  * reopening the archive and streaming the bodies back.

Run:  python3 -I demo.py
"""

from __future__ import annotations

import hashlib
import os
import struct
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import amqpingest

FRAME_END = 0xCE


def shortstr(text):
    raw = text.encode("utf-8")
    return bytes([len(raw)]) + raw


def frame(ftype, channel, payload):
    return struct.pack(">BHI", ftype, channel, len(payload)) + payload + b"\xce"


def deliver(channel, tag, redelivered=0):
    payload = (struct.pack(">HH", 60, 60) + shortstr("demo-consumer")
               + struct.pack(">Q", tag) + bytes([redelivered])
               + shortstr("demo-ex") + shortstr("demo-key"))
    return frame(1, channel, payload)


def content_header(channel, size, message_id):
    return frame(2, channel,
                 struct.pack(">HHQH", 60, 0, size, 0x0080) + shortstr(message_id))


def content_body(channel, data):
    return frame(3, channel, data)


HEARTBEAT = frame(8, 0, b"")


def expected_ack_bytes(channel, tag):
    payload = struct.pack(">HHQB", 60, 80, tag, 0)
    return struct.pack(">BHI", 1, channel, len(payload)) + payload + b"\xce"


def pump(decoder, archive, stream, block_size, acks):
    for i in range(0, len(stream), block_size):
        for event in decoder.feed(stream[i:i + block_size]):
            result = archive.accept(event)
            if result is not None:
                acks.append(result)


def main():
    workdir = tempfile.mkdtemp(prefix="amqpingest-demo-")
    db_path = os.path.join(workdir, "demo.db")
    print("archive database:", db_path)

    decoder = amqpingest.Decoder()
    archive = amqpingest.Archive(db_path)
    acks = []

    decoder.begin("conn-1")
    for event in decoder.open(1):
        archive.accept(event)
    for event in decoder.open(2):
        archive.accept(event)

    # Channel 1: two messages, channel 2 interleaved, heartbeat in between.
    body_a = b"hello-ingest"
    body_b = os.urandom(200000)

    def body_frames(channel, data):
        out = b""
        for i in range(0, len(data), 65536):
            out += content_body(channel, data[i:i + 65536])
        return out

    stream = (
        deliver(1, 1) + content_header(1, len(body_a), "order-1")
        + body_frames(1, body_a)
        + deliver(2, 1) + content_header(2, len(body_b), "order-2")
        + HEARTBEAT
        + body_frames(2, body_b[:70000])
        + deliver(1, 2) + content_header(1, len(body_a), "order-1")
        + body_frames(1, body_a)          # redelivery of order-1
        + body_frames(2, body_b[70000:])
    )
    pump(decoder, archive, stream, 4096, acks)
    print("\ndeliveries acknowledged:",
          [(a["connection"], a["channel"], a["generation"], a["tag"])
           for a in acks])
    assert acks[0]["frame"] == expected_ack_bytes(1, 1)
    assert acks[1]["frame"] == expected_ack_bytes(1, 2)
    assert acks[2]["frame"] == expected_ack_bytes(2, 1)
    print("exact basic.ack frame bytes verified for all three deliveries")

    # Tampered redelivery of order-1 on a fresh channel generation.
    for event in decoder.close(1):
        archive.accept(event)
    for event in decoder.open(1):
        archive.accept(event)
    tampered = body_a[:-1] + b"X"
    bad_stream = (deliver(1, 1, redelivered=1)
                  + content_header(1, len(tampered), "order-1")
                  + content_body(1, tampered))
    try:
        pump(decoder, archive, bad_stream, 123, [])
    except ValueError as exc:
        print("tampered redelivery rejected:", exc)
    else:
        raise AssertionError("tampered redelivery should have been rejected")

    # Correct redelivery after the rejection must still work.
    good_stream = (deliver(1, 2, redelivered=1)
                   + content_header(1, len(body_a), "order-1")
                   + content_body(1, body_a))
    pump(decoder, archive, good_stream, 7, acks)
    print("correct redelivery still ACKed on generation 2, tag 2")

    for event in decoder.close(1):
        archive.accept(event)
    for event in decoder.close(2):
        archive.accept(event)
    for event in decoder.disconnect():
        archive.accept(event)

    print("\ncommitted business messages (first-acceptance order):")
    listing = archive.items()
    for message_id, size, digest in listing:
        print("  %-8s size=%-7d sha256=%s..." % (message_id, size, digest[:16]))
    assert [mid for mid, _, _ in listing] == ["order-1", "order-2"]
    assert (listing[0][1], listing[0][2]) == (
        len(body_a), hashlib.sha256(body_a).hexdigest())

    with archive.read("order-1") as fh:
        assert fh.read() == body_a
    with archive.read("order-2") as fh:
        assert fh.read() == body_b
    print("streamed reads reproduce every persisted byte")
    archive.close()

    # Reopen: committed content survives; half messages do not.
    reopened = amqpingest.Archive(db_path)
    assert reopened.items() == listing
    assert reopened.read("order-1").read() == body_a
    print("\nreopen: %d business messages recovered, redelivery safe"
          % len(reopened.items()))
    reopened.close()
    print("\nDEMO OK")


if __name__ == "__main__":
    main()
