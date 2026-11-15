"""Independent business verification for amqpingest.

This script never imports frame helpers from amqpingest: every AMQP frame is
rebuilt here from the wire description in CONTRACT.md.  It covers

  1. byte-by-byte and multi-channel interleaved decoder reassembly,
  2. Archive driven directly with hand-built third-party events,
  3. redelivery dedup / content-mismatch rejection and exact ACK bytes,
  4. real SIGKILL of worker subprocesses (mid-body and after persistence),
  5. large-body streaming with a hard memory ceiling.

Run:  python3 -I verify.py
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import struct
import subprocess
import sys
import tempfile
import time

# -I (isolated mode) does not put the script directory on sys.path.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import amqpingest

CHECKS = 0


def check(condition, label):
    global CHECKS
    if not condition:
        raise AssertionError("FAILED: " + label)
    CHECKS += 1
    print("  ok -", label)


def expect_value_error(fn, label):
    global CHECKS
    try:
        fn()
    except ValueError:
        CHECKS += 1
        print("  ok -", label)
        return
    raise AssertionError("FAILED (expected ValueError): " + label)


# --------------------------------------------------------------------------
# Independent frame construction (no reuse of amqpingest internals)
# --------------------------------------------------------------------------

FRAME_END = 0xCE


def shortstr(text):
    raw = text.encode("utf-8")
    if len(raw) > 255:
        raise ValueError("short string too long")
    return bytes([len(raw)]) + raw


def wire_frame(ftype, channel, payload):
    return struct.pack(">BHI", ftype, channel, len(payload)) + payload + b"\xce"


def deliver(channel, tag, redelivered=0, consumer="consumer-1",
            exchange="ex", routing_key="rk"):
    payload = (struct.pack(">HH", 60, 60) + shortstr(consumer)
               + struct.pack(">Q", tag) + bytes([redelivered])
               + shortstr(exchange) + shortstr(routing_key))
    return wire_frame(1, channel, payload)


def header(channel, size, message_id, flags=0x0080):
    payload = struct.pack(">HHQH", 60, 0, size, flags) + shortstr(message_id)
    return wire_frame(2, channel, payload)


def body(channel, data):
    return wire_frame(3, channel, data)


HEARTBEAT = wire_frame(8, 0, b"")


def message_frames(channel, tag, message_id, content, redelivered=0):
    return (deliver(channel, tag, redelivered)
            + header(channel, len(content), message_id)
            + (body(channel, content) if content else b""))


def expected_ack(channel, tag):
    payload = struct.pack(">HHQB", 60, 80, tag, 0)
    return struct.pack(">BHI", 1, channel, len(payload)) + payload + b"\xce"


def feed_all(decoder, stream, block_size):
    events = []
    for i in range(0, len(stream), block_size):
        events.extend(decoder.feed(stream[i:i + block_size]))
    return events


def identity(event):
    return (event["connection"], event["channel"],
            event["generation"], event["tag"])


# --------------------------------------------------------------------------
# 1. Decoder: byte-level reassembly and protocol rejection
# --------------------------------------------------------------------------

def test_decoder():
    print("[1] Decoder: independent frames, byte-level feeding")
    stream = message_frames(1, 7, "x", b"abc") + HEARTBEAT
    reference = None
    # Every possible fixed block size exercises different cut points.
    for block_size in range(1, len(stream) + 1):
        dec = amqpingest.Decoder()
        dec.begin("c")
        dec.open(1)
        events = feed_all(dec, stream, block_size)
        if reference is None:
            reference = events
        check(events == reference, "identical events for block size %d" % block_size)
    kinds = [e["event"] for e in reference]
    check(kinds == ["begin", "chunk", "end"], "event order for x/abc")
    check(reference[0]["message_id"] == "x" and reference[0]["size"] == 3,
          "begin carries message_id and declared size")
    check(reference[1]["data"] == b"abc" and len(reference[1]["data"]) <= 65536,
          "chunk carries the body bytes, no reassembled body field")
    check(all("body" not in e for e in reference),
          "events never carry a reassembled full body")
    check(identity(reference[0]) == ("c", 1, 1, 7), "begin identity quadruple")
    check(identity(reference[1]) == identity(reference[2]) == ("c", 1, 1, 7),
          "chunk/end share the identity quadruple")
    check(dec.close(1)[0]["event"] == "close", "close returns its own event")

    print("[1b] empty body completes immediately on header")
    dec = amqpingest.Decoder()
    dec.begin("c2")
    dec.open(2)
    events = feed_all(dec, message_frames(2, 1, "empty", b""), 3)
    check([e["event"] for e in events] == ["begin", "end"],
          "zero-length body: begin then end, no chunk")

    print("[1c] multi-channel interleaving, heartbeats, half-message discard")
    dec = amqpingest.Decoder()
    dec.begin("c3")
    dec.open(1)
    dec.open(2)
    events = []
    events.extend(feed_all(dec, deliver(1, 7), 5))          # ch1 deliver
    events.extend(feed_all(dec, deliver(2, 3), 4))          # ch2 half starts
    events.extend(feed_all(dec, header(2, 5, "y") + body(2, b"ab"), 7))
    events.extend(dec.feed(HEARTBEAT))                      # channel 0 noise
    events.extend(feed_all(dec, header(1, 3, "x") + body(1, b"abc"), 2))
    ch1 = [e for e in events if e["channel"] == 1]
    ch2 = [e for e in events if e["channel"] == 2]
    check([e["event"] for e in ch1] == ["begin", "chunk", "end"],
          "channel 1 completes despite interleaved channel 2 traffic")
    check(identity(ch1[0]) == ("c3", 1, 1, 7), "channel 1 identity kept")
    check([e["event"] for e in ch2] == ["begin", "chunk"] and ch2[1]["data"] == b"ab",
          "channel 2 stays half-finished with its own bytes")
    close_event = dec.close(2)[0]
    check(close_event["generation"] == 1 and close_event["channel"] == 2,
          "close on half message returns close event and drops the half")
    reopen = dec.open(2)[0]
    check(reopen["generation"] == 2, "reopened channel gets generation 2")
    # Generation 2 may reuse delivery-tag 7; an old ACK cannot confirm it.
    events2 = feed_all(dec, message_frames(2, 7, "z", b"q"), 6)
    check(identity(events2[0]) == ("c3", 2, 2, 7)
          and [e["event"] for e in events2] == ["begin", "chunk", "end"],
          "tag reuse on a new generation is a brand-new delivery identity")

    print("[1d] disconnect discards all halves and ends the connection")
    dec = amqpingest.Decoder()
    dec.begin("c4")
    dec.open(1)
    dec.feed(deliver(1, 9) + header(1, 4, "h"))
    dec.feed(body(1, b"xx"))
    disc = dec.disconnect()
    check(disc == [{"event": "disconnect", "connection": "c4"}],
          "disconnect returns only its own event")
    expect_value_error(lambda: dec.feed(HEARTBEAT), "feed after disconnect rejected")
    expect_value_error(lambda: dec.begin("c4"), "connection id cannot be reused")
    dec.begin("c5")
    dec.open(1)
    check(dec.feed(HEARTBEAT) == [], "new connection starts clean; heartbeat silent")

    print("[1e] illegal frames / ordering permanently fail the decoder")
    illegal_streams = [
        (b"\x08\x00\x01\x00\x00\x00\x01x\xce", "non-empty heartbeat"),
        (wire_frame(8, 1, b""), "heartbeat on non-zero channel"),
        (wire_frame(9, 1, b""), "unknown frame type"),
        (wire_frame(1, 3, b"\x00\x3c\x00\x3c"), "frame on unknown channel"),
        (deliver(1, 1)[:-1] + b"\x00", "bad frame end marker"),
        (struct.pack(">BHI", 3, 1, 65537) + b"\x00" * 8, "payload size over 65536"),
        (deliver(1, 1) + deliver(1, 2), "second deliver before message completes"),
        (header(1, 0, "noid"), "header without deliver"),
        (body(1, b"x"), "body without header"),
        (deliver(1, 1) + header(1, 0, "a") + header(1, 0, "b"),
         "duplicate header"),
        (deliver(1, 1) + header(1, 2, "a") + body(1, b"123"),
         "body overflowing declared size"),
        (deliver(1, 1) + wire_frame(
            2, 1, struct.pack(">HHQH", 60, 0, 0, 0x0000) + shortstr("a")),
         "unsupported property flags"),
        (deliver(1, 1) + wire_frame(
            2, 1, struct.pack(">HHQH", 60, 0, 0, 0x0080) + b"\x00"),
         "empty message-id"),
    ]
    for bad_stream, label in illegal_streams:
        dec = amqpingest.Decoder()
        dec.begin("bad")
        dec.open(1)
        expect_value_error(lambda s=bad_stream: feed_all(dec, s, 5), label)
        expect_value_error(lambda: dec.feed(HEARTBEAT),
                           label + " -> decoder permanently failed")

    print("[1f] field-level validation")
    def deliver_raw(tag=1, red=0, klass=60, method=60, consumer="c",
                    exchange="e", routing_key="r", trailer=b""):
        payload = (struct.pack(">HH", klass, method) + shortstr(consumer)
                   + struct.pack(">Q", tag) + bytes([red])
                   + shortstr(exchange) + shortstr(routing_key) + trailer)
        return wire_frame(1, 1, payload)

    bad_fields = [
        deliver_raw(tag=0),
        deliver_raw(red=2),
        deliver_raw(klass=61),
        deliver_raw(method=61),
        deliver_raw(trailer=b"x"),
        # consumer_tag with an invalid UTF-8 short string (bytes 0xc3 0x28)
        wire_frame(1, 1, b"\x00\x3c\x00\x3c" + b"\x02\xc3\x28"
                   + struct.pack(">Q", 1) + b"\x00\x00\x00"),
    ]
    for bad in bad_fields:
        dec = amqpingest.Decoder()
        dec.begin("f")
        dec.open(1)
        expect_value_error(lambda s=bad: feed_all(dec, s, 4),
                           "malformed method field rejected")

    dec = amqpingest.Decoder()
    dec.begin("g")
    dec.open(1)
    dec.feed(deliver(1, 5))
    expect_value_error(lambda: dec.feed(deliver(1, 5)),
                       "delivery-tag must strictly increase")
    expect_value_error(lambda: amqpingest.Decoder().open(1),
                       "open without begin rejected")
    dec2 = amqpingest.Decoder()
    expect_value_error(lambda: dec2.begin(""), "empty connection id rejected")
    expect_value_error(lambda: dec2.feed(b"x"), "feed without connection rejected")
    dec2.begin("h")
    dec2.open(1)
    expect_value_error(lambda: dec2.feed(b"x" * 65537),
                       "feed block larger than 65536 rejected")
    check(dec2.feed(HEARTBEAT) == [], "API misuse is recoverable; decoder still alive")
    expect_value_error(lambda: dec2.open(0), "channel 0 rejected")
    expect_value_error(lambda: dec2.open(65536), "channel 65536 rejected")

    print("[1g] close drops the owning channel's partial frame bytes only")
    dec = amqpingest.Decoder()
    dec.begin("p")
    dec.open(1)
    dec.open(2)
    dec.feed(deliver(2, 1) + header(2, 1, "m"))
    dec.feed(body(2, b"q")[:5])
    check(dec._partial_channel == 2, "partial frame owner tracked")
    dec.close(1)
    dec.open(1)
    check(len(dec._buffer) == 5, "close of another channel keeps the partial bytes")
    events = dec.feed(body(2, b"q")[5:])
    check([e["event"] for e in events] == ["chunk", "end"]
          and events[0]["data"] == b"q",
          "surviving partial frame completes on its own channel")
    dec.feed(deliver(1, 1) + header(1, 10, "n"))
    dec.feed(body(1, b"0123456789")[:3])
    check(len(dec._buffer) == 3 and dec._partial_channel == 1,
          "sub-header partial still records its owner")
    dec.close(1)
    check(len(dec._buffer) == 0, "close discards the owned partial frame bytes")
    dec.open(1)
    events = dec.feed(deliver(1, 1) + header(1, 1, "o") + body(1, b"z"))
    check([e["event"] for e in events] == ["begin", "chunk", "end"]
          and events[0]["generation"] == 3,
          "reopened channel starts clean; no stale bytes leak in")


# --------------------------------------------------------------------------
# 2. Archive driven directly (third-party event source, no Decoder)
# --------------------------------------------------------------------------

def hand_events(connection, channel, generation, tag, message_id, chunks,
                declared_size=None):
    if declared_size is None:
        declared_size = sum(len(c) for c in chunks)
    base = {"connection": connection, "channel": channel,
            "generation": generation, "tag": tag}
    events = [dict(base, event="begin", message_id=message_id,
                   size=declared_size)]
    events.extend(dict(base, event="chunk", data=c) for c in chunks)
    events.append(dict(base, event="end"))
    return events


def test_archive(tmp):
    global CHECKS
    print("[2] Archive: direct third-party events and exact ACK bytes")
    db = os.path.join(tmp, "a.db")
    arc = amqpingest.Archive(db)
    check(arc.accept({"event": "open", "connection": "c", "channel": 1,
                      "generation": 1}) is None, "open returns None")
    ack = None
    for event in hand_events("c", 1, 1, 7, "x", [b"a", b"b", b"c"]):
        result = arc.accept(event)
        if event["event"] == "end":
            ack = result
    check(ack is not None, "end returns an ACK dict")
    check(ack["frame"] == expected_ack(1, 7), "ACK frame bytes are exact")
    check(ack == {"connection": "c", "channel": 1, "generation": 1,
                  "tag": 7, "frame": expected_ack(1, 7)},
          "ACK identity is exactly this delivery")
    check(arc.accept({"event": "begin", "connection": "c", "channel": 1,
                      "generation": 1, "tag": 8,
                      "message_id": "e", "size": 0}) is None,
          "non-end events return None")
    check(arc.accept({"event": "end", "connection": "c", "channel": 1,
                      "generation": 1, "tag": 8}) is not None,
          "empty body accepted with ACK")

    print("[2b] ordering, generation and tag rules are checked independently")
    def reject(event, label):
        expect_value_error(lambda: arc.accept(event), label)
        expect_value_error(lambda: arc.accept(
            {"event": "open", "connection": "c", "channel": 9,
             "generation": 1}), "structural failure is permanent: " + label)

    arc2 = amqpingest.Archive(os.path.join(tmp, "b.db"))
    arc2.accept({"event": "open", "connection": "c", "channel": 1,
                 "generation": 1})
    reject({"event": "begin", "connection": "c", "channel": 1,
            "generation": 2, "tag": 1, "message_id": "m", "size": 1},
           "begin with stale generation")
    arc3 = amqpingest.Archive(os.path.join(tmp, "c.db"))
    arc3.accept({"event": "open", "connection": "c", "channel": 1,
                 "generation": 1})
    reject({"event": "open", "connection": "c", "channel": 1,
            "generation": 1}, "generation must increase, never restart")
    arc4 = amqpingest.Archive(os.path.join(tmp, "d.db"))
    arc4.accept({"event": "open", "connection": "c", "channel": 1,
                 "generation": 1})
    arc4.accept({"event": "begin", "connection": "c", "channel": 1,
                 "generation": 1, "tag": 5, "message_id": "m", "size": 3})
    reject({"event": "begin", "connection": "c", "channel": 1,
            "generation": 1, "tag": 6, "message_id": "n", "size": 1},
           "begin while previous message open")
    arc5 = amqpingest.Archive(os.path.join(tmp, "e.db"))
    arc5.accept({"event": "open", "connection": "c", "channel": 1,
                 "generation": 1})
    for event in hand_events("c", 1, 1, 9, "p", [b"ab"], declared_size=3):
        if event["event"] == "end":
            expect_value_error(lambda e=event: arc5.accept(e),
                               "end before body is complete rejected")
        else:
            arc5.accept(event)
    reject({"event": "chunk", "connection": "c", "channel": 1,
            "generation": 1, "tag": 99, "data": b"x"},
           "chunk referencing an unknown tag rejected")
    arc6 = amqpingest.Archive(os.path.join(tmp, "f.db"))
    arc6.accept({"event": "open", "connection": "c", "channel": 1,
                 "generation": 1})
    arc6.accept({"event": "begin", "connection": "c", "channel": 1,
                 "generation": 1, "tag": 1, "message_id": "m", "size": 2})
    expect_value_error(
        lambda: arc6.accept({"event": "chunk", "connection": "c",
                             "channel": 1, "generation": 1, "tag": 1,
                             "data": b"123"}),
        "chunk overflowing declared size rejected")

    print("[2c] close/disconnect revoke halves; dead connections stay dead")
    arc7 = amqpingest.Archive(os.path.join(tmp, "g.db"))
    arc7.accept({"event": "open", "connection": "c", "channel": 1,
                 "generation": 1})
    arc7.accept({"event": "open", "connection": "c", "channel": 2,
                 "generation": 1})
    arc7.accept({"event": "begin", "connection": "c", "channel": 1,
                 "generation": 1, "tag": 1, "message_id": "half", "size": 4})
    arc7.accept({"event": "chunk", "connection": "c", "channel": 1,
                 "generation": 1, "tag": 1, "data": b"ab"})
    arc7.accept({"event": "close", "connection": "c", "channel": 1,
                 "generation": 1})
    arc7.accept({"event": "open", "connection": "c", "channel": 1,
                 "generation": 2})
    # The half must be gone: a fresh message with the same tag works.
    for event in hand_events("c", 1, 2, 1, "half", [b"abcd"]):
        arc7.accept(event)
    check([i[0] for i in arc7.items()] == ["half"],
          "closed half was discarded, full replacement accepted")
    arc7.accept({"event": "disconnect", "connection": "c"})
    expect_value_error(
        lambda: arc7.accept({"event": "open", "connection": "c",
                             "channel": 3, "generation": 1}),
        "disconnected connection cannot be revived in-process")

    print("[2d] dedup by message-id with full content verification")
    db8 = os.path.join(tmp, "h.db")
    arc8 = amqpingest.Archive(db8)
    arc8.accept({"event": "open", "connection": "c", "channel": 1,
                 "generation": 1})
    for event in hand_events("c", 1, 1, 7, "x", [b"abc"]):
        r = arc8.accept(event)
    check(r["frame"] == expected_ack(1, 7), "first delivery ACK exact bytes")
    # New channel generation reuses tag 7 and re-chunks identical content.
    arc8.accept({"event": "close", "connection": "c", "channel": 1,
                 "generation": 1})
    arc8.accept({"event": "open", "connection": "c", "channel": 1,
                 "generation": 2})
    ack2 = None
    for event in hand_events("c", 1, 2, 7, "x", [b"a", b"bc"]):
        ack2 = arc8.accept(event)
    check(ack2["frame"] == expected_ack(1, 7)
          and (ack2["connection"], ack2["generation"]) == ("c", 2),
          "redelivery gets a current-generation ACK")
    check(arc8.items() == [("x", 3, hashlib.sha256(b"abc").hexdigest())],
          "identical redelivery creates no new business message")
    # Different content under the same id: reject, keep the original, stay usable.
    expect_value_error(
        lambda: [arc8.accept(e) for e in
                 hand_events("c", 1, 2, 8, "x", [b"abd"])],
        "same id with different body rejected")
    check(arc8.items() == [("x", 3, hashlib.sha256(b"abc").hexdigest())],
          "original message preserved after rejected redelivery")
    expect_value_error(
        lambda: [arc8.accept(e) for e in
                 hand_events("c", 1, 2, 9, "x", [b"ab"], declared_size=2)],
        "same id with truncated body rejected")
    check([arc8.accept(e) for e in
           hand_events("c", 1, 2, 10, "x", [b"abc"])][-1] is not None,
          "archive still accepts correct redeliveries after a rejection")

    print("[2e] read handles are independent and stream stored content")
    with arc8.read("x") as fh:
        check(fh.read(2) == b"ab" and fh.read() == b"c" and fh.read() == b"",
              "read streams the body in pieces")
    with arc8.read("x") as fh1, arc8.read("x") as fh2:
        check(fh1.read(1) == b"a" and fh2.read() == b"abc",
              "two readers are independent")
    expect_value_error(lambda: fh1.read(), "closed reader refuses reads")
    try:
        arc8.read("missing")
    except KeyError:
        CHECKS += 1
        print("  ok - unknown message_id raises KeyError")
    else:
        raise AssertionError("unknown id should raise KeyError")

    print("[2f] reopen keeps committed messages and allows id reuse")
    arc8.close()
    arc9 = amqpingest.Archive(db8)
    check(arc9.items() == [("x", 3, hashlib.sha256(b"abc").hexdigest())],
          "reopened archive still knows the accepted message")
    check(arc9.read("x").read() == b"abc", "reopened archive serves the body")
    arc9.accept({"event": "open", "connection": "c", "channel": 5,
                 "generation": 1})
    check([arc9.accept(e) for e in
           hand_events("c", 5, 1, 1, "x", [b"a", b"b", b"c"])][-1] is not None,
          "connection id reused across processes; redelivery safely re-ACKed")
    check(len(arc9.items()) == 1, "still one business message after reopen redelivery")
    arc9.close()

    print("[2g] events without explicit kind (pure third-party dicts)")
    arc10 = amqpingest.Archive(os.path.join(tmp, "i.db"))
    raw_open = {"connection": "z", "channel": 1, "generation": 1}
    check(arc10.accept(dict(raw_open)) is None, "bare open dict accepted")
    base = {"connection": "z", "channel": 1, "generation": 1, "tag": 1}
    arc10.accept(dict(base, message_id="m", size=2))
    arc10.accept(dict(base, data=b"hi"))
    ack = arc10.accept(dict(base))
    check(ack["frame"] == expected_ack(1, 1), "bare end dict yields ACK")
    check(arc10.accept(dict(raw_open)) is None,
          "bare close dict inferred from active channel state")
    check(arc10.accept(dict(raw_open, generation=2)) is None,
          "bare open dict starts generation 2")
    arc10.close()

    print("[2h] in-flight same id on another channel; read() streaming semantics")
    arc11 = amqpingest.Archive(os.path.join(tmp, "j.db"))
    arc11.accept({"event": "open", "connection": "c", "channel": 1,
                  "generation": 1})
    arc11.accept({"event": "open", "connection": "c", "channel": 2,
                  "generation": 1})
    arc11.accept({"event": "begin", "connection": "c", "channel": 1,
                  "generation": 1, "tag": 1, "message_id": "dup", "size": 2})
    arc11.accept({"event": "chunk", "connection": "c", "channel": 1,
                  "generation": 1, "tag": 1, "data": b"ab"})
    expect_value_error(
        lambda: arc11.accept({"event": "begin", "connection": "c",
                              "channel": 2, "generation": 1, "tag": 1,
                              "message_id": "dup", "size": 2}),
        "same message-id opened twice while the first is still half")
    expect_value_error(
        lambda: arc11.accept({"event": "end", "connection": "c",
                              "channel": 1, "generation": 1, "tag": 1}),
        "structural error leaves the archive permanently failed")
    arc11.close()
    # A fresh archive proves the rejected attempt never published anything.
    arc11 = amqpingest.Archive(os.path.join(tmp, "j2.db"))
    arc11.accept({"event": "open", "connection": "c", "channel": 1,
                  "generation": 1})
    arc11.accept({"event": "open", "connection": "c", "channel": 2,
                  "generation": 1})
    check(arc11.accept({"event": "begin", "connection": "c", "channel": 1,
                        "generation": 1, "tag": 1,
                        "message_id": "dup", "size": 2}) is None
          and arc11.accept({"event": "chunk", "connection": "c",
                            "channel": 1, "generation": 1, "tag": 1,
                            "data": b"ab"}) is None
          and arc11.accept({"event": "end", "connection": "c", "channel": 1,
                            "generation": 1, "tag": 1}) is not None,
          "identical flow on a fresh archive completes normally")
    big = bytes((i * 31 + 7) & 0xFF for i in range(200000))
    arc11.accept({"event": "begin", "connection": "c", "channel": 2,
                  "generation": 1, "tag": 2,
                  "message_id": "stream", "size": len(big)})
    for i in range(0, len(big), 65536):
        arc11.accept({"event": "chunk", "connection": "c", "channel": 2,
                      "generation": 1, "tag": 2,
                      "data": big[i:i + 65536]})
    arc11.accept({"event": "end", "connection": "c", "channel": 2,
                  "generation": 1, "tag": 2})
    with arc11.read("stream") as fh:
        pieces = []
        while True:
            piece = fh.read(65536)   # bounded reads never materialize the body
            if not piece:
                break
            check(len(piece) <= 65536, "each bounded read stays small")
            pieces.append(piece)
        check(b"".join(pieces) == big,
              "bounded read() loop reproduces every byte without whole-body buffering")
    with arc11.read("stream") as fh:
        check(fh.read() == big, "unbounded read() follows standard file semantics")
    with arc11.read("stream") as fh:
        check(fh.read(100000) == big[:100000]
              and fh.read(100000) == big[100000:200000]
              and fh.read(10) == b"",
              "bounded reads cross chunk boundaries correctly")
    arc11.close()


# --------------------------------------------------------------------------
# 3. End-to-end public example from CONTRACT.md
# --------------------------------------------------------------------------

def test_end_to_end(tmp):
    print("[3] End-to-end: decoder events straight into the archive")
    arc = amqpingest.Archive(os.path.join(tmp, "e2e.db"))
    dec = amqpingest.Decoder()
    dec.begin("c")
    for event in dec.open(1):
        check(arc.accept(event) is None, "open event accepted")
    stream = (message_frames(1, 7, "x", b"abc") + HEARTBEAT
              + message_frames(1, 8, "w", b"zz"))
    acks = []
    for event in feed_all(dec, stream, 11):
        result = arc.accept(event)
        if result is not None:
            acks.append(result)
    check([a["tag"] for a in acks] == [7, 8], "one ACK per completed delivery")
    check(acks[0]["frame"] == expected_ack(1, 7), "first ACK bytes exact")
    for event in dec.close(1):
        arc.accept(event)
    for event in dec.open(1):
        arc.accept(event)
    # Generation 2, tag 7 reused, redelivered flag set this time.
    redeliver = (deliver(1, 7, redelivered=1) + header(1, 3, "x")
                 + body(1, b"abc"))
    ack = None
    for event in feed_all(dec, redeliver, 4):
        ack = arc.accept(event) or ack
    check(ack["generation"] == 2 and ack["frame"] == expected_ack(1, 7),
          "redelivery ACK belongs to the new generation, not the old one")
    check([i[0] for i in arc.items()] == ["x", "w"],
          "business messages deduped and ordered by first acceptance")
    expect_value_error(
        lambda: [arc.accept(e) for e in
                 feed_all(dec, deliver(1, 9, 1) + header(1, 3, "x")
                          + body(1, b"abd"), 4)],
        "rewritten redelivery content is rejected end-to-end")
    check(arc.read("x").read() == b"abc", "x stays abc after the attempt")
    for event in dec.disconnect():
        arc.accept(event)
    arc.close()


# --------------------------------------------------------------------------
# 4. Real SIGKILL subprocess durability
# --------------------------------------------------------------------------

WORKER_MARKER_TIMEOUT = 30


def worker(mode, db, marker, total_size, half_at):
    import amqpingest as mod  # local import; process runs with -I

    arc = mod.Archive(db)
    dec = mod.Decoder()
    dec.begin("kill-conn")
    for ev in dec.open(1):
        arc.accept(ev)

    def block(i):
        h = hashlib.sha256(struct.pack(">Q", i)).digest()
        out = bytearray()
        while len(out) < 65536:
            h = hashlib.sha256(h).digest()
            out.extend(h)
        return bytes(out[:65536])

    full = b"".join(block(i) for i in range(total_size // 65536))
    full += block(total_size // 65536)[:total_size % 65536]
    stream = deliver(1, 1) + header(1, total_size, "big")
    sent = 0
    chunk_no = 0
    # Stream body frames in 64KiB frames fed in 4KiB decoder blocks.
    while sent < total_size:
        piece = full[sent:sent + 65536]
        stream += body(1, piece)
        sent += len(piece)
        if mode == "mid" and sent >= half_at and not os.path.exists(marker):
            # Flush what we have through the decoder/archive, then park.
            for ev in feed_all(dec, stream, 4096):
                arc.accept(ev)
            stream = b""
            with open(marker, "w") as fh:
                fh.write("half:%d" % sent)
        chunk_no += 1
    for ev in feed_all(dec, stream, 4096):
        arc.accept(ev)
    if mode == "done":
        with open(marker, "w") as fh:
            fh.write("done")
    # Park until the parent SIGKILLs us; do not close anything.
    deadline = time.time() + 120
    while time.time() < deadline:
        time.sleep(0.2)


def expected_big_body(total_size):
    def block(i):
        h = hashlib.sha256(struct.pack(">Q", i)).digest()
        out = bytearray()
        while len(out) < 65536:
            h = hashlib.sha256(h).digest()
            out.extend(h)
        return bytes(out)[:65536]
    parts = [block(i) for i in range(total_size // 65536)]
    rem = total_size % 65536
    if rem:
        parts.append(block(total_size // 65536)[:rem])
    return b"".join(parts)


def run_worker(mode, tmp, total_size, half_at):
    db = os.path.join(tmp, "kill-%s.db" % mode)
    marker = os.path.join(tmp, "marker-%s" % mode)
    proc = subprocess.Popen(
        [sys.executable, "-I", os.path.abspath(__file__),
         "worker", mode, db, marker, str(total_size), str(half_at)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    deadline = time.time() + WORKER_MARKER_TIMEOUT
    while not os.path.exists(marker) and time.time() < deadline:
        if proc.poll() is not None:
            out, err = proc.communicate()
            raise AssertionError("worker exited early:\n" + err.decode())
        time.sleep(0.05)
    if not os.path.exists(marker):
        proc.kill()
        raise AssertionError("worker marker never appeared")
    proc.kill()  # SIGKILL
    proc.wait()
    check(proc.returncode == -9, "worker %s died from real SIGKILL" % mode)
    return db, marker


def test_kill_durability(tmp):
    print("[4] Real SIGKILL: mid-body and post-persistence")
    total = 5 * 1024 * 1024 + 123
    half = total // 2

    db_mid, marker_mid = run_worker("mid", tmp, total, half)
    check(os.path.getsize(marker_mid) > 0, "mid-kill marker recorded progress")
    inspect = sqlite3.connect(db_mid)
    staging = inspect.execute("SELECT COUNT(*) FROM ingest_staging").fetchone()[0]
    messages = inspect.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    chunks = inspect.execute("SELECT COUNT(*) FROM body_chunks").fetchone()[0]
    inspect.close()
    check(messages == 0 and chunks == 0,
          "SIGKILL published no business message for the un-ACKed half")
    check(staging > 0,
          "half body chunks were streamed to disk before the SIGKILL")
    arc = amqpingest.Archive(db_mid)
    inspect = sqlite3.connect(db_mid)
    check(inspect.execute("SELECT COUNT(*) FROM ingest_staging").fetchone()[0] == 0,
          "reopen cleans the never-ACKed staging rows")
    inspect.close()
    check(arc.items() == [], "reopened archive shows no half message")
    # Redelivery after the kill must succeed cleanly.
    arc.accept({"event": "open", "connection": "kill-conn",
                "channel": 1, "generation": 1})
    full = expected_big_body(total)
    digest = hashlib.sha256(full).hexdigest()
    events = [{"event": "begin", "connection": "kill-conn", "channel": 1,
               "generation": 1, "tag": 1, "message_id": "big",
               "size": total}]
    for i in range(0, total, 65536):
        events.append({"event": "chunk", "connection": "kill-conn",
                       "channel": 1, "generation": 1, "tag": 1,
                       "data": full[i:i + 65536]})
    events.append({"event": "end", "connection": "kill-conn",
                   "channel": 1, "generation": 1, "tag": 1})
    ack = None
    for event in events:
        ack = arc.accept(event) or ack
    check(ack is not None and ack["frame"] == expected_ack(1, 1),
          "redelivery after mid-body SIGKILL accepted and ACKed")
    check(arc.items() == [("big", total, digest)],
          "redelivered large body fully persisted")
    with arc.read("big") as fh:
        h = hashlib.sha256()
        while True:
            piece = fh.read(1 << 20)
            if not piece:
                break
            h.update(piece)
        check(h.hexdigest() == digest, "streamed read matches byte for byte")
    arc.close()

    db_done, _ = run_worker("done", tmp, total, half)
    arc2 = amqpingest.Archive(db_done)
    check(arc2.items() == [("big", total, digest)],
          "ACKed content survives SIGKILL after persistence")
    check(arc2.read("big").read(1024) == full[:1024],
      "reopened body readable after kill")
    arc2.accept({"event": "open", "connection": "kill-conn",
                 "channel": 1, "generation": 1})
    ack = None
    for event in hand_events("kill-conn", 1, 1, 2, "big",
                             [full[i:i + 65536]
                              for i in range(0, total, 65536)]):
        ack = arc2.accept(event) or ack
    check(ack["tag"] == 2 and len(arc2.items()) == 1,
          "post-kill redelivery safely re-ACKs without duplication")
    bad = bytearray(full)
    bad[total // 2] ^= 0xFF
    expect_value_error(
        lambda: [arc2.accept(e) for e in
                 hand_events("kill-conn", 1, 1, 3, "big", [bytes(bad)])],
        "tampered post-kill redelivery rejected")
    arc2.close()
    # A content mismatch must not corrupt or alter the archived business data.
    arc3 = amqpingest.Archive(db_done)
    check(arc3.items() == [("big", total, digest)],
          "archived content untouched by tampered redelivery")
    check(arc3.read("big").read() == full,
          "stored body still byte-identical after tampered redelivery")
    arc3.close()


# --------------------------------------------------------------------------
# 5. Large-body streaming memory measurement
# --------------------------------------------------------------------------

def memtest(db):
    import threading
    import tracemalloc

    size_per_channel = 40 * 1024 * 1024
    channels = (1, 2, 3)

    def block(i):
        h = hashlib.sha256(struct.pack(">Q", i)).digest()
        out = bytearray()
        while len(out) < 65536:
            h = hashlib.sha256(h).digest()
            out.extend(h)
        return bytes(out[:65536])

    def rss_kb():
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
        return 0

    arc = amqpingest.Archive(db)
    dec = amqpingest.Decoder()
    dec.begin("mem-conn")
    for ch in channels:
        for event in dec.open(ch):
            arc.accept(event)
    base_rss = rss_kb()
    peak_rss = [base_rss]

    def sampler():
        while sampler.running:
            peak_rss[0] = max(peak_rss[0], rss_kb())
            time.sleep(0.02)

    tracemalloc.start()
    tracemalloc.reset_peak()
    base_current, _ = tracemalloc.get_traced_memory()
    sampler.running = True
    thread = threading.Thread(target=sampler, daemon=True)
    thread.start()
    try:
        blocks_per = size_per_channel // 65536
        expected = {}
        hashers = {ch: hashlib.sha256() for ch in channels}
        # Begin all three messages, then interleave their body frames.  No
        # full stream or full body is ever held by the test or the library.
        for ch in channels:
            for event in feed_all(
                    dec, deliver(ch, 1) + header(ch, size_per_channel,
                                                 "msg-%d" % ch), 4096):
                arc.accept(event)
        for i in range(blocks_per):
            for ch in channels:
                piece = block((ch * 1000) + i)
                hashers[ch].update(piece)
                # Feed each 64KiB body frame in small 4KiB decoder blocks.
                for event in feed_all(dec, body(ch, piece), 4096):
                    arc.accept(event)
        for ch in channels:
            expected["msg-%d" % ch] = (size_per_channel, hashers[ch].hexdigest())
        _, traced_peak = tracemalloc.get_traced_memory()
    finally:
        sampler.running = False
        thread.join()
        tracemalloc.stop()
    rss_delta_bytes = (peak_rss[0] - base_rss) * 1024
    items = dict((mid, (size, digest)) for mid, size, digest in arc.items())
    ok = items == expected
    for mid, (size, digest) in expected.items():
        with arc.read(mid) as fh:
            h = hashlib.sha256()
            while True:
                piece = fh.read(1 << 20)
                if not piece:
                    break
                h.update(piece)
            ok = ok and h.hexdigest() == digest
    arc.close()
    traced_delta = traced_peak - base_current
    print("  traced Python peak delta: %.2f MiB" % (traced_delta / 1048576))
    print("  RSS peak delta:            %.2f MiB" % (rss_delta_bytes / 1048576))
    if not ok:
        print("MEMTEST-CONTENT-FAIL")
        sys.exit(2)
    if traced_delta > 8 * 1024 * 1024:
        print("MEMTEST-TRACE-EXCEEDED")
        sys.exit(3)
    # C-level RSS headroom: SQLite page caches are capped at 256 KiB each.
    if rss_delta_bytes > 48 * 1024 * 1024:
        print("MEMTEST-RSS-EXCEEDED")
        sys.exit(4)
    print("MEMTEST-OK")


def test_memory(tmp):
    print("[5] Large multi-channel body streaming memory test (subprocess)")
    db = os.path.join(tmp, "mem.db")
    proc = subprocess.run(
        [sys.executable, "-I", os.path.abspath(__file__), "memtest", db],
        capture_output=True, text=True)
    print(proc.stdout, end="")
    if proc.returncode != 0:
        print(proc.stderr)
        raise AssertionError("memory subprocess failed: %d" % proc.returncode)
    check("MEMTEST-OK" in proc.stdout, "120 MiB ingest stayed within memory budgets")
    check(os.path.getsize(db) > 100 * 1024 * 1024,
          "large bodies really landed on disk, not in memory")
    # Reopen and confirm durability of the memory-test archive.
    arc = amqpingest.Archive(db)
    ids = [mid for mid, _, _ in arc.items()]
    check(sorted(ids) == ["msg-1", "msg-2", "msg-3"],
          "all three interleaved large messages persisted")
    arc.close()


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "worker":
        worker(sys.argv[2], sys.argv[3], sys.argv[4],
               int(sys.argv[5]), int(sys.argv[6]))
        return
    if len(sys.argv) > 1 and sys.argv[1] == "memtest":
        memtest(sys.argv[2])
        return
    tmp = tempfile.mkdtemp(prefix="amqpingest-verify-")
    print("work dir:", tmp)
    test_decoder()
    test_archive(tmp)
    test_end_to_end(tmp)
    test_kill_durability(tmp)
    test_memory(tmp)
    print("\nALL %d CHECKS PASSED" % CHECKS)


if __name__ == "__main__":
    main()
