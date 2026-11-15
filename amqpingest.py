"""Limited AMQP 0-9-1 frame decoder and persistent ingestion archive.

Implements the subset defined in CONTRACT.md using only the Python
standard library:

* Decoder reassembles basic.deliver / content header / content body frames
  per channel from arbitrarily chunked byte blocks.  Heartbeats and other
  channels never leak into a message body.  Bodies are emitted as chunk
  events instead of being reassembled in memory.
* Archive persists complete bodies into SQLite one chunk at a time (no
  whole-body buffering), independently validates the event stream, dedups
  by message-id with full byte-for-byte verification and emits a single
  basic.ack frame per accepted delivery.
"""

from __future__ import annotations

import hashlib
import io
import sqlite3
import struct

__all__ = ["Decoder", "Archive"]

# AMQP frame types
FRAME_METHOD = 1
FRAME_HEADER = 2
FRAME_BODY = 3
FRAME_HEARTBEAT = 8

FRAME_END = 0xCE
MAX_FRAME_PAYLOAD = 65536
MAX_FEED_BLOCK = 65536

BASIC_CLASS = 60
DELIVER_METHOD = 60
ACK_METHOD = 80
PROPERTY_FLAGS_MESSAGE_ID = 0x0080

U64_MAX = (1 << 64) - 1
EVENT_KINDS = ("open", "close", "disconnect", "begin", "chunk", "end")


# --------------------------------------------------------------------------
# Decoder
# --------------------------------------------------------------------------


class _ChannelState:
    __slots__ = ("generation", "last_tag", "message")

    def __init__(self, generation):
        self.generation = generation
        self.last_tag = 0
        # None == idle, otherwise a dict describing the in-flight message.
        self.message = None


def _short_str(buf, pos):
    """Parse an AMQP short string (u8 length + strict UTF-8 bytes)."""
    length = buf[pos]
    start = pos + 1
    end = start + length
    if end > len(buf):
        raise ValueError("truncated short string")
    raw = bytes(buf[start:end])
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("short string is not valid UTF-8") from exc
    return text, end


class Decoder:
    """Parse the limited AMQP frame subset from arbitrary byte chunks."""

    def __init__(self):
        self._failed = False
        self._connection = None
        self._used_connections = set()
        self._generations = {}        # channel -> generation, current connection
        self._channels = {}           # channel -> _ChannelState
        self._buffer = bytearray()
        # Channel owning the incomplete frame at the tail of _buffer.
        self._partial_channel = None

    # -- lifecycle ---------------------------------------------------------

    def _require_live(self):
        if self._failed:
            raise ValueError("decoder has permanently failed")

    def begin(self, connection_id):
        self._require_live()
        if not isinstance(connection_id, str) or not connection_id:
            raise ValueError("connection_id must be a non-empty string")
        if self._connection is not None:
            raise ValueError("a connection is already active")
        if connection_id in self._used_connections:
            raise ValueError("connection_id was already used by this decoder")
        self._used_connections.add(connection_id)
        self._connection = connection_id
        self._generations = {}
        self._channels = {}
        self._buffer = bytearray()
        self._partial_channel = None

    def open(self, channel):
        self._require_live()
        self._require_connection()
        self._check_channel_number(channel)
        if channel in self._channels:
            raise ValueError("channel is already open")
        generation = self._generations.get(channel, 0) + 1
        self._generations[channel] = generation
        self._channels[channel] = _ChannelState(generation)
        return [{
            "event": "open",
            "connection": self._connection,
            "channel": channel,
            "generation": generation,
        }]

    def close(self, channel):
        self._require_live()
        self._require_connection()
        self._check_channel_number(channel)
        state = self._channels.get(channel)
        if state is None:
            raise ValueError("channel is not open")
        generation = state.generation
        # Dropping the state discards any half-finished message, and any
        # incomplete trailing frame in the byte buffer that belongs to it.
        del self._channels[channel]
        self._drop_trailing_partial(channel)
        return [{
            "event": "close",
            "connection": self._connection,
            "channel": channel,
            "generation": generation,
        }]

    def disconnect(self):
        self._require_live()
        self._require_connection()
        connection_id = self._connection
        # All half messages and the partial frame buffer are discarded.
        self._connection = None
        self._channels = {}
        self._buffer = bytearray()
        self._partial_channel = None
        return [{"event": "disconnect", "connection": connection_id}]

    # -- stream input ------------------------------------------------------

    def feed(self, data):
        self._require_live()
        self._require_connection()
        if not isinstance(data, (bytes, bytearray)):
            raise ValueError("feed expects bytes-like data")
        if len(data) > MAX_FEED_BLOCK:
            raise ValueError("feed block exceeds 65536 bytes")
        if not data:
            return []
        self._buffer.extend(data)
        events = []
        try:
            self._parse_frames(events)
        except ValueError:
            # The byte stream position is no longer trustworthy.
            self._failed = True
            raise
        return events

    # -- helpers -----------------------------------------------------------

    def _require_connection(self):
        if self._connection is None:
            raise ValueError("no active connection")

    @staticmethod
    def _check_channel_number(channel):
        if (not isinstance(channel, int) or isinstance(channel, bool)
                or not 1 <= channel <= 65535):
            raise ValueError("channel must be an int in 1..65535")

    def _parse_frames(self, events):
        buf = self._buffer
        pos = 0
        total = len(buf)
        while total - pos >= 7:
            frame_type = buf[pos]
            channel = struct.unpack_from(">H", buf, pos + 1)[0]
            payload_size = struct.unpack_from(">I", buf, pos + 3)[0]
            if payload_size > MAX_FRAME_PAYLOAD:
                raise ValueError("frame payload exceeds 65536 bytes")
            frame_end = pos + 7 + payload_size + 1
            if frame_end > total:
                self._partial_channel = channel  # incomplete trailing frame
                break  # wait for more bytes
            payload = bytes(buf[pos + 7:pos + 7 + payload_size])
            if buf[pos + 7 + payload_size] != FRAME_END:
                raise ValueError("bad frame end marker")
            pos = frame_end
            self._dispatch(frame_type, channel, payload, events)
        if pos:
            del buf[:pos]
        if not buf:
            self._partial_channel = None
        elif len(buf) >= 3:
            # The channel field (offset 1..2) is already present even though
            # the rest of the 7-byte header may still be missing.
            self._partial_channel = struct.unpack_from(">H", buf, 1)[0]
        # With 1-2 buffered bytes we cannot read the channel yet; keep the
        # owner recorded from the previous feed call.

    def _drop_trailing_partial(self, channel):
        """Remove an incomplete trailing frame that belongs to `channel`.

        Only the single trailing partial frame can exist, because feed()
        parses every complete frame before buffering more bytes.  Frames of
        other channels (and any complete frame still queued) are untouched.
        """
        if self._buffer and self._partial_channel == channel:
            self._buffer.clear()
            self._partial_channel = None

    def _dispatch(self, frame_type, channel, payload, events):
        if frame_type == FRAME_HEARTBEAT:
            if channel != 0 or payload:
                raise ValueError("heartbeat frame must be on channel 0 and empty")
            return
        if frame_type not in (FRAME_METHOD, FRAME_HEADER, FRAME_BODY):
            raise ValueError("unsupported frame type %r" % frame_type)
        state = self._channels.get(channel)
        if state is None:
            raise ValueError("frame on unknown channel %r" % channel)
        if frame_type == FRAME_METHOD:
            self._handle_deliver(state, payload)
        elif frame_type == FRAME_HEADER:
            self._handle_header(state, channel, payload, events)
        else:
            self._handle_body(state, channel, payload, events)

    def _handle_deliver(self, state, payload):
        if len(payload) < 4:
            raise ValueError("truncated basic.deliver method frame")
        klass, method = struct.unpack_from(">HH", payload, 0)
        if klass != BASIC_CLASS or method != DELIVER_METHOD:
            raise ValueError("unsupported method %r/%r" % (klass, method))
        pos = 4
        consumer_tag, pos = _short_str(payload, pos)
        if pos + 9 > len(payload):
            raise ValueError("truncated basic.deliver method frame")
        delivery_tag = struct.unpack_from(">Q", payload, pos)[0]
        pos += 8
        redelivered = payload[pos]
        pos += 1
        exchange, pos = _short_str(payload, pos)
        routing_key, pos = _short_str(payload, pos)
        if pos != len(payload):
            raise ValueError("trailing bytes in basic.deliver payload")
        if delivery_tag == 0:
            raise ValueError("delivery_tag must be a positive integer")
        if redelivered not in (0, 1):
            raise ValueError("redelivered must be 0 or 1")
        if state.message is not None:
            raise ValueError("previous message on channel is not complete")
        if delivery_tag <= state.last_tag:
            raise ValueError("delivery_tag must strictly increase per channel generation")
        state.last_tag = delivery_tag
        state.message = {
            "tag": delivery_tag,
            "header_seen": False,
            "body_size": None,
            "received": 0,
            # Parsed for validation; not part of the public events.
            "consumer_tag": consumer_tag,
            "exchange": exchange,
            "routing_key": routing_key,
            "redelivered": redelivered,
        }

    def _handle_header(self, state, channel, payload, events):
        message = state.message
        if message is None or message["header_seen"]:
            raise ValueError("content header is not valid at this point")
        if len(payload) < 14:
            raise ValueError("truncated content header frame")
        klass, weight = struct.unpack_from(">HH", payload, 0)
        body_size = struct.unpack_from(">Q", payload, 4)[0]
        flags = struct.unpack_from(">H", payload, 12)[0]
        if klass != BASIC_CLASS or weight != 0:
            raise ValueError("bad content header class/weight")
        if flags != PROPERTY_FLAGS_MESSAGE_ID:
            raise ValueError("only the message-id property flag is supported")
        message_id, pos = _short_str(payload, 14)
        if pos != len(payload):
            raise ValueError("trailing bytes in content header payload")
        if not message_id:
            raise ValueError("message_id must be non-empty")
        message["header_seen"] = True
        message["body_size"] = body_size
        identity = (self._connection, channel, state.generation, message["tag"])
        events.append({
            "event": "begin",
            "connection": identity[0],
            "channel": identity[1],
            "generation": identity[2],
            "tag": identity[3],
            "message_id": message_id,
            "size": body_size,
        })
        if body_size == 0:
            events.append({"event": "end",
                           "connection": identity[0], "channel": identity[1],
                           "generation": identity[2], "tag": identity[3]})
            state.message = None

    def _handle_body(self, state, channel, payload, events):
        message = state.message
        if message is None or not message["header_seen"]:
            raise ValueError("body frame without a preceding content header")
        body_size = message["body_size"]
        new_received = message["received"] + len(payload)
        if new_received > body_size:
            raise ValueError("body frames exceed declared body_size")
        identity = (self._connection, channel, state.generation, message["tag"])
        events.append({
            "event": "chunk",
            "connection": identity[0],
            "channel": identity[1],
            "generation": identity[2],
            "tag": identity[3],
            "data": payload,
        })
        message["received"] = new_received
        if new_received == body_size:
            events.append({"event": "end",
                           "connection": identity[0], "channel": identity[1],
                           "generation": identity[2], "tag": identity[3]})
            state.message = None


# --------------------------------------------------------------------------
# Archive
# --------------------------------------------------------------------------


class _ArchiveChannel:
    __slots__ = ("generation", "last_tag", "ingest")

    def __init__(self, generation):
        self.generation = generation
        self.last_tag = 0
        self.ingest = None


class _Ingest:
    """One in-flight message whose chunks stream straight into SQLite.

    Staging rows are committed immediately to ``ingest_staging`` (never held
    in Python memory), keyed by a unique ingest id, and are either moved into
    the published tables in one short transaction at ``end`` or deleted when
    the half message is discarded.
    """

    __slots__ = ("ingest_id", "connection", "channel", "generation", "tag",
                 "message_id", "declared_size", "received", "hasher", "seq")

    def __init__(self, ingest_id, connection, channel, generation, tag,
                 message_id, declared_size):
        self.ingest_id = ingest_id
        self.connection = connection
        self.channel = channel
        self.generation = generation
        self.tag = tag
        self.message_id = message_id
        self.declared_size = declared_size
        self.received = 0
        self.hasher = hashlib.sha256()
        self.seq = 0


class _BodyReader(io.BufferedIOBase):
    """Read-only, independent streaming handle over stored body chunks."""

    def __init__(self, db_path, message_id):
        self._db = sqlite3.connect(db_path)
        self._db.execute("PRAGMA cache_size=-256")
        self._cursor = self._db.execute(
            "SELECT data FROM body_chunks WHERE message_id=? ORDER BY seq",
            (message_id,))
        self._leftover = b""
        self._finished = False

    def readable(self):
        return True

    def writable(self):
        return False

    def read(self, size=-1):
        if self.closed:
            raise ValueError("read on closed file object")
        if size is None or size < 0:
            # Standard file-object semantics: return everything remaining.
            # Callers that must bound memory use read(n) in a loop.
            parts = [self._leftover] if self._leftover else []
            self._leftover = b""
            for (chunk,) in self._cursor:
                parts.append(chunk)
            return b"".join(parts)
        if size == 0:
            return b""
        needed = size - len(self._leftover)
        parts = [self._leftover] if self._leftover else []
        self._leftover = b""
        while needed > 0:
            row = self._cursor.fetchone()
            if row is None:
                self._finished = True
                break
            chunk = row[0]
            if len(chunk) <= needed:
                parts.append(chunk)
                needed -= len(chunk)
            else:
                parts.append(chunk[:needed])
                self._leftover = chunk[needed:]
                needed = 0
        return b"".join(parts)

    def close(self):
        if not self.closed:
            try:
                self._db.close()
            finally:
                super().close()


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


class Archive:
    """Persistent, streaming message archive backed by SQLite."""

    def __init__(self, db_path):
        self._path = str(db_path)
        self._failed = False
        self._closed = False
        self._dead_connections = set()
        self._active = {}        # connection -> {channel -> _ArchiveChannel}
        self._gen_seen = {}      # connection -> {channel -> max generation}
        self._next_ingest_id = 1
        self._ingests = {}       # ingest_id -> _Ingest
        self._db = sqlite3.connect(self._path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("PRAGMA cache_size=-256")
        self._db.execute("PRAGMA busy_timeout=30000")
        self._create_schema()
        # Staging rows from a process that died mid-message are safe to
        # remove on open: they never produced an ACK.
        self._db.execute("DELETE FROM ingest_staging")
        self._db.commit()

    def _create_schema(self):
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS messages (
                message_id TEXT PRIMARY KEY,
                size       INTEGER NOT NULL,
                sha256     TEXT NOT NULL,
                ord        INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS body_chunks (
                message_id TEXT NOT NULL,
                seq        INTEGER NOT NULL,
                data       BLOB NOT NULL,
                PRIMARY KEY (message_id, seq)
            );
            CREATE TABLE IF NOT EXISTS ingest_staging (
                ingest_id INTEGER NOT NULL,
                seq       INTEGER NOT NULL,
                data      BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_staging_id
                ON ingest_staging(ingest_id, seq);
            """
        )
        self._db.commit()

    # -- public API --------------------------------------------------------

    def accept(self, event):
        if self._failed:
            raise ValueError("archive has permanently failed")
        if self._closed:
            raise ValueError("archive is closed")
        if not isinstance(event, dict):
            self._structural("event must be a dict")
        kind = self._event_kind(event)
        try:
            if kind == "open":
                return self._accept_open(event)
            if kind == "close":
                return self._accept_close(event)
            if kind == "disconnect":
                return self._accept_disconnect(event)
            if kind == "begin":
                return self._accept_begin(event)
            if kind == "chunk":
                return self._accept_chunk(event)
            if kind == "end":
                return self._accept_end(event)
        except _ContentMismatch as exc:
            raise ValueError(str(exc)) from exc
        except sqlite3.Error as exc:
            # Storage-level violations (e.g. duplicate business identity)
            # invalidate the archive state rather than leaking driver errors.
            self._structural("storage error: %s" % exc)
        self._structural("unknown event kind")

    def close(self):
        if self._closed:
            return
        # Delete the staging rows of every unfinished body; committed
        # business messages and their body chunks stay on disk untouched.
        for ingest in list(self._ingests.values()):
            self._delete_staging(ingest)
        self._ingests.clear()
        self._db.commit()
        self._db.close()
        self._closed = True

    def _delete_staging(self, ingest):
        self._db.execute(
            "DELETE FROM ingest_staging WHERE ingest_id=?",
            (ingest.ingest_id,))

    def items(self):
        self._require_usable()
        rows = self._db.execute(
            "SELECT message_id, size, sha256 FROM messages ORDER BY ord"
        ).fetchall()
        return [(mid, size, digest) for mid, size, digest in rows]

    def read(self, message_id):
        self._require_usable()
        row = self._db.execute(
            "SELECT 1 FROM messages WHERE message_id=?", (message_id,)
        ).fetchone()
        if row is None:
            raise KeyError(message_id)
        return _BodyReader(self._path, message_id)

    # -- event dispatch ----------------------------------------------------

    def _event_kind(self, event):
        kind = event.get("event") or event.get("type")
        if kind is not None:
            if kind not in EVENT_KINDS:
                self._structural("unknown event kind %r" % (kind,))
            return kind
        # Infer the kind from the event shape for third-party sources.
        if "channel" not in event:
            return "disconnect"
        if "message_id" in event:
            return "begin"
        if "data" in event:
            return "chunk"
        if "tag" in event:
            return "end"
        # open vs close is decided by the current channel state.
        connection = event.get("connection")
        channel = event.get("channel")
        if (isinstance(connection, str) and _is_int(channel)
                and connection in self._active
                and channel in self._active[connection]):
            return "close"
        return "open"

    def _accept_open(self, event):
        connection = self._connection_field(event)
        channel = self._channel_field(event)
        generation = self._generation_field(event)
        if connection in self._dead_connections:
            self._structural("disconnected connection cannot be revived")
        channels = self._active.setdefault(connection, {})
        if channel in channels:
            self._structural("channel already open")
        seen = self._gen_seen.setdefault(connection, {})
        expected = seen.get(channel, 0) + 1
        if generation != expected:
            self._structural("generation must increase by one per channel open")
        seen[channel] = generation
        channels[channel] = _ArchiveChannel(generation)
        return None

    def _accept_close(self, event):
        connection = self._connection_field(event)
        channel = self._channel_field(event)
        generation = self._generation_field(event)
        arch_channel = self._require_active_channel(connection, channel, generation)
        ingest = arch_channel.ingest
        if ingest is not None:
            self._discard_ingest(ingest)
        del self._active[connection][channel]
        return None

    def _accept_disconnect(self, event):
        connection = self._connection_field(event)
        if connection not in self._active:
            self._structural("disconnect for unknown/inactive connection")
        for arch_channel in list(self._active[connection].values()):
            ingest = arch_channel.ingest
            if ingest is not None:
                self._discard_ingest(ingest)
        del self._active[connection]
        self._dead_connections.add(connection)
        return None

    def _accept_begin(self, event):
        connection = self._connection_field(event)
        channel = self._channel_field(event)
        generation = self._generation_field(event)
        tag = event.get("tag")
        message_id = event.get("message_id")
        size = event.get("size")
        if not _is_int(tag) or not 1 <= tag <= U64_MAX:
            self._structural("tag must be a positive 64-bit integer")
        if not isinstance(message_id, str) or not message_id:
            self._structural("message_id must be a non-empty string")
        if not _is_int(size) or not 0 <= size <= U64_MAX:
            self._structural("size must be a 64-bit non-negative integer")
        arch_channel = self._require_active_channel(connection, channel, generation)
        if arch_channel.ingest is not None:
            self._structural("previous message on channel is not complete")
        if tag <= arch_channel.last_tag:
            self._structural("tag must strictly increase per channel generation")
        for other in self._ingests.values():
            if other.message_id == message_id:
                self._structural(
                    "message_id is already in flight on another channel; "
                    "it must finish before redelivery")
        arch_channel.last_tag = tag
        ingest_id = self._next_ingest_id
        self._next_ingest_id += 1
        ingest = _Ingest(ingest_id, connection, channel, generation, tag,
                         message_id, size)
        self._ingests[ingest_id] = ingest
        arch_channel.ingest = ingest
        return None

    def _accept_chunk(self, event):
        ingest = self._require_current_ingest(event)
        data = event.get("data")
        if not isinstance(data, bytes):
            self._structural("chunk data must be bytes")
        if len(data) > MAX_FRAME_PAYLOAD:
            self._structural("chunk exceeds 65536 bytes")
        if ingest.received + len(data) > ingest.declared_size:
            self._structural("chunk exceeds declared body size")
        # Autocommit insert: the half body is on disk immediately and never
        # accumulates in Python memory.
        self._db.execute(
            "INSERT INTO ingest_staging(ingest_id, seq, data) VALUES(?,?,?)",
            (ingest.ingest_id, ingest.seq, data))
        self._db.commit()
        ingest.seq += 1
        ingest.hasher.update(data)
        ingest.received += len(data)
        return None

    def _accept_end(self, event):
        ingest = self._require_current_ingest(event)
        if ingest.received != ingest.declared_size:
            self._structural("end does not fill the declared body exactly")
        digest = ingest.hasher.hexdigest()
        existing = self._db.execute(
            "SELECT size, sha256 FROM messages WHERE message_id=?",
            (ingest.message_id,)).fetchone()
        if existing is not None:
            self._verify_redelivery(ingest, existing[0], existing[1], digest)
            # Same content: drop the staged copy, keep the original message.
            self._discard_ingest(ingest)
            return self._ack_frame(ingest)
        # One short publish transaction moves the staged chunks into the
        # permanent tables atomically.  Other ingests' staging rows are keyed
        # by their own ids and are left untouched.
        order = self._db.execute(
            "SELECT COALESCE(MAX(ord),0)+1 FROM messages").fetchone()[0]
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._db.execute(
                "INSERT INTO messages(message_id, size, sha256, ord) "
                "VALUES(?,?,?,?)",
                (ingest.message_id, ingest.received, digest, order))
            self._db.execute(
                "INSERT INTO body_chunks(message_id, seq, data) "
                "SELECT ?, seq, data FROM ingest_staging "
                "WHERE ingest_id=? ORDER BY seq",
                (ingest.message_id, ingest.ingest_id))
            self._db.execute(
                "DELETE FROM ingest_staging WHERE ingest_id=?",
                (ingest.ingest_id,))
            self._db.commit()
        except sqlite3.Error:
            self._db.rollback()
            raise
        self._ingests.pop(ingest.ingest_id, None)
        self._release_channel(ingest)
        return self._ack_frame(ingest)

    # -- validation helpers ------------------------------------------------

    def _connection_field(self, event):
        connection = event.get("connection")
        if not isinstance(connection, str) or not connection:
            self._structural("connection must be a non-empty string")
        return connection

    def _channel_field(self, event):
        channel = event.get("channel")
        if not _is_int(channel) or not 1 <= channel <= 65535:
            self._structural("channel must be an int in 1..65535")
        return channel

    def _generation_field(self, event):
        generation = event.get("generation")
        if not _is_int(generation) or generation < 1:
            self._structural("generation must be a positive integer")
        return generation

    def _require_active_channel(self, connection, channel, generation):
        channels = self._active.get(connection)
        if not channels or channel not in channels:
            self._structural("event for channel that is not open")
        arch_channel = channels[channel]
        if arch_channel.generation != generation:
            self._structural("event generation does not match active channel generation")
        return arch_channel

    def _require_current_ingest(self, event):
        connection = self._connection_field(event)
        channel = self._channel_field(event)
        generation = self._generation_field(event)
        tag = event.get("tag")
        if not _is_int(tag) or not 1 <= tag <= U64_MAX:
            self._structural("tag must be a positive 64-bit integer")
        arch_channel = self._require_active_channel(connection, channel, generation)
        ingest = arch_channel.ingest
        if ingest is None or ingest.tag != tag:
            self._structural("chunk/end does not reference the current message")
        return ingest

    def _verify_redelivery(self, ingest, stored_size, stored_digest, new_digest):
        mismatch = (ingest.received != stored_size or new_digest != stored_digest)
        if not mismatch:
            # Final byte-for-byte check against the persisted content.
            new_stream = self._db.execute(
                "SELECT data FROM ingest_staging WHERE ingest_id=? ORDER BY seq",
                (ingest.ingest_id,))
            old_stream = self._db.execute(
                "SELECT data FROM body_chunks WHERE message_id=? ORDER BY seq",
                (ingest.message_id,))
            if not _streams_equal(new_stream, old_stream):
                mismatch = True
        if mismatch:
            # Reject but keep the original message; archive stays usable.
            self._discard_ingest(ingest)
            raise _ContentMismatch(
                "redelivered body differs from archived message %r"
                % ingest.message_id)

    def _release_channel(self, ingest):
        arch_channel = self._active[ingest.connection][ingest.channel]
        arch_channel.ingest = None

    def _discard_ingest(self, ingest):
        """Delete one half message's staged rows without touching anything else."""
        self._delete_staging(ingest)
        self._db.commit()
        self._ingests.pop(ingest.ingest_id, None)
        self._release_channel(ingest)

    def _ack_frame(self, ingest):
        payload = struct.pack(">HHQB", BASIC_CLASS, ACK_METHOD, ingest.tag, 0)
        frame = struct.pack(">BHI", FRAME_METHOD, ingest.channel, len(payload))
        frame += payload + bytes([FRAME_END])
        return {
            "connection": ingest.connection,
            "channel": ingest.channel,
            "generation": ingest.generation,
            "tag": ingest.tag,
            "frame": frame,
        }

    def _require_usable(self):
        if self._failed:
            raise ValueError("archive has permanently failed")
        if self._closed:
            raise ValueError("archive is closed")

    def _structural(self, message):
        self._failed = True
        raise ValueError(message)


class _ContentMismatch(Exception):
    """Internal signal: same message-id, different body content."""


def _streams_equal(cursor_a, cursor_b):
    """Compare two chunk streams byte by byte with bounded memory."""
    leftover_a = leftover_b = b""
    while True:
        if not leftover_a:
            row = cursor_a.fetchone()
            leftover_a = row[0] if row is not None else b""
        if not leftover_b:
            row = cursor_b.fetchone()
            leftover_b = row[0] if row is not None else b""
        if not leftover_a or not leftover_b:
            return not leftover_a and not leftover_b
        common = min(len(leftover_a), len(leftover_b))
        if leftover_a[:common] != leftover_b[:common]:
            return False
        leftover_a = leftover_a[common:]
        leftover_b = leftover_b[common:]
