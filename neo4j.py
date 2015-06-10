#!/usr/bin/env python
#! -*- encoding: UTF-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module contains the main NDP driver components as well as several
helper and exception classes. The main entry point is the `connect`
function which returns an instance of the ConnectionV1 class that can
be used for running Cypher statements.
"""


from argparse import ArgumentParser
from io import BytesIO
import logging
from select import select
from socket import create_connection, SHUT_RDWR
import struct
from sys import stdout, stderr

# Serialisation and deserialisation routines
from packstream import Packer, Unpacker
# Hydration function for turning structures into their actual types
from typesystem import hydrated


# Signature bytes for each message type
INIT = b"\x01"             # 0000 0001 // INIT <user_agent>
ACK_FAILURE = b"\x0F"      # 0000 1111 // ACK_FAILURE
RUN = b"\x10"              # 0001 0000 // RUN <statement> <parameters>
DISCARD_ALL = b"\x2F"      # 0010 1111 // DISCARD *
PULL_ALL = b"\x3F"         # 0011 1111 // PULL *
SUCCESS = b"\x70"          # 0111 0000 // SUCCESS <metadata>
RECORD = b"\x71"           # 0111 0001 // RECORD <value>
IGNORED = b"\x7E"          # 0111 1110 // IGNORED <metadata>
FAILURE = b"\x7F"          # 0111 1111 // FAILURE <metadata>

log = logging.getLogger("neo4j")


class ProtocolError(Exception):

    pass


class CypherError(Exception):

    code = None

    def __init__(self, data):
        super(CypherError, self).__init__(data.get("message"))
        for key, value in data.items():
            if not key.startswith("_"):
                setattr(self, key, value)


class ChunkWriter(object):
    """ Writer for chunked data.
    """

    max_chunk_size = 65535

    def __init__(self):
        self.raw = BytesIO()
        self.output_buffer = []
        self.output_size = 0

    def write(self, b):
        """ Write some bytes, splitting into chunks if necessary.
        """
        max_chunk_size = self.max_chunk_size
        output_buffer = self.output_buffer
        while b:
            size = len(b)
            future_size = self.output_size + size
            if future_size >= max_chunk_size:
                end = max_chunk_size - self.output_size
                output_buffer.append(b[:end])
                self.output_size = max_chunk_size
                b = b[end:]
                self.flush()
            else:
                output_buffer.append(b)
                self.output_size = future_size
                b = b""

    def flush(self, zero_chunk=False):
        """ Flush everything written since the last chunk to the
        stream, followed by a zero-chunk if required.
        """
        output_buffer = self.output_buffer
        if output_buffer:
            lines = [struct.pack(">H", self.output_size)] + output_buffer
        else:
            lines = []
        if zero_chunk:
            lines.append(b"\x00\x00")
        if lines:
            self.raw.writelines(lines)
            self.raw.flush()
            del output_buffer[:]
            self.output_size = 0

    def to_bytes(self):
        """ Extract the written data as bytes.
        """
        return self.raw.getvalue()

    def close(self, zero_chunk=False):
        """ Close the stream.
        """
        self.flush(zero_chunk=zero_chunk)
        self.raw.close()


class ConnectionV1(object):
    """ Server connection through which all protocol messages
    are sent and received. This class is designed for protocol
    version 1.
    """

    def __init__(self, s):
        self.socket = s
        log.info("NDPv1 connection established!")
        self.init("ExampleDriver/1.0")

    def _send_messages(self, *messages):
        """ Send one or more messages to the server.
        """
        raw = ChunkWriter()
        packer = Packer(raw)

        for message in messages:
            packer.pack(message)
            raw.flush(zero_chunk=True)

        data = raw.to_bytes()
        log.debug("Sending request data: %r" % data)
        self.socket.sendall(data)

        raw.close()

    def _recv(self, size):
        """ Receive a required number of bytes from the network.
        """
        socket = self.socket

        # Try to read the required amount of data
        data = socket.recv(size)
        size -= len(data)

        # If more is needed, keep reading until all data has been received
        while size:
            # Check for available network data
            ready_to_read, _, _ = select((socket,), (), (), 0)
            while not ready_to_read:
                ready_to_read, _, _ = select((socket,), (), (), 0)

            # Read up to the required amount remaining
            b = socket.recv(size)
            size -= len(b)
            data += b

        return data

    def _recv_message(self):
        """ Receive exactly one message from the server.
        """
        raw = BytesIO()
        unpack = Unpacker(raw).unpack

        # Receive chunks of data until chunk_size == 0
        more = True
        while more:
            # Receive chunk header to establish size of chunk that follows
            chunk_header = self._recv(2)
            log.debug("Received chunk header data: %r" % chunk_header)
            chunk_size, = struct.unpack_from(">H", chunk_header)

            # Receive chunk data
            if chunk_size > 0:
                chunk_data = self._recv(chunk_size)
                log.debug("Received chunk data: %r" % chunk_data)
                raw.write(chunk_data)
            else:
                more = False

        # Unpack the message structure from the raw byte stream
        # (there should be only one)
        raw.seek(0)
        signature, fields = next(unpack())
        raw.close()

        # Acknowledge any failures immediately
        if signature == FAILURE:
            self.ack_failure()

        return signature, fields

    def init(self, user_agent):
        """ Initialise a connection with a user agent string.
        """

        # Ensure the user agent is a Unicode value
        if isinstance(user_agent, bytes):
            user_agent = user_agent.decode("UTF-8")

        log.info("Initialising connection")
        self._send_messages((INIT, (user_agent,)))

        signature, (data,) = self._recv_message()
        if signature == SUCCESS:
            log.info("Initialisation successful")
        else:
            raise ProtocolError("INIT was unsuccessful: %r" % data)

    def run(self, statement, parameters):
        """ Run a parameterised Cypher statement.
        """

        # Ensure the statement is a Unicode value
        if isinstance(statement, bytes):
            statement = statement.decode("UTF-8")

        log.info("Running statement %r with parameters %r" % (statement, parameters))
        self._send_messages((RUN, (statement, parameters)),
                            (PULL_ALL, ()))

        signature, (data,) = self._recv_message()
        if signature == SUCCESS:
            fields = tuple(data["fields"])
            log.info("Statement ran successfully with field list %r" % (fields,))
        else:
            raise CypherError(data)

        records = []
        more = True
        while more:
            signature, (data,) = self._recv_message()
            if signature == RECORD:
                log.info("Record received with value list %r" % data)
                records.append(tuple(map(hydrated, data)))
            elif signature == SUCCESS:
                log.info("All records successfully received: %r" % data)
                more = False
            else:
                raise CypherError(data)

        return fields, records

    def ack_failure(self):
        """ Send an acknowledgement for a previous failure.
        """
        log.info("Acknowledging failure")
        self._send_messages((ACK_FAILURE, ()))

        # Skip any ignored responses
        signature, _ = self._recv_message()
        while signature == IGNORED:
            signature, _ = self._recv_message()

        # Check the acknowledgement was successful
        if signature == SUCCESS:
            log.info("Acknowledgement successful")
        else:
            raise ProtocolError("ACK_FAILURE was unsuccessful")

    def close(self):
        """ Shut down and close the connection.
        """
        log.info("Shutting down and closing connection")
        self.socket.shutdown(SHUT_RDWR)
        self.socket.close()


def connect(host, port):
    """ Connect and perform a handshake in order to return a valid
    Connection object, assuming a protocol version can be agreed.
    """

    # Establish a connection to the host and port specified
    log.info("Creating connection to %s on port %d" % (host, port))
    s = create_connection((host, port))

    # Send details of the protocol versions supported
    supported_versions = [1, 0, 0, 0]
    log.info("Supported protocol versions are: %r" % supported_versions)
    data = b"".join(struct.pack(">I", version) for version in supported_versions)
    log.debug("Sending handshake data: %r" % data)
    s.sendall(data)
    
    # Handle the handshake response
    data = s.recv(4)
    log.debug("Received handshake data: %r" % data)
    agreed_version, = struct.unpack(">I", data)
    if agreed_version == 0:
        log.warning("Closing connection as no protocol version could be agreed")
        s.shutdown(SHUT_RDWR)
        s.close()
    else:
        log.info("Protocol version %d agreed" % agreed_version)
        return ConnectionV1(s)


class ColourFormatter(logging.Formatter):
    """ Colour formatter for pretty log output.
    """

    def format(self, record):
        s = super(ColourFormatter, self).format(record)
        if record.levelno == logging.CRITICAL:
            return "\x1b[31;1m%s\x1b[0m" % s  # bright red
        elif record.levelno == logging.ERROR:
            return "\x1b[33;1m%s\x1b[0m" % s  # bright yellow
        elif record.levelno == logging.WARNING:
            return "\x1b[33m%s\x1b[0m" % s    # yellow
        elif record.levelno == logging.INFO:
            return "\x1b[36m%s\x1b[0m" % s    # cyan
        elif record.levelno == logging.DEBUG:
            return "\x1b[34m%s\x1b[0m" % s    # blue
        else:
            return s


class Watcher(object):
    """ Log watcher for debug output.
    """

    handlers = {}

    def __init__(self, logger_name):
        super(Watcher, self).__init__()
        self.logger_name = logger_name
        self.logger = logging.getLogger(self.logger_name)
        self.formatter = ColourFormatter("%(asctime)s  %(message)s")

    def watch(self, level=logging.INFO, out=stdout):
        try:
            self.logger.removeHandler(self.handlers[self.logger_name])
        except KeyError:
            pass
        handler = logging.StreamHandler(out)
        handler.setFormatter(self.formatter)
        self.handlers[self.logger_name] = handler
        self.logger.addHandler(handler)
        self.logger.setLevel(level)


def main():
    parser = ArgumentParser(description="Execute one or more Cypher statements using NDP.")
    parser.add_argument("statement", nargs="+")
    parser.add_argument("-H", "--host", default="localhost")
    parser.add_argument("-P", "--port", type=int, default=7687)
    parser.add_argument("-q", "--quiet", action="store_true")
    parser.add_argument("-v", "--verbose", action="count")
    parser.add_argument("-x", "--times", type=int, default=1)
    args = parser.parse_args()

    if args.verbose:
        level = logging.INFO if args.verbose == 1 else logging.DEBUG
        Watcher("neo4j").watch(level, stderr)

    conn = connect(args.host, args.port)
    if conn:
        for _ in range(args.times):
            for statement in args.statement:
                try:
                    fields, records = conn.run(statement, {})
                except CypherError as error:
                    stderr.write("%s: %s\r\n" % (error.code, error.message))
                else:
                    if not args.quiet:
                        stdout.write("%s\r\n" % "\t".join(fields))
                        for record in records:
                            stdout.write("%s\r\n" % "\t".join(map(repr, record)))
                        stdout.write("\r\n")
        conn.close()


if __name__ == "__main__":
    main()
