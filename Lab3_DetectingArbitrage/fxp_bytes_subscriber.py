"""
Subscriber of a Forex Provider

This module contains useful unmarshalling functions for subscriber.
"""
import ipaddress
from array import array
from datetime import datetime, timezone

MAX_QUOTES_PER_MESSAGE = 50
MICROS_PER_SECOND = 1_000_000


def deserialize_price(x: bytes) -> float:
    """
    Convert a byte array to a float value used in the price feed messages.

    :param x: price received from Forex Provider message in byte array format
    :return: number in float format
    """
    return array('d', x)[0]


def serialize_address(socket_address: (str, int)) -> bytes:
    """

    :param socket_address: a tuple of ip address and port
    :return: 6-byte stream
    """
    address = bytes()
    ip_v4 = ipaddress.ip_address(socket_address[0])
    ip_v4_as_int = int(ip_v4)
    address += ip_v4_as_int.to_bytes(4, byteorder="big")
    port_byte_array = int(socket_address[1]).to_bytes(2, byteorder='big')  # array of 2-byte
    address += port_byte_array
    return address


def deserialize_utc_datetime(utc_byte: bytes):
    """
    Convert a byte stream of UTC datetime into datetime from a Forex Provider message.
    A 64-bit integer number of microseconds that have passed since 00:00:00 UTC on 1 January 1970
    (excluding leap seconds). Sent in big-endian network format.

    :param utc_byte: byte stream to convert to datetime timestamp
    :return: 8-byte stream
    """
    utc_val = int.from_bytes(utc_byte, "big")
    date_utc = datetime.fromtimestamp(utc_val / MICROS_PER_SECOND, timezone.utc)
    return date_utc


def unmarshal_message(quote_byte_stream: bytes):
    """
    Construct the message in a list format from forex provider from received byte stream.

    :param quote_byte_stream: list of quote structures ('cross' and 'price', may also have 'timestamp')
    :return: byte stream to send in UDP message
    """
    message = []
    quotes_count = int(len(quote_byte_stream) / 32)
    for i in range(quotes_count):
        offset = i * 32
        timestamp = deserialize_utc_datetime(quote_byte_stream[0 + offset:8 + offset])
        first_cross = quote_byte_stream[8 + offset:11 + offset].decode()
        second_cross = quote_byte_stream[11 + offset:14 + offset].decode()
        price = deserialize_price(quote_byte_stream[14 + offset:22 + offset])
        message.append({'timestamp': timestamp, 'cross': '{}/{}'.format(first_cross, second_cross), 'price': price})

    return message
