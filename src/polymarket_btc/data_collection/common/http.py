"""Minimal dependency-free HTTP/1.1 plumbing shared by the in-process web
servers (live dashboard, control panel). No routing, no framework -- just
line/byte-level request parsing and response building over asyncio streams.
"""

from __future__ import annotations

import asyncio

MAX_REQUEST_LINE_BYTES = 8192
MAX_HEADERS = 64
MAX_BODY_BYTES = 1_048_576  # control payloads are small JSON bodies, not uploads

SECURITY_HEADERS = (
    ("X-Content-Type-Options", "nosniff"),
    ("Referrer-Policy", "no-referrer"),
)


class MalformedRequest(Exception):
    """The request line, headers, or body could not be parsed."""


def build_response(
    status: str,
    body: bytes,
    content_type: str,
    *,
    extra: tuple[tuple[str, str], ...] = (),
) -> bytes:
    headers = [
        f"HTTP/1.1 {status}",
        f"Content-Type: {content_type}",
        f"Content-Length: {len(body)}",
        "Connection: close",
        *(f"{name}: {value}" for name, value in SECURITY_HEADERS),
        *(f"{name}: {value}" for name, value in extra),
    ]
    return ("\r\n".join(headers) + "\r\n\r\n").encode("utf-8") + body


def build_chunk(payload: bytes) -> bytes:
    return b"%x\r\n%s\r\n" % (len(payload), payload)


async def read_request(
    reader: asyncio.StreamReader,
) -> tuple[str, str, dict[str, str], bytes]:
    """Reads one HTTP/1.1 request and returns (method, target, headers, body).

    Headers are lower-cased keys. Raises MalformedRequest on a malformed
    request line/headers/body; propagates ConnectionResetError /
    IncompleteReadError / TimeoutError from the underlying stream as-is.
    """
    request_line = await reader.readline()
    if not request_line:
        raise MalformedRequest("empty request")
    if len(request_line) > MAX_REQUEST_LINE_BYTES:
        raise MalformedRequest("request line too long")
    parts = request_line.decode("latin-1").split()
    if len(parts) < 2:
        raise MalformedRequest("bad request line")
    method, target = parts[0], parts[1]

    headers: dict[str, str] = {}
    for _ in range(MAX_HEADERS):
        line = await reader.readline()
        if line in (b"\r\n", b"\n", b""):
            break
        if b":" not in line:
            continue
        name, _, value = line.decode("latin-1").partition(":")
        headers[name.strip().lower()] = value.strip()

    body = b""
    content_length = headers.get("content-length")
    if content_length is not None:
        try:
            size = int(content_length)
        except ValueError as exc:
            raise MalformedRequest("bad content-length") from exc
        if size < 0 or size > MAX_BODY_BYTES:
            raise MalformedRequest("body too large")
        if size:
            body = await reader.readexactly(size)
    return method, target, headers, body


__all__ = [
    "MAX_BODY_BYTES",
    "MalformedRequest",
    "SECURITY_HEADERS",
    "build_chunk",
    "build_response",
    "read_request",
]
