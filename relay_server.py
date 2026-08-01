from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import string
import time
from dataclasses import dataclass, field
from typing import Any

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
LOG = logging.getLogger("cell-defender-relay")

ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
MAX_MESSAGE_BYTES = 2_000_000
MAX_ROOMS = 1000


@dataclass
class Peer:
    socket: ServerConnection
    role: str
    nickname: str
    last_message: float = field(default_factory=time.monotonic)


@dataclass
class Room:
    code: str
    host: Peer
    guest: Peer | None = None
    created: float = field(default_factory=time.monotonic)
    started: bool = False


ROOMS: dict[str, Room] = {}
SOCKET_ROOM: dict[ServerConnection, str] = {}


def room_code() -> str:
    for _ in range(100):
        code = "".join(random.choice(ALPHABET) for _ in range(6))
        if code not in ROOMS:
            return code
    raise RuntimeError("Nie udało się utworzyć kodu pokoju")


async def send_json(socket: ServerConnection, payload: dict[str, Any]) -> None:
    await socket.send(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))


async def cleanup(socket: ServerConnection) -> None:
    code = SOCKET_ROOM.pop(socket, None)
    if not code:
        return
    room = ROOMS.get(code)
    if not room:
        return
    other: Peer | None = None
    if room.host.socket is socket:
        other = room.guest
        ROOMS.pop(code, None)
    elif room.guest and room.guest.socket is socket:
        other = room.host
        room.guest = None
        room.started = False
    if other:
        try:
            await send_json(other.socket, {"type": "peer_left", "room": code})
        except ConnectionClosed:
            pass
    LOG.info("Rozłączono klienta z pokoju %s", code)


async def create_room(socket: ServerConnection, payload: dict[str, Any]) -> None:
    if len(ROOMS) >= MAX_ROOMS:
        await send_json(socket, {"type": "error", "message": "Serwer ma za dużo aktywnych pokoi."})
        return
    code = room_code()
    nickname = str(payload.get("nickname", "Gracz 1"))[:24]
    room = Room(code, Peer(socket, "host", nickname))
    ROOMS[code] = room
    SOCKET_ROOM[socket] = code
    await send_json(socket, {"type": "room_created", "room": code, "role": "host", "nickname": nickname})
    LOG.info("Utworzono pokój %s", code)


async def join_room(socket: ServerConnection, payload: dict[str, Any]) -> None:
    code = str(payload.get("room", "")).upper().replace("-", "").strip()
    room = ROOMS.get(code)
    if not room:
        await send_json(socket, {"type": "error", "message": "Nie znaleziono pokoju."})
        return
    if room.guest is not None:
        await send_json(socket, {"type": "error", "message": "Pokój jest już pełny."})
        return
    nickname = str(payload.get("nickname", "Gracz 2"))[:24]
    room.guest = Peer(socket, "guest", nickname)
    SOCKET_ROOM[socket] = code
    await send_json(socket, {
        "type": "room_joined", "room": code, "role": "guest",
        "host_nickname": room.host.nickname, "nickname": nickname,
    })
    await send_json(room.host.socket, {"type": "peer_joined", "room": code, "nickname": nickname})
    LOG.info("Do pokoju %s dołączył %s", code, nickname)


async def relay(socket: ServerConnection, payload: dict[str, Any]) -> None:
    code = SOCKET_ROOM.get(socket)
    room = ROOMS.get(code or "")
    if not room:
        await send_json(socket, {"type": "error", "message": "Najpierw utwórz lub dołącz do pokoju."})
        return
    sender_is_host = room.host.socket is socket
    target = room.guest if sender_is_host else room.host
    msg_type = str(payload.get("type", ""))
    if msg_type == "start" and not sender_is_host:
        await send_json(socket, {"type": "error", "message": "Tylko host może rozpocząć grę."})
        return
    if msg_type == "start":
        room.started = True
    if target is None:
        return
    payload["relay_role"] = "host" if sender_is_host else "guest"
    try:
        await send_json(target.socket, payload)
    except ConnectionClosed:
        pass


async def handler(socket: ServerConnection) -> None:
    try:
        async for raw in socket:
            if isinstance(raw, bytes):
                if len(raw) > MAX_MESSAGE_BYTES:
                    await socket.close(code=1009, reason="message too large")
                    return
                raw = raw.decode("utf-8", errors="ignore")
            if len(raw.encode("utf-8")) > MAX_MESSAGE_BYTES:
                await socket.close(code=1009, reason="message too large")
                return
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            msg_type = str(payload.get("type", ""))
            if msg_type == "ping":
                await send_json(socket, {"type": "pong", "token": payload.get("token", "")})
            elif msg_type == "create":
                if socket not in SOCKET_ROOM:
                    await create_room(socket, payload)
            elif msg_type == "join":
                if socket not in SOCKET_ROOM:
                    await join_room(socket, payload)
            elif msg_type in {"start", "input", "snapshot", "ability", "chat", "upgrade_vote", "ready"}:
                await relay(socket, payload)
    except ConnectionClosed:
        pass
    except Exception:
        LOG.exception("Błąd klienta")
    finally:
        await cleanup(socket)


async def stale_room_cleaner() -> None:
    while True:
        await asyncio.sleep(60)
        cutoff = time.monotonic() - 60 * 60 * 4
        stale = [code for code, room in ROOMS.items() if room.created < cutoff]
        for code in stale:
            room = ROOMS.pop(code, None)
            if not room:
                continue
            for peer in (room.host, room.guest):
                if peer:
                    SOCKET_ROOM.pop(peer.socket, None)
                    try:
                        await peer.socket.close(code=1001, reason="room expired")
                    except Exception:
                        pass


async def main(host: str, port: int) -> None:
    async with serve(
        handler,
        host,
        port,
        max_size=MAX_MESSAGE_BYTES,
        ping_interval=20,
        ping_timeout=20,
        compression="deflate",
    ):
        LOG.info("Relay Cell Defender działa na ws://%s:%s", host, port)
        await stale_room_cleaner()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Serwer relay Cell Defender 2.0")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    asyncio.run(main(args.host, args.port))
