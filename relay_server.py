from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import string
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from aiohttp import WSMsgType, web

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
LOG = logging.getLogger("cell-defender-server")

ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
MAX_MESSAGE_BYTES = 2_000_000
MAX_ROOMS = 1000
BASE_DIR = Path(__file__).resolve().parent
UPDATES_DIR = BASE_DIR / "updates"
MANIFEST_PATH = UPDATES_DIR / "manifest.json"


@dataclass
class Peer:
    socket: web.WebSocketResponse
    role: str
    nickname: str
    last_message: float = field(default_factory=time.monotonic)


@dataclass
class GlobalPeer:
    socket: web.WebSocketResponse
    nickname: str
    last_sent: float = 0.0


@dataclass
class Room:
    code: str
    host: Peer
    guest: Peer | None = None
    created: float = field(default_factory=time.monotonic)
    started: bool = False


ROOMS: dict[str, Room] = {}
SOCKET_ROOM: dict[int, str] = {}
GLOBAL_CLIENTS: dict[int, GlobalPeer] = {}
GLOBAL_HISTORY: deque[dict[str, Any]] = deque(maxlen=60)
GLOBAL_CHAT_COOLDOWN = 0.8


def room_code() -> str:
    for _ in range(100):
        code = "".join(random.choice(ALPHABET) for _ in range(6))
        if code not in ROOMS:
            return code
    raise RuntimeError("Nie udało się utworzyć kodu pokoju")


async def send_json(socket: web.WebSocketResponse, payload: dict[str, Any]) -> None:
    if not socket.closed:
        await socket.send_str(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))


def clean_text(value: Any, limit: int) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    text = "".join(ch for ch in text if ch.isprintable())
    return " ".join(text.split())[:limit]


async def broadcast_global(payload: dict[str, Any]) -> None:
    stale: list[int] = []
    for socket_id, peer in list(GLOBAL_CLIENTS.items()):
        try:
            await send_json(peer.socket, payload)
        except Exception:
            stale.append(socket_id)
    for socket_id in stale:
        GLOBAL_CLIENTS.pop(socket_id, None)


async def broadcast_global_status() -> None:
    await broadcast_global({"type": "global_status", "online": len(GLOBAL_CLIENTS)})


async def global_join(socket: web.WebSocketResponse, payload: dict[str, Any]) -> None:
    nickname = clean_text(payload.get("nickname", "Komórka"), 24) or "Komórka"
    GLOBAL_CLIENTS[id(socket)] = GlobalPeer(socket, nickname)
    await send_json(socket, {"type": "global_history", "messages": list(GLOBAL_HISTORY)})
    await broadcast_global_status()
    LOG.info("%s dołączył do chatu globalnego", nickname)


async def global_chat(socket: web.WebSocketResponse, payload: dict[str, Any]) -> None:
    peer = GLOBAL_CLIENTS.get(id(socket))
    if not peer:
        await send_json(socket, {"type": "error", "message": "Najpierw połącz się z chatem globalnym."})
        return
    now = time.monotonic()
    if now - peer.last_sent < GLOBAL_CHAT_COOLDOWN:
        await send_json(socket, {"type": "error", "message": "Pisz trochę wolniej."})
        return
    text = clean_text(payload.get("text", ""), 180)
    if not text:
        return
    peer.last_sent = now
    message = {
        "type": "global_chat",
        "nickname": peer.nickname,
        "text": text,
        "timestamp": int(time.time()),
    }
    GLOBAL_HISTORY.append(message)
    await broadcast_global(message)


async def cleanup(socket: web.WebSocketResponse) -> None:
    global_peer = GLOBAL_CLIENTS.pop(id(socket), None)
    if global_peer:
        await broadcast_global_status()
        LOG.info("%s opuścił chat globalny", global_peer.nickname)
    code = SOCKET_ROOM.pop(id(socket), None)
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
    if other and not other.socket.closed:
        try:
            await send_json(other.socket, {"type": "peer_left", "room": code})
        except Exception:
            pass
    LOG.info("Rozłączono klienta z pokoju %s", code)


async def create_room(socket: web.WebSocketResponse, payload: dict[str, Any]) -> None:
    if len(ROOMS) >= MAX_ROOMS:
        await send_json(socket, {"type": "error", "message": "Serwer ma za dużo aktywnych pokoi."})
        return
    code = room_code()
    nickname = str(payload.get("nickname", "Gracz 1"))[:24]
    room = Room(code, Peer(socket, "host", nickname))
    ROOMS[code] = room
    SOCKET_ROOM[id(socket)] = code
    await send_json(socket, {"type": "room_created", "room": code, "role": "host", "nickname": nickname})
    LOG.info("Utworzono pokój %s", code)


async def join_room(socket: web.WebSocketResponse, payload: dict[str, Any]) -> None:
    code = str(payload.get("room", "")).upper().replace("-", "").replace(" ", "").strip()
    room = ROOMS.get(code)
    if not room:
        await send_json(socket, {"type": "error", "message": "Nie znaleziono pokoju."})
        return
    if room.guest is not None:
        await send_json(socket, {"type": "error", "message": "Pokój jest już pełny."})
        return
    nickname = str(payload.get("nickname", "Gracz 2"))[:24]
    room.guest = Peer(socket, "guest", nickname)
    SOCKET_ROOM[id(socket)] = code
    await send_json(socket, {
        "type": "room_joined",
        "room": code,
        "role": "guest",
        "host_nickname": room.host.nickname,
        "nickname": nickname,
    })
    await send_json(room.host.socket, {"type": "peer_joined", "room": code, "nickname": nickname})
    LOG.info("Do pokoju %s dołączył %s", code, nickname)


async def relay(socket: web.WebSocketResponse, payload: dict[str, Any]) -> None:
    code = SOCKET_ROOM.get(id(socket))
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
    if target is None or target.socket.closed:
        return
    payload["relay_role"] = "host" if sender_is_host else "guest"
    try:
        await send_json(target.socket, payload)
    except Exception:
        pass


async def websocket_handler(request: web.Request) -> web.StreamResponse:
    socket = web.WebSocketResponse(
        heartbeat=20,
        max_msg_size=MAX_MESSAGE_BYTES,
        compress=True,
        receive_timeout=90,
    )
    await socket.prepare(request)
    remote = request.remote or "?"
    LOG.info("Połączono WebSocket: %s", remote)
    try:
        async for message in socket:
            if message.type == WSMsgType.TEXT:
                raw = message.data
                if len(raw.encode("utf-8")) > MAX_MESSAGE_BYTES:
                    await socket.close(code=1009, message=b"message too large")
                    break
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                msg_type = str(payload.get("type", ""))
                if msg_type == "ping":
                    await send_json(socket, {"type": "pong", "token": payload.get("token", "")})
                elif msg_type == "global_join":
                    await global_join(socket, payload)
                elif msg_type == "global_chat":
                    await global_chat(socket, payload)
                elif msg_type == "global_leave":
                    GLOBAL_CLIENTS.pop(id(socket), None)
                    await broadcast_global_status()
                elif msg_type == "create":
                    if id(socket) not in SOCKET_ROOM:
                        await create_room(socket, payload)
                elif msg_type == "join":
                    if id(socket) not in SOCKET_ROOM:
                        await join_room(socket, payload)
                elif msg_type in {"start", "input", "snapshot", "ability", "chat", "upgrade_vote", "ready"}:
                    await relay(socket, payload)
            elif message.type in (WSMsgType.ERROR, WSMsgType.CLOSE, WSMsgType.CLOSED):
                break
    except asyncio.TimeoutError:
        LOG.info("Przekroczono czas bezczynności połączenia %s", remote)
    except Exception:
        LOG.exception("Błąd klienta WebSocket")
    finally:
        await cleanup(socket)
    return socket


async def index(request: web.Request) -> web.StreamResponse:
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return await websocket_handler(request)
    return web.json_response({
        "service": "Cell Defender CO-OP, Global Chat & Update Server",
        "status": "online",
        "rooms": len(ROOMS),
        "players": sum(1 + int(room.guest is not None) for room in ROOMS.values()),
        "global_chat_online": len(GLOBAL_CLIENTS),
        "websocket": "wss://" + request.host,
        "update_api": "/api/update",
    })


async def health(request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "rooms": len(ROOMS), "global_chat_online": len(GLOBAL_CLIENTS), "time": int(time.time())})


async def stats(request: web.Request) -> web.Response:
    return web.json_response({
        "rooms": len(ROOMS),
        "players": sum(1 + int(room.guest is not None) for room in ROOMS.values()),
        "started_rooms": sum(int(room.started) for room in ROOMS.values()),
        "global_chat_online": len(GLOBAL_CLIENTS),
        "global_chat_history": len(GLOBAL_HISTORY),
        "uptime_note": "Pokoje i historia chatu są przechowywane w pamięci serwera.",
    })


async def update_manifest(request: web.Request) -> web.Response:
    if not MANIFEST_PATH.exists():
        return web.json_response({
            "version": "2.2.0",
            "notes": "Brak opublikowanego pakietu aktualizacji.",
            "mandatory": False,
            "packages": {},
        }, headers={"Cache-Control": "no-store"})
    try:
        data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return web.json_response({"error": "manifest_invalid"}, status=500)
    return web.json_response(data, headers={"Cache-Control": "no-store, max-age=0"})


async def download_update(request: web.Request) -> web.StreamResponse:
    filename = request.match_info.get("filename", "")
    if not filename or Path(filename).name != filename:
        raise web.HTTPNotFound()
    path = (UPDATES_DIR / filename).resolve()
    try:
        path.relative_to(UPDATES_DIR.resolve())
    except ValueError:
        raise web.HTTPNotFound()
    if not path.is_file() or path.suffix.lower() != ".zip":
        raise web.HTTPNotFound()
    response = web.FileResponse(path)
    response.headers["Content-Type"] = "application/zip"
    response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    return response


async def stale_room_cleaner(app: web.Application) -> None:
    del app
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
                    SOCKET_ROOM.pop(id(peer.socket), None)
                    try:
                        await peer.socket.close(code=1001, message=b"room expired")
                    except Exception:
                        pass


async def cleaner_context(app: web.Application):
    task = asyncio.create_task(stale_room_cleaner(app))
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def create_app() -> web.Application:
    app = web.Application(client_max_size=MAX_MESSAGE_BYTES)
    app.cleanup_ctx.append(cleaner_context)
    app.router.add_get("/", index)
    app.router.add_get("/ws", websocket_handler)
    app.router.add_get("/healthz", health)
    app.router.add_get("/api/stats", stats)
    app.router.add_get("/api/update", update_manifest)
    app.router.add_get("/updates/{filename}", download_update)
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Cell Defender CO-OP i serwer aktualizacji")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    UPDATES_DIR.mkdir(parents=True, exist_ok=True)
    LOG.info("Start serwera na http://%s:%s", args.host, args.port)
    web.run_app(create_app(), host=args.host, port=args.port, print=None, access_log=LOG)


if __name__ == "__main__":
    main()
