"""
Xch Broadcasting Service — Backend
=====================================
pip install fastapi uvicorn python-multipart aiofiles apscheduler aiortc

WebRTC方式:
  aiortcあり → サーバーSFUとして配信者トラックを全視聴者へリレー
  aiortcなし → WebSocketシグナリングリレー（ブラウザ間P2P）

起動:
  uvicorn server:app --reload --port 8000
"""

import json
import uuid
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict

from fastapi import (FastAPI, File, UploadFile, Form,
                     HTTPException, Request, WebSocket, WebSocketDisconnect)
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import aiofiles
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("xch")

# ── aiortc optional ────────────────────────────────────────────────────────────
try:
    from aiortc import RTCPeerConnection, RTCSessionDescription
    from aiortc.contrib.media import MediaRelay
    # RTCIceCandidate は candidate_from_sdp で生成するのが正しいAPI
    from aioice.candidate import Candidate as _AioiceCandidate
    AIORTC = True
    relay = MediaRelay()
    logger.info("aiortc OK — SFU mode")
except ImportError:
    AIORTC = False
    relay = None
    logger.warning("aiortc missing — P2P relay mode")

def _parse_ice_candidate(c: dict):
    """
    ブラウザから届く RTCIceCandidate JSON を aiortc が受け付ける形に変換する。
    aiortc>=1.6 では addIceCandidate() に dict を直接渡せない。
    RTCIceCandidate オブジェクトを candidate_from_sdp() 経由で作る。
    """
    if not AIORTC:
        return None
    from aiortc.sdp import candidate_from_sdp
    raw = c.get("candidate", "")          # "candidate:..." or "..."
    if not raw:
        return None
    # ブラウザは "candidate:..." の形で送ってくる場合がある
    if raw.startswith("candidate:"):
        raw = raw[len("candidate:"):]
    try:
        cand = candidate_from_sdp(raw)
        cand.sdpMid         = c.get("sdpMid")
        cand.sdpMLineIndex  = c.get("sdpMLineIndex")
        return cand
    except Exception as e:
        logger.warning(f"[ICE parse] {e} — raw={raw!r}")
        return None

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
STATIC_DIR = BASE_DIR / "static"
DB_PATH    = BASE_DIR / "schedule.json"
UPLOAD_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)

# ── DB ─────────────────────────────────────────────────────────────────────────
def load_db() -> dict:
    if DB_PATH.exists():
        return json.loads(DB_PATH.read_text(encoding="utf-8"))
    return {"queue": [], "current_index": 0}

def save_db(data: dict):
    DB_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def recalc_schedule(db: dict):
    cursor = datetime.now()
    for item in db["queue"]:
        if item["status"] == "live":
            cursor = datetime.fromisoformat(item["scheduled_end"])
            break
    for item in db["queue"]:
        if item["status"] in ("done", "live"):
            continue
        end = cursor + timedelta(hours=item["duration_hours"])
        item["scheduled_start"] = cursor.isoformat()
        item["scheduled_end"]   = end.isoformat()
        cursor = end

# ── Global WebRTC state ────────────────────────────────────────────────────────
webrtc_session: Optional[dict]        = None
broadcaster_pc: Optional[object]      = None
broadcaster_tracks: list              = []
viewer_pcs: Dict[str, object]         = {}
broadcaster_ws: Optional[WebSocket]   = None
viewer_ws_map: Dict[str, WebSocket]   = {}
# ICE candidate buffers (setRemoteDescription完了前に届いたものを保存)
_bc_ice_buf: list       = []
_bc_remote_set: bool    = False
_vw_ice_buf: Dict[str, list] = {}
_vw_remote_set: set          = set()

# ── FastAPI ────────────────────────────────────────────────────────────────────
app = FastAPI(title="Xch")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                  allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ── Scheduler ──────────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler()

def advance_broadcast():
    db = load_db()
    for i, item in enumerate(db["queue"]):
        if item["status"] == "live":
            item["status"] = "done"
            db["current_index"] = i + 1
            break
    for item in db["queue"]:
        if item["status"] == "queued":
            now = datetime.now()
            end = now + timedelta(hours=item["duration_hours"])
            item["status"] = "live"
            item["scheduled_start"] = now.isoformat()
            item["scheduled_end"]   = end.isoformat()
            scheduler.add_job(advance_broadcast, "date", run_date=end,
                              id="next_advance", replace_existing=True)
            break
    save_db(db)

def maybe_start_first():
    db = load_db()
    if any(i["status"] == "live" for i in db["queue"]):
        for item in db["queue"]:
            if item["status"] == "live":
                end = datetime.fromisoformat(item["scheduled_end"])
                if end > datetime.now():
                    scheduler.add_job(advance_broadcast, "date", run_date=end,
                                      id="next_advance", replace_existing=True)
        return
    for item in db["queue"]:
        if item["status"] == "queued":
            now = datetime.now()
            end = now + timedelta(hours=item["duration_hours"])
            item["status"] = "live"
            item["scheduled_start"] = now.isoformat()
            item["scheduled_end"]   = end.isoformat()
            save_db(db)
            scheduler.add_job(advance_broadcast, "date", run_date=end,
                              id="next_advance", replace_existing=True)
            return

@app.on_event("startup")
async def _startup():
    scheduler.start()
    maybe_start_first()

@app.on_event("shutdown")
async def _shutdown():
    scheduler.shutdown()

# ── HTTP ───────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse((STATIC_DIR / "index.html").read_text(encoding="utf-8"))

@app.get("/api/status")
async def get_status():
    db  = load_db()
    now = datetime.now()

    if webrtc_session:
        started  = datetime.fromisoformat(webrtc_session["started_at"])
        end_dt   = started + timedelta(hours=webrtc_session["duration_hours"])
        remaining = max(0, (end_dt - now).total_seconds())
        return {
            "is_live": True, "webrtc_live": True, "aiortc": AIORTC,
            "current": {**webrtc_session, "id": "webrtc-live", "type": "webrtc",
                        "status": "live", "remaining_seconds": remaining,
                        "scheduled_start": webrtc_session["started_at"],
                        "scheduled_end": end_dt.isoformat()},
            "queue": [i for i in db["queue"] if i["status"] != "done"],
            "viewer_count": len(viewer_ws_map),
            "server_time": now.isoformat(),
        }

    current = next((i for i in db["queue"] if i["status"] == "live"), None)
    queue_items = [
        {**i, "remaining_seconds": max(0, (datetime.fromisoformat(i["scheduled_end"]) - now).total_seconds())
         if i["status"] == "live" else None}
        for i in db["queue"] if i["status"] != "done"
    ]
    return {
        "is_live": current is not None, "webrtc_live": False, "aiortc": AIORTC,
        "current": current, "queue": queue_items, "viewer_count": 0,
        "server_time": now.isoformat(),
    }

@app.post("/api/upload")
async def upload_video(
    file: UploadFile = File(...),
    title: str = Form(...),
    broadcaster: str = Form(...),
    duration_hours: float = Form(...),
):
    ext = Path(file.filename).suffix.lower()
    if ext not in (".mp4", ".webm", ".mov", ".mkv"):
        raise HTTPException(400, "対応: mp4 webm mov mkv")

    fid  = str(uuid.uuid4())
    path = UPLOAD_DIR / f"{fid}{ext}"
    async with aiofiles.open(path, "wb") as f:
        while chunk := await file.read(1 << 20):
            await f.write(chunk)

    db     = load_db()
    cursor = datetime.now()
    for item in reversed(db["queue"]):
        if item["status"] in ("queued", "live"):
            cursor = datetime.fromisoformat(item["scheduled_end"])
            break

    end = cursor + timedelta(hours=duration_hours)
    db["queue"].append({
        "id": fid, "type": "file", "filename": file.filename,
        "path": f"uploads/{fid}{ext}", "title": title,
        "broadcaster": broadcaster, "duration_hours": duration_hours,
        "scheduled_start": cursor.isoformat(), "scheduled_end": end.isoformat(),
        "status": "queued",
    })
    save_db(db)
    if not any(i["status"] == "live" for i in db["queue"]):
        maybe_start_first()
    return {"id": fid, "scheduled_start": cursor.isoformat(),
            "scheduled_end": end.isoformat(), "message": "予約完了"}

@app.get("/api/stream/{file_id}")
async def stream_video(file_id: str, request: Request):
    db    = load_db()
    entry = next((i for i in db["queue"] if i["id"] == file_id), None)
    if not entry:
        raise HTTPException(404, "Not found")
    vp = BASE_DIR / entry["path"]
    if not vp.exists():
        raise HTTPException(404, "File missing")

    size = vp.stat().st_size
    ct   = {".mp4": "video/mp4", ".webm": "video/webm",
            ".mov": "video/quicktime", ".mkv": "video/x-matroska"}.get(vp.suffix.lower(), "video/mp4")

    rh = request.headers.get("range")
    if rh:
        s, e = rh.replace("bytes=", "").split("-")
        s = int(s); e = int(e) if e else size - 1; n = e - s + 1
        async def ranged():
            async with aiofiles.open(vp, "rb") as f:
                await f.seek(s)
                rem = n
                while rem > 0:
                    d = await f.read(min(65536, rem))
                    if not d: break
                    rem -= len(d); yield d
        return StreamingResponse(ranged(), 206, headers={
            "Content-Range": f"bytes {s}-{e}/{size}", "Accept-Ranges": "bytes",
            "Content-Length": str(n), "Content-Type": ct})

    async def full():
        async with aiofiles.open(vp, "rb") as f:
            while c := await f.read(65536): yield c
    return StreamingResponse(full(), headers={"Accept-Ranges": "bytes",
        "Content-Length": str(size), "Content-Type": ct})

@app.delete("/api/queue/{file_id}")
async def delete_queue(file_id: str):
    db = load_db()
    prev = len(db["queue"])
    db["queue"] = [i for i in db["queue"] if not (i["id"] == file_id and i["status"] != "live")]
    if len(db["queue"]) == prev:
        raise HTTPException(404, "削除できません")
    recalc_schedule(db); save_db(db)
    return {"message": "削除しました"}

@app.post("/api/skip")
async def skip():
    advance_broadcast()
    return {"message": "スキップしました"}

# ══════════════════════════════════════════════════════════════════════════════
#  WebRTC シグナリング WebSocket
# ══════════════════════════════════════════════════════════════════════════════

@app.websocket("/ws/broadcast")
async def ws_broadcast(ws: WebSocket):
    global broadcaster_ws, broadcaster_pc, broadcaster_tracks, webrtc_session
    global _bc_ice_buf, _bc_remote_set
    await ws.accept()

    if broadcaster_ws is not None:
        await ws.send_json({"type": "error", "message": "既に配信中です"})
        await ws.close(); return

    broadcaster_ws = ws
    logger.info("[WS] Broadcaster connected")

    try:
        while True:
            msg   = await ws.receive_json()
            mtype = msg.get("type")

            # ── セッション開始通知 ────────────────────────────────────────
            if mtype == "start":
                webrtc_session = {
                    "title":          msg.get("title", "Live配信"),
                    "broadcaster":    msg.get("broadcaster", "配信者"),
                    "duration_hours": float(msg.get("duration_hours", 1)),
                    "started_at":     datetime.now().isoformat(),
                    "type":           "webrtc",
                }
                end_dt = datetime.now() + timedelta(hours=webrtc_session["duration_hours"])
                scheduler.add_job(_end_webrtc, "date", run_date=end_dt,
                                  id="webrtc_end", replace_existing=True)
                await ws.send_json({"type": "started", "aiortc": AIORTC,
                                    "session": webrtc_session})
                await _broadcast_viewers({"type": "stream_start",
                                         "session": webrtc_session})
                logger.info(f"[WebRTC] Session: {webrtc_session['title']}")

            # ── SDP offer → サーバーSFU (aiortcあり) ─────────────────────
            elif mtype == "offer" and AIORTC:
                if broadcaster_pc:
                    try: await broadcaster_pc.close()
                    except: pass
                broadcaster_pc = RTCPeerConnection()
                broadcaster_tracks.clear()
                _bc_ice_buf.clear()
                _bc_remote_set = False

                @broadcaster_pc.on("track")
                async def on_track(track):
                    logger.info(f"[WebRTC] Track: {track.kind}")
                    bt = relay.subscribe(track)
                    broadcaster_tracks.append(bt)
                    for vid, vpc in list(viewer_pcs.items()):
                        try: vpc.addTrack(relay.subscribe(track))
                        except Exception as ex: logger.warning(f"add track {vid}: {ex}")

                await broadcaster_pc.setRemoteDescription(
                    RTCSessionDescription(sdp=msg["sdp"], type=msg.get("sdpType","offer")))
                _bc_remote_set = True

                # バッファに溜まっていたICE candidateを適用
                for buffered in _bc_ice_buf:
                    try: await broadcaster_pc.addIceCandidate(buffered)
                    except Exception as e: logger.warning(f"[ICE flush] {e}")
                _bc_ice_buf.clear()

                answer = await broadcaster_pc.createAnswer()
                await broadcaster_pc.setLocalDescription(answer)
                await ws.send_json({"type": "answer",
                                    "sdp": broadcaster_pc.localDescription.sdp,
                                    "sdpType": broadcaster_pc.localDescription.type})

            # ── ICE candidate (broadcaster→server SFU) ────────────────────
            elif mtype == "ice" and AIORTC:
                c = msg.get("candidate") or {}
                cand = _parse_ice_candidate(c)
                if cand:
                    if _bc_remote_set and broadcaster_pc:
                        try: await broadcaster_pc.addIceCandidate(cand)
                        except Exception as e: logger.warning(f"[ICE add] {e}")
                    else:
                        _bc_ice_buf.append(cand)
                        logger.debug(f"[ICE] buffered (remote not set yet)")

            # ── P2Pリレー: 視聴者のofferへの回答を視聴者へ転送 ───────────
            elif mtype == "answer_viewer":
                vid = msg.get("viewer_id")
                if vid in viewer_ws_map:
                    await viewer_ws_map[vid].send_json({
                        "type": "broadcaster_answer",
                        "sdp": msg.get("sdp"), "sdpType": msg.get("sdpType","answer")})

            # ── P2Pリレー: ICE candidateを特定視聴者へ ───────────────────
            elif mtype == "ice_relay":
                vid = msg.get("viewer_id")
                payload = {"type": "broadcaster_ice", "candidate": msg.get("candidate")}
                if vid and vid in viewer_ws_map:
                    await viewer_ws_map[vid].send_json(payload)
                else:
                    await _broadcast_viewers(payload)

            # ── 配信終了 ──────────────────────────────────────────────────
            elif mtype == "end":
                await _end_webrtc()
                await ws.send_json({"type": "ended"})

    except WebSocketDisconnect:
        logger.info("[WS] Broadcaster disconnected")
    except Exception as e:
        logger.error(f"[WS Broadcast] {e}")
    finally:
        broadcaster_ws = None
        await _end_webrtc()


@app.websocket("/ws/view/{viewer_id}")
async def ws_view(ws: WebSocket, viewer_id: str):
    await ws.accept()
    viewer_ws_map[viewer_id] = ws
    logger.info(f"[WS] Viewer {viewer_id}")

    # セッション情報を送る
    if webrtc_session:
        await ws.send_json({"type": "stream_start", "session": webrtc_session,
                            "aiortc": AIORTC})
    else:
        await ws.send_json({"type": "no_stream", "aiortc": AIORTC})

    # P2Pモード: 配信者に新規視聴者を通知
    if broadcaster_ws and not AIORTC and webrtc_session:
        await broadcaster_ws.send_json({"type": "new_viewer", "viewer_id": viewer_id})

    try:
        while True:
            msg   = await ws.receive_json()
            mtype = msg.get("type")

            # ── SFUモード: 視聴者offer → サーバー ────────────────────────
            if mtype == "offer" and AIORTC:
                # 既存PCをクリーンアップ
                if viewer_id in viewer_pcs:
                    try: await viewer_pcs[viewer_id].close()
                    except: pass
                _vw_ice_buf[viewer_id] = []
                _vw_remote_set.discard(viewer_id)

                vpc = RTCPeerConnection()
                viewer_pcs[viewer_id] = vpc
                for bt in broadcaster_tracks:
                    vpc.addTrack(bt)

                await vpc.setRemoteDescription(
                    RTCSessionDescription(sdp=msg["sdp"], type=msg.get("sdpType","offer")))
                _vw_remote_set.add(viewer_id)

                # バッファを適用
                for buffered in _vw_ice_buf.get(viewer_id, []):
                    try: await vpc.addIceCandidate(buffered)
                    except Exception as e: logger.warning(f"[ICE viewer flush] {e}")
                _vw_ice_buf.pop(viewer_id, None)

                answer = await vpc.createAnswer()
                await vpc.setLocalDescription(answer)
                await ws.send_json({"type": "answer",
                                    "sdp": vpc.localDescription.sdp,
                                    "sdpType": vpc.localDescription.type})

            # ── SFUモード: ICE candidate (viewer→server) ──────────────────
            elif mtype == "ice" and AIORTC:
                c = msg.get("candidate") or {}
                cand = _parse_ice_candidate(c)
                if cand:
                    if viewer_id in _vw_remote_set and viewer_id in viewer_pcs:
                        try: await viewer_pcs[viewer_id].addIceCandidate(cand)
                        except Exception as e: logger.warning(f"[ICE viewer] {e}")
                    else:
                        _vw_ice_buf.setdefault(viewer_id, []).append(cand)

            # ── P2Pリレー: 視聴者のofferを配信者へ ───────────────────────
            elif mtype == "offer" and not AIORTC:
                if broadcaster_ws:
                    await broadcaster_ws.send_json({
                        "type": "viewer_offer", "viewer_id": viewer_id,
                        "sdp": msg.get("sdp"), "sdpType": msg.get("sdpType","offer")})

            # ── P2Pリレー: 視聴者のICE → 配信者へ ───────────────────────
            elif mtype == "ice" and not AIORTC:
                if broadcaster_ws:
                    await broadcaster_ws.send_json({
                        "type": "viewer_ice", "viewer_id": viewer_id,
                        "candidate": msg.get("candidate")})

    except WebSocketDisconnect:
        logger.info(f"[WS] Viewer {viewer_id} left")
    except Exception as e:
        logger.error(f"[WS View] {viewer_id}: {e}")
    finally:
        viewer_ws_map.pop(viewer_id, None)
        if viewer_id in viewer_pcs:
            try: await viewer_pcs[viewer_id].close()
            except: pass
            viewer_pcs.pop(viewer_id, None)


# ── helpers ────────────────────────────────────────────────────────────────────
async def _broadcast_viewers(data: dict):
    dead = []
    for vid, vws in list(viewer_ws_map.items()):
        try:
            await vws.send_json(data)
        except:
            dead.append(vid)
    for vid in dead:
        viewer_ws_map.pop(vid, None)

async def _end_webrtc():
    global webrtc_session, broadcaster_pc, broadcaster_tracks
    global _bc_ice_buf, _bc_remote_set, _vw_ice_buf, _vw_remote_set
    if not webrtc_session:
        return
    webrtc_session = None
    broadcaster_tracks.clear()
    _bc_ice_buf.clear()
    _bc_remote_set = False
    _vw_ice_buf.clear()
    _vw_remote_set.clear()
    if broadcaster_pc:
        try: await broadcaster_pc.close()
        except: pass
        broadcaster_pc = None
    for vpc in list(viewer_pcs.values()):
        try: await vpc.close()
        except: pass
    viewer_pcs.clear()
    await _broadcast_viewers({"type": "stream_end"})
    maybe_start_first()
    logger.info("[WebRTC] Session ended")
