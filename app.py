"""
Xch Broadcasting Service - Backend
====================================
pip install fastapi uvicorn python-multipart aiofiles apscheduler
uvicorn server:app --reload --port 8000
"""

import os
import json
import uuid
import asyncio
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, File, UploadFile, Form, HTTPException, Request
from fastapi.responses import (
    JSONResponse, StreamingResponse, FileResponse, HTMLResponse
)
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import aiofiles
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
STATIC_DIR = BASE_DIR / "static"
DB_PATH    = BASE_DIR / "schedule.json"
UPLOAD_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)

# ── State ──────────────────────────────────────────────────────────────────────
# schedule.json schema:
# {
#   "queue": [
#     {
#       "id": "uuid",
#       "filename": "original_name.mp4",
#       "path": "uploads/uuid.mp4",
#       "title": "配信タイトル",
#       "broadcaster": "配信者名",
#       "duration_hours": 2,          # 配信者が指定した配信時間
#       "scheduled_start": "ISO8601", # 予定開始時刻
#       "scheduled_end": "ISO8601",   # 予定終了時刻
#       "status": "queued|live|done"
#     },
#     ...
#   ],
#   "current_index": 0
# }

def load_db() -> dict:
    if DB_PATH.exists():
        with open(DB_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"queue": [], "current_index": 0}

def save_db(data: dict):
    with open(DB_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def recalc_schedule(db: dict):
    """queueの順番から開始・終了時刻を再計算する。
    現在liveなものはそのまま。その後は順番に連結。"""
    now = datetime.now()
    cursor = now

    # 現在liveのエントリがあればそこから始める
    for item in db["queue"]:
        if item["status"] == "live":
            # liveのendをcursorに
            cursor = datetime.fromisoformat(item["scheduled_end"])
            break

    for item in db["queue"]:
        if item["status"] in ("done",):
            continue
        if item["status"] == "live":
            continue  # liveは変更しない
        # queued
        start = cursor
        end = start + timedelta(hours=item["duration_hours"])
        item["scheduled_start"] = start.isoformat()
        item["scheduled_end"]   = end.isoformat()
        cursor = end

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Xch")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files (HTML, CSS, fonts)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ── Scheduler ─────────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler()

def advance_broadcast():
    """現在の配信を終了し次へ。スケジューラから呼ばれる。"""
    db = load_db()
    queue = db["queue"]

    # 現在liveをdoneに
    for i, item in enumerate(queue):
        if item["status"] == "live":
            item["status"] = "done"
            db["current_index"] = i + 1
            break

    # 次のqueuedを探してliveに
    for item in queue:
        if item["status"] == "queued":
            item["status"] = "live"
            # 実際の開始時刻を今に更新
            now = datetime.now()
            end = now + timedelta(hours=item["duration_hours"])
            item["scheduled_start"] = now.isoformat()
            item["scheduled_end"]   = end.isoformat()
            # 次のadvanceをスケジュール
            scheduler.add_job(
                advance_broadcast,
                "date",
                run_date=end,
                id="next_advance",
                replace_existing=True,
            )
            break

    save_db(db)
    print(f"[Scheduler] Advanced broadcast at {datetime.now()}")

def maybe_start_first():
    """アプリ起動時、liveがなければ最初のqueuedをliveにする。"""
    db = load_db()
    has_live = any(item["status"] == "live" for item in db["queue"])
    if has_live:
        # 既存のliveのendでジョブを登録
        for item in db["queue"]:
            if item["status"] == "live":
                end = datetime.fromisoformat(item["scheduled_end"])
                if end > datetime.now():
                    scheduler.add_job(
                        advance_broadcast,
                        "date",
                        run_date=end,
                        id="next_advance",
                        replace_existing=True,
                    )
        return

    for item in db["queue"]:
        if item["status"] == "queued":
            now = datetime.now()
            end = now + timedelta(hours=item["duration_hours"])
            item["status"] = "live"
            item["scheduled_start"] = now.isoformat()
            item["scheduled_end"]   = end.isoformat()
            save_db(db)
            scheduler.add_job(
                advance_broadcast,
                "date",
                run_date=end,
                id="next_advance",
                replace_existing=True,
            )
            return

@app.on_event("startup")
async def startup():
    scheduler.start()
    maybe_start_first()

@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()

# ── API Endpoints ──────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(html)

# ---- 配信状況 ---------------------------------------------------------------

@app.get("/api/status")
async def get_status():
    """現在の配信状況とキューを返す"""
    db = load_db()
    now = datetime.now()

    current = None
    for item in db["queue"]:
        if item["status"] == "live":
            current = item
            break

    queue_items = [
        {**item, "remaining_seconds": max(0, (
            datetime.fromisoformat(item["scheduled_end"]) - now
        ).total_seconds()) if item["status"] == "live" else None}
        for item in db["queue"]
        if item["status"] != "done"
    ]

    return {
        "is_live": current is not None,
        "current": current,
        "queue": queue_items,
        "server_time": now.isoformat(),
    }

# ---- 動画アップロード・予約 -------------------------------------------------

@app.post("/api/upload")
async def upload_video(
    file: UploadFile = File(...),
    title: str = Form(...),
    broadcaster: str = Form(...),
    duration_hours: float = Form(...),
):
    """動画をアップロードしてキューに追加する"""
    # ファイル保存
    ext = Path(file.filename).suffix.lower()
    if ext not in (".mp4", ".webm", ".mov", ".mkv"):
        raise HTTPException(400, "対応フォーマット: mp4, webm, mov, mkv")

    file_id   = str(uuid.uuid4())
    save_name = f"{file_id}{ext}"
    save_path = UPLOAD_DIR / save_name

    async with aiofiles.open(save_path, "wb") as f:
        while chunk := await file.read(1024 * 1024):  # 1MB chunks
            await f.write(chunk)

    # スケジュール計算
    db = load_db()
    now = datetime.now()

    # queuedの最後の終了時刻 or liveの終了時刻の後
    cursor = now
    for item in reversed(db["queue"]):
        if item["status"] in ("queued", "live"):
            cursor = datetime.fromisoformat(item["scheduled_end"])
            break

    start = cursor
    end   = start + timedelta(hours=duration_hours)

    entry = {
        "id": file_id,
        "filename": file.filename,
        "path": f"uploads/{save_name}",
        "title": title,
        "broadcaster": broadcaster,
        "duration_hours": duration_hours,
        "scheduled_start": start.isoformat(),
        "scheduled_end": end.isoformat(),
        "status": "queued",
    }

    db["queue"].append(entry)
    save_db(db)

    # もし今liveがなければ即開始
    has_live = any(i["status"] == "live" for i in db["queue"])
    if not has_live:
        maybe_start_first()

    return {
        "id": file_id,
        "scheduled_start": start.isoformat(),
        "scheduled_end": end.isoformat(),
        "message": "予約完了",
    }

# ---- 動画ストリーミング -----------------------------------------------------

@app.get("/api/stream/{file_id}")
async def stream_video(file_id: str, request: Request):
    """HTTP Range対応のビデオストリーミング"""
    db = load_db()

    # file_idに対応するエントリを探す
    entry = next((item for item in db["queue"] if item["id"] == file_id), None)
    if not entry:
        raise HTTPException(404, "動画が見つかりません")

    video_path = BASE_DIR / entry["path"]
    if not video_path.exists():
        raise HTTPException(404, "ファイルが見つかりません")

    file_size = video_path.stat().st_size
    ext = video_path.suffix.lower()
    content_type = {
        ".mp4":  "video/mp4",
        ".webm": "video/webm",
        ".mov":  "video/quicktime",
        ".mkv":  "video/x-matroska",
    }.get(ext, "video/mp4")

    # Range ヘッダー処理
    range_header = request.headers.get("range")
    if range_header:
        range_start, range_end = range_header.replace("bytes=", "").split("-")
        range_start = int(range_start)
        range_end   = int(range_end) if range_end else file_size - 1
        chunk_size  = range_end - range_start + 1

        async def file_iterator():
            async with aiofiles.open(video_path, "rb") as f:
                await f.seek(range_start)
                remaining = chunk_size
                while remaining > 0:
                    read_size = min(65536, remaining)
                    data = await f.read(read_size)
                    if not data:
                        break
                    remaining -= len(data)
                    yield data

        return StreamingResponse(
            file_iterator(),
            status_code=206,
            headers={
                "Content-Range": f"bytes {range_start}-{range_end}/{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": str(chunk_size),
                "Content-Type": content_type,
            },
        )

    # Rangeなし
    async def full_iterator():
        async with aiofiles.open(video_path, "rb") as f:
            while chunk := await f.read(65536):
                yield chunk

    return StreamingResponse(
        full_iterator(),
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(file_size),
            "Content-Type": content_type,
        },
    )

# ---- 管理: キュー削除 -------------------------------------------------------

@app.delete("/api/queue/{file_id}")
async def delete_from_queue(file_id: str):
    db = load_db()
    original = len(db["queue"])
    db["queue"] = [i for i in db["queue"] if i["id"] != file_id or i["status"] == "live"]
    if len(db["queue"]) == original:
        raise HTTPException(404, "見つからないか配信中のため削除できません")
    recalc_schedule(db)
    save_db(db)
    return {"message": "削除しました"}

# ---- 管理: 今すぐスキップ ---------------------------------------------------

@app.post("/api/skip")
async def skip_current():
    """現在の配信を強制スキップ"""
    advance_broadcast()
    return {"message": "スキップしました"}
