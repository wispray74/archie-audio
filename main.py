import os
import shutil
import asyncio
import tempfile
import subprocess
import time
import mimetypes
from collections import defaultdict
from urllib.parse import urlparse
from fastapi import FastAPI, UploadFile, File, HTTPException, Header, Request
from fastapi.responses import Response
from pydantic import BaseModel

app = FastAPI(title="Archie Audio Service")

SERVICE_SECRET   = os.environ.get("SERVICE_SECRET", "")
FFMPEG_THREADS   = int(os.environ.get("FFMPEG_THREADS", "2"))
COOKIES_FILE     = os.environ.get("COOKIES_FILE", "")
MAX_CONCURRENT   = int(os.environ.get("MAX_CONCURRENT", "2"))

RATE_LIMIT_WINDOW   = int(os.environ.get("RATE_LIMIT_WINDOW", "60"))
RATE_LIMIT_MAX      = int(os.environ.get("RATE_LIMIT_MAX", "10"))

_semaphore = asyncio.Semaphore(MAX_CONCURRENT)

_rate_store: dict[str, list[float]] = defaultdict(list)
_rate_lock = asyncio.Lock()

ALLOWED_DOMAINS = {
    "youtube.com", "www.youtube.com", "youtu.be",
    "music.youtube.com",
    "soundcloud.com", "www.soundcloud.com",
}

ALLOWED_MIME_TYPES = {
    "audio/mpeg", "audio/mp3", "audio/ogg", "audio/wav",
    "audio/x-wav", "audio/wave", "audio/x-m4a", "audio/mp4",
    "audio/aac", "audio/flac", "audio/x-flac",
    "video/mp4", "video/webm",
}

ALLOWED_EXTENSIONS = {
    ".mp3", ".ogg", ".wav", ".m4a", ".aac", ".flac", ".mp4", ".webm"
}

AUDIO_FILTERS = ",".join([
    "highpass=f=30",
    "silenceremove=start_periods=1:start_silence=0.1:start_threshold=-60dB",
    "asetrate=88200,aresample=44100",
    "loudnorm=I=-14:TP=-1:LRA=11",
    "apad=pad_dur=0.5",
])
FFMPEG_ARGS = ["-codec:a", "libmp3lame", "-q:a", "2", "-ar", "44100"]

ROBLOX_MAX_DURATION = 415
MAX_FILE_SIZE       = 150 * 1024 * 1024
CHUNK_SIZE          = 1024 * 1024


def check_auth(x_service_key: str = ""):
    if not SERVICE_SECRET:
        raise HTTPException(status_code=500, detail="Service not configured — SERVICE_SECRET belum di-set")
    if x_service_key != SERVICE_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden — invalid service key")


async def check_rate_limit(request: Request):
    client_ip = request.client.host if request.client else "unknown"
    now = time.time()
    async with _rate_lock:
        timestamps = _rate_store[client_ip]
        cutoff = now - RATE_LIMIT_WINDOW
        _rate_store[client_ip] = [t for t in timestamps if t > cutoff]
        if len(_rate_store[client_ip]) >= RATE_LIMIT_MAX:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded — max {RATE_LIMIT_MAX} requests per {RATE_LIMIT_WINDOW}s"
            )
        _rate_store[client_ip].append(now)


def validate_file_type(filename: str, content_type: str):
    ext = os.path.splitext(filename or "")[1].lower()
    if ext and ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"Ekstensi tidak diizinkan: {ext}")
    mime = (content_type or "").split(";")[0].strip().lower()
    if mime and mime not in ALLOWED_MIME_TYPES and mime != "application/octet-stream":
        raise HTTPException(status_code=400, detail=f"Tipe file tidak diizinkan: {mime}")


def validate_url(url: str):
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
    except Exception:
        raise HTTPException(status_code=400, detail="URL tidak valid")
    domain_stripped = domain.replace("www.", "")
    if domain not in ALLOWED_DOMAINS and domain_stripped not in ALLOWED_DOMAINS:
        raise HTTPException(status_code=400, detail=f"Domain tidak diizinkan: {domain}")


def get_duration(path: str) -> float:
    result = subprocess.run(
        ["ffprobe", "-v", "error",
         "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1",
         path],
        capture_output=True, text=True
    )
    try:
        return float(result.stdout.strip())
    except Exception:
        return 0.0


def validate_duration(input_path: str):
    duration = get_duration(input_path)
    if duration <= 0:
        return
    estimated = duration / 2.0
    if estimated > ROBLOX_MAX_DURATION:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Lagu terlalu panjang ({duration:.0f}s / {duration/60:.1f} menit). "
                f"Estimasi setelah diproses: {estimated:.0f}s ({estimated/60:.1f} menit), "
                f"melebihi limit Roblox 7 menit. "
                f"Maksimal input: {ROBLOX_MAX_DURATION * 2:.0f}s ({ROBLOX_MAX_DURATION * 2 / 60:.1f} menit)."
            )
        )


def run_ffmpeg(input_path: str, output_path: str, timeout: int = 300):
    cmd = [
        "ffmpeg", "-y",
        "-threads", str(FFMPEG_THREADS),
        "-i", input_path,
        "-af", AUDIO_FILTERS,
        *FFMPEG_ARGS,
        output_path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail=f"FFmpeg error: {result.stderr[-500:]}")
    if not os.path.exists(output_path) or os.path.getsize(output_path) < 1000:
        raise HTTPException(status_code=500, detail="Output file terlalu kecil atau tidak terbuat")


def build_ydl_opts(output_template: str) -> dict:
    opts = {
        "format": "bestaudio[ext=m4a]/bestaudio/best",
        "outtmpl": output_template,
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }],
        "quiet": False,
        "no_warnings": False,
        "socket_timeout": 30,
        "extractor_args": {
            "youtube": {
                "player_client": ["ios", "web_creator", "android"],
            }
        },
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        },
    }
    if COOKIES_FILE and os.path.exists(COOKIES_FILE):
        opts["cookiefile"] = COOKIES_FILE
    return opts


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "archie-audio-service",
        "ffmpeg_threads": FFMPEG_THREADS,
        "max_concurrent": MAX_CONCURRENT,
        "cookies_loaded": bool(COOKIES_FILE and os.path.exists(COOKIES_FILE)),
        "max_input_duration_minutes": round(ROBLOX_MAX_DURATION * 2.0 / 60, 1),
        "speed_factor": 2.0,
        "rate_limit": f"{RATE_LIMIT_MAX} req/{RATE_LIMIT_WINDOW}s per IP",
    }


@app.post("/process")
async def process_audio(
    request: Request,
    file: UploadFile = File(...),
    x_service_key: str = Header(default="")
):
    check_auth(x_service_key)
    await check_rate_limit(request)

    validate_file_type(file.filename or "", file.content_type or "")

    suffix = os.path.splitext(file.filename or "audio")[1].lower() or ".mp3"

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        input_path = tmp.name
        total_size = 0
        while True:
            chunk = await file.read(CHUNK_SIZE)
            if not chunk:
                break
            total_size += len(chunk)
            if total_size > MAX_FILE_SIZE:
                tmp.close()
                os.unlink(input_path)
                raise HTTPException(status_code=400, detail="File terlalu besar (max 150MB)")
            tmp.write(chunk)

    output_path = input_path + "_out.mp3"
    try:
        validate_duration(input_path)
        run_ffmpeg(input_path, output_path)
        with open(output_path, "rb") as f:
            content = f.read()
        return Response(
            content=content,
            media_type="audio/mpeg",
            headers={"Content-Disposition": "attachment; filename=processed.mp3"}
        )
    finally:
        for p in [input_path, output_path]:
            if os.path.exists(p):
                try:
                    os.unlink(p)
                except Exception:
                    pass


class DownloadRequest(BaseModel):
    url: str


@app.post("/download")
async def download_and_process(
    request: Request,
    req: DownloadRequest,
    x_service_key: str = Header(default="")
):
    check_auth(x_service_key)
    await check_rate_limit(request)
    validate_url(req.url)

    async with _semaphore:
        import yt_dlp

        dl_dir = tempfile.mkdtemp()
        try:
            output_template = os.path.join(dl_dir, "audio.%(ext)s")
            ydl_opts = build_ydl_opts(output_template)

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(req.url, download=True)
                if info is None:
                    raise HTTPException(status_code=400, detail="Tidak bisa mengambil info dari URL")

            downloaded = next(
                (os.path.join(dl_dir, f) for f in os.listdir(dl_dir) if f.endswith(".mp3")),
                None
            )
            if not downloaded:
                raise HTTPException(status_code=500, detail="File hasil download tidak ditemukan")
            if os.path.getsize(downloaded) < 1000:
                raise HTTPException(status_code=500, detail="File terlalu kecil, kemungkinan download gagal")
            if os.path.getsize(downloaded) > MAX_FILE_SIZE:
                raise HTTPException(status_code=400, detail="File terlalu besar (max 150MB)")

            validate_duration(downloaded)

            output_path = downloaded + "_out.mp3"
            run_ffmpeg(downloaded, output_path, timeout=300)

            with open(output_path, "rb") as f:
                content = f.read()

            return Response(
                content=content,
                media_type="audio/mpeg",
                headers={"Content-Disposition": "attachment; filename=processed.mp3"}
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Download error: {str(e)[:400]}")
        finally:
            shutil.rmtree(dl_dir, ignore_errors=True)


if __name__ == "__main__":
    import uvicorn
    port    = int(os.environ.get("PORT", 8000))
    workers = int(os.environ.get("WEB_CONCURRENCY", 4))
    uvicorn.run("main:app", host="0.0.0.0", port=port, workers=workers)
