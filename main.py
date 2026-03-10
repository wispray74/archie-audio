import os
import shutil
import tempfile
import subprocess
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel

app = FastAPI(title="Archie Audio Service")

# asetrate=88200  →  44100 × 2.0 = 88200
#   efek: pitch naik 2.0x (1 oktaf penuh) + speed naik 2.0x sekaligus
# aresample=44100 →  kembalikan sample rate ke 44100
#   efek: pitch tetap naik 2.0x, speed tetap naik 2.0x, durasi = asli / 2
#
# Keunggulan vs 1.5x:
#   - Pitch naik 1 oktaf penuh → lebih susah dideteksi Roblox
#   - Mendukung lagu hingga ~14 menit (vs 10.5 menit di 1.5x)
#   - Kualitas identik karena murni sample rate manipulation (tidak ada WSOLA)
#
# Di Roblox: PlaybackSpeed = 1/2.0 = 0.5 (nilai bersih, tepat setengah)
#   speed : 2.0x / 2.0 = 1.0x normal
#   pitch : naik 2.0x * turun 0.5 = 1.0x normal
#   → tidak perlu PitchShiftSoundEffect di Roblox sama sekali
AUDIO_FILTERS = ",".join([
    "highpass=f=30",                                                           # hapus frekuensi <30Hz yang tidak terdengar tapi memakan bits
    "silenceremove=start_periods=1:start_silence=0.1:start_threshold=-60dB",  # trim silence di awal
    "asetrate=88200,aresample=44100",                                          # pitch+speed 2.0x (1 oktaf penuh, murni sample rate)
    "loudnorm=I=-14:TP=-1:LRA=11",                                             # normalize ke -14 LUFS standar streaming
    "apad=pad_dur=0.5",                                                        # padding 0.5 detik di akhir, cegah terpotong tiba-tiba
])

# VBR -q:a 2 (~190kbps rata-rata), distribusi bit lebih cerdas dari CBR
FFMPEG_ARGS = ["-codec:a", "libmp3lame", "-q:a", "2", "-ar", "44100"]

FFMPEG_THREADS = int(os.environ.get("FFMPEG_THREADS", "2"))

# Limit Roblox 7 menit = 420 detik, pakai 415 sebagai safety margin
ROBLOX_MAX_DURATION = 415


def get_duration(path: str) -> float:
    """Ambil durasi file audio dalam detik menggunakan ffprobe."""
    result = subprocess.run(
        [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            path
        ],
        capture_output=True, text=True
    )
    try:
        return float(result.stdout.strip())
    except Exception:
        return 0.0


def run_ffmpeg(input_path: str, output_path: str, timeout: int = 300) -> None:
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
        raise HTTPException(status_code=500, detail="Output file terlalu kecil atau tidak ada")


def validate_duration(input_path: str) -> None:
    """
    Cek durasi file sebelum diproses.
    Durasi hasil = durasi asli / 2.0 (karena speed 2.0x).
    Tolak jika hasil estimasi melebihi limit Roblox.
    """
    duration = get_duration(input_path)
    if duration <= 0:
        return  # tidak bisa detect durasi, lanjutkan saja

    estimated_output = duration / 2.0
    if estimated_output > ROBLOX_MAX_DURATION:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Lagu terlalu panjang ({duration:.0f}s / {duration/60:.1f} menit). "
                f"Estimasi hasil setelah diproses: {estimated_output:.0f}s ({estimated_output/60:.1f} menit), "
                f"melebihi limit Roblox 7 menit. "
                f"Maksimal durasi lagu yang bisa diproses: {ROBLOX_MAX_DURATION * 2.0:.0f}s "
                f"({ROBLOX_MAX_DURATION * 2.0 / 60:.1f} menit)."
            )
        )


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "audio-processor",
        "ffmpeg_threads": FFMPEG_THREADS,
        "max_input_duration_minutes": round(ROBLOX_MAX_DURATION * 2.0 / 60, 1),
        "bitrate": "VBR -q:a 2 (~190kbps)",
        "speed_factor": 2.0,
    }


@app.post("/process")
async def process_audio(file: UploadFile = File(...)):
    suffix = os.path.splitext(file.filename)[1] or ".mp3"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_in:
        tmp_in.write(await file.read())
        input_path = tmp_in.name

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
                os.unlink(p)


class DownloadRequest(BaseModel):
    url: str


@app.post("/download")
async def download_and_process(req: DownloadRequest):
    import yt_dlp

    dl_dir = tempfile.mkdtemp()
    try:
        output_template = os.path.join(dl_dir, "audio.%(ext)s")
        ydl_opts = {
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
            "http_headers": {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            },
            "extractor_args": {
                "youtube": {
                    "player_client": ["web", "android"],
                }
            },
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(req.url, download=True)
            if info is None:
                raise HTTPException(status_code=400, detail="Tidak bisa mengambil info dari URL")

        downloaded = next(
            (os.path.join(dl_dir, f) for f in os.listdir(dl_dir) if f.endswith(".mp3")),
            None
        )
        if not downloaded or os.path.getsize(downloaded) < 1000:
            raise HTTPException(status_code=500, detail="Download gagal atau file terlalu kecil")

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
