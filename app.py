#!/usr/bin/env python3
"""
Private Video Capture v2 — yt-dlp downloader for your OWN or licensed content.

Adds on top of v1:
  • Metadata      — yt-dlp --write-info-json + --embed-metadata, indexed in SQLite so you
                    can search your own library later.
  • Aspect ratio  — optional ffmpeg pass to 9:16 / 1:1 / 16:9, center-crop or blurred-pad.
  • Batch audio   — paste many links, pick "Audio only (mp3)".
  • Photo posts   — TikTok/Instagram "photo mode" slideshows are bundled
                    (images + any background audio) into one downloadable zip.
  • Library search — /api/library?q=... over saved metadata.
  • Watermark removal — opt-in, corner-preset ffmpeg delogo pass. For YOUR OWN content
                    only (e.g. cross-posting your own video to another platform without
                    double branding) — not for stripping other creators' marks.

Jobs are queued via Redis/RQ (see worker.py) instead of in-process threads,
so job state survives a web-process restart/redeploy and concurrency is
controlled by how many `worker.py` processes you run (not MAX_CONCURRENT,
which no longer exists — run more worker instances for more concurrency).

Env vars (see README): DOWNLOAD_DIR,
FILE_TTL_MIN (default 60), MAX_URLS_PER_REQUEST (default 10),
DOWNLOAD_TIMEOUT_SEC (default 1800), CONVERT_TIMEOUT_SEC (default 600),
RATE_LIMIT_MAX / RATE_LIMIT_WINDOW_SEC (default 5 per 60s per IP),
MIN_FREE_DISK_MB (default 1024), ALLOWED_DOMAINS (comma-separated
hostnames; empty = allow all), MAX_FILE_SIZE_MB (default 2048),
YTDLP_MAX_RETRIES / YTDLP_RETRY_BACKOFF_SEC (default 2 retries, 5s backoff),
REDIS_HOST / REDIS_PORT / REDIS_DB (default localhost:6379/0),
RQ_QUEUE_NAME (default video-downloader), RQ_JOB_TIMEOUT_SEC.
Needs on the server: python3, ffmpeg, yt-dlp, redis-server, and (optional,
for photo/gallery posts yt-dlp can't parse) gallery-dl.
"""
import os, re, json, time, uuid, shutil, socket, ipaddress, sqlite3, threading, subprocess
from flask import (Flask, request, jsonify,
                   send_file, render_template_string, abort)
import redis as redis_lib
from rq import Queue

DOWNLOAD_DIR        = os.environ.get("DOWNLOAD_DIR", "/var/lib/vidcapture")
FILE_TTL_MIN        = int(os.environ.get("FILE_TTL_MIN", "60"))
def _find_binary(name):
    """shutil.which() alone can miss binaries installed only in the venv if
    the caller's PATH doesn't include venv/bin (e.g. a systemd unit that
    invokes python3 by absolute path without setting Environment=PATH=...).
    Fall back to checking venv/bin directly, relative to this file."""
    found = shutil.which(name)
    if found:
        return found
    venv_candidate = os.path.join(os.path.dirname(os.path.abspath(__file__)), "venv", "bin", name)
    return venv_candidate if os.path.exists(venv_candidate) else None

YTDLP               = _find_binary("yt-dlp") or "yt-dlp"
FFMPEG              = _find_binary("ffmpeg") or "ffmpeg"
FFPROBE             = _find_binary("ffprobe") or "ffprobe"
GALLERYDL           = _find_binary("gallery-dl")  # optional: fallback for photo/gallery posts yt-dlp can't parse
GALLERYDL_TIMEOUT_SEC = int(os.environ.get("GALLERYDL_TIMEOUT_SEC", "300"))
WATERMARK_TIMEOUT_SEC = int(os.environ.get("WATERMARK_TIMEOUT_SEC", "300"))
MAX_URLS_PER_REQUEST = int(os.environ.get("MAX_URLS_PER_REQUEST", "10"))
DOWNLOAD_TIMEOUT_SEC = int(os.environ.get("DOWNLOAD_TIMEOUT_SEC", "1800"))
CONVERT_TIMEOUT_SEC  = int(os.environ.get("CONVERT_TIMEOUT_SEC", "600"))
RATE_LIMIT_MAX       = int(os.environ.get("RATE_LIMIT_MAX", "5"))
RATE_LIMIT_WINDOW_SEC = int(os.environ.get("RATE_LIMIT_WINDOW_SEC", "60"))
MIN_FREE_DISK_MB    = int(os.environ.get("MIN_FREE_DISK_MB", "1024"))
ALLOWED_DOMAINS     = [d.strip().lower() for d in os.environ.get("ALLOWED_DOMAINS", "").split(",") if d.strip()]
MAX_FILE_SIZE_MB    = int(os.environ.get("MAX_FILE_SIZE_MB", "2048"))
YTDLP_MAX_RETRIES   = int(os.environ.get("YTDLP_MAX_RETRIES", "2"))
YTDLP_RETRY_BACKOFF_SEC = int(os.environ.get("YTDLP_RETRY_BACKOFF_SEC", "5"))
PREVIEW_TIMEOUT_SEC = int(os.environ.get("PREVIEW_TIMEOUT_SEC", "20"))
REDIS_HOST          = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT          = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_DB            = int(os.environ.get("REDIS_DB", "0"))
RQ_QUEUE_NAME       = os.environ.get("RQ_QUEUE_NAME", "video-downloader")
RQ_JOB_TIMEOUT_SEC  = int(os.environ.get("RQ_JOB_TIMEOUT_SEC",
                          str(DOWNLOAD_TIMEOUT_SEC + CONVERT_TIMEOUT_SEC + WATERMARK_TIMEOUT_SEC + 300)))

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 1 * 1024 * 1024  # 1MB: plenty for a list of URLs
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
DB = os.path.join(DOWNLOAD_DIR, "library.db")

redis_conn = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
# RQ stores pickled binary job payloads internally, so it needs its own
# connection WITHOUT decode_responses (which would break on non-UTF-8 bytes).
rq_redis_conn = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=False)
job_queue = Queue(RQ_QUEUE_NAME, connection=rq_redis_conn)
JOB_KEY_PREFIX = "vidcapture:job:"
JOB_INDEX_KEY = "vidcapture:jobs:index"

_rate_lock = threading.Lock()
_rate_hits = {}

FORMATS = {
    "1080": "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
    "720":  "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
    "best": "bestvideo+bestaudio/best",
    "audio": "bestaudio/best",
}
CANVAS = {"9x16": (720, 1280), "1x1": (720, 720), "16x9": (1280, 720)}

# Watermark removal is for YOUR OWN content only (e.g. stripping a platform
# watermark before cross-posting your own video elsewhere). Regions are
# fractions (0-1) of frame width/height so they scale to any resolution.
WATERMARK_PRESETS = {
    "bl": {"x": 0.02, "y": 0.80, "w": 0.22, "h": 0.16},
    "br": {"x": 0.76, "y": 0.80, "w": 0.22, "h": 0.16},
    "tl": {"x": 0.02, "y": 0.04, "w": 0.22, "h": 0.16},
    "tr": {"x": 0.76, "y": 0.04, "w": 0.22, "h": 0.16},
}

# TikTok/Instagram "photo mode" posts are a slideshow of images (+ optional
# background audio) rather than a single video stream.
IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp")
VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".mov", ".m4v")
AUDIO_EXTS = (".mp3", ".m4a", ".aac", ".opus", ".ogg", ".wav")
SKIP_EXTS = (".part", ".info.json", ".srt", ".vtt")

def classify_outdir(outdir):
    files = os.listdir(outdir)
    content = [f for f in files if not f.endswith(SKIP_EXTS)]
    images = sorted(f for f in content if f.lower().endswith(IMAGE_EXTS))
    videos = [f for f in content if f.lower().endswith(VIDEO_EXTS)]
    audios = [f for f in content if f.lower().endswith(AUDIO_EXTS)]
    return images, videos, audios

def try_gallerydl_fallback(url, outdir):
    """Best-effort fallback for photo/gallery posts yt-dlp's TikTok extractor
    doesn't recognize (e.g. /photo/ slideshow URLs). gallery-dl nests output
    in extractor/user subdirectories; flatten anything it produced into
    outdir directly so classify_outdir() finds it."""
    if not GALLERYDL:
        return False
    cmd = [GALLERYDL, "-o", "base-directory=%s" % outdir, url]
    try:
        proc = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                              timeout=GALLERYDL_TIMEOUT_SEC)
    except (subprocess.TimeoutExpired, OSError):
        return False
    if proc.returncode != 0:
        return False
    moved = False
    for root, _dirs, files in os.walk(outdir):
        if root == outdir:
            continue
        for f in files:
            src = os.path.join(root, f)
            dst = os.path.join(outdir, f)
            if os.path.exists(dst):
                base, ext = os.path.splitext(f)
                dst = os.path.join(outdir, "%s_%s%s" % (base, uuid.uuid4().hex[:6], ext))
            shutil.move(src, dst)
            moved = True
    for root, _dirs, _files in os.walk(outdir, topdown=False):
        if root != outdir and not os.listdir(root):
            os.rmdir(root)
    return moved

# ── db ──────────────────────────────────────────────────────────────────────
def db():
    c = sqlite3.connect(DB, timeout=30)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    with db() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS library(
            id TEXT PRIMARY KEY, url TEXT, title TEXT, uploader TEXT, upload_date TEXT,
            duration REAL, view_count INTEGER, like_count INTEGER, tags TEXT,
            filename TEXT, size INTEGER, created REAL)""")
init_db()

# ── helpers ──────────────────────────────────────────────────────────────────
def client_ip():
    return request.headers.get("X-Real-IP") or request.remote_addr or "unknown"

def rate_limited(ip):
    now = time.time()
    with _rate_lock:
        hits = _rate_hits.setdefault(ip, [])
        hits[:] = [t for t in hits if now - t < RATE_LIMIT_WINDOW_SEC]
        if len(hits) >= RATE_LIMIT_MAX:
            return True
        hits.append(now)
        return False

def has_disk_space():
    try:
        return shutil.disk_usage(DOWNLOAD_DIR).free >= MIN_FREE_DISK_MB * 1024 * 1024
    except OSError:
        return True

def domain_allowed(url):
    if not ALLOWED_DOMAINS:
        return True
    m = re.match(r"^https?://([^/:?#]+)", url)
    if not m:
        return False
    host = m.group(1).lower()
    return any(host == d or host.endswith("." + d) for d in ALLOWED_DOMAINS)

def is_safe_url(url):
    m = re.match(r"^https?://([^/:?#]+)", url)
    if not m:
        return False
    host = m.group(1)
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        return False
    for info in infos:
        try:
            addr = ipaddress.ip_address(info[4][0])
        except ValueError:
            continue
        if (addr.is_private or addr.is_loopback or addr.is_link_local
                or addr.is_reserved or addr.is_multicast or addr.is_unspecified):
            return False
    return True

# ── job store (Redis-backed; survives web-process restarts) ─────────────────
_JOB_JSON_FIELDS = {"meta", "subs", "photos"}
_JOB_FLOAT_FIELDS = {"progress", "created"}
_JOB_INT_FIELDS = {"size"}
_JOB_BOOL_FIELDS = {"captions", "watermark"}

def _encode_field(key, value):
    if value is None:
        return ""
    if key in _JOB_JSON_FIELDS:
        return json.dumps(value)
    if key in _JOB_BOOL_FIELDS:
        return "1" if value else "0"
    return str(value)

def _decode_field(key, raw):
    if raw is None or raw == "":
        if key in _JOB_JSON_FIELDS:
            return {} if key == "meta" else []
        return None
    if key in _JOB_JSON_FIELDS:
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return {} if key == "meta" else []
    if key in _JOB_FLOAT_FIELDS:
        try:
            return float(raw)
        except ValueError:
            return 0.0
    if key in _JOB_INT_FIELDS:
        try:
            return int(raw)
        except ValueError:
            return 0
    if key in _JOB_BOOL_FIELDS:
        return raw == "1"
    return raw

class RedisJob:
    """Dict-like view over a job's Redis hash, read/written field-by-field."""
    __slots__ = ("id",)

    def __init__(self, jid):
        self.id = jid

    def _key(self):
        return JOB_KEY_PREFIX + self.id

    def __getitem__(self, key):
        return _decode_field(key, redis_conn.hget(self._key(), key))

    def get(self, key, default=None):
        raw = redis_conn.hget(self._key(), key)
        if raw is None:
            return default
        return _decode_field(key, raw)

    def __setitem__(self, key, value):
        redis_conn.hset(self._key(), key, _encode_field(key, value))

    def exists(self):
        return redis_conn.exists(self._key()) == 1

def create_job(jid, **fields):
    key = JOB_KEY_PREFIX + jid
    mapping = {k: _encode_field(k, v) for k, v in fields.items()}
    redis_conn.hset(key, mapping=mapping)
    redis_conn.zadd(JOB_INDEX_KEY, {jid: fields.get("created") or time.time()})

def get_job_dict(jid):
    raw = redis_conn.hgetall(JOB_KEY_PREFIX + jid)
    if not raw:
        return None
    return {k: _decode_field(k, v) for k, v in raw.items()}

def list_job_ids(limit=10000):
    return redis_conn.zrevrange(JOB_INDEX_KEY, 0, limit - 1)

def delete_job_record(jid):
    redis_conn.delete(JOB_KEY_PREFIX + jid)
    redis_conn.zrem(JOB_INDEX_KEY, jid)

def load_meta(outdir):
    for f in os.listdir(outdir):
        if f.endswith(".info.json"):
            try:
                with open(os.path.join(outdir, f), encoding="utf-8") as fh:
                    d = json.load(fh)
                desc = d.get("description") or ""
                tag_field = d.get("tags") or []
                combined, seen = [], set()
                for t in re.findall(r"#(\w+)", desc) + list(tag_field):
                    t = str(t).lstrip("#").strip()
                    if t and t.lower() not in seen:
                        seen.add(t.lower())
                        combined.append(t)
                return {
                    "title": d.get("title"),
                    "uploader": d.get("uploader") or d.get("channel"),
                    "upload_date": d.get("upload_date"),
                    "duration": d.get("duration"),
                    "view_count": d.get("view_count"),
                    "like_count": d.get("like_count"),
                    "tags": ",".join(str(t) for t in tag_field)[:500],
                    "hashtags": combined[:40],
                    "description": desc[:2000],
                }
            except Exception:
                return {}
    return {}

def convert_aspect(src, dst, ratio, mode):
    W, H = CANVAS[ratio]
    if mode == "crop":
        vf = "scale=%d:%d:force_original_aspect_ratio=increase,crop=%d:%d" % (W, H, W, H)
        cmd = [FFMPEG, "-y", "-i", src, "-vf", vf, "-c:a", "copy",
               "-c:v", "libx264", "-preset", "veryfast", "-crf", "20", "-threads", "0", dst]
    else:  # blurred-pad background — blur a downscaled copy, then scale back up (much cheaper
           # than blurring at full resolution: ~16x fewer pixels, single-pass boxblur)
        sw, sh = max(2, W // 4), max(2, H // 4)
        fc = ("split=2[bg][fg];"
              "[bg]scale=%d:%d:force_original_aspect_ratio=increase,crop=%d:%d,"
              "boxblur=6:1,scale=%d:%d[bgb];"
              "[fg]scale=%d:%d:force_original_aspect_ratio=decrease[fgs];"
              "[bgb][fgs]overlay=(W-w)/2:(H-h)/2" % (sw, sh, sw, sh, W, H, W, H))
        cmd = [FFMPEG, "-y", "-i", src, "-filter_complex", fc, "-map", "0:a?",
               "-c:a", "aac", "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
               "-threads", "0", dst]
    try:
        return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                              timeout=CONVERT_TIMEOUT_SEC).returncode
    except subprocess.TimeoutExpired:
        return -1

def probe_dims(path):
    cmd = [FFPROBE, "-v", "error", "-select_streams", "v:0",
           "-show_entries", "stream=width,height", "-of", "csv=s=x:p=0", path]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout.strip()
        w, h = out.split("x")
        return int(w), int(h)
    except Exception:
        return None

def remove_watermark(src, dst, region):
    dims = probe_dims(src)
    if not dims:
        return False
    W, H = dims
    x = max(0, int(region["x"] * W))
    y = max(0, int(region["y"] * H))
    w = max(2, int(region["w"] * W))
    h = max(2, int(region["h"] * H))
    vf = "delogo=x=%d:y=%d:w=%d:h=%d" % (x, y, w, h)
    cmd = [FFMPEG, "-y", "-i", src, "-vf", vf, "-c:v", "libx264",
           "-preset", "veryfast", "-crf", "20", "-c:a", "copy", dst]
    try:
        return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                              timeout=WATERMARK_TIMEOUT_SEC).returncode == 0
    except subprocess.TimeoutExpired:
        return False

def _run_ytdlp_once(cmd, job):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1)
    timer = threading.Timer(DOWNLOAD_TIMEOUT_SEC, proc.kill)
    timer.start()
    try:
        for line in proc.stdout:
            m = re.search(r"(\d{1,3}(?:\.\d)?)%", line)
            if m:
                try:
                    job["progress"] = min(99.0, float(m.group(1)))
                except ValueError:
                    pass
        proc.wait()
    finally:
        timed_out = not timer.is_alive()
        timer.cancel()
    return proc.returncode, timed_out

def run_job(job_id):
    """RQ task entry point. Concurrency = number of running worker.py processes."""
    job = RedisJob(job_id)
    if not job.exists():
        return
    job["status"] = "downloading"
    outdir = os.path.join(DOWNLOAD_DIR, job_id)
    os.makedirs(outdir, exist_ok=True)
    fmt = FORMATS.get(job["format"], FORMATS["1080"])
    outtmpl = os.path.join(outdir, "%(title).120B%(playlist_index&_{0}|)s.%(ext)s")
    cmd = [YTDLP, "-f", fmt, "-o", outtmpl, "--no-playlist", "--newline",
           "--restrict-filenames", "--no-mtime", "--no-progress",
           "--write-info-json", "--embed-metadata",
           "--max-filesize", "%dM" % MAX_FILE_SIZE_MB]
    if job.get("captions"):
        cmd += ["--write-subs", "--write-auto-subs", "--sub-langs", "all", "--convert-subs", "srt"]
    if job["format"] == "audio":
        cmd += ["--extract-audio", "--audio-format", "mp3"]
    else:
        cmd += ["--merge-output-format", "mp4"]
    cmd.append(job["url"])
    try:
        attempts = YTDLP_MAX_RETRIES + 1
        returncode, timed_out = 1, False
        for attempt in range(attempts):
            job["progress"] = 0.0
            returncode, timed_out = _run_ytdlp_once(cmd, job)
            if returncode == 0 or timed_out:
                break
            if attempt < attempts - 1:
                job["status"] = "retrying"
                time.sleep(YTDLP_RETRY_BACKOFF_SEC)
                job["status"] = "downloading"
        if returncode != 0 and not timed_out:
            # yt-dlp couldn't parse this URL at all (e.g. TikTok/Instagram
            # photo-post slideshows) — gallery-dl has broader gallery support
            job["status"] = "downloading"
            if try_gallerydl_fallback(job["url"], outdir):
                returncode = 0
        if returncode != 0:
            job["status"] = "error"
            job["error"] = ("Download timed out." if timed_out else
                            "Download failed after %d attempt(s). The site may be unsupported, "
                            "the link protected/expired/region-locked, or the file exceeds the "
                            "%dMB size cap." % (attempts, MAX_FILE_SIZE_MB))
            return

        images, videos, audios = classify_outdir(outdir)
        # A stray image alongside a real video is treated as a thumbnail and
        # ignored (see the else branch below). But with no video at all, any
        # image(s) present are the actual content — a photo post can be a
        # single photo, not just multi-image slideshows.
        is_slideshow = bool(images) and not videos

        if is_slideshow:
            # TikTok/Instagram "photo mode" post: no single "video" file to
            # save, so expose each image (and any audio track) as its own
            # download link instead of a zip, same pattern as captions below.
            job["status"] = "packaging"
            job["photos"] = images
            primary = os.path.join(outdir, audios[0]) if audios else None
        else:
            media = videos + audios
            if not media:
                media = [f for f in os.listdir(outdir) if not f.endswith(SKIP_EXTS)]
            if not media:
                job["status"] = "error"
                job["error"] = "No file was produced."
                return
            media.sort(key=lambda f: os.path.getsize(os.path.join(outdir, f)), reverse=True)
            primary = os.path.join(outdir, media[0])

            # optional watermark removal (your own content only; video only)
            wm_pos = job.get("watermark_pos")
            if job.get("watermark") and wm_pos in WATERMARK_PRESETS and job["format"] != "audio":
                job["status"] = "watermarking"
                base = os.path.splitext(media[0])[0]
                dst = os.path.join(outdir, "%s.nowm.mp4" % base)
                if remove_watermark(primary, dst, WATERMARK_PRESETS[wm_pos]) and os.path.exists(dst):
                    primary = dst

            # optional aspect-ratio conversion (video only)
            ratio = job.get("convert")
            if ratio in CANVAS and job["format"] != "audio":
                job["status"] = "converting"
                base = os.path.splitext(media[0])[0]
                dst = os.path.join(outdir, "%s.%s.mp4" % (base, ratio))
                rc = convert_aspect(primary, dst, ratio, job.get("convert_mode", "blur"))
                if rc == 0 and os.path.exists(dst):
                    primary = dst

        job["file"] = primary
        job["filename"] = os.path.basename(primary) if primary else None
        job["size"] = os.path.getsize(primary) if primary else 0
        job["subs"] = sorted(f for f in os.listdir(outdir) if f.endswith((".srt", ".vtt")))
        job["progress"] = 100.0
        job["status"] = "done"

        meta = load_meta(outdir)
        job["meta"] = meta
        try:
            with db() as c:
                c.execute("""INSERT OR REPLACE INTO library
                    (id,url,title,uploader,upload_date,duration,view_count,like_count,tags,filename,size,created)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (job_id, job["url"], meta.get("title"), meta.get("uploader"),
                     meta.get("upload_date"), meta.get("duration"), meta.get("view_count"),
                     meta.get("like_count"), meta.get("tags"), job["filename"],
                     job["size"], job["created"]))
        except Exception:
            pass
    except Exception as e:  # noqa: BLE001
        job["status"] = "error"
        job["error"] = "Server error: " + str(e)

def cleanup_loop():
    while True:
        now = time.time()
        for jid in list_job_ids():
            j = get_job_dict(jid)
            created = j.get("created") if j else None
            if created is None or now - created > FILE_TTL_MIN * 60:
                delete_job_record(jid)
                shutil.rmtree(os.path.join(DOWNLOAD_DIR, jid), ignore_errors=True)
        # sweep orphaned job directories with no matching Redis record
        # (e.g. left behind by a Redis flush or an old in-memory-jobs deploy)
        try:
            for name in os.listdir(DOWNLOAD_DIR):
                path = os.path.join(DOWNLOAD_DIR, name)
                if not os.path.isdir(path) or redis_conn.exists(JOB_KEY_PREFIX + name):
                    continue
                if now - os.path.getmtime(path) > FILE_TTL_MIN * 60:
                    shutil.rmtree(path, ignore_errors=True)
        except OSError:
            pass
        time.sleep(300)

# Only the web process runs cleanup — worker.py sets VIDCAPTURE_WORKER before
# importing this module so N worker processes don't all sweep redundantly.
if not os.environ.get("VIDCAPTURE_WORKER"):
    threading.Thread(target=cleanup_loop, daemon=True).start()

# ── routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template_string(APP_HTML)

@app.post("/api/preview")
def preview():
    if rate_limited(client_ip()):
        return jsonify({"error": "Too many requests. Please slow down and try again shortly."}), 429
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    if not re.match(r"^https?://", url):
        return jsonify({"error": "Invalid URL"}), 400
    if not is_safe_url(url) or not domain_allowed(url):
        return jsonify({"error": "This URL isn't allowed"}), 400
    cmd = [YTDLP, "-j", "--no-playlist", "--skip-download", "--no-warnings", url]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=PREVIEW_TIMEOUT_SEC)
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Preview timed out"}), 504
    except OSError:
        return jsonify({"error": "Preview unavailable"}), 500
    if proc.returncode != 0:
        return jsonify({"error": "Couldn't fetch a preview for this link"}), 422
    try:
        lines = [l for l in proc.stdout.strip().splitlines() if l.strip()]
        info = json.loads(lines[-1]) if lines else {}
    except (ValueError, IndexError):
        return jsonify({"error": "Couldn't parse preview data"}), 422
    return jsonify({
        "title": info.get("title"),
        "thumbnail": info.get("thumbnail"),
        "duration": info.get("duration"),
        "uploader": info.get("uploader") or info.get("channel"),
        "view_count": info.get("view_count"),
    })

@app.post("/api/jobs")
def submit():
    if rate_limited(client_ip()):
        return jsonify({"error": "Too many requests. Please slow down and try again shortly."}), 429
    if not has_disk_space():
        return jsonify({"error": "Server storage is full. Try again later."}), 507
    data = request.get_json(silent=True) or {}
    urls = (data.get("urls") or [])[:MAX_URLS_PER_REQUEST]
    fmt = data.get("format", "1080")
    if fmt not in FORMATS:
        fmt = "1080"
    convert = data.get("convert", "none")
    convert = convert if convert in CANVAS else "none"
    convert_mode = "crop" if data.get("convert_mode") == "crop" else "blur"
    captions = bool(data.get("captions"))
    watermark = bool(data.get("watermark"))
    watermark_pos = data.get("watermark_pos", "bl")
    watermark_pos = watermark_pos if watermark_pos in WATERMARK_PRESETS else "bl"
    created = []
    for u in urls:
        u = (u or "").strip()
        if not re.match(r"^https?://", u):
            continue
        if not is_safe_url(u) or not domain_allowed(u):
            continue
        jid = uuid.uuid4().hex[:12]
        create_job(jid, id=jid, url=u, format=fmt, convert=convert,
                   convert_mode=convert_mode, captions=captions,
                   watermark=watermark, watermark_pos=watermark_pos,
                   status="queued", progress=0.0, file=None,
                   filename=None, error=None, meta={}, subs=[], photos=[],
                   created=time.time())
        job_queue.enqueue(run_job, jid, job_timeout=RQ_JOB_TIMEOUT_SEC)
        created.append(jid)
    return jsonify({"created": created})

@app.get("/api/jobs")
def list_jobs():
    keys = ("id", "url", "status", "progress", "filename", "error", "size", "meta", "subs", "photos")
    out = []
    for jid in list_job_ids():
        j = get_job_dict(jid)
        if j:
            out.append({k: j.get(k) for k in keys})
    return jsonify({"jobs": out})

@app.post("/api/jobs/<jid>/delete")
def delete_job(jid):
    delete_job_record(jid)
    shutil.rmtree(os.path.join(DOWNLOAD_DIR, jid), ignore_errors=True)
    return jsonify({"ok": True})

@app.get("/api/jobs/<jid>/file")
def download(jid):
    j = get_job_dict(jid)
    if not j or j.get("status") != "done" or not j.get("file"):
        abort(404)
    path = os.path.realpath(j["file"])
    if not path.startswith(os.path.realpath(DOWNLOAD_DIR)) or not os.path.exists(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=j["filename"])

@app.get("/api/jobs/<jid>/sub/<int:idx>")
def sub(jid, idx):
    j = get_job_dict(jid)
    if not j:
        abort(404)
    subs = j.get("subs") or []
    if idx < 0 or idx >= len(subs):
        abort(404)
    path = os.path.realpath(os.path.join(DOWNLOAD_DIR, jid, subs[idx]))
    if not path.startswith(os.path.realpath(DOWNLOAD_DIR)) or not os.path.exists(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=subs[idx])

@app.get("/api/jobs/<jid>/photo/<int:idx>")
def photo(jid, idx):
    j = get_job_dict(jid)
    if not j:
        abort(404)
    photos = j.get("photos") or []
    if idx < 0 or idx >= len(photos):
        abort(404)
    path = os.path.realpath(os.path.join(DOWNLOAD_DIR, jid, photos[idx]))
    if not path.startswith(os.path.realpath(DOWNLOAD_DIR)) or not os.path.exists(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=photos[idx])

@app.get("/api/library")
def library():
    q = (request.args.get("q") or "").strip()
    with db() as c:
        if q:
            like = "%" + q + "%"
            rows = c.execute("""SELECT * FROM library
                WHERE title LIKE ? OR uploader LIKE ? OR tags LIKE ?
                ORDER BY created DESC LIMIT 100""", (like, like, like)).fetchall()
        else:
            rows = c.execute("SELECT * FROM library ORDER BY created DESC LIMIT 100").fetchall()
    return jsonify({"items": [dict(r) for r in rows]})

# ── templates ──────────────────────────────────────────────────────────────────
BASE_CSS = """
:root{
 --bg:#0E1524; --fg:#E6EBF5; --accent:#F6A73B; --accent-shadow:rgba(246,167,59,.16);
 --muted:#7C8AA8; --card:#161F33; --border:#2A3853; --border-hover:#3a4a6b;
 --btn-fg:#241605; --done:#35D6A0; --done-fg:#04231a; --error:#F2627E;
 --banner-bg:rgba(246,167,59,.10); --banner-border:rgba(246,167,59,.3); --banner-fg:#F6C77B;
 --link:#7FA8FF; --glow:rgba(91,157,255,.10);
}
:root[data-theme="light"]{
 --bg:#F5F7FB; --fg:#1B2333; --accent:#B9720A; --accent-shadow:rgba(185,114,10,.16);
 --muted:#5B6B8C; --card:#FFFFFF; --border:#DCE3F0; --border-hover:#B9C4D9;
 --btn-fg:#241605; --done:#1E9E73; --done-fg:#eafff5; --error:#C23A54;
 --banner-bg:rgba(185,114,10,.08); --banner-border:rgba(185,114,10,.25); --banner-fg:#7A4A06;
 --link:#2A5FD9; --glow:rgba(91,157,255,.06);
}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;background:var(--bg);color:var(--fg);
 font-family:system-ui,-apple-system,"Segoe UI",Roboto,"Noto Nastaliq Urdu",sans-serif;
 background-image:radial-gradient(1000px 500px at 80% -10%,var(--glow),transparent 60%);
 transition:background-color .2s,color .2s}
.wrap{max-width:880px;margin:0 auto;padding:26px 18px 60px}
.eyebrow{font-size:11px;letter-spacing:.3em;color:var(--accent);font-weight:600}
h1{font-size:26px;font-weight:800;letter-spacing:-.02em;margin:6px 0 2px}
.sub{color:var(--muted);font-size:13px;margin-bottom:20px}
.card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:16px;margin-bottom:14px}
textarea,input,select{background:var(--bg);color:var(--fg);border:1px solid var(--border);
 border-radius:10px;padding:11px 12px;font-size:14px;font-family:inherit}
textarea{width:100%;min-height:92px;resize:vertical;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:13px}
textarea:focus,input:focus,select:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-shadow)}
.controls{display:flex;gap:8px;flex-wrap:wrap;margin-top:11px;align-items:center}
.controls label{font-size:11px;color:var(--muted);margin-right:2px}
.btn{background:var(--accent);color:var(--btn-fg);border:none;font-weight:700;font-size:14px;
 padding:11px 18px;border-radius:10px;cursor:pointer;margin-left:auto}
.btn.ghost{background:transparent;color:var(--fg);border:1px solid var(--border);font-weight:600;margin-left:0}
.btn:disabled{opacity:.5;cursor:not-allowed}
.note{font-size:12px;color:var(--muted);line-height:1.5;margin-top:10px}
.job{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:12px 13px;margin-bottom:9px}
.job .u{font-size:12px;color:var(--muted);font-family:ui-monospace,monospace;word-break:break-all}
.job .t{font-size:13.5px;font-weight:600;margin-bottom:3px;word-break:break-all}
.meta{font-size:11px;color:var(--muted);margin-top:3px}
.bar{height:6px;background:var(--bg);border:1px solid var(--border);border-radius:3px;overflow:hidden;margin:8px 0}
.bar>i{display:block;height:100%;background:var(--accent);width:0;transition:width .3s}
.bar.done>i{background:var(--done);width:100%}
.bar.error>i{background:var(--error);width:100%}
.st{font-size:11px;text-transform:uppercase;letter-spacing:.1em;color:var(--muted)}
.st.done{color:var(--done)}.st.error{color:var(--error)}.st.downloading,.st.converting,.st.retrying,.st.watermarking,.st.packaging{color:var(--accent)}
.err{color:var(--error);font-size:12px;margin-top:4px}
a.dl{display:inline-block;margin-top:8px;background:var(--done);color:var(--done-fg);font-weight:700;
 font-size:13px;padding:8px 14px;border-radius:9px;text-decoration:none}
.top{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;gap:10px}
.toggles{display:flex;gap:8px;flex-shrink:0}
.toggles .btn.ghost{padding:8px 12px;font-size:12px}
.banner{background:var(--banner-bg);border:1px solid var(--banner-border);color:var(--banner-fg);
 font-size:12px;padding:9px 12px;border-radius:10px;margin-bottom:16px;line-height:1.5}
.libitem{border-bottom:1px solid var(--border);padding:8px 0;font-size:13px}
.libitem .m{font-size:11px;color:var(--muted)}
.tags{margin-top:7px;font-size:12px;color:var(--link);line-height:1.6;word-break:break-word}
.subrow{margin-top:7px;font-size:12px;color:var(--muted)}
.subdl{display:inline-block;margin:3px 6px 0 0;background:var(--bg);border:1px solid var(--border);
 color:var(--fg);font-size:12px;padding:5px 10px;border-radius:8px;text-decoration:none;cursor:pointer}
.subdl:hover{border-color:var(--border-hover)}
.preview{display:flex;gap:10px;align-items:center;background:var(--bg);border:1px solid var(--border);
 border-radius:10px;padding:8px 10px;margin-top:8px}
.preview img{width:52px;height:52px;object-fit:cover;border-radius:6px;flex-shrink:0;background:var(--border)}
.preview .pinfo{flex:1;min-width:0}
.preview .ptitle{font-size:12.5px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.preview .pmeta{font-size:11px;color:var(--muted)}
.preview .premove{cursor:pointer;color:var(--muted);font-size:16px;padding:0 4px;flex-shrink:0}
.preview .premove:hover{color:var(--error)}
"""

APP_HTML = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Private Video Capture</title>
<style>""" + BASE_CSS + """</style></head>
<body><div class="wrap">
 <div class="top">
  <div><div class="eyebrow" data-i18n="eyebrow">PRIVATE CAPTURE</div><h1 data-i18n="title">Video Capture</h1></div>
  <div class="toggles">
   <button class="btn ghost" id="theme-toggle" title="Toggle theme">🌙</button>
   <button class="btn ghost" id="lang-toggle">اردو</button>
  </div>
 </div>
 <div class="banner" data-i18n="banner">For content you own or are licensed to download — your own uploads, client or product footage, and public-domain / Creative-Commons material. Watermark removal is for your own content only (e.g. cross-posting your own video to another platform).</div>
 <div class="card">
  <textarea id="urls" data-i18n-ph="urls_ph" placeholder="Paste one or more links, one per line…"></textarea>
  <div id="previews"></div>
  <div class="controls">
   <label data-i18n="quality">Quality</label>
   <select id="fmt">
    <option value="1080" data-i18n="fmt_1080">Up to 1080p</option>
    <option value="720" data-i18n="fmt_720">Up to 720p</option>
    <option value="best" data-i18n="fmt_best">Best</option>
    <option value="audio" data-i18n="fmt_audio">Audio (mp3)</option>
   </select>
   <label data-i18n="reframe">Reframe</label>
   <select id="conv">
    <option value="none" data-i18n="conv_none">Keep original</option>
    <option value="9x16" data-i18n="conv_9x16">9:16 (Reels/TikTok)</option>
    <option value="1x1" data-i18n="conv_1x1">1:1 (Square)</option>
    <option value="16x9" data-i18n="conv_16x9">16:9 (YouTube)</option>
   </select>
   <select id="cmode">
    <option value="blur" data-i18n="cmode_blur">Blurred pad</option>
    <option value="crop" data-i18n="cmode_crop">Center crop</option>
   </select>
   <label style="display:flex;align-items:center;gap:6px;margin-left:2px"><input type="checkbox" id="caps" style="width:auto;accent-color:#F6A73B"> <span data-i18n="captions_label">Captions</span></label>
   <label style="display:flex;align-items:center;gap:6px;margin-left:2px"><input type="checkbox" id="wm" style="width:auto;accent-color:#F6A73B"> <span data-i18n="watermark_label">Remove watermark (own content only)</span></label>
   <select id="wmpos">
    <option value="bl" data-i18n="wmpos_bl">Bottom-left</option>
    <option value="br" data-i18n="wmpos_br">Bottom-right</option>
    <option value="tl" data-i18n="wmpos_tl">Top-left</option>
    <option value="tr" data-i18n="wmpos_tr">Top-right</option>
   </select>
   <button class="btn" id="go" data-i18n="btn_download">Download</button>
  </div>
  <p class="note" data-i18n="note_text">Server downloads, embeds metadata, and (optionally) reframes each file. A Save button appears when it's ready.</p>
  <p class="err" id="submitErr"></p>
 </div>

 <div id="jobs"></div>

 <div class="card">
  <div class="controls" style="margin-top:0">
   <input id="q" data-i18n-ph="lib_ph" placeholder="Search your library (title, uploader, tag)" style="flex:1">
   <button class="btn ghost" id="search" data-i18n="btn_search">Search</button>
  </div>
  <div id="lib" class="note"></div>
 </div>
</div>
<script>
const $=s=>document.querySelector(s);
function esc(s){return (s||'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function fmtSize(b){if(!b)return'';const u=['B','KB','MB','GB'];let i=0,n=b;while(n>=1024&&i<3){n/=1024;i++}return n.toFixed(1)+' '+u[i];}

// ── theme ────────────────────────────────────────────────────────────────
function applyTheme(theme){
  document.documentElement.setAttribute('data-theme',theme);
  localStorage.setItem('theme',theme);
  $('#theme-toggle').textContent=theme==='dark'?'☀️':'🌙';
}
let THEME=localStorage.getItem('theme')||(matchMedia('(prefers-color-scheme: light)').matches?'light':'dark');
applyTheme(THEME);
$('#theme-toggle').addEventListener('click',()=>{THEME=THEME==='dark'?'light':'dark';applyTheme(THEME);});

// ── language ─────────────────────────────────────────────────────────────
const STRINGS={
 en:{eyebrow:'PRIVATE CAPTURE',title:'Video Capture',
  banner:'For content you own or are licensed to download — your own uploads, client or product footage, and public-domain / Creative-Commons material. Watermark removal is for your own content only (e.g. cross-posting your own video to another platform).',
  urls_ph:'Paste one or more links, one per line…',quality:'Quality',fmt_1080:'Up to 1080p',fmt_720:'Up to 720p',fmt_best:'Best',fmt_audio:'Audio (mp3)',
  reframe:'Reframe',conv_none:'Keep original',conv_9x16:'9:16 (Reels/TikTok)',conv_1x1:'1:1 (Square)',conv_16x9:'16:9 (YouTube)',
  cmode_blur:'Blurred pad',cmode_crop:'Center crop',captions_label:'Captions',watermark_label:'Remove watermark (own content only)',
  wmpos_bl:'Bottom-left',wmpos_br:'Bottom-right',wmpos_tl:'Top-left',wmpos_tr:'Top-right',
  btn_download:'Download',note_text:"Server downloads, embeds metadata, and (optionally) reframes each file. A Save button appears when it's ready.",
  lib_ph:'Search your library (title, uploader, tag)',btn_search:'Search',lib_empty:'No saved items yet.',
  save_file:'Save file',photo:'Photo',captions_prefix:'Captions:',photos_prefix:'Photos:',views:'views',
  copy_desc:'Copy description',copied:'Copied ✓',remove:'Remove',fetching:'Fetching…',done_label:'Done',
  preview_loading:'Loading preview…',preview_failed:'No preview available',lang_name:'اردو'},
 ur:{eyebrow:'ذاتی کیپچر',title:'ویڈیو کیپچر',
  banner:'صرف اپنے یا لائسنس یافتہ مواد کے لیے — آپ کی اپنی اپلوڈز، کلائنٹ یا پروڈکٹ فوٹیج، اور پبلک ڈومین / کریئیٹو کامنز مواد۔ واٹر مارک ہٹانا صرف آپ کے اپنے مواد کے لیے ہے۔',
  urls_ph:'ایک یا زیادہ لنکس پیسٹ کریں، ہر لائن میں ایک…',quality:'کوالٹی',fmt_1080:'1080p تک',fmt_720:'720p تک',fmt_best:'بہترین',fmt_audio:'صرف آڈیو (mp3)',
  reframe:'ری فریم',conv_none:'اصل رکھیں',conv_9x16:'9:16 (ریلز/ٹک ٹاک)',conv_1x1:'1:1 (مربع)',conv_16x9:'16:9 (یوٹیوب)',
  cmode_blur:'دھندلا پیڈ',cmode_crop:'مرکزی کراپ',captions_label:'کیپشنز',watermark_label:'واٹر مارک ہٹائیں (صرف اپنا مواد)',
  wmpos_bl:'نیچے بائیں',wmpos_br:'نیچے دائیں',wmpos_tl:'اوپر بائیں',wmpos_tr:'اوپر دائیں',
  btn_download:'ڈاؤن لوڈ',note_text:'سرور ڈاؤن لوڈ کرتا ہے، میٹا ڈیٹا شامل کرتا ہے، اور (اختیاری طور پر) ہر فائل کو ری فریم کرتا ہے۔ تیار ہونے پر سیو بٹن ظاہر ہوگا۔',
  lib_ph:'اپنی لائبریری تلاش کریں (عنوان، اپ لوڈر، ٹیگ)',btn_search:'تلاش کریں',lib_empty:'ابھی تک کوئی محفوظ آئٹم نہیں۔',
  save_file:'فائل محفوظ کریں',photo:'تصویر',captions_prefix:'کیپشنز:',photos_prefix:'تصاویر:',views:'ملاحظات',
  copy_desc:'تفصیل کاپی کریں',copied:'کاپی ہو گیا ✓',remove:'ہٹائیں',fetching:'حاصل ہو رہا ہے…',done_label:'مکمل',
  preview_loading:'پیش منظر لوڈ ہو رہا ہے…',preview_failed:'پیش منظر دستیاب نہیں',lang_name:'English'}
};
let LANG=localStorage.getItem('lang')||'en';
function t(key){return (STRINGS[LANG]&&STRINGS[LANG][key])||STRINGS.en[key]||key;}
function applyLanguage(){
  document.documentElement.setAttribute('lang',LANG==='ur'?'ur':'en');
  document.documentElement.setAttribute('dir',LANG==='ur'?'rtl':'ltr');
  document.querySelectorAll('[data-i18n]').forEach(el=>{el.textContent=t(el.getAttribute('data-i18n'));});
  document.querySelectorAll('[data-i18n-ph]').forEach(el=>{el.placeholder=t(el.getAttribute('data-i18n-ph'));});
  $('#lang-toggle').textContent=t('lang_name');
  render(lastJobs);
  renderPreviews();
}
$('#lang-toggle').addEventListener('click',()=>{LANG=LANG==='en'?'ur':'en';localStorage.setItem('lang',LANG);applyLanguage();});

// ── preview / paste-and-go ───────────────────────────────────────────────
let previewCache={};
let previewTimer=null;
function detectUrls(text){return [...new Set(text.split(/\\s+/).map(s=>s.trim()).filter(u=>/^https?:\\/\\//.test(u)))];}
function removeUrlFromTextarea(url){
  $('#urls').value=$('#urls').value.split(/\\n/).filter(line=>line.trim()!==url).join('\\n');
  delete previewCache[url];
  renderPreviews();
}
function renderPreviews(){
  const urls=detectUrls($('#urls').value);
  const container=$('#previews');
  if(!urls.length){container.innerHTML='';return;}
  container.innerHTML=urls.map(u=>{
    const p=previewCache[u];
    let inner;
    if(!p||p==='loading'){
      inner=`<div class="pinfo"><div class="ptitle">${esc(u)}</div><div class="pmeta">${t('preview_loading')}</div></div>`;
    }else if(p==='error'){
      inner=`<div class="pinfo"><div class="ptitle">${esc(u)}</div><div class="pmeta">${t('preview_failed')}</div></div>`;
    }else{
      const dur=p.duration?Math.floor(p.duration/60)+':'+String(Math.floor(p.duration%60)).padStart(2,'0'):'';
      const meta=[p.uploader,dur].filter(Boolean).join(' · ');
      const thumb=p.thumbnail?`<img src="${esc(p.thumbnail)}" loading="lazy">`:'';
      inner=`${thumb}<div class="pinfo"><div class="ptitle">${esc(p.title||u)}</div><div class="pmeta">${esc(meta)}</div></div>`;
    }
    return `<div class="preview">${inner}<span class="premove" data-url="${esc(u)}">✕</span></div>`;
  }).join('');
  container.querySelectorAll('.premove').forEach(el=>{
    el.addEventListener('click',()=>removeUrlFromTextarea(el.getAttribute('data-url')));
  });
}
async function fetchPreview(url){
  previewCache[url]='loading';
  renderPreviews();
  try{
    const r=await fetch('/api/preview',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})});
    previewCache[url]=r.ok?await r.json():'error';
  }catch(e){previewCache[url]='error';}
  renderPreviews();
}
function onUrlsChanged(){
  clearTimeout(previewTimer);
  previewTimer=setTimeout(()=>{
    const urls=detectUrls($('#urls').value);
    Object.keys(previewCache).forEach(u=>{if(!urls.includes(u))delete previewCache[u];});
    renderPreviews();
    urls.forEach(u=>{if(!(u in previewCache))fetchPreview(u);});
  },500);
}
$('#urls').addEventListener('input',onUrlsChanged);
$('#urls').addEventListener('paste',()=>setTimeout(onUrlsChanged,30));

// ── jobs ─────────────────────────────────────────────────────────────────
let lastJobs=[];
async function submit(){
  const urls=$('#urls').value.split(/\\s+/).map(s=>s.trim()).filter(Boolean);
  if(!urls.length) return;
  $('#go').disabled=true;
  $('#submitErr').textContent='';
  try{
    const r=await fetch('/api/jobs',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({urls,format:$('#fmt').value,convert:$('#conv').value,convert_mode:$('#cmode').value,captions:$('#caps').checked,watermark:$('#wm').checked,watermark_pos:$('#wmpos').value})});
    if(!r.ok){
      const d=await r.json().catch(()=>({}));
      $('#submitErr').textContent=d.error||'Something went wrong. Please try again.';
      return;
    }
    $('#urls').value='';
    previewCache={};
    renderPreviews();
  }finally{$('#go').disabled=false;}
  poll();
}
function metaLine(m){if(!m)return'';const p=[];if(m.uploader)p.push(esc(m.uploader));
  if(m.view_count)p.push(m.view_count.toLocaleString()+' '+t('views'));
  if(m.upload_date)p.push(m.upload_date);return p.join(' · ');}
function render(list){
  $('#jobs').innerHTML=list.map(j=>{
    const cls=j.status==='done'?'done':j.status==='error'?'error':'';
    const pct=Math.round(j.progress||0);
    const title=j.filename?esc(j.filename):(j.status==='done'?(j.photos&&j.photos.length?j.photos.length+' '+t('photo')+'(s)':t('done_label')):t('fetching'));
    return `<div class="job">
      <div class="t">${title}</div>
      <div class="u">${esc(j.url||'')}</div>
      ${metaLine(j.meta)?`<div class="meta">${metaLine(j.meta)}</div>`:''}
      <div class="bar ${cls}"><i style="width:${pct}%"></i></div>
      <div class="controls" style="justify-content:space-between;margin-top:2px">
        <span class="st ${j.status}">${j.status}${(j.status==='downloading'||j.status==='queued')?' · '+pct+'%':''}${j.size?' · '+fmtSize(j.size):''}</span>
        <button class="btn ghost" style="padding:5px 10px;font-size:12px;margin-left:0" onclick="del('${j.id}')">${t('remove')}</button>
      </div>
      ${j.error?`<div class="err">${esc(j.error)}</div>`:''}
      ${j.status==='done'?doneExtra(j):''}
    </div>`;}).join('');
}
function subLabel(s){const p=s.split('.');return p.length>=2?p[p.length-2]:'srt';}
function copyDesc(el){navigator.clipboard.writeText(el.getAttribute('data-desc')||'');const prevText=el.textContent;el.textContent=t('copied');setTimeout(()=>el.textContent=prevText,1200);}
function doneExtra(j){
  const m=j.meta||{};
  const save=j.filename?`<div><a class="dl" href="/api/jobs/${j.id}/file">${t('save_file')}</a></div>`:'';
  const photos=(j.photos&&j.photos.length)?`<div class="subrow">${t('photos_prefix')} ${j.photos.map((p,i)=>`<a class="subdl" href="/api/jobs/${j.id}/photo/${i}">${t('photo')} ${i+1}</a>`).join('')}</div>`:'';
  const tags=(m.hashtags&&m.hashtags.length)?`<div class="tags">${m.hashtags.map(h=>'#'+esc(h)).join(' ')}</div>`:'';
  const subs=(j.subs&&j.subs.length)?`<div class="subrow">${t('captions_prefix')} ${j.subs.map((s,i)=>`<a class="subdl" href="/api/jobs/${j.id}/sub/${i}">${esc(subLabel(s))}</a>`).join('')}</div>`:'';
  const desc=m.description?`<div style="margin-top:7px"><span class="subdl" data-desc="${esc(m.description)}" onclick="copyDesc(this)">${t('copy_desc')}</span></div>`:'';
  return save+photos+tags+subs+desc;
}
async function del(id){await fetch('/api/jobs/'+id+'/delete',{method:'POST'});poll();}
async function poll(){
  try{const r=await fetch('/api/jobs');
    const d=await r.json();lastJobs=d.jobs||[];render(lastJobs);
    if(lastJobs.some(j=>['downloading','queued','converting','retrying','watermarking','packaging'].includes(j.status))) setTimeout(poll,1500);
  }catch(e){setTimeout(poll,3000);}
}
async function search(){
  const r=await fetch('/api/library?q='+encodeURIComponent($('#q').value));
  const d=await r.json();
  $('#lib').innerHTML=(d.items||[]).map(it=>`<div class="libitem">
    <div>${esc(it.title||it.filename||'—')}</div>
    <div class="m">${[esc(it.uploader||''),it.upload_date||'',fmtSize(it.size)].filter(Boolean).join(' · ')}</div>
  </div>`).join('')||`<span>${t('lib_empty')}</span>`;
}
$('#go').addEventListener('click',submit);
$('#search').addEventListener('click',search);
applyLanguage();
poll();
</script>
</body></html>"""

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000)
