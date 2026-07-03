#!/usr/bin/env python3
"""
Private Video Capture v2 — yt-dlp downloader for your OWN or licensed content.

Adds on top of v1:
  • Metadata      — yt-dlp --write-info-json + --embed-metadata, indexed in SQLite so you
                    can search your own library later.
  • Aspect ratio  — optional ffmpeg pass to 9:16 / 1:1 / 16:9, center-crop or blurred-pad.
  • Batch audio   — paste many links, pick "Audio only (mp3)".
  • Library search — /api/library?q=... over saved metadata.
  • Watermark removal — opt-in, corner-preset ffmpeg delogo pass. For YOUR OWN content
                    only (e.g. cross-posting your own video to another platform without
                    double branding) — not for stripping other creators' marks.

Env vars (see README): DOWNLOAD_DIR, MAX_CONCURRENT (default 2),
FILE_TTL_MIN (default 60), MAX_URLS_PER_REQUEST (default 10),
DOWNLOAD_TIMEOUT_SEC (default 1800), CONVERT_TIMEOUT_SEC (default 600),
RATE_LIMIT_MAX / RATE_LIMIT_WINDOW_SEC (default 5 per 60s per IP),
MIN_FREE_DISK_MB (default 1024), ALLOWED_DOMAINS (comma-separated
hostnames; empty = allow all), MAX_FILE_SIZE_MB (default 2048),
YTDLP_MAX_RETRIES / YTDLP_RETRY_BACKOFF_SEC (default 2 retries, 5s backoff).
Needs on the server: python3, ffmpeg, and yt-dlp (installed in the venv).
"""
import os, re, json, time, uuid, shutil, socket, ipaddress, sqlite3, threading, subprocess
from flask import (Flask, request, jsonify,
                   send_file, render_template_string, abort)

DOWNLOAD_DIR        = os.environ.get("DOWNLOAD_DIR", "/var/lib/vidcapture")
MAX_CONCURRENT      = int(os.environ.get("MAX_CONCURRENT", "2"))
FILE_TTL_MIN        = int(os.environ.get("FILE_TTL_MIN", "60"))
YTDLP               = shutil.which("yt-dlp") or "yt-dlp"
FFMPEG              = shutil.which("ffmpeg") or "ffmpeg"
FFPROBE             = shutil.which("ffprobe") or "ffprobe"
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

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 1 * 1024 * 1024  # 1MB: plenty for a list of URLs
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
DB = os.path.join(DOWNLOAD_DIR, "library.db")

jobs = {}
sem = threading.Semaphore(MAX_CONCURRENT)

_rate_lock = threading.Lock()
_rate_hits = {}

FORMATS = {
    "1080": "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
    "720":  "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
    "best": "bestvideo+bestaudio/best",
    "audio": "bestaudio/best",
}
CANVAS = {"9x16": (1080, 1920), "1x1": (1080, 1080), "16x9": (1920, 1080)}

# Watermark removal is for YOUR OWN content only (e.g. stripping a platform
# watermark before cross-posting your own video elsewhere). Regions are
# fractions (0-1) of frame width/height so they scale to any resolution.
WATERMARK_PRESETS = {
    "bl": {"x": 0.02, "y": 0.80, "w": 0.22, "h": 0.16},
    "br": {"x": 0.76, "y": 0.80, "w": 0.22, "h": 0.16},
    "tl": {"x": 0.02, "y": 0.04, "w": 0.22, "h": 0.16},
    "tr": {"x": 0.76, "y": 0.04, "w": 0.22, "h": 0.16},
}

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
               "-c:v", "libx264", "-preset", "veryfast", "-crf", "20", dst]
    else:  # blurred-pad background — blur a downscaled copy, then scale back up (much cheaper
           # than blurring at full resolution: ~16x fewer pixels, single-pass boxblur)
        sw, sh = max(2, W // 4), max(2, H // 4)
        fc = ("split=2[bg][fg];"
              "[bg]scale=%d:%d:force_original_aspect_ratio=increase,crop=%d:%d,"
              "boxblur=6:1,scale=%d:%d[bgb];"
              "[fg]scale=%d:%d:force_original_aspect_ratio=decrease[fgs];"
              "[bgb][fgs]overlay=(W-w)/2:(H-h)/2" % (sw, sh, sw, sh, W, H, W, H))
        cmd = [FFMPEG, "-y", "-i", src, "-filter_complex", fc, "-map", "0:a?",
               "-c:a", "aac", "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23", dst]
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
    with sem:
        job = jobs.get(job_id)
        if not job:
            return
        job["status"] = "downloading"
        outdir = os.path.join(DOWNLOAD_DIR, job_id)
        os.makedirs(outdir, exist_ok=True)
        fmt = FORMATS.get(job["format"], FORMATS["1080"])
        outtmpl = os.path.join(outdir, "%(title).150B.%(ext)s")
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
            if returncode != 0:
                job["status"] = "error"
                job["error"] = ("Download timed out." if timed_out else
                                "Download failed after %d attempt(s). The site may be unsupported, "
                                "the link protected/expired/region-locked, or the file exceeds the "
                                "%dMB size cap." % (attempts, MAX_FILE_SIZE_MB))
                return

            media = [f for f in os.listdir(outdir)
                     if not f.endswith((".part", ".info.json", ".webp", ".jpg", ".png", ".srt", ".vtt"))]
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
            job["filename"] = os.path.basename(primary)
            job["size"] = os.path.getsize(primary)
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
        for jid, j in list(jobs.items()):
            if now - j.get("created", now) > FILE_TTL_MIN * 60:
                shutil.rmtree(os.path.join(DOWNLOAD_DIR, jid), ignore_errors=True)
                jobs.pop(jid, None)
        # sweep orphaned job directories left behind by a service restart
        try:
            for name in os.listdir(DOWNLOAD_DIR):
                path = os.path.join(DOWNLOAD_DIR, name)
                if name in jobs or not os.path.isdir(path):
                    continue
                if now - os.path.getmtime(path) > FILE_TTL_MIN * 60:
                    shutil.rmtree(path, ignore_errors=True)
        except OSError:
            pass
        time.sleep(300)
threading.Thread(target=cleanup_loop, daemon=True).start()

# ── routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template_string(APP_HTML)

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
        jobs[jid] = {"id": jid, "url": u, "format": fmt, "convert": convert,
                     "convert_mode": convert_mode, "captions": captions,
                     "watermark": watermark, "watermark_pos": watermark_pos,
                     "status": "queued", "progress": 0.0, "file": None,
                     "filename": None, "error": None, "meta": {}, "subs": [],
                     "created": time.time()}
        threading.Thread(target=run_job, args=(jid,), daemon=True).start()
        created.append(jid)
    return jsonify({"created": created})

@app.get("/api/jobs")
def list_jobs():
    keys = ("id", "url", "status", "progress", "filename", "error", "size", "meta", "subs")
    out = [{k: j.get(k) for k in keys}
           for j in sorted(jobs.values(), key=lambda x: x["created"], reverse=True)]
    return jsonify({"jobs": out})

@app.post("/api/jobs/<jid>/delete")
def delete_job(jid):
    jobs.pop(jid, None)
    shutil.rmtree(os.path.join(DOWNLOAD_DIR, jid), ignore_errors=True)
    return jsonify({"ok": True})

@app.get("/api/jobs/<jid>/file")
def download(jid):
    j = jobs.get(jid)
    if not j or j.get("status") != "done" or not j.get("file"):
        abort(404)
    path = os.path.realpath(j["file"])
    if not path.startswith(os.path.realpath(DOWNLOAD_DIR)) or not os.path.exists(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=j["filename"])

@app.get("/api/jobs/<jid>/sub/<int:idx>")
def sub(jid, idx):
    j = jobs.get(jid)
    if not j:
        abort(404)
    subs = j.get("subs") or []
    if idx < 0 or idx >= len(subs):
        abort(404)
    path = os.path.realpath(os.path.join(DOWNLOAD_DIR, jid, subs[idx]))
    if not path.startswith(os.path.realpath(DOWNLOAD_DIR)) or not os.path.exists(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=subs[idx])

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
*{box-sizing:border-box}
body{margin:0;min-height:100vh;background:#0E1524;color:#E6EBF5;
 font-family:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
 background-image:radial-gradient(1000px 500px at 80% -10%,rgba(91,157,255,.10),transparent 60%);}
.wrap{max-width:880px;margin:0 auto;padding:26px 18px 60px}
.eyebrow{font-size:11px;letter-spacing:.3em;color:#F6A73B;font-weight:600}
h1{font-size:26px;font-weight:800;letter-spacing:-.02em;margin:6px 0 2px}
.sub{color:#7C8AA8;font-size:13px;margin-bottom:20px}
.card{background:#161F33;border:1px solid #2A3853;border-radius:14px;padding:16px;margin-bottom:14px}
textarea,input,select{background:#0E1524;color:#E6EBF5;border:1px solid #2A3853;
 border-radius:10px;padding:11px 12px;font-size:14px;font-family:inherit}
textarea{width:100%;min-height:92px;resize:vertical;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:13px}
textarea:focus,input:focus,select:focus{outline:none;border-color:#F6A73B;box-shadow:0 0 0 3px rgba(246,167,59,.16)}
.controls{display:flex;gap:8px;flex-wrap:wrap;margin-top:11px;align-items:center}
.controls label{font-size:11px;color:#7C8AA8;margin-right:2px}
.btn{background:#F6A73B;color:#241605;border:none;font-weight:700;font-size:14px;
 padding:11px 18px;border-radius:10px;cursor:pointer;margin-left:auto}
.btn.ghost{background:transparent;color:#E6EBF5;border:1px solid #2A3853;font-weight:600;margin-left:0}
.btn:disabled{opacity:.5;cursor:not-allowed}
.note{font-size:12px;color:#7C8AA8;line-height:1.5;margin-top:10px}
.job{background:#161F33;border:1px solid #2A3853;border-radius:12px;padding:12px 13px;margin-bottom:9px}
.job .u{font-size:12px;color:#7C8AA8;font-family:ui-monospace,monospace;word-break:break-all}
.job .t{font-size:13.5px;font-weight:600;margin-bottom:3px;word-break:break-all}
.meta{font-size:11px;color:#8A98B6;margin-top:3px}
.bar{height:6px;background:#0E1524;border:1px solid #2A3853;border-radius:3px;overflow:hidden;margin:8px 0}
.bar>i{display:block;height:100%;background:#F6A73B;width:0;transition:width .3s}
.bar.done>i{background:#35D6A0;width:100%}
.bar.error>i{background:#F2627E;width:100%}
.st{font-size:11px;text-transform:uppercase;letter-spacing:.1em;color:#7C8AA8}
.st.done{color:#35D6A0}.st.error{color:#F2627E}.st.downloading,.st.converting,.st.retrying,.st.watermarking{color:#F6A73B}
.err{color:#F2627E;font-size:12px;margin-top:4px}
a.dl{display:inline-block;margin-top:8px;background:#35D6A0;color:#04231a;font-weight:700;
 font-size:13px;padding:8px 14px;border-radius:9px;text-decoration:none}
.top{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px}
.banner{background:rgba(246,167,59,.10);border:1px solid rgba(246,167,59,.3);color:#F6C77B;
 font-size:12px;padding:9px 12px;border-radius:10px;margin-bottom:16px;line-height:1.5}
.libitem{border-bottom:1px solid #22304c;padding:8px 0;font-size:13px}
.libitem .m{font-size:11px;color:#7C8AA8}
.tags{margin-top:7px;font-size:12px;color:#7FA8FF;line-height:1.6;word-break:break-word}
.subrow{margin-top:7px;font-size:12px;color:#7C8AA8}
.subdl{display:inline-block;margin:3px 6px 0 0;background:#0E1524;border:1px solid #2A3853;
 color:#E6EBF5;font-size:12px;padding:5px 10px;border-radius:8px;text-decoration:none;cursor:pointer}
.subdl:hover{border-color:#3a4a6b}
"""

APP_HTML = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Private Video Capture</title>
<style>""" + BASE_CSS + """</style></head>
<body><div class="wrap">
 <div class="top"><div><div class="eyebrow">PRIVATE CAPTURE</div><h1>Video Capture</h1></div></div>
 <div class="banner">For content you own or are licensed to download — your own uploads, client or product footage, and public-domain / Creative-Commons material. Watermark removal is for your own content only (e.g. cross-posting your own video to another platform).</div>
 <div class="card">
  <textarea id="urls" placeholder="Paste one or more links, one per line…"></textarea>
  <div class="controls">
   <label>Quality</label>
   <select id="fmt">
    <option value="1080">Up to 1080p</option>
    <option value="720">Up to 720p</option>
    <option value="best">Best</option>
    <option value="audio">Audio (mp3)</option>
   </select>
   <label>Reframe</label>
   <select id="conv">
    <option value="none">Keep original</option>
    <option value="9x16">9:16 (Reels/TikTok)</option>
    <option value="1x1">1:1 (Square)</option>
    <option value="16x9">16:9 (YouTube)</option>
   </select>
   <select id="cmode">
    <option value="blur">Blurred pad</option>
    <option value="crop">Center crop</option>
   </select>
   <label style="display:flex;align-items:center;gap:6px;margin-left:2px"><input type="checkbox" id="caps" style="width:auto;accent-color:#F6A73B"> Captions</label>
   <label style="display:flex;align-items:center;gap:6px;margin-left:2px"><input type="checkbox" id="wm" style="width:auto;accent-color:#F6A73B"> Remove watermark (own content only)</label>
   <select id="wmpos">
    <option value="bl">Bottom-left</option>
    <option value="br">Bottom-right</option>
    <option value="tl">Top-left</option>
    <option value="tr">Top-right</option>
   </select>
   <button class="btn" id="go">Download</button>
  </div>
  <p class="note">Server downloads, embeds metadata, and (optionally) reframes each file. A Save button appears when it's ready.</p>
  <p class="err" id="submitErr"></p>
 </div>

 <div id="jobs"></div>

 <div class="card">
  <div class="controls" style="margin-top:0">
   <input id="q" placeholder="Search your library (title, uploader, tag)" style="flex:1">
   <button class="btn ghost" id="search">Search</button>
  </div>
  <div id="lib" class="note"></div>
 </div>
</div>
<script>
const $=s=>document.querySelector(s);
function esc(s){return (s||'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function fmtSize(b){if(!b)return'';const u=['B','KB','MB','GB'];let i=0,n=b;while(n>=1024&&i<3){n/=1024;i++}return n.toFixed(1)+' '+u[i];}
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
  }finally{$('#go').disabled=false;}
  poll();
}
function metaLine(m){if(!m)return'';const p=[];if(m.uploader)p.push(esc(m.uploader));
  if(m.view_count)p.push(m.view_count.toLocaleString()+' views');
  if(m.upload_date)p.push(m.upload_date);return p.join(' · ');}
function render(list){
  $('#jobs').innerHTML=list.map(j=>{
    const cls=j.status==='done'?'done':j.status==='error'?'error':'';
    const pct=Math.round(j.progress||0);
    return `<div class="job">
      <div class="t">${j.filename?esc(j.filename):'Fetching…'}</div>
      <div class="u">${esc(j.url||'')}</div>
      ${metaLine(j.meta)?`<div class="meta">${metaLine(j.meta)}</div>`:''}
      <div class="bar ${cls}"><i style="width:${pct}%"></i></div>
      <div class="controls" style="justify-content:space-between;margin-top:2px">
        <span class="st ${j.status}">${j.status}${(j.status==='downloading'||j.status==='queued')?' · '+pct+'%':''}${j.size?' · '+fmtSize(j.size):''}</span>
        <button class="btn ghost" style="padding:5px 10px;font-size:12px;margin-left:0" onclick="del('${j.id}')">Remove</button>
      </div>
      ${j.error?`<div class="err">${esc(j.error)}</div>`:''}
      ${j.status==='done'?doneExtra(j):''}
    </div>`;}).join('');
}
function subLabel(s){const p=s.split('.');return p.length>=2?p[p.length-2]:'srt';}
function copyDesc(el){navigator.clipboard.writeText(el.getAttribute('data-desc')||'');const t=el.textContent;el.textContent='Copied ✓';setTimeout(()=>el.textContent=t,1200);}
function doneExtra(j){
  const m=j.meta||{};
  const save=`<div><a class="dl" href="/api/jobs/${j.id}/file">Save file</a></div>`;
  const tags=(m.hashtags&&m.hashtags.length)?`<div class="tags">${m.hashtags.map(h=>'#'+esc(h)).join(' ')}</div>`:'';
  const subs=(j.subs&&j.subs.length)?`<div class="subrow">Captions: ${j.subs.map((s,i)=>`<a class="subdl" href="/api/jobs/${j.id}/sub/${i}">${esc(subLabel(s))}</a>`).join('')}</div>`:'';
  const desc=m.description?`<div style="margin-top:7px"><span class="subdl" data-desc="${esc(m.description)}" onclick="copyDesc(this)">Copy description</span></div>`:'';
  return save+tags+subs+desc;
}
async function del(id){await fetch('/api/jobs/'+id+'/delete',{method:'POST'});poll();}
async function poll(){
  try{const r=await fetch('/api/jobs');
    const d=await r.json();render(d.jobs||[]);
    if((d.jobs||[]).some(j=>['downloading','queued','converting','retrying','watermarking'].includes(j.status))) setTimeout(poll,1500);
  }catch(e){setTimeout(poll,3000);}
}
async function search(){
  const r=await fetch('/api/library?q='+encodeURIComponent($('#q').value));
  const d=await r.json();
  $('#lib').innerHTML=(d.items||[]).map(it=>`<div class="libitem">
    <div>${esc(it.title||it.filename||'—')}</div>
    <div class="m">${[esc(it.uploader||''),it.upload_date||'',fmtSize(it.size)].filter(Boolean).join(' · ')}</div>
  </div>`).join('')||'<span>No saved items yet.</span>';
}
$('#go').addEventListener('click',submit);
$('#search').addEventListener('click',search);
poll();
</script>
</body></html>"""

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000)
