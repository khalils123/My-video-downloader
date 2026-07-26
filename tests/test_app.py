import os
import time
import zipfile
import io
from unittest.mock import patch

from conftest import dispatch_and_close


# ── URL safety / domain rules ────────────────────────────────────────────

def test_is_safe_url_allows_public(app_module):
    assert app_module.is_safe_url("https://www.youtube.com/watch?v=abc") is True


def test_is_safe_url_blocks_loopback(app_module):
    assert app_module.is_safe_url("http://127.0.0.1:8000/") is False


def test_is_safe_url_blocks_link_local(app_module):
    assert app_module.is_safe_url("http://169.254.169.254/") is False  # cloud metadata endpoint


def test_is_safe_url_blocks_private_range(app_module):
    assert app_module.is_safe_url("http://192.168.1.1/") is False


def test_is_safe_url_rejects_malformed(app_module):
    assert app_module.is_safe_url("not-a-url") is False


def test_domain_allowed_empty_allowlist_allows_everything(app_module):
    app_module.ALLOWED_DOMAINS = []
    assert app_module.domain_allowed("https://anything.example/x") is True


def test_domain_allowed_enforces_allowlist(app_module):
    app_module.ALLOWED_DOMAINS = ["youtube.com"]
    try:
        assert app_module.domain_allowed("https://www.youtube.com/x") is True
        assert app_module.domain_allowed("https://tiktok.com/x") is False
    finally:
        app_module.ALLOWED_DOMAINS = []


# ── rate limiting ─────────────────────────────────────────────────────────

def test_rate_limited_allows_then_blocks(app_module):
    app_module._rate_hits.clear()
    ip = "1.2.3.4"
    results = [app_module.rate_limited(ip) for _ in range(app_module.RATE_LIMIT_MAX + 2)]
    assert results[:app_module.RATE_LIMIT_MAX] == [False] * app_module.RATE_LIMIT_MAX
    assert all(results[app_module.RATE_LIMIT_MAX:])


# ── output classification (video / photo-post detection) ────────────────

def test_classify_outdir_single_video_not_slideshow(app_module, tmp_path):
    outdir = tmp_path / "job1"
    outdir.mkdir()
    (outdir / "video.mp4").write_bytes(b"x")
    images, videos, audios = app_module.classify_outdir(str(outdir))
    assert videos == ["video.mp4"]
    assert not (bool(images) and not videos)  # is_slideshow condition


def test_classify_outdir_video_with_stray_thumbnail_ignores_image(app_module, tmp_path):
    outdir = tmp_path / "job1"
    outdir.mkdir()
    (outdir / "video.mp4").write_bytes(b"x")
    (outdir / "thumb.jpg").write_bytes(b"x")
    images, videos, audios = app_module.classify_outdir(str(outdir))
    is_slideshow = bool(images) and not videos
    assert is_slideshow is False  # a video is present, so the stray image is not content


def test_classify_outdir_single_photo_post_is_slideshow(app_module, tmp_path):
    """Regression test: a single-photo post (1 image + audio, no video) must
    still be treated as a slideshow, not silently dropped."""
    outdir = tmp_path / "job1"
    outdir.mkdir()
    (outdir / "photo.jpg").write_bytes(b"x")
    (outdir / "song.mp3").write_bytes(b"x")
    images, videos, audios = app_module.classify_outdir(str(outdir))
    is_slideshow = bool(images) and not videos
    assert is_slideshow is True
    assert images == ["photo.jpg"]


def test_classify_outdir_multi_photo_slideshow(app_module, tmp_path):
    outdir = tmp_path / "job1"
    outdir.mkdir()
    (outdir / "p1.jpg").write_bytes(b"x")
    (outdir / "p2.jpg").write_bytes(b"x")
    images, videos, audios = app_module.classify_outdir(str(outdir))
    assert len(images) == 2 and not videos


# ── job store round-trip ─────────────────────────────────────────────────

def test_redis_job_create_and_read(app_module):
    jid = "job_rw_test"
    app_module.create_job(jid, id=jid, url="https://x", format="1080", convert="none",
                          convert_mode="blur", captions=False, watermark=False, watermark_pos="bl",
                          burn_captions=False, strip_metadata=False, status="queued", progress=0.0,
                          file=None, filename=None, error=None, meta={}, subs=[], photos=[],
                          served_file=False, served_photos=[], served_subs=[], duplicate={},
                          created=time.time())
    j = app_module.get_job_dict(jid)
    assert j["status"] == "queued"
    assert j["captions"] is False
    assert j["meta"] == {}
    assert j["subs"] == []


# ── full run_job pipeline (mocked yt-dlp) ────────────────────────────────

def _fake_download(app_module, outdir, files):
    def fake_run_once(cmd, job):
        os.makedirs(outdir, exist_ok=True)
        for name, content in files.items():
            with open(os.path.join(outdir, name), "wb") as f:
                f.write(content)
        return 0, False
    return fake_run_once


def test_run_job_normal_video_completes(app_module, client):
    r = client.post("/api/jobs", json={"urls": ["https://example.com/v"], "format": "1080"})
    jid = r.get_json()["created"][0]
    outdir = os.path.join(app_module.DOWNLOAD_DIR, jid)
    fake = _fake_download(app_module, outdir, {
        "video.mp4": b"VIDEODATA",
        "video.info.json": b'{"title": "Test Video"}',
    })
    with patch("app._run_ytdlp_once", side_effect=fake):
        app_module.run_job(jid)
    j = app_module.get_job_dict(jid)
    assert j["status"] == "done"
    assert j["filename"] == "video.mp4"


def test_run_job_single_photo_post_keeps_the_photo(app_module, client):
    """Regression test for the bug where a single-photo post lost its image
    and only the audio survived."""
    r = client.post("/api/jobs", json={"urls": ["https://example.com/photopost"], "format": "1080"})
    jid = r.get_json()["created"][0]
    outdir = os.path.join(app_module.DOWNLOAD_DIR, jid)
    fake = _fake_download(app_module, outdir, {
        "photo.jpg": b"IMG",
        "song.mp3": b"AUD",
        "photo.info.json": b'{"title": "Single Photo"}',
    })
    with patch("app._run_ytdlp_once", side_effect=fake):
        app_module.run_job(jid)
    j = app_module.get_job_dict(jid)
    assert j["status"] == "done"
    assert j["photos"] == ["photo.jpg"]
    assert j["filename"] == "song.mp3"  # audio becomes the primary "Save file" download


def test_run_job_failure_sets_error(app_module, client):
    r = client.post("/api/jobs", json={"urls": ["https://example.com/bad"], "format": "1080"})
    jid = r.get_json()["created"][0]

    def fake_fail(cmd, job):
        return 1, False
    with patch("app._run_ytdlp_once", side_effect=fake_fail), \
         patch("app.try_gallerydl_fallback", return_value=False):
        app_module.run_job(jid)
    j = app_module.get_job_dict(jid)
    assert j["status"] == "error"
    assert j["error"]


# ── duplicate detection ───────────────────────────────────────────────────

def test_duplicate_detection_flags_repeat_content(app_module, client, tmp_path):
    if os.path.exists(app_module.DB):
        os.remove(app_module.DB)
    app_module.init_db()

    def make_job(url, title, content):
        r = client.post("/api/jobs", json={"urls": [url], "format": "1080"})
        jid = r.get_json()["created"][0]
        outdir = os.path.join(app_module.DOWNLOAD_DIR, jid)
        fake = _fake_download(app_module, outdir, {
            "video.mp4": content,
            "video.info.json": ('{"title": "%s"}' % title).encode(),
        })
        with patch("app._run_ytdlp_once", side_effect=fake):
            app_module.run_job(jid)
        return app_module.get_job_dict(jid)

    j1 = make_job("https://example.com/original", "Original", b"IDENTICAL_BYTES")
    assert j1["duplicate"] == {}

    j2 = make_job("https://example.com/repost", "Repost", b"IDENTICAL_BYTES")
    assert j2["duplicate"].get("title") == "Original"


# ── on-download cleanup (the critical WSGI-level fix) ────────────────────

def test_file_deleted_after_download(app_module, tmp_path):
    jid = "cleanup_test_1"
    outdir = os.path.join(app_module.DOWNLOAD_DIR, jid)
    os.makedirs(outdir)
    with open(os.path.join(outdir, "video.mp4"), "wb") as f:
        f.write(b"DATA" * 100)
    app_module.create_job(jid, id=jid, url="https://x", format="1080", convert="none",
                          convert_mode="blur", captions=False, watermark=False, watermark_pos="bl",
                          burn_captions=False, strip_metadata=False, status="done", progress=100.0,
                          file=os.path.join(outdir, "video.mp4"), filename="video.mp4", error=None,
                          meta={}, subs=[], photos=[], served_file=False, served_photos=[],
                          served_subs=[], duplicate={}, created=time.time())

    _, status, data = dispatch_and_close(app_module, "/api/jobs/%s/file" % jid)
    assert status == 200
    assert data == b"DATA" * 100
    assert app_module.get_job_dict(jid) is None
    assert not os.path.isdir(outdir)


def test_multipart_job_survives_partial_download(app_module):
    jid = "cleanup_test_2"
    outdir = os.path.join(app_module.DOWNLOAD_DIR, jid)
    os.makedirs(outdir)
    with open(os.path.join(outdir, "video.mp4"), "wb") as f:
        f.write(b"V")
    with open(os.path.join(outdir, "video.srt"), "wb") as f:
        f.write(b"S")
    app_module.create_job(jid, id=jid, url="https://x", format="1080", convert="none",
                          convert_mode="blur", captions=True, watermark=False, watermark_pos="bl",
                          burn_captions=False, strip_metadata=False, status="done", progress=100.0,
                          file=os.path.join(outdir, "video.mp4"), filename="video.mp4", error=None,
                          meta={}, subs=["video.srt"], photos=[], served_file=False, served_photos=[],
                          served_subs=[], duplicate={}, created=time.time())

    dispatch_and_close(app_module, "/api/jobs/%s/file" % jid)
    assert app_module.get_job_dict(jid) is not None  # caption not downloaded yet

    dispatch_and_close(app_module, "/api/jobs/%s/sub/0" % jid)
    assert app_module.get_job_dict(jid) is None
    assert not os.path.isdir(outdir)


def test_ttl_sweep_removes_old_undownloaded_jobs(app_module):
    app_module.create_job("old", id="old", url="https://x", format="1080", convert="none",
                          convert_mode="blur", captions=False, watermark=False, watermark_pos="bl",
                          burn_captions=False, strip_metadata=False, status="done", progress=100.0,
                          file=None, filename=None, error=None, meta={}, subs=[], photos=[],
                          served_file=False, served_photos=[], served_subs=[], duplicate={},
                          created=time.time() - app_module.FILE_TTL_MIN * 60 - 100)
    app_module.create_job("fresh", id="fresh", url="https://y", format="1080", convert="none",
                          convert_mode="blur", captions=False, watermark=False, watermark_pos="bl",
                          burn_captions=False, strip_metadata=False, status="done", progress=100.0,
                          file=None, filename=None, error=None, meta={}, subs=[], photos=[],
                          served_file=False, served_photos=[], served_subs=[], duplicate={},
                          created=time.time())
    now = time.time()
    for jid in app_module.list_job_ids():
        j = app_module.get_job_dict(jid)
        created = j.get("created") if j else None
        if created is None or now - created > app_module.FILE_TTL_MIN * 60:
            app_module.delete_job_record(jid)
    assert app_module.get_job_dict("old") is None
    assert app_module.get_job_dict("fresh") is not None


# ── bulk export ────────────────────────────────────────────────────────────

def test_export_bundles_and_cleans_up(app_module):
    jobs = []
    for name, title in (("a", "A"), ("b", "B")):
        jid = "exp_%s" % name
        outdir = os.path.join(app_module.DOWNLOAD_DIR, jid)
        os.makedirs(outdir)
        with open(os.path.join(outdir, "video.mp4"), "wb") as f:  # same basename on purpose
            f.write(name.encode() * 100)
        app_module.create_job(jid, id=jid, url="https://x/" + name, format="1080", convert="none",
                              convert_mode="blur", captions=False, watermark=False, watermark_pos="bl",
                              burn_captions=False, strip_metadata=False, status="done", progress=100.0,
                              file=os.path.join(outdir, "video.mp4"), filename="video.mp4", error=None,
                              meta={"title": title}, subs=[], photos=[], served_file=False,
                              served_photos=[], served_subs=[], duplicate={}, created=time.time())
        jobs.append(jid)

    _, status, data = dispatch_and_close(app_module, "/api/export", method="POST",
                                          json_body={"job_ids": jobs})
    assert status == 200
    zf = zipfile.ZipFile(io.BytesIO(data))
    names = zf.namelist()
    assert "manifest.csv" in names
    assert len([n for n in names if n.endswith(".mp4")]) == 2  # collision-avoided filenames

    for jid in jobs:
        assert app_module.get_job_dict(jid) is None


# ── health endpoint ─────────────────────────────────────────────────────

def test_healthz_reports_ok(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    body = r.get_json()
    assert body["status"] == "ok"
    assert body["checks"]["redis"] == "ok"
