import os
import time
import zipfile
import io
import subprocess
from unittest.mock import patch, MagicMock

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
        return 0, False, []
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
        return 1, False, ["ERROR: Video unavailable"]
    with patch("app._run_ytdlp_once", side_effect=fake_fail), \
         patch("app.try_gallerydl_fallback", return_value=False):
        app_module.run_job(jid)
    j = app_module.get_job_dict(jid)
    assert j["status"] == "error"
    assert "Video unavailable" in j["error"]


def test_run_job_failure_reports_generic_message_with_no_detail(app_module, client):
    r = client.post("/api/jobs", json={"urls": ["https://example.com/bad"], "format": "1080"})
    jid = r.get_json()["created"][0]

    def fake_fail(cmd, job):
        return 1, False, []
    with patch("app._run_ytdlp_once", side_effect=fake_fail), \
         patch("app.try_gallerydl_fallback", return_value=False):
        app_module.run_job(jid)
    j = app_module.get_job_dict(jid)
    assert j["status"] == "error"
    assert "unsupported" in j["error"].lower()


def test_run_job_failure_reports_size_cap_specifically(app_module, client):
    r = client.post("/api/jobs", json={"urls": ["https://example.com/huge"], "format": "1080"})
    jid = r.get_json()["created"][0]

    def fake_fail(cmd, job):
        return 1, False, ["ERROR: File is larger than max-filesize (4096.0MiB > 4096MiB)"]
    with patch("app._run_ytdlp_once", side_effect=fake_fail), \
         patch("app.try_gallerydl_fallback", return_value=False):
        app_module.run_job(jid)
    j = app_module.get_job_dict(jid)
    assert j["status"] == "error"
    assert str(app_module.MAX_FILE_SIZE_MB) in j["error"]
    assert "size cap" in j["error"]


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


# ── video trimming ────────────────────────────────────────────────────────

def test_parse_timecode(app_module):
    assert app_module.parse_timecode("90") == 90
    assert app_module.parse_timecode("1:30") == 90
    assert app_module.parse_timecode("1:02:03") == 3723
    assert app_module.parse_timecode("") is None
    assert app_module.parse_timecode(None) is None
    assert app_module.parse_timecode("abc") is None
    assert app_module.parse_timecode("1:2:3:4") is None


def test_submit_rejects_start_after_end(client):
    r = client.post("/api/jobs", json={"urls": ["https://example.com/v"], "format": "1080",
                                        "trim_start": "2:00", "trim_end": "1:00"})
    assert r.status_code == 400


def test_submit_stores_trim_range(client, app_module):
    r = client.post("/api/jobs", json={"urls": ["https://example.com/v"], "format": "1080",
                                        "trim_start": "0:30", "trim_end": "1:15"})
    jid = r.get_json()["created"][0]
    j = app_module.get_job_dict(jid)
    assert j["trim_start"] == 30
    assert j["trim_end"] == 75


def test_run_job_passes_download_sections_to_ytdlp(app_module, client):
    r = client.post("/api/jobs", json={"urls": ["https://example.com/v"], "format": "1080",
                                        "trim_start": "0:30", "trim_end": "1:15"})
    jid = r.get_json()["created"][0]
    outdir = os.path.join(app_module.DOWNLOAD_DIR, jid)
    captured = []

    def fake_run_once(cmd, job):
        captured.extend(cmd)
        os.makedirs(outdir, exist_ok=True)
        with open(os.path.join(outdir, "video.mp4"), "wb") as f:
            f.write(b"x")
        return 0, False, []

    with patch("app._run_ytdlp_once", side_effect=fake_run_once):
        app_module.run_job(jid)

    assert "--download-sections" in captured
    assert captured[captured.index("--download-sections") + 1] == "*30-75"
    assert "--force-keyframes-at-cuts" in captured


def test_run_job_no_trim_omits_download_sections(app_module, client):
    r = client.post("/api/jobs", json={"urls": ["https://example.com/v"], "format": "1080"})
    jid = r.get_json()["created"][0]
    outdir = os.path.join(app_module.DOWNLOAD_DIR, jid)
    captured = []

    def fake_run_once(cmd, job):
        captured.extend(cmd)
        os.makedirs(outdir, exist_ok=True)
        with open(os.path.join(outdir, "video.mp4"), "wb") as f:
            f.write(b"x")
        return 0, False, []

    with patch("app._run_ytdlp_once", side_effect=fake_run_once):
        app_module.run_job(jid)

    assert "--download-sections" not in captured


# ── playlist/channel bulk import ────────────────────────────────────────────

def test_expand_playlist_urls_falls_back_when_not_a_playlist(app_module):
    fake_proc = MagicMock(stdout="", returncode=0)
    with patch("app.subprocess.run", return_value=fake_proc):
        assert app_module.expand_playlist_urls("https://example.com/v") == ["https://example.com/v"]


def test_expand_playlist_urls_dedupes_and_caps(app_module):
    urls = [f"https://example.com/v{i}" for i in range(30)]
    stdout = "\n".join(urls + [urls[0]])  # duplicate first entry
    fake_proc = MagicMock(stdout=stdout, returncode=0)
    with patch("app.subprocess.run", return_value=fake_proc):
        result = app_module.expand_playlist_urls("https://example.com/channel")
    assert len(result) == app_module.MAX_PLAYLIST_ITEMS
    assert len(result) == len(set(result))
    assert result == urls[:app_module.MAX_PLAYLIST_ITEMS]


def test_expand_playlist_urls_falls_back_on_timeout(app_module):
    with patch("app.subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="yt-dlp", timeout=1)):
        assert app_module.expand_playlist_urls("https://example.com/v") == ["https://example.com/v"]


def test_submit_expand_playlist_creates_one_job_per_video(client, app_module):
    expanded = ["https://example.com/v1", "https://example.com/v2", "https://example.com/v3"]
    with patch("app.expand_playlist_urls", return_value=expanded):
        r = client.post("/api/jobs", json={"urls": ["https://example.com/channel"],
                                            "format": "1080", "expand_playlist": True})
    assert r.status_code == 200
    created = r.get_json()["created"]
    assert len(created) == 3
    stored_urls = {app_module.get_job_dict(jid)["url"] for jid in created}
    assert stored_urls == set(expanded)


def test_submit_without_expand_playlist_ignores_flag_off(client, app_module):
    with patch("app.expand_playlist_urls") as mock_expand:
        r = client.post("/api/jobs", json={"urls": ["https://example.com/v"], "format": "1080"})
    mock_expand.assert_not_called()
    assert len(r.get_json()["created"]) == 1


def test_submit_expand_playlist_caps_total_across_seeds(client, app_module):
    with patch("app.expand_playlist_urls",
               return_value=[f"https://example.com/v{i}" for i in range(app_module.MAX_PLAYLIST_ITEMS)]):
        r = client.post("/api/jobs", json={"urls": ["https://example.com/c1", "https://example.com/c2"],
                                            "format": "1080", "expand_playlist": True})
    assert len(r.get_json()["created"]) == app_module.MAX_PLAYLIST_ITEMS


# ── cookies (YouTube anti-bot workaround) ───────────────────────────────────

def test_cookies_args_empty_when_unset(app_module):
    app_module.YTDLP_COOKIES_FILE = ""
    assert app_module.cookies_args() == []


def test_cookies_args_empty_when_file_missing(app_module):
    app_module.YTDLP_COOKIES_FILE = "/nonexistent/cookies.txt"
    assert app_module.cookies_args() == []


def test_cookies_args_included_when_file_present(app_module, tmp_path):
    cookie_file = tmp_path / "cookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File\n")
    app_module.YTDLP_COOKIES_FILE = str(cookie_file)
    try:
        assert app_module.cookies_args() == ["--cookies", str(cookie_file)]
    finally:
        app_module.YTDLP_COOKIES_FILE = ""


# ── shared-credential site gate ──────────────────────────────────────────────

def test_site_open_by_default(client):
    assert client.get("/").status_code == 200


def test_site_auth_blocks_without_credentials(client, app_module):
    app_module.SITE_USER = "team"
    app_module.SITE_PASSWORD = "secret"
    r = client.get("/")
    assert r.status_code == 401


def test_site_auth_blocks_wrong_credentials(client, app_module):
    app_module.SITE_USER = "team"
    app_module.SITE_PASSWORD = "secret"
    r = client.get("/", auth=("team", "wrong"))
    assert r.status_code == 401


def test_site_auth_allows_correct_credentials(client, app_module):
    app_module.SITE_USER = "team"
    app_module.SITE_PASSWORD = "secret"
    r = client.get("/", auth=("team", "secret"))
    assert r.status_code == 200


def test_site_auth_exempts_healthz(client, app_module):
    app_module.SITE_USER = "team"
    app_module.SITE_PASSWORD = "secret"
    r = client.get("/healthz")
    assert r.status_code != 401


def test_site_auth_protects_api_routes(client, app_module):
    app_module.SITE_USER = "team"
    app_module.SITE_PASSWORD = "secret"
    r = client.post("/api/jobs", json={"urls": ["https://example.com/v"], "format": "1080"})
    assert r.status_code == 401


# ── PO token provider (cookie-free YouTube anti-bot workaround) ─────────────

def test_pot_provider_args_empty_when_unset(app_module):
    app_module.YTDLP_POT_PROVIDER_URL = ""
    assert app_module.pot_provider_args() == []


def test_pot_provider_args_included_when_set(app_module):
    app_module.YTDLP_POT_PROVIDER_URL = "http://127.0.0.1:4416"
    assert app_module.pot_provider_args() == [
        "--extractor-args", "youtubepot-bgutilhttp:base_url=http://127.0.0.1:4416"]


# ── outbound proxy ───────────────────────────────────────────────────────────

def test_proxy_for_attempt_empty_when_unset(app_module):
    app_module.YTDLP_PROXY_URL = ""
    app_module.YTDLP_PROXY_URLS = []
    assert app_module.proxy_for_attempt() == []


def test_proxy_for_attempt_uses_single_url_when_set(app_module):
    app_module.YTDLP_PROXY_URL = "http://user:pass@147.79.5.40:7753"
    assert app_module.proxy_for_attempt() == ["--proxy", "http://user:pass@147.79.5.40:7753"]
    # single-URL mode ignores the attempt index — same proxy every time
    assert app_module.proxy_for_attempt(3) == ["--proxy", "http://user:pass@147.79.5.40:7753"]


def test_proxy_for_attempt_rotates_through_pool(app_module):
    app_module.YTDLP_PROXY_URLS = [
        "http://user:pass@1.1.1.1:1000",
        "http://user:pass@2.2.2.2:2000",
        "http://user:pass@3.3.3.3:3000",
    ]
    assert app_module.proxy_for_attempt(0) == ["--proxy", "http://user:pass@1.1.1.1:1000"]
    assert app_module.proxy_for_attempt(1) == ["--proxy", "http://user:pass@2.2.2.2:2000"]
    assert app_module.proxy_for_attempt(2) == ["--proxy", "http://user:pass@3.3.3.3:3000"]
    # wraps around past the end of the pool
    assert app_module.proxy_for_attempt(3) == ["--proxy", "http://user:pass@1.1.1.1:1000"]


def test_proxy_for_attempt_pool_takes_priority_over_single_url(app_module):
    app_module.YTDLP_PROXY_URL = "http://user:pass@single.example:1000"
    app_module.YTDLP_PROXY_URLS = ["http://user:pass@pooled.example:2000"]
    assert app_module.proxy_for_attempt() == ["--proxy", "http://user:pass@pooled.example:2000"]


def test_run_job_rotates_proxy_across_retry_attempts(app_module, client):
    app_module.YTDLP_PROXY_URLS = [
        "http://user:pass@1.1.1.1:1000",
        "http://user:pass@2.2.2.2:2000",
    ]
    r = client.post("/api/jobs", json={"urls": ["https://example.com/v"], "format": "1080"})
    jid = r.get_json()["created"][0]
    seen_proxies = []

    def fake_run_once(cmd, job):
        seen_proxies.append(cmd[cmd.index("--proxy") + 1])
        return 1, False, ["ERROR: proxy connection failed"]

    with patch("app._run_ytdlp_once", side_effect=fake_run_once), \
         patch("app.try_gallerydl_fallback", return_value=False), \
         patch("app.YTDLP_RETRY_BACKOFF_SEC", 0):
        app_module.run_job(jid)

    assert seen_proxies == [
        "http://user:pass@1.1.1.1:1000",
        "http://user:pass@2.2.2.2:2000",
        "http://user:pass@1.1.1.1:1000",
    ]


# ── YouTube SABR/n-challenge workaround ──────────────────────────────────────

def test_youtube_client_args_defaults(app_module):
    assert app_module.youtube_client_args() == [
        "--extractor-args", "youtube:player_client=tv",
        "--js-runtimes", "node",
    ]


def test_youtube_client_args_respects_custom_client(app_module):
    app_module.YTDLP_PLAYER_CLIENT = "web,tv"
    assert app_module.youtube_client_args() == [
        "--extractor-args", "youtube:player_client=web,tv",
        "--js-runtimes", "node",
    ]


def test_youtube_client_args_disabled_when_both_empty(app_module):
    app_module.YTDLP_PLAYER_CLIENT = ""
    app_module.YTDLP_JS_RUNTIME = ""
    assert app_module.youtube_client_args() == []
