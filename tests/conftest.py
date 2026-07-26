import os
import sys

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TEST_DIR)
sys.path.insert(0, REPO_ROOT)

os.environ.setdefault("REDIS_PORT", "6379")

import pytest

import app as m


def pytest_configure(config):
    try:
        m.redis_conn.ping()
    except Exception as e:
        raise RuntimeError(
            "Tests need a real Redis reachable at REDIS_HOST/REDIS_PORT "
            "(default localhost:6379). Start one with: "
            "redis-server --daemonize yes"
        ) from e


@pytest.fixture
def app_module(tmp_path):
    """Points the already-imported app module at a fresh temp download dir
    and a clean Redis DB for each test, then cleans up after."""
    m.DOWNLOAD_DIR = str(tmp_path)
    m.redis_conn.flushdb()
    m._rate_hits.clear()
    yield m
    m.redis_conn.flushdb()
    m._rate_hits.clear()


@pytest.fixture
def client(app_module):
    return app_module.app.test_client()


def dispatch_and_close(app_module, path, method="GET", json_body=None):
    """Simulates exactly what a real WSGI server (gunicorn) does: fully
    consume the response body, then close the *app_iter it was actually
    handed back* — not the Flask Response object.

    This distinction is the whole point of this helper. Calling
    Response.close() directly (e.g. via Flask's test client) always walks
    Response._on_close, regardless of direct_passthrough — so it would
    pass even against the old, buggy call_on_close()-based code and never
    catch the regression this suite exists to catch. A real WSGI server
    never calls Response.close(); for direct_passthrough responses (every
    send_file() call in this app) it only ever closes response.response,
    per Werkzeug's get_app_iter(). See app.py's _OnCloseWrapper docstring."""
    kwargs = {"method": method}
    if json_body is not None:
        kwargs["json"] = json_body
    with app_module.app.test_request_context(path, **kwargs):
        resp = app_module.app.dispatch_request()
        if isinstance(resp, tuple):
            return resp[0], resp[1] if len(resp) > 1 else None, None
        status_code = resp.status_code
        if getattr(resp, "direct_passthrough", False):
            app_iter = resp.response
            data = b"".join(c if isinstance(c, bytes) else c.encode() for c in app_iter)
            if hasattr(app_iter, "close"):
                app_iter.close()
        else:
            data = resp.get_data()
            resp.close()
        return None, status_code, data
