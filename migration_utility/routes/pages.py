"""Pages blueprint — serves HTML templates (cached at startup)."""
from flask import Blueprint, current_app
import os

from .auth import login_required

pages_bp = Blueprint("pages", __name__)

_TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "templates")

# Cache templates in memory at import time — avoids reading 11k+ line files per request
_TEMPLATE_CACHE: dict[str, str] = {}
for _fname in ("index.html", "help.html", "bom.html"):
    _path = os.path.join(_TEMPLATE_DIR, _fname)
    if os.path.isfile(_path):
        with open(_path, encoding="utf-8") as _f:
            _TEMPLATE_CACHE[_fname] = _f.read()


def _serve(filename):
    # In debug mode always read from disk so template edits take effect immediately
    if current_app.debug:
        fpath = os.path.join(_TEMPLATE_DIR, filename)
        if os.path.isfile(fpath):
            with open(fpath, encoding="utf-8") as f:
                html = f.read()
            _TEMPLATE_CACHE[filename] = html
            return html
    cached = _TEMPLATE_CACHE.get(filename)
    if cached:
        return cached
    # Fallback: read from disk (new template added at runtime)
    with open(os.path.join(_TEMPLATE_DIR, filename), encoding="utf-8") as f:
        html = f.read()
    _TEMPLATE_CACHE[filename] = html
    return html


@pages_bp.route("/")
@login_required
def index():
    return _serve("index.html")


@pages_bp.route("/help")
def help_page():
    return _serve("help.html")


@pages_bp.route("/bom")
def bom_page():
    return _serve("bom.html")


@pages_bp.route("/lakebase")
def lakebase_page():
    return _serve("lakebase.html")
