"""Packaged, dependency free workspace assets."""

from __future__ import annotations

import base64
import hashlib
from functools import lru_cache
from importlib.resources import files

from fastapi import Response


@lru_cache(maxsize=2)
def asset(name: str) -> Response:
    if name not in {"index.html", "workspace.js"}:
        raise ValueError("Unknown workspace asset.")
    content = files(__package__).joinpath(name).read_text(encoding="utf-8")
    headers = {"Cache-Control": "no-store"}
    if name == "index.html":
        style = content.split("<style>", 1)[1].split("</style>", 1)[0]
        digest = base64.b64encode(hashlib.sha256(style.encode()).digest()).decode("ascii")
        headers["Content-Security-Policy"] = (
            "default-src 'none'; script-src 'self'; "
            f"style-src 'sha256-{digest}'; connect-src 'self'; "
            "base-uri 'none'; form-action 'self'; frame-ancestors 'none'"
        )
    return Response(
        content,
        media_type="text/html" if name == "index.html" else "application/javascript",
        headers=headers,
    )
