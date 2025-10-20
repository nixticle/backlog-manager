from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Mapping

import orjson

from .config import Config


class JSONFormatter(logging.Formatter):
    """Emit structured JSON logs."""

    def format(self, record: logging.LogRecord) -> str:
        base: dict[str, Any] = {
            "level": record.levelname,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            base["exc_info"] = self.formatException(record.exc_info)
        extra = {
            key: value
            for key, value in record.__dict__.items()
            if key not in logging.LogRecord.__slots__
            and key
            not in {"name", "msg", "args", "levelname", "levelno", "pathname", "filename",
                    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
                    "created", "msecs", "relativeCreated", "thread", "threadName",
                    "processName", "process"}
        }
        if extra:
            base.update(_serialize_extra(extra))
        return orjson.dumps(base).decode("utf-8")


def _serialize_extra(extra: Mapping[str, Any]) -> Mapping[str, Any]:
    ready: dict[str, Any] = {}
    for key, value in extra.items():
        try:
            orjson.dumps(value)
            ready[key] = value
        except TypeError:
            ready[key] = repr(value)
    return ready


def configure_logging(cfg: Config) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    level = getattr(logging, cfg.logging.level.upper(), logging.INFO)
    root.setLevel(level)

    handler = logging.StreamHandler(stream=sys.stdout)
    if cfg.logging.json:
        handler.setFormatter(JSONFormatter())
    else:
        fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
        handler.setFormatter(logging.Formatter(fmt=fmt))
    root.addHandler(handler)

