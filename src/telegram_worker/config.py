from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def configure_logging() -> None:
    level_name = os.getenv("WORKER_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, None)
    if not isinstance(level, int):
        raise ValueError(f"Invalid WORKER_LOG_LEVEL: {level_name}")
    logging.addLevelName(logging.DEBUG, "调试")
    logging.addLevelName(logging.INFO, "信息")
    logging.addLevelName(logging.WARNING, "警告")
    logging.addLevelName(logging.ERROR, "错误")
    logging.addLevelName(logging.CRITICAL, "严重")
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(message)s")
    logging.getLogger("telethon").setLevel(logging.WARNING)


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        os.environ.setdefault(name.strip(), value.strip())


@dataclass(frozen=True)
class Config:
    api_id: int
    api_hash: str
    session_path: Path
    proxy_host: Optional[str]
    proxy_port: Optional[int]
    state_path: Path
    api_token: str
    host: str
    port: int

    @classmethod
    def from_env(cls) -> "Config":
        required = ("TG_API_ID", "TG_API_HASH", "WORKER_API_TOKEN")
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")
        return cls(
            api_id=int(os.environ["TG_API_ID"]),
            api_hash=os.environ["TG_API_HASH"],
            session_path=Path(os.getenv("TG_SESSION_PATH", "./data/telegram")),
            proxy_host=os.getenv("TG_PROXY_HOST") or None,
            proxy_port=int(os.environ["TG_PROXY_PORT"]) if os.getenv("TG_PROXY_HOST") else None,
            state_path=Path(os.getenv("WORKER_STATE_PATH", "./data/state.json")),
            api_token=os.environ["WORKER_API_TOKEN"],
            host=os.getenv("WORKER_HOST", "127.0.0.1"),
            port=int(os.getenv("WORKER_PORT", "8081")),
        )
