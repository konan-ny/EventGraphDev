"""Settings loaded from config/.env (plain KEY=VALUE, split on first '=')."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

DEFAULT_ENV = Path(__file__).resolve().parent.parent / "config" / ".env"


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_user: str
    db_pwd: str
    db_name: str
    db_table: str
    anthropic_api_key: str = ""
    anthropic_model: str = "claude-haiku-5-5"
    pg_dsn: str = "postgresql://eventgraph:eventgraph@localhost:5434/eventgraph"
    max_docs: int = 300
    llm_concurrency: int = 5
    data_dir: Path = Path("data")


def _parse_env(path: Path) -> dict[str, str]:
    # split on first '=' only, so passwords containing '#' or '=' survive
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def load_settings(path: Path | None = None) -> Settings:
    e = _parse_env(path or DEFAULT_ENV)
    return Settings(
        db_host=e["DB_HOST"],
        db_user=e["DB_USER"],
        db_pwd=e["DB_PWD"],
        db_name=e["DB_NAME"],
        db_table=e["DB_TABLENAME"],
        anthropic_api_key=e.get("ANTHROPIC_API_KEY", ""),
        anthropic_model=e.get("ANTHROPIC_MODEL", Settings.anthropic_model),
        pg_dsn=e.get("PG_DSN", Settings.pg_dsn),
        max_docs=int(e.get("MAX_DOCS", Settings.max_docs)),
        llm_concurrency=int(e.get("LLM_CONCURRENCY", Settings.llm_concurrency)),
        data_dir=Path(e.get("DATA_DIR", "data")),
    )
