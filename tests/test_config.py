from pathlib import Path

from eventgraph.config import load_settings


def test_load_settings_reads_env(tmp_path):
    p = tmp_path / ".env"
    p.write_text(
        "DB_HOST=h\nDB_USER=u\nDB_PWD=p#x#\nDB_NAME=n\nDB_TABLENAME=t\nPG_DSN=postgresql://x\n"
        "MAX_DOCS=12\nLLM_CONCURRENCY=2\nDATA_DIR=d\nANTHROPIC_MODEL=m\nANTHROPIC_API_KEY=k\n",
        encoding="utf-8",
    )
    s = load_settings(p)
    assert s.db_table == "t" and s.max_docs == 12 and s.llm_concurrency == 2
    assert s.db_pwd == "p#x#"
    assert s.data_dir == Path("d") and s.anthropic_model == "m" and s.pg_dsn == "postgresql://x"


def test_load_settings_defaults(tmp_path):
    p = tmp_path / ".env"
    p.write_text("DB_HOST=h\nDB_USER=u\nDB_PWD=p\nDB_NAME=n\nDB_TABLENAME=t\n", encoding="utf-8")
    s = load_settings(p)
    assert s.max_docs == 300 and s.llm_concurrency == 5 and s.anthropic_model == "claude-haiku-5-5"
    assert s.anthropic_api_key == "" and s.data_dir == Path("data")
