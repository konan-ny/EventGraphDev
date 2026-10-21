import pytest

from eventgraph.config import load_settings
from eventgraph.db import pg_connect

TEST_RUN_PREFIX = "test-"
RUN_TABLES = ["event_participant", "evidence", "event", "relation", "affiliation", "entity", "run_document", "run"]


@pytest.fixture(scope="session")
def settings():
    return load_settings()


@pytest.fixture
def pg(settings):
    conn = pg_connect(settings)
    conn.autocommit = True
    yield conn
    with conn.cursor() as cur:
        cur.execute("select to_regclass('run')")
        if cur.fetchone()[0] is not None:
            for t in RUN_TABLES:
                if t == "event_participant":
                    cur.execute("delete from event_participant where event_id in (select id from event where run_id like %s)", (TEST_RUN_PREFIX + "%",))
                else:
                    cur.execute(f"delete from {t} where run_id like %s", (TEST_RUN_PREFIX + "%",))
    conn.close()
