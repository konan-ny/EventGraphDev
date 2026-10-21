import pytest

from eventgraph.migrate import migrate


@pytest.mark.integration
def test_migrate_creates_tables_and_seed(settings, pg):
    migrate(settings)
    migrate(settings)  # idempotent
    assert pg.execute("select count(*) from onto_relation_type").fetchone()[0] == 16
    assert pg.execute("select count(*) from onto_class").fetchone()[0] == 5
    row = pg.execute(
        "select to_regclass('relation'), to_regclass('evidence'), to_regclass('event_participant'), to_regclass('run_document')"
    ).fetchone()
    assert None not in row
    cols = {
        r[0]
        for r in pg.execute(
            "select column_name from information_schema.columns where table_name='relation'"
        ).fetchall()
    }
    assert {"date_min", "date_max", "raw_label", "doc_count"} <= cols
    cols = {
        r[0]
        for r in pg.execute(
            "select column_name from information_schema.columns where table_name='run'"
        ).fetchall()
    }
    assert {
        "error",
        "expanded_keywords",
        "tokens_in",
        "tokens_out",
        "doc_failed",
    } <= cols
