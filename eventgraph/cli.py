"""python -m eventgraph {migrate|run|rerun|serve}"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from .config import load_settings
from .migrate import migrate
from .pipeline import STAGES, create_run, run_pipeline


def _csv(s: str | None) -> list[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def _print_summary(run_id: str, results: dict) -> None:
    print(f"\nrun {run_id}")
    for stage, r in results.items():
        line = ", ".join(f"{k}={v}" for k, v in r.items() if k != "keywords")
        print(f"  {stage:8s} {line}")


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    p = argparse.ArgumentParser(prog="eventgraph")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate")
    r = sub.add_parser("run")
    r.add_argument("--event", required=True)
    r.add_argument("--from", dest="date_from", required=True, type=date.fromisoformat)
    r.add_argument("--to", dest="date_to", required=True, type=date.fromisoformat)
    r.add_argument("--persons", default="")
    r.add_argument("--orgs", default="")
    r.add_argument("--countries", default="")
    r.add_argument("--stages", default=",".join(STAGES))
    rr = sub.add_parser("rerun")
    rr.add_argument("run_id")
    rr.add_argument("--stages", default="batch")
    sv = sub.add_parser("serve")
    sv.add_argument("--port", type=int, default=8000)
    a = p.parse_args(argv)
    settings = load_settings()

    if a.cmd == "migrate":
        migrate(settings)
        print("migrated")
    elif a.cmd == "run":
        migrate(settings)
        rid = create_run(
            settings,
            a.event,
            a.date_from,
            a.date_to,
            _csv(a.persons),
            _csv(a.orgs),
            _csv(a.countries),
        )
        _print_summary(rid, run_pipeline(settings, rid, tuple(_csv(a.stages))))
    elif a.cmd == "rerun":
        _print_summary(a.run_id, run_pipeline(settings, a.run_id, tuple(_csv(a.stages))))
    elif a.cmd == "serve":
        import uvicorn

        uvicorn.run("eventgraph.api:app", host="127.0.0.1", port=a.port)
    return 0


if __name__ == "__main__":
    sys.exit(main())
