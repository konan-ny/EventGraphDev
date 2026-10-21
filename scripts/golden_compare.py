"""Compare a run's extraction + merged entities against a hand-written answer key.

usage: uv run python scripts/golden_compare.py <run_id> <answers.jsonl> [--out result.md]

Per document (from the run's Parquet): entity recall by class, relation accuracy.
Corpus level (from PostgreSQL): split persons (one answer person -> 2+ entity rows) and
wrong merges (one entity row -> 2+ distinct answer persons).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eventgraph.batch.normalize import norm_key
from eventgraph.config import load_settings
from eventgraph.db import pg_connect

CLASSES = {"persons": "PERSON", "orgs": "ORG", "countries": "COUNTRY"}


def same(a: str, b: str, typ: str) -> bool:
    """Equal keys, or a single-token name equal to the other name's last token (surname)."""
    ka, kb = norm_key(typ, a), norm_key(typ, b)
    if ka == kb:
        return True
    for one, other in ((a, b), (b, a)):
        toks = one.split()
        if len(toks) == 1 and len(ka if one is a else kb) >= 2:
            k1 = norm_key(typ, one)
            if norm_key(typ, other.split()[-1]) == k1:
                return True
    return False


def find(name: str, typ: str, pool: list[tuple[str, list[str]]]) -> int | None:
    for i, (n, forms) in enumerate(pool):
        if same(name, n, typ) or any(norm_key(typ, f) == norm_key(typ, name) for f in forms):
            return i
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_id")
    ap.add_argument("answers")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    s = load_settings()
    answers = [
        json.loads(line)
        for line in Path(a.answers).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    table = pq.read_table(s.data_dir / "runs" / a.run_id / "extracted").to_pylist()
    by_doc = {r["run_document_id"]: json.loads(r["extraction"]) for r in table}

    lines = [f"# 골든 세트 비교 — run `{a.run_id}`", ""]
    hit = defaultdict(int)
    tot = defaultdict(int)
    rel_hit = rel_tot = 0
    missed: list[str] = []
    rel_missed: list[str] = []
    for ans in answers:
        x = by_doc.get(ans["run_document_id"])
        if x is None:
            lines.append(f"- doc {ans['run_document_id']}: 추출 결과 없음")
            continue
        ents = [(e["name_ko"], e.get("surface_forms", []), e["type"]) for e in x["entities"]]
        for key, typ in CLASSES.items():
            pool = [(n, f) for n, f, t in ents if t == typ]
            for name in ans.get(key, []):
                tot[typ] += 1
                if find(name, typ, pool) is not None:
                    hit[typ] += 1
                else:
                    missed.append(f"doc {ans['run_document_id']} {typ} `{name}`")
        tid = {
            e["temp_id"]: (e["name_ko"], e.get("surface_forms", []), e["type"])
            for e in x["entities"]
        }
        rels = [
            (tid[r["src_temp_id"]], tid[r["dst_temp_id"]], r["type"])
            for r in x["relations"]
            if r["src_temp_id"] in tid and r["dst_temp_id"] in tid
        ]
        for r in ans.get("relations", []):
            rel_tot += 1
            ok = any(
                (same(r["src"], sn, st) or any(same(r["src"], f, st) for f in sf))
                and (same(r["dst"], dn, dt) or any(same(r["dst"], f, dt) for f in df))
                and (r["type"] == "OTHER" or rt == r["type"])
                for (sn, sf, st), (dn, df, dt), rt in rels
            )
            rel_hit += ok
            if not ok:
                rel_missed.append(
                    f"doc {ans['run_document_id']} {r['src']} -{r['type']}-> {r['dst']}"
                )

    lines += [
        "## 개체 재현율 (문서 단위, 정답 기준)",
        "",
        "| 클래스 | 정답 | 적중 | 재현율 |",
        "|---|---|---|---|",
    ]
    for typ in CLASSES.values():
        lines.append(
            f"| {typ} | {tot[typ]} | {hit[typ]} | {hit[typ] / tot[typ]:.0%} |"
            if tot[typ]
            else f"| {typ} | 0 | 0 | - |"
        )
    lines += [
        "",
        f"**관계 정확도**: {rel_hit}/{rel_tot} = {rel_hit / rel_tot:.0%}"
        if rel_tot
        else "관계 정답 없음",
        "",
    ]

    # merge quality on the loaded graph
    with pg_connect(s) as conn:
        rows = conn.execute(
            "select e.id, e.name_ko, e.surface_forms from entity e join onto_class c on c.id=e.class_id"
            " where e.run_id=%s and c.name='PERSON'",
            (a.run_id,),
        ).fetchall()
    persons = sorted({p for ans in answers for p in ans.get("persons", [])})
    splits, merges = [], []
    owner: dict[int, set[str]] = defaultdict(set)
    for p in persons:
        kp = norm_key("PERSON", p)
        matches = [
            (rid, n)
            for rid, n, forms in rows
            if same(p, n, "PERSON") or any(norm_key("PERSON", f) == kp for f in forms)
        ]
        for rid, _ in matches:
            owner[rid].add(p)
        if len(matches) > 1:
            splits.append(f"`{p}` → " + ", ".join(f"{n}(#{rid})" for rid, n in matches))
    for rid, names in owner.items():
        if len(names) > 1:
            n = next(nm for r, nm, _ in rows if r == rid)
            merges.append(f"#{rid} `{n}` ← " + ", ".join(sorted(names)))
    lines += [
        "## 병합 품질 (PostgreSQL entity, PERSON)",
        "",
        f"- 쪼개짐(정답 1인 → 노드 2개 이상): {len(splits)}건",
    ]
    lines += [f"  - {x}" for x in splits]
    lines += [f"- 잘못 합침(노드 1개 ← 정답 2인 이상): {len(merges)}건"]
    lines += [f"  - {x}" for x in merges]
    lines += ["", "## 놓친 개체", ""] + [f"- {m}" for m in missed]
    lines += ["", "## 놓친 관계", ""] + [f"- {m}" for m in rel_missed]
    out = "\n".join(lines) + "\n"
    sys.stdout.reconfigure(encoding="utf-8")
    print(out)
    if a.out:
        Path(a.out).write_text(out, encoding="utf-8")


if __name__ == "__main__":
    main()
