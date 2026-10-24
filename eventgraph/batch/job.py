"""Stage C: PySpark batch — merge entities, aggregate relations/events, load PostgreSQL."""

from __future__ import annotations

import logging
from collections import Counter
from urllib.parse import urlparse

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession, Window

from ..config import Settings
from ..db import pg_connect
from .normalize import name_tokens, norm_key
from .spark import get_spark, read_extracted

log = logging.getLogger(__name__)

JACCARD_MERGE = 0.5
CLASS_ID = {
    "PERSON": 1,
    "ORG": 2,
    "LOCATION": 3,
    "COUNTRY": 4,
    "EVENT": 5,
}  # mirrors schema.sql seed

_AFF = T.StructType(
    [T.StructField("target_temp_id", T.StringType()), T.StructField("role", T.StringType())]
)
_ENT = T.StructType(
    [
        T.StructField("temp_id", T.StringType()),
        T.StructField("type", T.StringType()),
        T.StructField("name_ko", T.StringType()),
        T.StructField("surface_forms", T.ArrayType(T.StringType())),
        T.StructField("affiliations", T.ArrayType(_AFF)),
    ]
)
_REL = T.StructType(
    [
        T.StructField("src_temp_id", T.StringType()),
        T.StructField("dst_temp_id", T.StringType()),
        T.StructField("type", T.StringType()),
        T.StructField("raw_label", T.StringType()),
        T.StructField("evidence", T.StringType()),
        T.StructField("evidence_date", T.StringType()),
    ]
)
_EVT = T.StructType(
    [
        T.StructField("date", T.StringType()),
        T.StructField("summary_ko", T.StringType()),
        T.StructField("participants", T.ArrayType(T.StringType())),
        T.StructField("location_temp_id", T.StringType()),
        T.StructField("evidence", T.StringType()),
    ]
)
EXTRACTION_SCHEMA = T.StructType(
    [
        T.StructField("entities", T.ArrayType(_ENT)),
        T.StructField("relations", T.ArrayType(_REL)),
        T.StructField("events", T.ArrayType(_EVT)),
    ]
)

_MEMBER = T.StructType(
    [T.StructField("doc", T.LongType()), T.StructField("evidence", T.StringType())]
)
_CLUSTER = T.StructType(
    [
        T.StructField("summary_ko", T.StringType()),
        T.StructField("participants", T.ArrayType(T.LongType())),
        T.StructField("location_id", T.LongType()),
        T.StructField("members", T.ArrayType(_MEMBER)),
    ]
)


# ---------- pure helpers (driver side / UDF bodies) ----------


def _edit1(a: str, b: str) -> bool:
    """Levenshtein distance <= 1 (both at least 2 chars)."""
    if min(len(a), len(b)) < 2 or abs(len(a) - len(b)) > 1:
        return False
    if len(a) == len(b):
        return sum(x != y for x, y in zip(a, b, strict=True)) <= 1
    short, long_ = sorted((a, b), key=len)
    return any(long_[:i] + long_[i + 1 :] == short for i in range(len(long_)))


def canonical_keys(
    rows: list[tuple[str, str, list[str], list[str]]],
) -> dict[tuple[str, str], str]:
    """(type, key, alias_keys, name_tokens) -> mapping (type,key) -> canonical key. PERSON only:

    A. key equals an alias (surface form) of exactly one other key -> that key
    B. single-token name whose key is a suffix of exactly one longer key ('트럼프' -> '도널드트럼프')
    C. transliteration variant: same/suffix surname (last token) and first token within edit
       distance 1 ('마르코 루비오' ~ '마코 루비오'), exactly one candidate
    """
    keys_by_type: dict[str, set[str]] = {}
    aliases: dict[str, dict[str, set[str]]] = {}
    tokens: dict[tuple[str, str], list[str]] = {}
    for typ, key, als, toks in rows:
        keys_by_type.setdefault(typ, set()).add(key)
        tokens[(typ, key)] = toks
        for a in als:
            if a != key:
                aliases.setdefault(typ, {}).setdefault(a, set()).add(key)
    canon: dict[tuple[str, str], str] = {}
    for typ, keys in keys_by_type.items():
        for k in keys:
            target = k
            if typ == "PERSON":
                toks = tokens[(typ, k)]
                owners = aliases.get(typ, {}).get(k, set()) - {k}
                longer = (
                    [k2 for k2 in keys if k2 != k and len(k) >= 2 and k2.endswith(k)]
                    if len(toks) == 1
                    else []
                )
                variants = []
                if len(toks) >= 2:
                    for k2 in keys:
                        t2 = tokens[(typ, k2)]
                        if k2 == k or len(t2) < 2:
                            continue
                        last, last2 = toks[-1], t2[-1]
                        same_last = last == last2 or (
                            min(len(last), len(last2)) >= 3
                            and (last.endswith(last2) or last2.endswith(last))
                        )
                        # first token of the shorter name must match any non-surname token
                        # of the other ('압바스 아라그치' ~ '세예드 아바스 아라그치')
                        a_, b_ = (toks, t2) if len(toks) <= len(t2) else (t2, toks)
                        if same_last and any(a_[0] == t or _edit1(a_[0], t) for t in b_[:-1]):
                            variants.append(k2)
                if len(owners) == 1:
                    target = next(iter(owners))
                elif len(longer) == 1:
                    target = longer[0]
                elif len(variants) == 1 and (len(variants[0]), variants[0]) > (len(k), k):
                    target = variants[0]  # fold into the longer (tie: lexically larger) spelling
            canon[(typ, k)] = target
    _resolve_chains(canon)
    # rule B again against resolved targets: '갈리바프' with two spellings that merged into one
    for typ, keys in keys_by_type.items():
        if typ != "PERSON":
            continue
        for k in keys:
            if canon[(typ, k)] == k and len(tokens[(typ, k)]) == 1 and len(k) >= 2:
                targets = {canon[(typ, k2)] for k2 in keys if k2 != k and k2.endswith(k)}
                if len(targets) == 1:
                    canon[(typ, k)] = next(iter(targets))
    _resolve_chains(canon)
    return canon


def _resolve_chains(canon: dict[tuple[str, str], str]) -> None:
    for (typ, k), t in list(canon.items()):  # a -> b -> c  ==>  a -> c
        seen = {k}
        while canon.get((typ, t), t) != t and t not in seen:
            seen.add(t)
            t = canon[(typ, t)]
        canon[(typ, k)] = t


def cluster_events(events: list) -> list:
    """Greedy same-date merge: participant Jaccard >= JACCARD_MERGE (identical summary if both empty)."""
    # ponytail: O(n²) within one date; switch to MinHash if a day exceeds thousands of events
    clusters: list[dict] = []
    for ev in events:
        parts = set(ev["participants"] or [])
        hit = None
        for c in clusters:
            cp = c["participants"]
            if not parts and not cp:
                sim = 1.0 if ev["summary_ko"] == c["summary_ko"] else 0.0
            else:
                sim = len(parts & cp) / len(parts | cp) if (parts | cp) else 0.0
            if sim >= JACCARD_MERGE:
                hit = c
                break
        if hit is None:
            clusters.append(
                {
                    "summary_ko": ev["summary_ko"],
                    "participants": set(parts),
                    "location_id": ev["location_id"],
                    "members": [(ev["doc"], ev["evidence"])],
                }
            )
        else:
            hit["participants"] |= parts
            hit["location_id"] = (
                hit["location_id"] if hit["location_id"] is not None else ev["location_id"]
            )
            hit["members"].append((ev["doc"], ev["evidence"]))
    return [
        (c["summary_ko"], sorted(c["participants"]), c["location_id"], c["members"])
        for c in clusters
    ]


# ---------- the job ----------


def _jdbc_opts(settings: Settings) -> dict[str, str]:
    u = urlparse(settings.pg_dsn)
    return {
        "url": f"jdbc:postgresql://{u.hostname}:{u.port or 5432}{u.path}",
        "user": u.username or "",
        "password": u.password or "",
        "driver": "org.postgresql.Driver",
    }


def _read_pg(spark: SparkSession, settings: Settings, query: str) -> DataFrame:
    return spark.read.format("jdbc").options(**_jdbc_opts(settings), query=query).load()


def _write_pg(df: DataFrame, settings: Settings, table: str) -> int:
    n = df.count()
    df.write.format("jdbc").options(**_jdbc_opts(settings), dbtable=table).mode("append").save()
    log.info("loaded %s: %d rows", table, n)
    return n


def _next_id(settings: Settings, table: str) -> int:
    with pg_connect(settings) as conn:
        return conn.execute(f"select coalesce(max(id),0)+1 from {table}").fetchone()[0]


def _with_ids(df: DataFrame, start: int, order_cols: list[str]) -> DataFrame:
    # ponytail: single-partition row_number; fine for thousands of rows per run
    w = Window.orderBy(*order_cols)
    return df.withColumn("id", (F.row_number().over(w) + F.lit(start - 1)).cast("long"))


def run_batch(settings: Settings, run_id: str) -> dict:
    spark = get_spark()
    src = settings.data_dir / "runs" / run_id / "extracted"
    raw = read_extracted(spark, src)
    docs = _read_pg(
        spark,
        settings,
        f"select id as run_document_id, cast(writedate as date) as doc_date from run_document where run_id = '{run_id}'",
    )
    rtypes = {
        r["name"]: r["id"]
        for r in _read_pg(spark, settings, "select id, name from onto_relation_type").collect()
    }
    parsed = raw.select(
        "run_document_id", F.from_json("extraction", EXTRACTION_SCHEMA).alias("x")
    ).join(docs, "run_document_id")
    parsed.cache()
    n_docs = parsed.count()
    log.info("run_batch %s: %d extracted docs", run_id, n_docs)

    norm_udf = F.udf(norm_key, T.StringType())
    norm_list_udf = F.udf(
        lambda t, forms: [norm_key(t, f) for f in (forms or [])], T.ArrayType(T.StringType())
    )

    # --- entities per document (temp_id level) ---
    ex = (
        parsed.select("run_document_id", F.explode("x.entities").alias("e"))
        .select(
            "run_document_id",
            F.col("e.temp_id").alias("temp_id"),
            F.col("e.type").alias("type"),
            F.col("e.name_ko").alias("name_ko"),
            F.coalesce(F.col("e.surface_forms"), F.array()).alias("surface_forms"),
            F.coalesce(F.col("e.affiliations"), F.array()).alias("affiliations"),
        )
        .withColumn("key", norm_udf("type", "name_ko"))
    )
    ex.cache()

    # canonical key resolution on the driver (few thousand distinct keys at most)
    key_rows = (
        ex.select(
            "type",
            "key",
            "name_ko",
            norm_list_udf("type", "surface_forms").alias("aliases"),
        )
        .groupBy("type", "key")
        .agg(
            F.array_distinct(F.flatten(F.collect_list("aliases"))).alias("aliases"),
            F.min_by("name_ko", F.length("name_ko")).alias("name"),  # deterministic
        )
        .collect()
    )
    canon = canonical_keys(
        [(r["type"], r["key"], list(r["aliases"]), name_tokens(r["name"])) for r in key_rows]
    )
    merged = sum(1 for (t, k), c in canon.items() if k != c)
    log.info("entity merge: %d keys folded into others", merged)
    # ponytail: dict pickled into the UDF closure (thousands of keys); broadcast if it grows
    canon_udf = F.udf(lambda t, k: canon.get((t, k), k), T.StringType())
    ex = ex.withColumn("ckey", canon_udf("type", "key"))

    # representative name: most frequent name whose own key is canonical, else any; tie -> longer
    names = ex.groupBy("type", "ckey", "name_ko", "key").agg(F.count("*").alias("n"))
    w_name = Window.partitionBy("type", "ckey").orderBy(
        F.when(F.col("key") == F.col("ckey"), 0).otherwise(1),
        F.col("n").desc(),
        F.length("name_ko").desc(),
    )
    best = (
        names.withColumn("rn", F.row_number().over(w_name))
        .filter("rn = 1")
        .select("type", "ckey", "name_ko")
    )
    agg = ex.groupBy("type", "ckey").agg(
        F.countDistinct("run_document_id").alias("doc_count"),
        F.array_distinct(
            F.flatten(F.collect_list(F.array_union("surface_forms", F.array("name_ko"))))
        ).alias("surface_forms"),
    )
    ents = _with_ids(
        agg.join(best, ["type", "ckey"]),
        _next_id(settings, "entity"),
        ["type", "ckey"],
    ).select(
        "id",
        "type",
        "ckey",
        "name_ko",
        "surface_forms",
        "doc_count",
    )
    ents.cache()

    # temp_id -> entity id per document
    tmap = ex.join(
        ents.select("type", "ckey", F.col("id").alias("entity_id")), ["type", "ckey"]
    ).select("run_document_id", "temp_id", "entity_id", F.col("type").alias("etype"))
    tmap.cache()

    def _map(df: DataFrame, col: str, out: str) -> DataFrame:
        m = tmap.select(
            "run_document_id",
            F.col("temp_id").alias(col),
            F.col("entity_id").alias(out),
            F.col("etype").alias(out + "_type"),
        )
        return df.join(m, ["run_document_id", col], "inner")

    # --- affiliations ---
    aff = (
        ex.select("run_document_id", "temp_id", F.explode("affiliations").alias("a"))
        .select(
            "run_document_id",
            "temp_id",
            F.col("a.target_temp_id").alias("target_temp_id"),
            F.col("a.role").alias("role"),
        )
        .transform(lambda d: _map(d, "temp_id", "person_id"))
        .transform(lambda d: _map(d, "target_temp_id", "target_id"))
        .filter(F.col("person_id") != F.col("target_id"))
    )
    role_rank = Window.partitionBy("person_id", "target_id").orderBy(
        F.when(F.col("role") == "", 1).otherwise(0), F.col("rn").desc()
    )
    roles = (
        aff.groupBy("person_id", "target_id", "role")
        .agg(F.count("*").alias("rn"))
        .withColumn("rk", F.row_number().over(role_rank))
        .filter("rk = 1")
        .select("person_id", "target_id", "role")
    )
    aff_agg = aff.groupBy("person_id", "target_id").agg(
        F.countDistinct("run_document_id").alias("doc_count")
    )
    primary = Window.partitionBy("person_id").orderBy(F.col("doc_count").desc(), F.col("target_id"))
    affiliations = (
        aff_agg.join(roles, ["person_id", "target_id"])
        .withColumn("is_primary", F.row_number().over(primary) == 1)
        .withColumn("run_id", F.lit(run_id))
        .select("run_id", "person_id", "target_id", "role", "doc_count", "is_primary")
    )

    # --- relations ---
    rel_rows = (
        parsed.select("run_document_id", "doc_date", F.explode("x.relations").alias("r"))
        .select(
            "run_document_id",
            "doc_date",
            F.col("r.src_temp_id").alias("src_temp_id"),
            F.col("r.dst_temp_id").alias("dst_temp_id"),
            F.col("r.type").alias("rtype"),
            F.coalesce(F.col("r.raw_label"), F.lit("")).alias("raw_label"),
            F.col("r.evidence").alias("sentence"),
            F.to_date("r.evidence_date").alias("evidence_date"),
        )
        .transform(lambda d: _map(d, "src_temp_id", "src_id"))
        .transform(lambda d: _map(d, "dst_temp_id", "dst_id"))
        .withColumn("ev_date", F.coalesce("evidence_date", "doc_date"))
        .filter(F.col("src_id") != F.col("dst_id"))
    )
    type_map = F.create_map(*[x for kv in rtypes.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
    rel_rows = rel_rows.withColumn(
        "type_id", F.coalesce(type_map[F.col("rtype")], F.lit(rtypes["OTHER"]))
    )
    rel_rows.cache()

    label_rank = Window.partitionBy("type_id", "src_id", "dst_id").orderBy(
        F.when(F.col("raw_label") == "", 1).otherwise(0), F.col("n").desc()
    )
    labels = (
        rel_rows.groupBy("type_id", "src_id", "dst_id", "raw_label")
        .agg(F.count("*").alias("n"))
        .withColumn("rk", F.row_number().over(label_rank))
        .filter("rk = 1")
        .select("type_id", "src_id", "dst_id", "raw_label")
    )
    rel_agg = rel_rows.groupBy("type_id", "src_id", "dst_id").agg(
        F.countDistinct("run_document_id").alias("doc_count"),
        F.min("ev_date").alias("date_min"),
        F.max("ev_date").alias("date_max"),
    )
    relations = _with_ids(
        rel_agg.join(labels, ["type_id", "src_id", "dst_id"]),
        _next_id(settings, "relation"),
        ["type_id", "src_id", "dst_id"],
    ).withColumn("run_id", F.lit(run_id))
    relations.cache()
    rel_evidence = rel_rows.join(
        relations.select("type_id", "src_id", "dst_id", F.col("id").alias("relation_id")),
        ["type_id", "src_id", "dst_id"],
    ).select(
        F.lit(run_id).alias("run_id"),
        "relation_id",
        F.lit(None).cast("long").alias("event_id"),
        "run_document_id",
        "sentence",
        "evidence_date",
    )

    # degree = distinct neighbours over relations
    nb = relations.select(F.col("src_id").alias("eid"), F.col("dst_id").alias("nid")).union(
        relations.select(F.col("dst_id").alias("eid"), F.col("src_id").alias("nid"))
    )
    degree = nb.filter("eid <> nid").groupBy("eid").agg(F.countDistinct("nid").alias("degree"))
    class_map = F.create_map(*[x for kv in CLASS_ID.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
    entities = ents.join(degree, ents.id == degree.eid, "left").select(
        "id",
        F.lit(run_id).alias("run_id"),
        class_map[F.col("type")].cast("short").alias("class_id"),
        "name_ko",
        F.col("ckey").alias("norm_key"),
        "surface_forms",
        "doc_count",
        F.coalesce("degree", F.lit(0)).cast("int").alias("degree"),
    )

    # --- events ---
    ev_rows = parsed.select("run_document_id", F.posexplode("x.events").alias("pos", "e")).select(
        "run_document_id",
        "pos",
        F.to_date("e.date").alias("date"),
        F.col("e.summary_ko").alias("summary_ko"),
        F.coalesce(F.col("e.participants"), F.array()).alias("participants"),
        F.col("e.location_temp_id").alias("location_temp_id"),
        F.col("e.evidence").alias("evidence"),
    )
    parts = (
        ev_rows.select("run_document_id", "pos", F.explode("participants").alias("temp_id"))
        .transform(lambda d: _map(d, "temp_id", "pid"))
        .groupBy("run_document_id", "pos")
        .agg(F.collect_set("pid").alias("pids"))
    )
    locs = (
        ev_rows.filter(F.col("location_temp_id").isNotNull())
        .select("run_document_id", "pos", F.col("location_temp_id").alias("temp_id"))
        .transform(lambda d: _map(d, "temp_id", "location_id"))
        .select("run_document_id", "pos", "location_id")
    )
    ev_mapped = (
        ev_rows.join(parts, ["run_document_id", "pos"], "left")
        .join(locs, ["run_document_id", "pos"], "left")
        .select(
            "date",
            F.struct(
                F.col("run_document_id").alias("doc"),
                "summary_ko",
                F.coalesce("pids", F.array().cast("array<long>")).alias("participants"),
                "location_id",
                "evidence",
            ).alias("ev"),
        )
    )
    cluster_udf = F.udf(
        lambda evs: cluster_events([e.asDict() for e in evs]), T.ArrayType(_CLUSTER)
    )
    clusters = (
        ev_mapped.groupBy("date")
        .agg(F.collect_list("ev").alias("evs"))
        .select("date", F.explode(cluster_udf("evs")).alias("c"))
        .select(
            "date",
            F.col("c.summary_ko").alias("summary_ko"),
            F.col("c.participants").alias("participants"),
            F.col("c.location_id").alias("location_id"),
            F.col("c.members").alias("members"),
        )
    )
    events = _with_ids(clusters, _next_id(settings, "event"), ["date", "summary_ko"])
    events.cache()
    event_rows = events.select(
        "id",
        F.lit(run_id).alias("run_id"),
        "date",
        "summary_ko",
        "location_id",
        F.size(F.array_distinct(F.transform("members", lambda m: m["doc"]))).alias("doc_count"),
    )
    participants = events.select(
        F.col("id").alias("event_id"), F.explode("participants").alias("entity_id")
    )
    ev_evidence = events.select(
        F.col("id").alias("event_id"), F.explode("members").alias("m")
    ).select(
        F.lit(run_id).alias("run_id"),
        F.lit(None).cast("long").alias("relation_id"),
        "event_id",
        F.col("m.doc").alias("run_document_id"),
        F.col("m.evidence").alias("sentence"),
        F.lit(None).cast("date").alias("evidence_date"),
    )

    # --- observability: OTHER labels and class combos ---
    other_id = rtypes["OTHER"]
    other_n = rel_rows.filter(F.col("type_id") == other_id).count()
    combos = Counter(
        (r["rtype"], r["src_id_type"], r["dst_id_type"])
        for r in rel_rows.select("rtype", "src_id_type", "dst_id_type").collect()
    )
    log.info("relation (type, src_class, dst_class) counts: %s", dict(combos.most_common(30)))
    if other_n:
        top = (
            rel_rows.filter(F.col("type_id") == other_id)
            .groupBy("raw_label")
            .count()
            .orderBy(F.desc("count"))
            .limit(15)
            .collect()
        )
        log.info("OTHER raw_labels: %s", [(r["raw_label"], r["count"]) for r in top])

    # --- load: delete run_id rows, then append in dependency order ---
    with pg_connect(settings) as conn:
        conn.execute("delete from evidence where run_id=%s", (run_id,))
        conn.execute(
            "delete from event_participant where event_id in (select id from event where run_id=%s)",
            (run_id,),
        )
        conn.execute("delete from event where run_id=%s", (run_id,))
        conn.execute("delete from relation where run_id=%s", (run_id,))
        conn.execute("delete from affiliation where run_id=%s", (run_id,))
        conn.execute("delete from entity where run_id=%s", (run_id,))
        conn.commit()
    counts = {
        "entities": _write_pg(entities, settings, "entity"),
        "affiliations": _write_pg(affiliations, settings, "affiliation"),
        "relations": _write_pg(
            relations.select(
                "id",
                "run_id",
                "type_id",
                "src_id",
                "dst_id",
                "raw_label",
                "doc_count",
                "date_min",
                "date_max",
            ),
            settings,
            "relation",
        ),
        "events": _write_pg(event_rows, settings, "event"),
        "event_participants": _write_pg(participants, settings, "event_participant"),
        "evidence": _write_pg(rel_evidence.union(ev_evidence), settings, "evidence"),
        "other_relations": other_n,
        "merged_keys": merged,
    }
    with pg_connect(settings) as conn:  # keep sequences ahead of explicitly assigned ids
        for t in ("entity", "relation", "event", "evidence"):
            conn.execute(f"select setval('{t}_id_seq', (select coalesce(max(id),1) from {t}))")
        conn.commit()
    if counts["entities"] == 0:
        log.warning("run_batch %s loaded ZERO entities", run_id)
    log.info("run_batch %s done: %s", run_id, counts)
    spark.catalog.clearCache()
    return counts
