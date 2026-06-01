#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Hudi BATCH vector search demo — certifies `hudi_vector_search_batch` at non-
trivial scale against a numpy ground-truth oracle.

Sibling to `hudi_sql_vector_blob_demo.py` (single-query). Same dataset, same
embedding model, same Hudi DDL — but the search step is the **table-to-table**
batch TVF described in RFC-102:

  SELECT *
  FROM hudi_vector_search_batch(
      'pets_batch_corpus_<format>',  'embedding',
      'pets_batch_queries_<format>', 'embedding',
      k, 'cosine')

Flow:
  1. Load N_CORPUS + N_QUERIES Oxford-IIIT Pet images.
  2. Generate L2-normalized embeddings with `mobilenetv3_small_100`.
  3. Split into corpus (N_CORPUS) + held-out queries (N_QUERIES).
  4. Stage both via PyArrow, write each to its own Hudi table.
  5. Run `hudi_vector_search_batch` and collect results.
  6. **Oracle validation:** compute the cosine distance matrix in numpy from
     the same embeddings and assert the TVF's top-k per query matches.
  7. Render a result panel (one row per query showing its top-k matches).

Env vars:
  HUDI_BUNDLE_JAR         (defaults to ~/Downloads/hudi-spark3.5-bundle_2.12-1.2.0-rc1.jar)
  HUDI_BASE_FILE_FORMAT   (default 'lance'; set to 'parquet' to use Parquet)
  LANCE_BUNDLE_JAR        (defaults to ~/Downloads/lance-spark-bundle-3.5_2.12-0.4.0.jar; only used when HUDI_BASE_FILE_FORMAT=lance)
  HUDI_BATCH_N_CORPUS     (default 1000; rows in the corpus Hudi table)
  HUDI_BATCH_N_QUERIES    (default 20;   rows in the query Hudi table)
  HUDI_BATCH_TOP_K        (default 5)
  PYSPARK_DRIVER_MEMORY   (default '4g')
  HUDI_LANCE_DEMO_OUTDIR  (default './outputs')
"""

import io
import os
import shutil
import sys
from pathlib import Path

# MUST run before any `pyspark` import — local-mode driver heap is fixed at
# JVM launch time and cannot be raised via SparkSession.config() later.
_driver_mem = os.getenv("PYSPARK_DRIVER_MEMORY", "4g")
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    f"--driver-memory {_driver_mem} --conf spark.driver.maxResultSize=2g pyspark-shell",
)

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import torch
import timm
from sklearn.preprocessing import normalize
from PIL import Image

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from torchvision.datasets import OxfordIIITPet  # noqa: E402

from pyspark.sql import SparkSession


# ======================================================
# CONFIGURATION
# ======================================================

_file_format = os.getenv("HUDI_BASE_FILE_FORMAT", "lance").lower()
if _file_format not in ("lance", "parquet"):
    sys.exit(f"ERROR: HUDI_BASE_FILE_FORMAT must be 'lance' or 'parquet', got '{_file_format}'")

CONFIG = {
    "dataset": "OxfordIIITPet",
    "base_file_format": _file_format,
    "corpus_table_path": f"/tmp/hudi_batch_corpus_{_file_format}_pets",
    "corpus_table_name": f"pets_batch_corpus_{_file_format}",
    "queries_table_path": f"/tmp/hudi_batch_queries_{_file_format}_pets",
    "queries_table_name": f"pets_batch_queries_{_file_format}",
    "n_corpus": int(os.getenv("HUDI_BATCH_N_CORPUS", "1000")),
    "n_queries": int(os.getenv("HUDI_BATCH_N_QUERIES", "20")),
    "top_k": int(os.getenv("HUDI_BATCH_TOP_K", "5")),
    "embedding_model": "mobilenetv3_small_100",
    "output_dir": os.getenv("HUDI_LANCE_DEMO_OUTDIR", "./outputs"),
    "panel_filename": f"hudi_vector_search_batch_{_file_format}_results.png",
    "log_level": "ERROR",
    "hide_progress": True,
    # Oracle tolerance: cosine distance computed on L2-normalized float32 vectors
    # in numpy vs JVM-side DenseVector(Double) UDF. Float32 → Float64 widening +
    # different summation orders allow ~1e-5 deltas.
    "oracle_distance_tol": 1e-5,
}

BLOB_REFERENCE_CAST = (
    "struct<external_path:string,offset:bigint,length:bigint,managed:boolean>"
)


# ======================================================
# UTILITIES
# ======================================================

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def wipe_prior_state() -> None:
    """
    Remove this script's prior table dirs and staging Parquets so re-runs are
    idempotent. `DROP TABLE IF EXISTS` (run inside `create_hudi_table_sql`) only
    removes the catalog entry — the data dir and `.hoodie/` timeline at
    LOCATION persist, so a re-run would query stale rows alongside fresh ones
    and the oracle would (correctly) flag the mismatch.
    """
    targets = [
        CONFIG["corpus_table_path"],
        CONFIG["queries_table_path"],
        f"/tmp/staging_pets_batch_corpus_{CONFIG['base_file_format']}.parquet",
        f"/tmp/staging_pets_batch_queries_{CONFIG['base_file_format']}.parquet",
    ]
    for t in targets:
        p = Path(t)
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        elif p.is_file():
            p.unlink(missing_ok=True)
    # Catalog warehouse from prior runs in this cwd.
    shutil.rmtree("spark-warehouse", ignore_errors=True)


def save_png_bytes(img_bytes: bytes, path: Path) -> None:
    ensure_dir(path.parent)
    with open(path, "wb") as f:
        f.write(img_bytes)


def default_hudi_bundle_jar() -> str:
    return str(Path.home() / "Downloads" / "hudi-spark3.5-bundle_2.12-1.2.0-rc1.jar")


def default_lance_bundle_jar() -> str:
    return str(Path.home() / "Downloads" / "lance-spark-bundle-3.5_2.12-0.4.0.jar")


def resolve_jars() -> str:
    hudi_jar = os.getenv("HUDI_BUNDLE_JAR", default_hudi_bundle_jar())
    if not Path(hudi_jar).is_file():
        sys.exit(
            f"ERROR: HUDI_BUNDLE_JAR does not exist at {hudi_jar}\n"
            "Download the Apache 1.2.0-rc1 staging jar with:\n"
            "  curl -L -o ~/Downloads/hudi-spark3.5-bundle_2.12-1.2.0-rc1.jar \\\n"
            "    https://repository.apache.org/content/repositories/orgapachehudi-1176/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.2.0-rc1/hudi-spark3.5-bundle_2.12-1.2.0-rc1.jar\n"
            "or set HUDI_BUNDLE_JAR=/abs/path/to/locally-built.jar."
        )
    if CONFIG["base_file_format"] != "lance":
        return hudi_jar
    lance_jar = os.getenv("LANCE_BUNDLE_JAR", default_lance_bundle_jar())
    if not Path(lance_jar).is_file():
        sys.exit(
            f"ERROR: LANCE_BUNDLE_JAR does not exist at {lance_jar}\n"
            "Download the Lance 0.4.0 bundle from Maven Central with:\n"
            "  curl -L -o ~/Downloads/lance-spark-bundle-3.5_2.12-0.4.0.jar \\\n"
            "    https://repo1.maven.org/maven2/com/lancedb/lance-spark-bundle-3.5_2.12/0.4.0/lance-spark-bundle-3.5_2.12-0.4.0.jar\n"
            "or set LANCE_BUNDLE_JAR=/abs/path/to/jar."
        )
    return f"{hudi_jar},{lance_jar}"


# ======================================================
# 1. SPARK SESSION SETUP
# ======================================================

def create_spark() -> SparkSession:
    jars = resolve_jars()
    builder = (
        SparkSession.builder.appName("Hudi-Vector-Search-Batch-Demo")
        .config("spark.jars", jars)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(
            "spark.sql.extensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("hoodie.read.blob.inline.mode", "CONTENT")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.shuffle.partitions", "2")
    )
    if CONFIG.get("hide_progress", True):
        builder = builder.config("spark.ui.showConsoleProgress", "false")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(CONFIG.get("log_level", "ERROR"))
    return spark


# ======================================================
# 2. LOAD DATASET (Oxford-IIIT Pet)
# ======================================================

def load_dataset(n_samples):
    print(f"Loading dataset: Oxford-IIIT Pet ({n_samples} samples)...")
    root = os.path.expanduser("~/.cache/torchvision")
    ds = OxfordIIITPet(root=root, split="trainval", download=True)
    class_names = ds.classes

    rng = np.random.default_rng()
    n = min(n_samples, len(ds))
    indices = rng.choice(len(ds), size=n, replace=False)

    data = []
    for idx in indices:
        img, label = ds[int(idx)]
        img = img.convert("RGB")
        bio = io.BytesIO()
        img.save(bio, format="PNG")
        img_bytes = bio.getvalue()
        w, h = img.size
        category = class_names[label] if isinstance(class_names, list) else str(label)
        safe_category = category.replace("/", "_")
        data.append(
            {
                "image_id": f"pets_{int(idx):06d}",
                "category": category,
                "category_sanitized": safe_category,
                "label": int(label),
                "description": f"{category} from Oxford-IIIT Pet",
                "image_bytes_raw": img_bytes,
                "width": int(w),
                "height": int(h),
            }
        )
    print(f"✓ Loaded {len(data)} images")
    return data, class_names


# ======================================================
# 3. EMBEDDING MODEL (timm)
# ======================================================

def create_embedding_model():
    print(f"Loading embedding model: {CONFIG['embedding_model']}...")
    model = timm.create_model(CONFIG["embedding_model"], pretrained=True, num_classes=0)
    model.eval()
    data_config = timm.data.resolve_model_data_config(model)
    transform = timm.data.create_transform(**data_config, is_training=False)
    print("✓ Model loaded")
    return model, transform


def generate_embeddings(data, model, transform):
    print(f"Generating embeddings for {len(data)} images...")
    images = []
    for item in data:
        img = Image.open(io.BytesIO(item["image_bytes_raw"])).convert("RGB")
        images.append(transform(img))
    batch = torch.stack(images)
    with torch.no_grad():
        feats = model(batch).detach().cpu().numpy()
    feats = normalize(feats)
    for i, item in enumerate(data):
        item["embedding"] = feats[i].tolist()
    print(f"✓ Generated embeddings (dimension: {feats.shape[1]})")
    return data, int(feats.shape[1])


# ======================================================
# 4. SPLIT corpus + queries
# ======================================================

def split_corpus_and_queries(data):
    """
    First N_CORPUS rows → corpus, last N_QUERIES rows → queries. Order is
    already randomized by `load_dataset`'s `rng.choice(replace=False)`, so a
    simple slice gives two disjoint subsets.

    We hold both subsets in Python so the oracle (Step 7) can recompute the
    distance matrix from the exact same embeddings that were written to Hudi.
    """
    n_corpus = CONFIG["n_corpus"]
    n_queries = CONFIG["n_queries"]
    if len(data) < n_corpus + n_queries:
        sys.exit(
            f"ERROR: requested n_corpus={n_corpus} + n_queries={n_queries}={n_corpus + n_queries} "
            f"rows but dataset returned only {len(data)}"
        )
    corpus = data[:n_corpus]
    queries = data[n_corpus : n_corpus + n_queries]

    # Sanity: image_ids must be disjoint (no row appears in both tables).
    corpus_ids = {r["image_id"] for r in corpus}
    query_ids = {r["image_id"] for r in queries}
    assert corpus_ids.isdisjoint(query_ids), "corpus and queries overlap"
    print(f"✓ Split: {len(corpus)} corpus rows, {len(queries)} query rows (disjoint)")
    return corpus, queries


# ======================================================
# 5. STAGE → PARQUET → TEMP VIEW (PyArrow, bypassing PythonRDD)
# ======================================================

def stage_to_parquet_with_pyarrow(data, embedding_dim: int, staging_path: str) -> None:
    arrow_schema = pa.schema(
        [
            pa.field("image_id", pa.string(), nullable=False),
            pa.field("category", pa.string(), nullable=False),
            pa.field("category_sanitized", pa.string(), nullable=False),
            pa.field("label", pa.int32(), nullable=False),
            pa.field("description", pa.string(), nullable=True),
            pa.field("image_bytes_raw", pa.binary(), nullable=False),
            pa.field("width", pa.int32(), nullable=False),
            pa.field("height", pa.int32(), nullable=False),
            pa.field(
                "embedding",
                pa.list_(
                    pa.field("element", pa.float32(), nullable=False),
                    list_size=embedding_dim,
                ),
                nullable=False,
            ),
        ]
    )
    columns = {
        "image_id": [d["image_id"] for d in data],
        "category": [d["category"] for d in data],
        "category_sanitized": [d["category_sanitized"] for d in data],
        "label": [int(d["label"]) for d in data],
        "description": [d.get("description") for d in data],
        "image_bytes_raw": [d["image_bytes_raw"] for d in data],
        "width": [int(d["width"]) for d in data],
        "height": [int(d["height"]) for d in data],
        "embedding": [d["embedding"] for d in data],
    }
    pq.write_table(pa.table(columns, schema=arrow_schema), staging_path)


def register_staging_view(spark, data, embedding_dim, view_name, staging_path):
    print(f"Staging Python data → Parquet at {staging_path} (PyArrow, no Spark)...")
    stage_to_parquet_with_pyarrow(data, embedding_dim, staging_path)
    spark.read.parquet(staging_path).createOrReplaceTempView(view_name)
    print(f"✓ Registered Spark temp view: {view_name}")


# ======================================================
# 6. CREATE TABLE + INSERT — SQL  (run twice: corpus + queries)
# ======================================================

def create_hudi_table_sql(spark, embedding_dim, table_name, table_path):
    print(f"\nDDL: CREATE TABLE {table_name} ...  [{CONFIG['base_file_format']} base files]")
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    ddl = f"""
        CREATE TABLE {table_name} (
            image_id            STRING,
            category            STRING,
            category_sanitized  STRING,
            label               INT,
            description         STRING,
            image_bytes         BLOB           COMMENT 'Pet image bytes (INLINE)',
            width               INT,
            height              INT,
            embedding           VECTOR({embedding_dim})
                                               COMMENT 'Image embedding for ANN search'
        ) USING hudi
        PARTITIONED BY (category_sanitized)
        LOCATION '{table_path}'
        TBLPROPERTIES (
            primaryKey = 'image_id',
            preCombineField = 'image_id',
            type = 'cow',
            'hoodie.table.base.file.format' = '{CONFIG['base_file_format']}',
            'hoodie.write.record.merge.custom.implementation.classes' = 'org.apache.hudi.DefaultSparkRecordMerger'
        )
    """
    spark.sql(ddl)
    print(f"✓ Created table {table_name} at {table_path}")


def insert_into_hudi_sql(spark, table_name, staging_view):
    print(f"\nDML: INSERT INTO {table_name} SELECT ... FROM {staging_view}")
    insert = f"""
        INSERT INTO {table_name}
        SELECT
            image_id,
            category,
            category_sanitized,
            label,
            description,
            named_struct(
                'type',      'INLINE',
                'data',      image_bytes_raw,
                'reference', cast(null as {BLOB_REFERENCE_CAST})
            ) AS image_bytes,
            width,
            height,
            embedding
        FROM {staging_view}
    """
    spark.sql(insert)
    count = spark.sql(f"SELECT COUNT(image_id) AS c FROM {table_name}").collect()[0]["c"]
    print(f"✓ Inserted {count} records into {table_name}")


# ======================================================
# 7. BATCH SEARCH — `hudi_vector_search_batch` TVF
# ======================================================

def run_batch_search_sql(spark):
    """
    Run the batch TVF. Both tables share column names ({image_id, category,
    category_sanitized, label, description, image_bytes, width, height,
    embedding}), so every non-embedding column on the query side gets the
    `_hudi_query_` prefix per HoodieVectorSearchPlanBuilder.scala (the
    clashing-column rename path is automatically exercised).
    """
    k = CONFIG["top_k"]
    corpus = CONFIG["corpus_table_name"]
    queries = CONFIG["queries_table_name"]
    print(f"\nDQL: hudi_vector_search_batch(corpus={corpus}, queries={queries}, k={k}, cosine)")
    sql = f"""
        SELECT
            image_id              AS corpus_image_id,
            category              AS corpus_category,
            image_bytes           AS corpus_image_bytes,
            _hudi_query_image_id  AS query_image_id,
            _hudi_query_category  AS query_category,
            _hudi_distance,
            _hudi_query_index
        FROM hudi_vector_search_batch(
            '{corpus}',
            'embedding',
            '{queries}',
            'embedding',
            {k},
            'cosine'
        )
        ORDER BY _hudi_query_index, _hudi_distance
    """
    print(sql.strip())
    rows = spark.sql(sql).collect()
    print(f"✓ TVF returned {len(rows)} rows ({CONFIG['n_queries']} queries × {k} matches)")
    return rows


# ======================================================
# 8. ORACLE VALIDATION (numpy) — the certification step
# ======================================================

def oracle_validate_with_numpy(corpus, queries, tvf_rows, k):
    """
    Re-derive top-k from the exact same Python-side embeddings and confirm the
    TVF agrees. This is the load-bearing assertion of the script — if it
    passes, batch mode is correct on this dataset; if it fails, the TVF and
    numpy disagree and the script exits non-zero.

    Cosine distance via L2-normalized dot product: `dist = 1 - sim`. Embeddings
    are already L2-normalized by `generate_embeddings` (sklearn.preprocessing
    .normalize), so no re-normalization is needed here.
    """
    print("\n" + "-" * 80)
    print("ORACLE: numpy ground-truth vs TVF result")
    print("-" * 80)

    corpus_embs = np.asarray([r["embedding"] for r in corpus], dtype=np.float32)
    query_embs = np.asarray([r["embedding"] for r in queries], dtype=np.float32)
    corpus_ids = np.asarray([r["image_id"] for r in corpus])
    query_ids = np.asarray([r["image_id"] for r in queries])

    # 1 - similarity; shape (n_corpus, n_queries). Use float64 to match the
    # JVM-side DenseVector(Double) arithmetic.
    sim = corpus_embs.astype(np.float64) @ query_embs.astype(np.float64).T
    dist = 1.0 - sim
    # Cosine distance is bounded in [0, 2]; clamp to align with the JVM-side
    # clamp at VectorDistanceUtils (FP rounding can push slightly outside).
    dist = np.clip(dist, 0.0, 2.0)

    # Group TVF rows by query_image_id (stable, content-addressed) — _hudi_query_index
    # is an opaque grouping ID generated by monotonically_increasing_id(), not a
    # contiguous index, so we key by the actual query image id.
    tvf_by_query = {}
    for r in tvf_rows:
        tvf_by_query.setdefault(r["query_image_id"], []).append(r)

    if set(tvf_by_query.keys()) != set(query_ids.tolist()):
        missing = set(query_ids.tolist()) - set(tvf_by_query.keys())
        extra = set(tvf_by_query.keys()) - set(query_ids.tolist())
        sys.exit(
            f"ORACLE FAILED: query-id sets disagree. "
            f"missing from TVF: {sorted(missing)[:5]}, "
            f"unexpected in TVF: {sorted(extra)[:5]}"
        )

    max_distance_delta = 0.0
    matches = 0
    for j, qid in enumerate(query_ids):
        # numpy top-k by ascending distance (stable sort for tie reproducibility).
        order = np.argsort(dist[:, j], kind="stable")[:k]
        expected_ids = corpus_ids[order].tolist()
        expected_dists = dist[order, j].tolist()

        # TVF rows already ORDER BY _hudi_query_index, _hudi_distance — pick by
        # query id and they're in ascending-distance order within the group.
        tvf_rows_for_q = tvf_by_query[qid]
        if len(tvf_rows_for_q) != k:
            sys.exit(
                f"ORACLE FAILED: query {qid} returned {len(tvf_rows_for_q)} rows, expected {k}"
            )
        tvf_ids = [r["corpus_image_id"] for r in tvf_rows_for_q]
        tvf_dists = [float(r["_hudi_distance"]) for r in tvf_rows_for_q]

        # ID-set must match exactly. Order may differ across ties (which is OK
        # because numpy and the JVM may break ties differently), so we compare
        # as multisets first, then verify per-rank distance agreement.
        if sorted(tvf_ids) != sorted(expected_ids):
            sys.exit(
                f"ORACLE FAILED for query {qid}:\n"
                f"  expected ids: {expected_ids}\n"
                f"  tvf ids     : {tvf_ids}\n"
                f"  expected dist: {expected_dists}\n"
                f"  tvf dist     : {tvf_dists}"
            )

        # Position-wise distance check is tolerant to tied-distance reordering:
        # since both lists are sorted ascending, the i-th distance must agree
        # regardless of which id sits at position i.
        tol = CONFIG["oracle_distance_tol"]
        for i, (ed, td) in enumerate(zip(expected_dists, tvf_dists)):
            delta = abs(ed - td)
            max_distance_delta = max(max_distance_delta, delta)
            if delta > tol:
                sys.exit(
                    f"ORACLE FAILED for query {qid} rank {i}: "
                    f"expected dist={ed:.8f}, tvf dist={td:.8f}, delta={delta:.2e} > {tol:.0e}"
                )
        matches += 1

    print(f"✓ {matches}/{len(query_ids)} queries match numpy ground truth")
    print(f"  Max per-rank distance delta: {max_distance_delta:.2e} (tol {CONFIG['oracle_distance_tol']:.0e})")
    return max_distance_delta


# ======================================================
# 9. VISUALIZATION (panel of all queries × top-k)
# ======================================================

def visualize_batch_panel(queries, tvf_rows):
    """
    One row per query: query image on the left, then the top-k matches with
    distance overlays. Saved as a single PNG so the user can scroll/open and
    eyeball that the matches are visually plausible (same breed / similar
    breed). The numpy oracle is the load-bearing correctness check; this is
    sanity.
    """
    print("\nRendering result panel...")
    out_dir = Path(CONFIG["output_dir"])
    ensure_dir(out_dir)

    by_query = {}
    for r in tvf_rows:
        by_query.setdefault(r["query_image_id"], []).append(r)

    k = CONFIG["top_k"]
    n = len(queries)
    fig, axes = plt.subplots(n, k + 1, figsize=(2.0 * (k + 1), 2.2 * n))
    if n == 1:
        axes = np.array([axes])

    for row_idx, q in enumerate(queries):
        # Column 0: query image
        ax = axes[row_idx, 0]
        ax.imshow(Image.open(io.BytesIO(q["image_bytes_raw"])).convert("RGB"))
        ax.set_title(f"QUERY\n{q['category']}", fontsize=8, fontweight="bold")
        ax.axis("off")

        # Columns 1..k: top-k matches
        matches = by_query.get(q["image_id"], [])
        for col_idx in range(k):
            ax = axes[row_idx, col_idx + 1]
            if col_idx < len(matches):
                m = matches[col_idx]
                blob = m["corpus_image_bytes"]
                inline = blob["data"] if blob is not None else None
                if inline is not None:
                    ax.imshow(Image.open(io.BytesIO(bytes(inline))).convert("RGB"))
                ax.set_title(
                    f"{m['corpus_category']}\nd={float(m['_hudi_distance']):.3f}",
                    fontsize=7,
                )
            ax.axis("off")

    plt.tight_layout()
    panel_path = out_dir / CONFIG["panel_filename"]
    plt.savefig(str(panel_path), dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"✓ Panel: {panel_path.resolve()}")
    return str(panel_path.resolve())


# ======================================================
# MAIN
# ======================================================

def main():
    fmt = CONFIG["base_file_format"].upper()
    print("\n" + "=" * 80)
    print(f"HUDI BATCH VECTOR SEARCH DEMO  [base file format: {fmt}]")
    print("Oxford-IIIT Pet — table-to-table KNN via hudi_vector_search_batch")
    print("=" * 80)
    print(f"  n_corpus         : {CONFIG['n_corpus']}")
    print(f"  n_queries        : {CONFIG['n_queries']}")
    print(f"  top_k            : {CONFIG['top_k']}")
    print(f"  embedding_model  : {CONFIG['embedding_model']}")
    print(f"  base_file_format : {CONFIG['base_file_format']}")
    print(f"  corpus path      : {CONFIG['corpus_table_path']}")
    print(f"  queries path     : {CONFIG['queries_table_path']}")
    print("=" * 80 + "\n")

    wipe_prior_state()
    spark = create_spark()

    # 1-3. Load dataset + generate embeddings for everyone in one pass.
    total = CONFIG["n_corpus"] + CONFIG["n_queries"]
    data, _ = load_dataset(total)
    model, transform = create_embedding_model()
    data, embedding_dim = generate_embeddings(data, model, transform)

    # 4. Split.
    corpus, queries = split_corpus_and_queries(data)

    # 5-6. Stage + write both Hudi tables.
    register_staging_view(
        spark, corpus, embedding_dim, "staging_corpus",
        f"/tmp/staging_pets_batch_corpus_{CONFIG['base_file_format']}.parquet",
    )
    create_hudi_table_sql(
        spark, embedding_dim, CONFIG["corpus_table_name"], CONFIG["corpus_table_path"]
    )
    insert_into_hudi_sql(spark, CONFIG["corpus_table_name"], "staging_corpus")

    register_staging_view(
        spark, queries, embedding_dim, "staging_queries",
        f"/tmp/staging_pets_batch_queries_{CONFIG['base_file_format']}.parquet",
    )
    create_hudi_table_sql(
        spark, embedding_dim, CONFIG["queries_table_name"], CONFIG["queries_table_path"]
    )
    insert_into_hudi_sql(spark, CONFIG["queries_table_name"], "staging_queries")

    # 7. Batch search.
    tvf_rows = run_batch_search_sql(spark)

    # Schema-level coverage: the prefix-renaming code path at
    # HoodieVectorSearchPlanBuilder.scala:305-310 should produce
    # `_hudi_query_image_id` / `_hudi_query_category` / `_hudi_query_index`
    # in the select list. Assert they're present.
    expected_cols = {
        "corpus_image_id", "corpus_category", "corpus_image_bytes",
        "query_image_id", "query_category",
        "_hudi_distance", "_hudi_query_index",
    }
    actual_cols = set(tvf_rows[0].asDict().keys()) if tvf_rows else set()
    assert expected_cols.issubset(actual_cols), (
        f"unexpected TVF schema: missing {expected_cols - actual_cols}, "
        f"got {actual_cols}"
    )

    # 8. Oracle validation — the certification step.
    max_delta = oracle_validate_with_numpy(corpus, queries, tvf_rows, CONFIG["top_k"])

    # 9. Visualization.
    panel = visualize_batch_panel(queries, tvf_rows)

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"✓ Dataset:        {CONFIG['dataset']}")
    print(f"✓ Corpus:         {CONFIG['n_corpus']} rows @ {CONFIG['corpus_table_path']}")
    print(f"✓ Queries:        {CONFIG['n_queries']} rows @ {CONFIG['queries_table_path']}")
    print(f"✓ Model:          {CONFIG['embedding_model']} (dim={embedding_dim})")
    print(f"✓ Base format:    {CONFIG['base_file_format']}")
    print(f"✓ TVF rows:       {len(tvf_rows)} ({CONFIG['n_queries']}×{CONFIG['top_k']})")
    print(f"✓ Oracle delta:   max {max_delta:.2e} (tol {CONFIG['oracle_distance_tol']:.0e})")
    print(f"✓ Panel:          {panel}")
    print("=" * 80)
    print("CERTIFIED ✓  hudi_vector_search_batch matches numpy ground truth")
    print("=" * 80 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
