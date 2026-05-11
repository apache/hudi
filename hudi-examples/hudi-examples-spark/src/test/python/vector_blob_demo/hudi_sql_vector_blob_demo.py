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
Hudi VECTOR + BLOB + Vector Search demo — **Spark SQL variant**.

Same shape as `hudi_dataframe_vector_blob_demo.py`, but every Hudi-touching
operation is a SQL statement so viewers see the actual DDL/DML surface:

  CREATE TABLE ... (embedding VECTOR(1024), image_bytes BLOB, ...) USING hudi
  INSERT  INTO ... SELECT ..., named_struct('type', 'INLINE', ...) FROM staging
  SELECT  ... FROM hudi_vector_search('<path>', 'embedding', ARRAY(...), k, 'cosine')

Image loading (torchvision) and embedding generation (timm) stay in Python —
those cannot be SQL. The bridge between the two is a Spark temp view.

Env vars (same as the DataFrame variant):
  HUDI_BUNDLE_JAR         (defaults to ~/Downloads/hudi-spark3.5-bundle_2.12-1.2.0-rc1.jar)
  HUDI_BASE_FILE_FORMAT   (default 'lance'; set to 'parquet' to use Parquet)
  LANCE_BUNDLE_JAR        (defaults to ~/Downloads/lance-spark-bundle-3.5_2.12-0.4.0.jar; only used when HUDI_BASE_FILE_FORMAT=lance)
  HUDI_LANCE_DEMO_N       (default 1000; number of images to ingest)
  PYSPARK_DRIVER_MEMORY   (default '4g')
  HUDI_LANCE_DEMO_OUTDIR  (default './outputs')
"""

import io
import os
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
    # Separate paths from the DataFrame variant so both can coexist on disk.
    "table_path": f"/tmp/hudi_sql_{_file_format}_pets",
    "table_name": f"pets_sql_{_file_format}",
    "base_file_format": _file_format,
    "n_samples": int(os.getenv("HUDI_LANCE_DEMO_N", "1000")),
    "top_k": 5,
    "embedding_model": "mobilenetv3_small_100",
    "output_dir": os.getenv("HUDI_LANCE_DEMO_OUTDIR", "./outputs"),
    "panel_filename": f"hudi_sql_{_file_format}_results.png",
    "log_level": "ERROR",
    "hide_progress": True,
}

# The cast target used inside the BLOB INLINE struct's null reference field.
BLOB_REFERENCE_CAST = (
    "struct<external_path:string,offset:bigint,length:bigint,managed:boolean>"
)


# ======================================================
# UTILITIES
# ======================================================

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def save_png_bytes(img_bytes: bytes, path: Path) -> None:
    ensure_dir(path.parent)
    with open(path, "wb") as f:
        f.write(img_bytes)


def default_hudi_bundle_jar() -> str:
    # Defaults to the Apache 1.2.0-rc1 staging jar in ~/Downloads/. Grab it with:
    #   curl -L -o ~/Downloads/hudi-spark3.5-bundle_2.12-1.2.0-rc1.jar \
    #     https://repository.apache.org/content/repositories/orgapachehudi-1176/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.2.0-rc1/hudi-spark3.5-bundle_2.12-1.2.0-rc1.jar
    # Override via HUDI_BUNDLE_JAR=/abs/path/to/jar to point at a locally built bundle.
    return str(Path.home() / "Downloads" / "hudi-spark3.5-bundle_2.12-1.2.0-rc1.jar")


def default_lance_bundle_jar() -> str:
    # Defaults to the Maven Central Lance 0.4.0 jar in ~/Downloads/. Grab it with:
    #   curl -L -o ~/Downloads/lance-spark-bundle-3.5_2.12-0.4.0.jar \
    #     https://repo1.maven.org/maven2/com/lancedb/lance-spark-bundle-3.5_2.12/0.4.0/lance-spark-bundle-3.5_2.12-0.4.0.jar
    # Override via LANCE_BUNDLE_JAR=/abs/path/to/jar.
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

    # Lance jar is only needed when writing/reading Lance base files.
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
    """Identical Spark session to the DataFrame variant."""
    jars = resolve_jars()

    builder = (
        SparkSession.builder.appName("Hudi-SQL-Vector-Blob-Demo")
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
# 2. LOAD DATASET (Oxford-IIIT Pet) — pure Python
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

    print(f"✓ Loaded {len(data)} images from Oxford-IIIT Pet")
    return data, class_names


# ======================================================
# 3. EMBEDDING MODEL (timm) — pure Python
# ======================================================

def create_embedding_model():
    print(f"Loading embedding model: {CONFIG['embedding_model']}...")
    model = timm.create_model(
        CONFIG["embedding_model"], pretrained=True, num_classes=0
    )
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
# 4. BRIDGE: register the Python data as a Spark temp view
# ======================================================

STAGING_VIEW = "staging_pets"


def stage_to_parquet_with_pyarrow(data, embedding_dim: int, staging_path: str) -> None:
    """
    Write the Python data to a Parquet file **directly via PyArrow**, bypassing
    Spark for the staging step.

    Why not `spark.createDataFrame(data).write.parquet(...)`? Because that
    internally builds a `PythonRDD` — its tasks stream pickled rows to Python
    workers through localhost sockets during the write. On some local
    environments (notably macOS) socket buffer resources are limited, and
    sustained binary streaming at scale can saturate them, causing the write
    to fail with `No buffer space available (Write failed)`.

    PyArrow writes the Parquet file in-process from Python, then
    `spark.read.parquet(...)` loads it JVM-natively. No `PythonRDD` ever
    exists in the lineage, so no localhost socket traffic occurs.
    """
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
    table = pa.table(columns, schema=arrow_schema)
    pq.write_table(table, staging_path)


def register_staging_view(spark: SparkSession, data, embedding_dim: int):
    """
    Stage the Python data to a Parquet file via PyArrow, then register it as
    a Spark temp view for the INSERT INTO SELECT below. No Hudi metadata is
    needed here — the Hudi table's DDL carries the VECTOR/BLOB types, and
    INSERT INTO type-checks/shapes the SELECT expressions against that schema.
    """
    staging_parquet_path = f"/tmp/staging_{CONFIG['table_name']}_parquet.parquet"
    print(f"Staging Python data → Parquet at {staging_parquet_path} (PyArrow, no Spark)...")
    stage_to_parquet_with_pyarrow(data, embedding_dim, staging_parquet_path)
    spark.read.parquet(staging_parquet_path).createOrReplaceTempView(STAGING_VIEW)
    print(f"✓ Registered Spark temp view: {STAGING_VIEW} (JVM-native)")


# ======================================================
# 5. CREATE TABLE — SQL
# ======================================================

def create_hudi_table_sql(spark: SparkSession, embedding_dim: int):
    table = CONFIG["table_name"]
    print(
        f"\nDDL: CREATE TABLE {table} ...  "
        f"[{CONFIG['base_file_format']} base files]"
    )

    # Drop first so repeat runs are idempotent (also wipes the table_path dir
    # since the catalog's managed-path semantics follow LOCATION).
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    ddl = f"""
        CREATE TABLE {table} (
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
        LOCATION '{CONFIG['table_path']}'
        TBLPROPERTIES (
            primaryKey = 'image_id',
            preCombineField = 'image_id',
            type = 'cow',
            'hoodie.table.base.file.format' = '{CONFIG['base_file_format']}',
            -- Required when writing Lance base files. Without this, Hudi's
            -- writer factory defaults to the AVRO record type, whose Lance
            -- writer is a stub that throws:
            --   "Lance base file format is currently only supported with the Spark engine"
            -- Setting this merger class flips the record type to SPARK, which
            -- routes through HoodieSparkFileWriterFactory → HoodieSparkLanceWriter.
            -- Harmless for Parquet base files; safe to leave on always.
            'hoodie.write.record.merge.custom.implementation.classes' = 'org.apache.hudi.DefaultSparkRecordMerger'
        )
    """
    print(ddl.strip())
    spark.sql(ddl)
    print(f"✓ Created table {table} at {CONFIG['table_path']}")


# ======================================================
# 6. INSERT INTO ... SELECT — SQL
# ======================================================

def insert_into_hudi_sql(spark: SparkSession):
    table = CONFIG["table_name"]
    print(
        f"\nDML: INSERT INTO {table} SELECT ... FROM {STAGING_VIEW}  "
        f"[{CONFIG['base_file_format']} base files]"
    )

    # `named_struct('type', 'INLINE', 'data', <bytes>, 'reference', null)` is
    # the canonical shape for a Hudi BLOB INLINE value (matches the pattern
    # in TestCreateTable.scala's BLOB INSERT test).
    insert = f"""
        INSERT INTO {table}
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
        FROM {STAGING_VIEW}
    """
    print(insert.strip())
    spark.sql(insert)

    # COUNT(image_id), not COUNT(*): the Spark planner strips all columns from a
    # COUNT(*) read (it only needs row counts, not column values), but Hudi's
    # Lance reader at LanceRecordIterator.java:123 has a strict equality check
    # `sparkFields.length == fieldVectors.size()` and throws when asked for zero
    # columns. Naming `image_id` forces a 1-column projection that the reader
    # handles correctly.
    count = spark.sql(f"SELECT COUNT(image_id) AS c FROM {table}").collect()[0]["c"]
    print(f"✓ Inserted {count} records into {table}")


# ======================================================
# 7. SIMILARITY SEARCH — SQL (hudi_vector_search TVF)
# ======================================================

def find_similar_sql(spark: SparkSession, query_embedding):
    print(f"\nDQL: hudi_vector_search(...) top {CONFIG['top_k']}")

    array_literal = "ARRAY(" + ", ".join(f"{float(v)}" for v in query_embedding) + ")"

    query = f"""
        SELECT image_id, category, image_bytes, _hudi_distance
        FROM hudi_vector_search(
            '{CONFIG['table_path']}',
            'embedding',
            {array_literal},
            {CONFIG['top_k'] + 1},
            'cosine'
        )
        ORDER BY _hudi_distance
    """
    # Only print the shape — the literal itself is 1024 floats wide.
    print(
        query.replace(array_literal, "ARRAY(<1024 floats inlined from query embedding>)").strip()
    )

    rows = spark.sql(query).collect()

    results = []
    for row in rows:
        distance = float(row["_hudi_distance"])
        # Skip the query row itself (distance ~ 0 on L2-normalized embeddings).
        if distance < 0.001:
            continue
        if len(results) >= CONFIG["top_k"]:
            break

        # BLOB INLINE + CONTENT mode: `image_bytes` is a struct, the bytes are in `data`.
        blob = row["image_bytes"]
        inline_bytes = blob["data"] if blob is not None else None

        results.append(
            {
                "image_id": row["image_id"],
                "category": row["category"],
                "image_bytes": bytes(inline_bytes) if inline_bytes is not None else b"",
                "similarity": 1.0 - distance,
            }
        )

    print(f"✓ Found {len(results)} similar images")
    return results


# ======================================================
# 8. VISUALIZATION
# ======================================================

def visualize_and_save(query_image_bytes, query_category, results):
    print("\nCreating visualization and saving images...")

    out_dir = Path(CONFIG["output_dir"])
    ensure_dir(out_dir)

    query_path = out_dir / f"query_sql_{CONFIG['base_file_format']}.png"
    save_png_bytes(query_image_bytes, query_path)

    result_paths = []
    for i, r in enumerate(results, 1):
        p = out_dir / f"top{i}_sql_{CONFIG['base_file_format']}.png"
        save_png_bytes(r["image_bytes"], p)
        result_paths.append(str(p.resolve()))

    n_results = len(results)
    fig, axes = plt.subplots(1, n_results + 1, figsize=(3 * (n_results + 1), 3.2))

    query_img = Image.open(io.BytesIO(query_image_bytes)).convert("RGB")
    axes[0].imshow(query_img)
    axes[0].set_title(f"QUERY\n{query_category}", fontweight="bold")
    axes[0].axis("off")

    for i, result in enumerate(results):
        img = Image.open(io.BytesIO(result["image_bytes"])).convert("RGB")
        axes[i + 1].imshow(img)
        axes[i + 1].set_title(f"{result['category']}\nSim: {result['similarity']:.3f}")
        axes[i + 1].axis("off")

    plt.tight_layout()
    panel_path = out_dir / CONFIG["panel_filename"]
    plt.savefig(str(panel_path), dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"✓ Panel: {panel_path.resolve()}")
    return {"panel": str(panel_path.resolve())}


# ======================================================
# MAIN
# ======================================================

def main():
    fmt = CONFIG["base_file_format"].upper()
    print("\n" + "=" * 80)
    print(f"HUDI VECTOR + BLOB + VECTOR SEARCH DEMO  [base file format: {fmt}]")
    print("Oxford-IIIT Pet — Spark SQL variant")
    print("=" * 80)
    print(f"  base_file_format : {CONFIG['base_file_format']}")
    print(f"  table_path       : {CONFIG['table_path']}")
    print(f"  table_name       : {CONFIG['table_name']}")
    print(f"  n_samples        : {CONFIG['n_samples']}")
    print(f"  embedding_model  : {CONFIG['embedding_model']}")
    print("=" * 80 + "\n")

    spark = create_spark()

    data, _class_names = load_dataset(CONFIG["n_samples"])
    model, transform = create_embedding_model()
    data, embedding_dim = generate_embeddings(data, model, transform)

    register_staging_view(spark, data, embedding_dim)
    create_hudi_table_sql(spark, embedding_dim)
    insert_into_hudi_sql(spark)

    print("\nSample rows (SELECT via SQL):")
    spark.sql(
        f"SELECT image_id, category, description FROM {CONFIG['table_name']} LIMIT 5"
    ).show(truncate=False)

    query_idx = np.random.randint(len(data))
    query_item = data[query_idx]
    print(f"\nQuery: {query_item['image_id']} ({query_item['category']})")

    results = find_similar_sql(spark, query_item["embedding"])

    print("\nTop matches:")
    for i, r in enumerate(results, 1):
        print(
            f"  {i}. {r['image_id']} - {r['category']} "
            f"(similarity: {r['similarity']:.3f})"
        )

    saved = visualize_and_save(
        query_item["image_bytes_raw"], query_item["category"], results
    )

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"✓ Dataset:   {CONFIG['dataset']} ({CONFIG['n_samples']} images)")
    print(f"✓ Model:     {CONFIG['embedding_model']} (dim={embedding_dim})")
    print(f"✓ Storage:   Hudi table, {CONFIG['base_file_format']} base file format")
    print(f"✓ Path:      {CONFIG['table_path']}")
    print("✓ Types:     embedding = VECTOR(%d), image_bytes = BLOB (INLINE)" % embedding_dim)
    print("✓ Surface:   All Hudi ops via Spark SQL (CREATE / INSERT / hudi_vector_search)")
    print(f"✓ Panel:     {saved['panel']}")
    print("=" * 80 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
