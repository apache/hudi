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
Hudi VECTOR + BLOB + Vector Search demo (DataFrame variant).

End-to-end flow:
  1) Load Oxford-IIIT Pet (cats & dogs, real photos) via torchvision.
  2) Generate image embeddings with a timm backbone.
  3) Write rows into a Hudi table (Lance or Parquet base files), with:
       - `embedding`   tagged as Hudi VECTOR(<dim>)
       - `image_bytes` tagged as Hudi BLOB (INLINE)
  4) Run cosine similarity search via the `hudi_vector_search` SQL TVF.
  5) Save the query image, top-K neighbors, and a combined panel figure.

Env vars:
  HUDI_BUNDLE_JAR        (defaults to ~/Downloads/hudi-spark3.5-bundle_2.12-1.2.0-rc1.jar)
  HUDI_BASE_FILE_FORMAT  (default 'lance'; set to 'parquet' to use Parquet base files)
  LANCE_BUNDLE_JAR       (defaults to ~/Downloads/lance-spark-bundle-3.5_2.12-0.4.0.jar;
                          only used when HUDI_BASE_FILE_FORMAT=lance)
  HUDI_LANCE_DEMO_N      (default 1000; number of images to ingest)
  PYSPARK_DRIVER_MEMORY  (default '4g')
  HUDI_LANCE_DEMO_OUTDIR (default './outputs')
"""

import io
import os
import sys
from pathlib import Path

# MUST run before any `pyspark` import — local-mode driver heap is fixed at
# JVM launch time and cannot be raised via SparkSession.config() later.
# Override with:  export PYSPARK_DRIVER_MEMORY=12g
_driver_mem = os.getenv("PYSPARK_DRIVER_MEMORY", "4g")
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    f"--driver-memory {_driver_mem} --conf spark.driver.maxResultSize=2g pyspark-shell",
)

import numpy as np
import torch
import timm
from sklearn.preprocessing import normalize
from PIL import Image

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from torchvision.datasets import OxfordIIITPet  # noqa: E402

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ======================================================
# CONFIGURATION
# ======================================================

# Switch the Hudi base file format with HUDI_BASE_FILE_FORMAT=parquet|lance (default lance).
_file_format = os.getenv("HUDI_BASE_FILE_FORMAT", "lance").lower()
if _file_format not in ("lance", "parquet"):
    sys.exit(f"ERROR: HUDI_BASE_FILE_FORMAT must be 'lance' or 'parquet', got '{_file_format}'")

CONFIG = {
    "dataset": "OxfordIIITPet",
    # Separate table paths per format so you can run both back-to-back without collision.
    "table_path": f"/tmp/hudi_{_file_format}_pets",
    "table_name": f"pets_{_file_format}",
    "base_file_format": _file_format,
    # Override via env var for quick dry-runs, e.g. HUDI_LANCE_DEMO_N=200
    "n_samples": int(os.getenv("HUDI_LANCE_DEMO_N", "1000")),
    "top_k": 5,
    "embedding_model": "mobilenetv3_small_100",
    "output_dir": os.getenv("HUDI_LANCE_DEMO_OUTDIR", "./outputs"),
    "panel_filename": f"hudi_{_file_format}_results.png",
    "log_level": "ERROR",
    "hide_progress": True,
}

# Matches HoodieSchema.Blob constants in hudi-common.
HUDI_TYPE_METADATA_KEY = "hudi_type"
BLOB_TYPE_INLINE = "INLINE"
BLOB_FIELD_TYPE = "type"
BLOB_FIELD_DATA = "data"
BLOB_FIELD_REFERENCE = "reference"


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

    # Lance jar is only needed when writing/reading Lance base files. For Parquet
    # runs we skip it entirely so users don't need to download it.
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
    """Initialize Spark with Hudi + Lance support."""
    jars = resolve_jars()

    builder = (
        SparkSession.builder.appName("Hudi-Vector-Blob-Demo")
        .config("spark.jars", jars)
        .config(
            "spark.serializer",
            "org.apache.spark.serializer.KryoSerializer",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        # Explicit default — CONTENT returns inline bytes as written.
        # Switch to DESCRIPTOR to defer byte reads via read_blob().
        .config("hoodie.read.blob.inline.mode", "CONTENT")
        # Low parallelism in local mode: the Python source list flows to JVM
        # as a PythonRDD, and downstream collects push records BACK to Python
        # workers through localhost sockets. One worker per core overwhelms
        # macOS socket buffers on binary (BLOB) payloads. 2 is plenty for a demo.
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
    """Load Oxford-IIIT Pet (trainval split), return list[dict] + class names."""
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
# 3. EMBEDDING MODEL (timm)
# ======================================================

def create_embedding_model():
    print(f"Loading embedding model: {CONFIG['embedding_model']}...")
    model = timm.create_model(
        CONFIG["embedding_model"],
        pretrained=True,
        num_classes=0,
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
# 4. WRITE TO HUDI TABLE WITH LANCE FORMAT
# ======================================================

BLOB_STRUCT_TYPE = StructType(
    [
        StructField(BLOB_FIELD_TYPE, StringType(), nullable=False),
        StructField(BLOB_FIELD_DATA, BinaryType(), nullable=True),
        StructField(
            BLOB_FIELD_REFERENCE,
            StructType(
                [
                    StructField("external_path", StringType(), nullable=True),
                    StructField("offset", LongType(), nullable=True),
                    StructField("length", LongType(), nullable=True),
                    StructField("managed", BooleanType(), nullable=True),
                ]
            ),
            nullable=True,
        ),
    ]
)


def build_target_schema(embedding_dim: int) -> StructType:
    """Construct the target StructType with all Hudi logical-type annotations.

    Mirrors `TestVectorDataSource.testVectorRoundTrip` — the schema declares
    `embedding` as `ArrayType(FloatType, containsNull=False)` with
    `metadata={"hudi_type": "VECTOR(N)"}`, and `image_bytes` with
    `metadata={"hudi_type": "BLOB"}`. When `spark.createDataFrame(rows, schema)`
    is called with this schema, Spark accepts it as authoritative, so the
    `containsNull=False` flag and the `hudi_type` metadata both survive into
    the Hudi write — no Parquet round-trip resetting them, no implicit
    analyzer cast applied.
    """
    return StructType(
        [
            StructField("image_id", StringType(), nullable=False),
            StructField("category", StringType(), nullable=False),
            StructField("category_sanitized", StringType(), nullable=False),
            StructField("label", IntegerType(), nullable=False),
            StructField("description", StringType(), nullable=True),
            StructField(
                "image_bytes",
                BLOB_STRUCT_TYPE,
                nullable=True,
                metadata={HUDI_TYPE_METADATA_KEY: "BLOB"},
            ),
            StructField("width", IntegerType(), nullable=False),
            StructField("height", IntegerType(), nullable=False),
            StructField(
                "embedding",
                ArrayType(FloatType(), containsNull=False),
                nullable=False,
                metadata={HUDI_TYPE_METADATA_KEY: f"VECTOR({embedding_dim})"},
            ),
        ]
    )


def rows_for_hudi(data):
    """Project the raw dicts into Row tuples matching the target schema order.

    The `image_bytes` field becomes a nested `Row` representing the BLOB INLINE
    struct: `(type='INLINE', data=<bytes>, reference=None)`. Field order has to
    match `BLOB_STRUCT_TYPE` exactly.
    """
    for d in data:
        yield Row(
            d["image_id"],
            d["category"],
            d["category_sanitized"],
            int(d["label"]),
            d.get("description"),
            Row(BLOB_TYPE_INLINE, d["image_bytes_raw"], None),
            int(d["width"]),
            int(d["height"]),
            [float(v) for v in d["embedding"]],
        )


def write_to_hudi(spark, data, embedding_dim: int):
    """Write to Hudi end-to-end via pure DataFrame API — no Parquet staging, no SQL.

    Mirrors the canonical pattern from `TestVectorDataSource.testVectorRoundTrip`:
      1. Build a `StructType` that declares `embedding` as
         `ArrayType(FloatType, containsNull=False)` with VECTOR(N) metadata,
         and `image_bytes` as the BLOB struct with BLOB metadata.
      2. `spark.createDataFrame(rows, schema)` accepts the schema as authoritative
         — `containsNull=False` and the column-level metadata both survive.
      3. `df.write.format("hudi").save(path)` writes through; Hudi sees a valid
         VECTOR + BLOB schema and the table is created on first write.

    The earlier PyArrow → Parquet → `spark.read.parquet(...)` staging path was
    introduced to avoid socket buffer pressure during high-volume DataFrame
    writes on local-mode Spark. It introduces a Parquet round-trip that resets
    `containsNull=True` regardless of the originating schema, which then
    requires a separate SQL DDL path. `spark.createDataFrame(rows, schema)` is
    a one-shot serialization at construction — no sustained streaming, no
    socket pressure at demo size.
    """
    print(
        f"\nWriting to Hudi table ({CONFIG['base_file_format']} base file format, "
        f"VECTOR + BLOB) via DataFrame .save()..."
    )

    schema = build_target_schema(embedding_dim)
    df = spark.createDataFrame(list(rows_for_hudi(data)), schema)

    print("Schema seen by Hudi:")
    df.printSchema()

    (
        df.write.format("hudi")
        .option("hoodie.table.name", CONFIG["table_name"])
        .option("hoodie.datasource.write.recordkey.field", "image_id")
        .option("hoodie.datasource.write.precombine.field", "image_id")
        .option("hoodie.datasource.write.partitionpath.field", "category_sanitized")
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
        .option("hoodie.table.base.file.format", CONFIG["base_file_format"])
        .option(
            "hoodie.write.record.merge.custom.implementation.classes",
            "org.apache.hudi.DefaultSparkRecordMerger",
        )
        .mode("overwrite")
        .save(CONFIG["table_path"])
    )

    print(f"✓ Wrote {len(data)} records to Hudi table at {CONFIG['table_path']}")
    return df


# ======================================================
# 5. SIMILARITY SEARCH USING HUDI VECTOR SEARCH
# ======================================================

def hudi_vector_search_df(
    spark: SparkSession,
    table_path: str,
    column: str,
    query_vector,
    k: int,
    metric: str = "cosine",
) -> DataFrame:
    """Wrap the `hudi_vector_search` TVF and return a DataFrame.

    `hudi_vector_search` is registered by Hudi's SQL extension as a TVF — there
    is no native DataFrame builder for it. This helper inlines the call so the
    rest of the demo speaks DataFrame: chain `.orderBy(...)`, `.select(...)`,
    `.collect()` on the result like any other DataFrame.

    The TVF requires the query vector to be a constant expression, so we inline
    the embedding as a literal `ARRAY(f1, f2, ...)` — matches the pattern in
    TestHoodieVectorSearchFunction.scala.
    """
    array_literal = "ARRAY(" + ", ".join(f"{float(v)}" for v in query_vector) + ")"
    return spark.sql(
        f"""
        SELECT image_id, category, image_bytes, _hudi_distance
        FROM hudi_vector_search(
            '{table_path}',
            '{column}',
            {array_literal},
            {k},
            '{metric}'
        )
        """
    )


def find_similar(spark, query_embedding):
    """Find similar images using the DataFrame-wrapped vector search."""
    print(f"\nPerforming similarity search (top {CONFIG['top_k']})...")

    rows = (
        hudi_vector_search_df(
            spark,
            CONFIG["table_path"],
            "embedding",
            query_embedding,
            k=CONFIG["top_k"] + 1,
            metric="cosine",
        )
        .orderBy("_hudi_distance")
        .collect()
    )

    results = []
    for row in rows:
        distance = float(row["_hudi_distance"])

        # Skip the query image itself (distance ~ 0 for L2-normalized embeddings).
        if distance < 0.001:
            continue
        if len(results) >= CONFIG["top_k"]:
            break

        # With BLOB INLINE + CONTENT mode, `image_bytes` is a struct<type,data,reference>.
        blob = row["image_bytes"]
        inline_bytes = blob[BLOB_FIELD_DATA] if blob is not None else None

        results.append(
            {
                "image_id": row["image_id"],
                "category": row["category"],
                "image_bytes": bytes(inline_bytes) if inline_bytes is not None else b"",
                "similarity": 1.0 - distance,
            }
        )

    print(f"✓ Found {len(results)} similar images using Hudi vector search")
    return results


# ======================================================
# 6. VISUALIZATION
# ======================================================

def visualize_and_save(query_image_bytes, query_category, results):
    print("\nCreating visualization and saving images...")

    out_dir = Path(CONFIG["output_dir"])
    ensure_dir(out_dir)

    query_path = out_dir / "query.png"
    save_png_bytes(query_image_bytes, query_path)

    result_paths = []
    for i, r in enumerate(results, 1):
        p = out_dir / f"top{i}.png"
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

    print("✓ Saved files:")
    print(f"  Query image: {query_path.resolve()}")
    for i, p in enumerate(result_paths, 1):
        print(f"  Top{i}: {p}")
    print(f"  Panel: {panel_path.resolve()}")

    return {
        "query": str(query_path.resolve()),
        "tops": result_paths,
        "panel": str(panel_path.resolve()),
    }


# ======================================================
# MAIN
# ======================================================

def main():
    fmt = CONFIG["base_file_format"].upper()
    print("\n" + "=" * 80)
    print(f"HUDI VECTOR + BLOB + VECTOR SEARCH DEMO  [base file format: {fmt}]")
    print("Oxford-IIIT Pet — DataFrame variant")
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

    df = write_to_hudi(spark, data, embedding_dim)

    print("\nSample rows:")
    df.select("image_id", "category", "description").show(5, truncate=False)

    query_idx = np.random.randint(len(data))
    query_item = data[query_idx]
    print(f"\nQuery: {query_item['image_id']} ({query_item['category']})")

    results = find_similar(spark, query_item["embedding"])

    print("\nTop matches:")
    for i, r in enumerate(results, 1):
        print(
            f"  {i}. {r['image_id']} - {r['category']} "
            f"(similarity: {r['similarity']:.3f})"
        )

    saved = visualize_and_save(
        query_item["image_bytes_raw"],
        query_item["category"],
        results,
    )

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"✓ Dataset:   {CONFIG['dataset']} ({CONFIG['n_samples']} images)")
    print(f"✓ Model:     {CONFIG['embedding_model']} (dim={embedding_dim})")
    print(f"✓ Storage:   Hudi table, {CONFIG['base_file_format']} base file format")
    print("✓ Types:     embedding = VECTOR(%d), image_bytes = BLOB (INLINE)" % embedding_dim)
    print("✓ Search:    hudi_vector_search, cosine distance, L2-normalized")
    print(f"✓ Table:     {CONFIG['table_path']}")
    print(f"✓ Panel:     {saved['panel']}")
    print("=" * 80 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
