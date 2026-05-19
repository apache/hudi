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
Hudi BLOB reader demo — OUT_OF_LINE blobs + `read_blob()`.

What this script shows, in order:
  1) We pack N raw PNG images into a single large binary "container" file
     on local disk. The Hudi table never stores the image bytes inline.
  2) We CREATE TABLE with a BLOB column and INSERT OUT_OF_LINE descriptors
     pointing at (external_path, offset, length) slices of that container.
  3) Query the table — the `image_bytes` column comes back as a descriptor
     struct; the bytes are NOT read yet. The Hudi table is tiny: just
     metadata + pointers.
  4) Use the `read_blob(image_bytes)` SQL function to resolve the descriptor
     to actual bytes at query time.
  5) Save the resolved bytes back to .png files and confirm they round-trip.

This is the "lakehouse that references unstructured data without copying it"
story. Complementary to the `hudi_sql_vector_blob_demo.py` script (which
shows INLINE blobs + vector search); this one shows OUT_OF_LINE blobs +
`read_blob()`.

Env vars (shares the same conventions as the other demos):
  HUDI_BUNDLE_JAR         (defaults to ~/Downloads/hudi-spark3.5-bundle_2.12-1.2.0-rc1.jar)
  HUDI_BASE_FILE_FORMAT   (default 'lance'; set to 'parquet' to use Parquet)
  LANCE_BUNDLE_JAR        (defaults to ~/Downloads/lance-spark-bundle-3.5_2.12-0.4.0.jar; only used when HUDI_BASE_FILE_FORMAT=lance)
  HUDI_BLOB_MODE          (default 'out_of_line'; 'inline' stores bytes in the Hudi table)
  HUDI_INLINE_READ_MODE   (default 'content'; 'descriptor' forces lazy reads via read_blob().
                           Only meaningful when HUDI_BLOB_MODE=inline.)
  HUDI_LANCE_DEMO_N       (default 100; number of images)
  PYSPARK_DRIVER_MEMORY   (default '4g')
  HUDI_LANCE_DEMO_OUTDIR  (default './outputs')
"""

import io
import os
import sys
from pathlib import Path

# MUST run before any `pyspark` import — see the other demo scripts for
# the rationale.
_driver_mem = os.getenv("PYSPARK_DRIVER_MEMORY", "4g")
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    f"--driver-memory {_driver_mem} --conf spark.driver.maxResultSize=2g pyspark-shell",
)

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image

from torchvision.datasets import OxfordIIITPet

from pyspark.sql import SparkSession


# ======================================================
# CONFIGURATION
# ======================================================

_file_format = os.getenv("HUDI_BASE_FILE_FORMAT", "lance").lower()
if _file_format not in ("lance", "parquet"):
    sys.exit(f"ERROR: HUDI_BASE_FILE_FORMAT must be 'lance' or 'parquet', got '{_file_format}'")

_blob_mode = os.getenv("HUDI_BLOB_MODE", "out_of_line").lower()
if _blob_mode not in ("inline", "out_of_line"):
    sys.exit(f"ERROR: HUDI_BLOB_MODE must be 'inline' or 'out_of_line', got '{_blob_mode}'")

# Controls hoodie.read.blob.inline.mode. Only meaningful when blob_mode=inline:
#   CONTENT    → image_bytes.data returns bytes directly
#   DESCRIPTOR → image_bytes.data is null; you must call read_blob() to materialize
# For blob_mode=out_of_line the row is already a descriptor either way.
_inline_read_mode = os.getenv("HUDI_INLINE_READ_MODE", "content").lower()
if _inline_read_mode not in ("content", "descriptor"):
    sys.exit(
        f"ERROR: HUDI_INLINE_READ_MODE must be 'content' or 'descriptor', got '{_inline_read_mode}'"
    )

CONFIG = {
    "dataset": "OxfordIIITPet",
    "table_path": f"/tmp/hudi_blob_reader_{_blob_mode}_{_file_format}_pets",
    "table_name": f"pets_blob_reader_{_blob_mode}_{_file_format}",
    "base_file_format": _file_format,
    "blob_mode": _blob_mode,  # 'inline' or 'out_of_line'
    "inline_read_mode": _inline_read_mode,  # 'content' or 'descriptor'
    "n_samples": int(os.getenv("HUDI_LANCE_DEMO_N", "100")),
    "blob_container_path": "/tmp/pets_blob_container.bin",
    "output_dir": os.getenv("HUDI_LANCE_DEMO_OUTDIR", "./outputs"),
    "resolved_images_to_save": 3,
    "log_level": "ERROR",
    "hide_progress": True,
}


# ======================================================
# UTILITIES
# ======================================================

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


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
    jars = resolve_jars()

    builder = (
        SparkSession.builder.appName("Hudi-Blob-Reader-Demo")
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
        # NOTE: `hoodie.read.blob.inline.mode` is intentionally NOT set on the SparkSession.
        # If it were, EVERY hudi load — including the one read_blob() runs internally —
        # would suppress INLINE bytes, and read_blob() would return null. Instead we scope
        # the DESCRIPTOR option per-load in show_descriptors() so read_blob() in
        # read_blob_and_save() runs against a default-mode (CONTENT) load and can
        # materialize bytes. See TestLanceDataSource.testBlobInlineDescriptorMode for the
        # canonical pattern.
        .config("spark.default.parallelism", "2")
        .config("spark.sql.shuffle.partitions", "2")
    )

    if CONFIG.get("hide_progress", True):
        builder = builder.config("spark.ui.showConsoleProgress", "false")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(CONFIG.get("log_level", "ERROR"))
    return spark


# ======================================================
# 2. LOAD IMAGES + PACK INTO ONE BLOB CONTAINER FILE
# ======================================================

def load_images(n_samples: int):
    """
    Load N pet images and return their raw PNG bytes + metadata. Mode-agnostic.
    """
    print(f"Loading Oxford-IIIT Pet ({n_samples} samples)...")
    root = os.path.expanduser("~/.cache/torchvision")
    ds = OxfordIIITPet(root=root, split="trainval", download=True)
    class_names = ds.classes

    rng = np.random.default_rng()
    n = min(n_samples, len(ds))
    indices = rng.choice(len(ds), size=n, replace=False)

    rows = []
    for idx in indices:
        img, label = ds[int(idx)]
        img = img.convert("RGB")

        bio = io.BytesIO()
        img.save(bio, format="PNG")
        png_bytes = bio.getvalue()

        category = class_names[label] if isinstance(class_names, list) else str(label)
        rows.append(
            {
                "image_id": f"pets_{int(idx):06d}",
                "category": category,
                "png_bytes": png_bytes,
            }
        )

    print(f"✓ Loaded {n} images")
    return rows


def pack_into_container(rows, container_path: str):
    """
    OUT_OF_LINE path only: write all PNG bytes back-to-back into one file and
    annotate each row with (external_path, offset, length). Mutates `rows`.
    """
    print(f"Packing {len(rows)} PNGs into {container_path}...")
    offset = 0
    with open(container_path, "wb") as container:
        for row in rows:
            png_bytes = row["png_bytes"]
            container.write(png_bytes)
            row["external_path"] = container_path
            row["offset"] = offset
            row["length"] = len(png_bytes)
            offset += len(png_bytes)

    total_mb = offset / (1024 * 1024)
    print(f"✓ Packed {len(rows)} images — {total_mb:.1f} MB total, container at {container_path}")
    return rows


# ======================================================
# 3. STAGE ROWS VIA PYARROW → SPARK TEMP VIEW
# ======================================================

STAGING_VIEW = "staging_blob_refs"


def stage_and_register(spark: SparkSession, rows):
    """
    Write the staging rows to a tiny Parquet file and register as a Spark
    temp view. PyArrow-direct to avoid the PythonRDD socket-buffer issue.

    Schema depends on blob mode:
      OUT_OF_LINE → (image_id, category, external_path, offset, length)
      INLINE      → (image_id, category, png_bytes_raw)
    """
    if CONFIG["blob_mode"] == "out_of_line":
        arrow_schema = pa.schema(
            [
                pa.field("image_id", pa.string(), nullable=False),
                pa.field("category", pa.string(), nullable=False),
                pa.field("external_path", pa.string(), nullable=False),
                pa.field("offset", pa.int64(), nullable=False),
                pa.field("length", pa.int64(), nullable=False),
            ]
        )
        columns = {
            "image_id": [r["image_id"] for r in rows],
            "category": [r["category"] for r in rows],
            "external_path": [r["external_path"] for r in rows],
            "offset": [int(r["offset"]) for r in rows],
            "length": [int(r["length"]) for r in rows],
        }
    else:  # inline
        arrow_schema = pa.schema(
            [
                pa.field("image_id", pa.string(), nullable=False),
                pa.field("category", pa.string(), nullable=False),
                pa.field("png_bytes_raw", pa.binary(), nullable=False),
            ]
        )
        columns = {
            "image_id": [r["image_id"] for r in rows],
            "category": [r["category"] for r in rows],
            "png_bytes_raw": [r["png_bytes"] for r in rows],
        }

    staging_path = f"/tmp/staging_{CONFIG['table_name']}.parquet"
    print(f"Staging rows → {staging_path} (PyArrow, {CONFIG['blob_mode']} mode)...")
    pq.write_table(pa.table(columns, schema=arrow_schema), staging_path)

    spark.read.parquet(staging_path).createOrReplaceTempView(STAGING_VIEW)
    print(f"✓ Registered Spark temp view: {STAGING_VIEW}")


# ======================================================
# 4. CREATE TABLE (SQL) — tiny table, just references
# ======================================================

def create_hudi_table(spark: SparkSession):
    table = CONFIG["table_name"]
    print(f"\nDDL: CREATE TABLE {table}  [{CONFIG['base_file_format']} base files]")

    spark.sql(f"DROP TABLE IF EXISTS {table}")

    ddl = f"""
        CREATE TABLE {table} (
            image_id     STRING,
            category     STRING,
            image_bytes  BLOB   COMMENT 'Pet image bytes stored OUT_OF_LINE as references'
        ) USING hudi
        LOCATION '{CONFIG['table_path']}'
        TBLPROPERTIES (
            primaryKey = 'image_id',
            preCombineField = 'image_id',
            type = 'cow',
            'hoodie.table.base.file.format' = '{CONFIG['base_file_format']}',
            'hoodie.write.record.merge.custom.implementation.classes' = 'org.apache.hudi.DefaultSparkRecordMerger'
        )
    """
    print(ddl.strip())
    spark.sql(ddl)
    print(f"✓ Created table {table} at {CONFIG['table_path']}")


# ======================================================
# 5. INSERT INTO ... SELECT — build the OUT_OF_LINE struct
# ======================================================

def insert_blob_rows(spark: SparkSession):
    table = CONFIG["table_name"]
    print(
        f"\nDML: INSERT INTO {table} SELECT ...  "
        f"[blob_mode={CONFIG['blob_mode']}, base={CONFIG['base_file_format']}]"
    )

    # OUT_OF_LINE shape from TestDeleteFromTable.scala / TestUpdateTable.scala:
    #   named_struct('type','OUT_OF_LINE','data', null, 'reference', named_struct(path, offset, length, managed))
    # INLINE shape from TestCreateTable.scala:
    #   named_struct('type','INLINE', 'data', <bytes>, 'reference', null<struct>)
    if CONFIG["blob_mode"] == "out_of_line":
        # `managed = false` means Hudi does NOT own the external file lifecycle.
        blob_expr = """
            named_struct(
                'type',      'OUT_OF_LINE',
                'data',      cast(null as binary),
                'reference', named_struct(
                    'external_path', external_path,
                    'offset',        offset,
                    'length',        length,
                    'managed',       false
                )
            )
        """
    else:  # inline
        blob_expr = """
            named_struct(
                'type',      'INLINE',
                'data',      png_bytes_raw,
                'reference', cast(null as struct<external_path:string,
                                                 offset:bigint,
                                                 length:bigint,
                                                 managed:boolean>)
            )
        """

    insert = f"""
        INSERT INTO {table}
        SELECT
            image_id,
            category,
            {blob_expr.strip()} AS image_bytes
        FROM {STAGING_VIEW}
    """
    print(insert.strip())
    spark.sql(insert)

    count = spark.sql(
        f"SELECT COUNT(image_id) AS c FROM {table}"  # naming a column ensures a non-empty projection
    ).collect()[0]["c"]
    print(f"✓ Inserted {count} rows into {table}")


# ======================================================
# 6. INSPECT THE DESCRIPTORS (bytes NOT materialized yet)
# ======================================================

DESCRIPTORS_VIEW = "blob_descriptors_view"


def show_descriptors(spark: SparkSession):
    """
    Inspect each row's descriptor metadata. When inline_read_mode=DESCRIPTOR, we scope
    the option to THIS load only — registering a temp view that surfaces the descriptor
    form. read_blob() lives in read_blob_and_save() and runs against a separate default-
    mode read so the underlying bytes are still available to resolve.
    """
    table = CONFIG["table_name"]
    rmode = CONFIG["inline_read_mode"]
    path = CONFIG["table_path"]

    if CONFIG["blob_mode"] == "out_of_line":
        print(
            "\nInspecting stored OUT_OF_LINE blobs — `data` is null, `reference.*` points "
            f"at the container (inline_read_mode={rmode} is a no-op here):"
        )
    elif rmode == "content":
        print(
            "\nInspecting stored INLINE blobs with inline_read_mode=CONTENT —\n"
            "  `data` holds the raw bytes (length(data) > 0), `reference.*` is null/empty:"
        )
    else:  # descriptor
        print(
            "\nInspecting stored INLINE blobs with inline_read_mode=DESCRIPTOR —\n"
            "  `data` is suppressed (length(data) is null), `reference.*` synthesized to point\n"
            "  at the .lance file. read_blob() (run in read_blob_and_save) materializes bytes."
        )

    # Register a view from a load that's scoped to the user's chosen inline read mode.
    # We deliberately do this on a per-load basis (not at the SparkSession level) so the
    # later read_blob() call in read_blob_and_save() can use a default-mode read and
    # actually materialize bytes — see the comment in create_spark().
    reader = spark.read.format("hudi")
    if CONFIG["blob_mode"] == "inline":
        reader = reader.option("hoodie.read.blob.inline.mode", rmode.upper())
    reader.load(path).createOrReplaceTempView(DESCRIPTORS_VIEW)

    sql = f"""
        SELECT image_id,
               category,
               image_bytes.type                       AS blob_type,
               length(image_bytes.data)               AS inline_bytes_len,
               image_bytes.reference.external_path    AS ref_path,
               image_bytes.reference.offset           AS ref_offset,
               image_bytes.reference.length           AS ref_length,
               image_bytes.reference.managed          AS ref_managed
        FROM {DESCRIPTORS_VIEW}
        LIMIT 3
    """
    print(sql.strip())
    spark.sql(sql).show(truncate=False)


# ======================================================
# 7. read_blob() — materialize bytes on demand
# ======================================================

RESOLVE_VIEW = "blob_resolve_view"


def read_blob_and_save(spark: SparkSession):
    print(
        f"\n`read_blob(image_bytes)` — resolves each descriptor to its bytes "
        f"(works regardless of inline_read_mode={CONFIG['inline_read_mode']}):"
    )

    # IMPORTANT: register a fresh load WITHOUT the inline.mode option so the underlying
    # read sees `data` populated (CONTENT mode). If we read from the DESCRIPTORS_VIEW
    # registered in show_descriptors(), read_blob() would see data=null because that
    # view was loaded in DESCRIPTOR mode — and BatchedBlobReader dispatches on the row's
    # storage_type=INLINE before checking `reference`, so it would return null bytes.
    spark.read.format("hudi").load(CONFIG["table_path"]).createOrReplaceTempView(RESOLVE_VIEW)

    sql = f"""
        SELECT image_id,
               category,
               length(read_blob(image_bytes)) AS resolved_byte_count
        FROM {RESOLVE_VIEW}
        ORDER BY image_id
        LIMIT 5
    """
    print(sql.strip())
    spark.sql(sql).show(truncate=False)

    # Now actually pull the bytes back into the driver for the first few rows
    # and save them as .png files so we can visually confirm the round-trip.
    print(
        f"\nResolving the first {CONFIG['resolved_images_to_save']} rows' bytes and "
        f"saving them to PNG..."
    )
    pull_sql = f"""
        SELECT image_id, category, read_blob(image_bytes) AS data
        FROM {RESOLVE_VIEW}
        ORDER BY image_id
        LIMIT {CONFIG['resolved_images_to_save']}
    """
    rows = spark.sql(pull_sql).collect()

    out_dir = Path(CONFIG["output_dir"]) / "blob_reader_resolved"
    ensure_dir(out_dir)

    for row in rows:
        bytes_data = bytes(row["data"])
        # Decode to verify it's a real PNG, then re-save.
        img = Image.open(io.BytesIO(bytes_data)).convert("RGB")
        w, h = img.size
        out_path = out_dir / f"{row['image_id']}_{row['category'].replace('/', '_')}.png"
        img.save(out_path, format="PNG")
        print(f"  ✓ {row['image_id']} ({row['category']}) — {len(bytes_data):,} bytes, {w}x{h} → {out_path}")

    return rows


# ======================================================
# 8. COMPARE FOOTPRINTS — table vs container
# ======================================================

def compare_footprints():
    print("\nOn-disk footprint:")
    table_size = _dir_size(Path(CONFIG["table_path"]))
    print(f"  Hudi table:      {table_size / (1024*1024):7.2f} MB  ({CONFIG['table_path']})")

    if CONFIG["blob_mode"] == "out_of_line":
        container_size = Path(CONFIG["blob_container_path"]).stat().st_size
        print(f"  Blob container:  {container_size / (1024*1024):7.2f} MB  ({CONFIG['blob_container_path']})")
        if container_size > 0:
            ratio = table_size / container_size
            print(f"  Hudi/container ratio: {ratio:.3%}  — table holds pointers, not bytes.")
    else:
        print("  (INLINE mode: bytes live INSIDE the Hudi table; no external container)")


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


# ======================================================
# MAIN
# ======================================================

def main():
    fmt = CONFIG["base_file_format"].upper()
    mode = CONFIG["blob_mode"].upper()
    rmode = CONFIG["inline_read_mode"].upper()
    print("\n" + "=" * 80)
    print(
        f"HUDI BLOB READER DEMO  "
        f"[base: {fmt} | blob_mode: {mode} | inline_read: {rmode}]"
    )
    print("read_blob() round-trip on Oxford-IIIT Pet")
    print("=" * 80)
    print(f"  base_file_format : {CONFIG['base_file_format']}")
    print(f"  blob_mode        : {CONFIG['blob_mode']}")
    print(
        f"  inline_read_mode : {CONFIG['inline_read_mode']}"
        + ("  (no-op for out_of_line)" if CONFIG["blob_mode"] == "out_of_line" else "")
    )
    print(f"  table_path       : {CONFIG['table_path']}")
    print(f"  table_name       : {CONFIG['table_name']}")
    print(f"  n_samples        : {CONFIG['n_samples']}")
    if CONFIG["blob_mode"] == "out_of_line":
        print(f"  blob_container   : {CONFIG['blob_container_path']}")
    print("=" * 80 + "\n")

    spark = create_spark()

    rows = load_images(CONFIG["n_samples"])
    if CONFIG["blob_mode"] == "out_of_line":
        pack_into_container(rows, CONFIG["blob_container_path"])
    stage_and_register(spark, rows)

    create_hudi_table(spark)
    insert_blob_rows(spark)

    show_descriptors(spark)
    read_blob_and_save(spark)
    compare_footprints()

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"✓ Dataset:     {CONFIG['dataset']} ({CONFIG['n_samples']} images)")
    print(f"✓ Mode:        {CONFIG['blob_mode']}")
    if CONFIG["blob_mode"] == "out_of_line":
        print(f"✓ Container:   {CONFIG['blob_container_path']} (all raw PNG bytes)")
        print(f"✓ Table:       {CONFIG['table_path']} (references only, no bytes)")
    else:
        print(f"✓ Table:       {CONFIG['table_path']} (bytes embedded INLINE)")
    print(f"✓ Base format: {CONFIG['base_file_format']}")
    print(
        f"✓ Inline read: {CONFIG['inline_read_mode']}"
        + ("  (no-op — out_of_line rows are descriptors regardless)"
           if CONFIG["blob_mode"] == "out_of_line" else "")
    )
    if CONFIG["blob_mode"] == "out_of_line":
        print("✓ Story:       tiny Hudi table of descriptors; `read_blob()` resolves bytes on demand.")
    elif CONFIG["inline_read_mode"] == "content":
        print("✓ Story:       bytes live in the Hudi base files and are returned directly via image_bytes.data.")
    else:  # inline + descriptor
        print("✓ Story:       bytes live in the Hudi base files but are read lazily — image_bytes.data is null,")
        print("               read_blob() materializes them on demand. Same lazy story as out_of_line.")
    print("=" * 80 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
