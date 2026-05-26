#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Spark Connect + GCS end-to-end smoke test for Hudi VECTOR indexes.

Writes a small COPY_ON_WRITE table under gs://mystique_qa/hudi-vector/ (or HOODIE_GCS_VECTOR_BASE),
reads it back, validates VECTOR(dim) schema metadata (hudi_type), builds a VECTOR index through SQL,
and checks that vector index metadata records are visible through hudi_metadata(...).

Prerequisites on the Connect cluster:
  - Hudi Spark bundle compatible with your table version
  - GCS connector + credentials (workload identity, ADC, or GOOGLE_APPLICATION_CREDENTIALS)

Usage (example — adjust proxy/cert to your environment):

  export HTTP_PROXY=http://sysproxy.wal-mart.com:8080
  export HTTPS_PROXY=http://sysproxy.wal-mart.com:8080
  export SPARK_REMOTE="sc://lh-benchmark-query-runner.sample-app.spark.apps.useast4-dev-gke-st-spark00.k8s.us.walmart.net:443/;use_ssl=true"
  export HOODIE_GCS_VECTOR_BASE="gs://mystique_qa/hudi-vector"
  export GRPC_DEFAULT_SSL_ROOTS_FILE_PATH="$HOME/lakehouse/sample-app-root-ca.crt"

  python3 scripts/spark_connect_vector_gcs_test.py
"""

from __future__ import annotations

import json
import math
import struct
from typing import Callable, Optional, Tuple
import os
import random
import time
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType, StringType, StructField, StructType

# HoodieSchema.TYPE_METADATA_FIELD — must match org.apache.hudi.common.schema.HoodieSchema
HOODIE_TYPE_METADATA_KEY = "hudi_type"

SPARK_REMOTE = os.getenv(
    "SPARK_REMOTE",
    "sc://lh-benchmark-query-runner.sample-app.spark.apps.useast4-dev-gke-st-spark00.k8s.us.walmart.net:443/;use_ssl=true",
)
PARQUET_VECTORIZED_READER = os.getenv(
    "HOODIE_VECTOR_TEST_PARQUET_VECTORIZED_READER",
    "true",
)

HOODIE_GCS_VECTOR_BASE = os.getenv("HOODIE_GCS_VECTOR_BASE", "gs://mystique_qa/hudi-vector").rstrip("/")

TABLE_NAME = os.getenv("HOODIE_VECTOR_TEST_TABLE_NAME", "spark_connect_vector_gcs_it")
INDEX_NAME = os.getenv("HOODIE_VECTOR_TEST_INDEX_NAME", "vec_idx")
BENCHMARK_ITERATIONS = int(os.getenv("HOODIE_VECTOR_BENCHMARK_ITERATIONS", "3"))
VECTOR_QUANTIZER = os.getenv("HOODIE_VECTOR_TEST_QUANTIZER", "IVF_RABITQ")
MATERIALIZE_ON_CREATE = os.getenv("HOODIE_VECTOR_TEST_MATERIALIZE_ON_CREATE", "false").lower() == "true"
VECTOR_NUM_CLUSTERS = int(os.getenv("HOODIE_VECTOR_TEST_NUM_CLUSTERS", "256"))
VECTOR_NUM_CLUSTERS_AUTO = os.getenv("HOODIE_VECTOR_TEST_NUM_CLUSTERS_AUTO", "false").lower() == "true"
LOCK_PROVIDER = os.getenv(
    "HOODIE_VECTOR_TEST_LOCK_PROVIDER",
    "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
)
VECTOR_READ_INDEX_OPTION = "hoodie.datasource.read.vector.index.name"
VECTOR_READ_QUERY_OPTION = "hoodie.datasource.read.vector.query.vector"
VECTOR_READ_NPROBES_OPTION = "hoodie.datasource.read.vector.query.nprobes"
VECTOR_RECORD_TYPE = 8
VECTOR_FG_KEY_PREFIX = "__fg__/"
VECTOR_CENTROIDS_KEY = "__centroids__"
VECTOR_QUANTIZER_KEY = "__quantizer__"
VECTOR_MANIFEST_KEY = "__manifest__"
VECTOR_GENERATION_MANIFEST_KEY_PREFIX = "M|"
VECTOR_CLUSTER_KEY_PREFIX = "C|"
VECTOR_ASSIGNMENT_KEY_PREFIX = "A|"
VECTOR_POSTING_KEY_PREFIX = "P|"
RABITQ_APPROX_DISTANCE_FN = "hudi_rabitq_distance"
RABITQ_APPROX_DISTANCE_UDF_CLASS = "org.apache.hudi.sql.RaBitQApproxDistanceUDF"
VECTOR_QUERY_TOPK = int(os.getenv("HOODIE_VECTOR_QUERY_TOPK", "10"))
VECTOR_QUERY_NPROBES = int(os.getenv("HOODIE_VECTOR_QUERY_NPROBES", "4"))
VECTOR_QUERY_REFINE_FACTOR = int(os.getenv("HOODIE_VECTOR_QUERY_REFINE_FACTOR", "10"))
ENABLE_FULL_SCAN_BASELINE = os.getenv("HOODIE_VECTOR_ENABLE_FULL_SCAN_BASELINE", "true").lower() == "true"
STRICT_ROW_COUNT_CHECK = os.getenv("HOODIE_VECTOR_STRICT_ROW_COUNT_CHECK", "true").lower() == "true"
RABITQ_STORAGE = os.getenv("HOODIE_VECTOR_TEST_RABITQ_STORAGE", "hidden_columns").lower()
ENABLE_MDT_POSTING_BENCHMARK = os.getenv("HOODIE_VECTOR_ENABLE_MDT_POSTING_BENCHMARK", "true").lower() == "true"
USE_SPARK_RANGE_GENERATOR = os.getenv("HOODIE_VECTOR_USE_SPARK_RANGE_GENERATOR", "true").lower() == "true"
USE_WRITE_SCHEMA_OVERRIDE = os.getenv("HOODIE_VECTOR_USE_WRITE_SCHEMA_OVERRIDE", "true").lower() == "true"
RABITQ_POSTING_TARGET_ROWS_PER_SHARD = int(
    os.getenv("HOODIE_VECTOR_TEST_RABITQ_POSTING_TARGET_ROWS_PER_SHARD", "4096")
)
RABITQ_POSTING_MAX_SHARDS_PER_CLUSTER = int(
    os.getenv("HOODIE_VECTOR_TEST_RABITQ_POSTING_MAX_SHARDS_PER_CLUSTER", "64")
)
INSERT_PARALLELISM = int(os.getenv("HOODIE_VECTOR_TEST_INSERT_PARALLELISM", "200"))
UPSERT_PARALLELISM = int(os.getenv("HOODIE_VECTOR_TEST_UPSERT_PARALLELISM", "200"))
PARQUET_MAX_FILE_SIZE = os.getenv("HOODIE_VECTOR_TEST_PARQUET_MAX_FILE_SIZE", "1048576")
MDT_RECORD_PREPARATION_PARALLELISM = int(
    os.getenv("HOODIE_VECTOR_TEST_MDT_RECORD_PREPARATION_PARALLELISM", str(max(INSERT_PARALLELISM, 1)))
)
MDT_REPARTITION_DEFAULT_PARTITIONS = int(
    os.getenv("HOODIE_VECTOR_TEST_MDT_REPARTITION_DEFAULT_PARTITIONS", str(max(INSERT_PARALLELISM, 1)))
)
SPARK_SQL_SHUFFLE_PARTITIONS = int(
    os.getenv("HOODIE_VECTOR_TEST_SPARK_SQL_SHUFFLE_PARTITIONS", str(max(INSERT_PARALLELISM, 1)))
)
NUM_ROWS_LIST_RAW = os.getenv("HOODIE_VECTOR_TEST_NUM_ROWS_LIST", "")

# Optional: point SSL at a corporate root (same pattern as enable_rli_on_existing_tables.py)
_WALMART_CA = os.path.expanduser(
    os.getenv("WALMART_SAMPLE_APP_CA", os.path.join(os.path.expanduser("~"), "lakehouse/sample-app-root-ca.crt"))
)
if os.path.isfile(_WALMART_CA):
    os.environ.setdefault("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", _WALMART_CA)
    os.environ.setdefault("SSL_CERT_FILE", _WALMART_CA)
    print(f"SSL: using {_WALMART_CA}")


def _vector_schema(dim: int) -> StructType:
    meta = {HOODIE_TYPE_METADATA_KEY: f"VECTOR({dim})"}
    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("embedding", ArrayType(FloatType(), False), False, metadata=meta),
            StructField("label", StringType(), True),
        ]
    )


def _build_spark_generated_df(spark: SparkSession, dim: int, n_rows: int):
    rand_cols = [F.rand(seed=7 + i).cast("float") for i in range(dim)]
    vector_meta = {HOODIE_TYPE_METADATA_KEY: f"VECTOR({dim})"}
    return (
        spark.range(0, n_rows)
        .select(
            F.concat(F.lit("k"), F.col("id")).alias("id"),
            F.array(*rand_cols).alias("embedding", metadata=vector_meta),
            F.concat(F.lit("l"), F.col("id")).alias("label"),
        )
    )


def _vector_write_schema_json(dim: int) -> str:
    return json.dumps(
        {
            "type": "record",
            "name": "vector_connect_record",
            "namespace": "hoodie.vector.connect",
            "fields": [
                {"name": "id", "type": "string"},
                {
                    "name": "embedding",
                    "type": {
                        "type": "fixed",
                        "name": f"vector_float_{dim}",
                        "size": dim * 4,
                        "logicalType": "vector",
                        "dimension": dim,
                        "elementType": "FLOAT",
                        "storageBacking": "FIXED_BYTES",
                    },
                },
                {"name": "label", "type": ["null", "string"], "default": None},
            ],
        }
    )


def _l2(a: list[float], b: list[float]) -> float:
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


def _deserialize_float_centroids(raw_bytes: bytes, dim: int) -> list[list[float]]:
    if raw_bytes is None:
        return []
    centroid_stride = dim * 4
    if centroid_stride == 0:
        return []
    if len(raw_bytes) % centroid_stride != 0:
        raise ValueError(
            f"Centroid payload size {len(raw_bytes)} is not divisible by dim stride {centroid_stride}"
        )
    centroids = []
    for offset in range(0, len(raw_bytes), centroid_stride):
        chunk = raw_bytes[offset : offset + centroid_stride]
        centroids.append(list(struct.unpack("<" + "f" * dim, chunk)))
    return centroids


def _top_probe_clusters(query_vector: list[float], centroids: list[list[float]], nprobes: int) -> list[int]:
    ranked = sorted(
        enumerate(centroids),
        key=lambda pair: _l2(query_vector, pair[1]),
    )
    return [cluster_id for cluster_id, _ in ranked[: max(1, nprobes)]]


def _brute_force_nearest(rows: list, query: list[float]) -> Tuple[Optional[str], float]:
    best_id: Optional[str] = None
    best_d = float("inf")
    for r in rows:
        d = _l2(r.embedding, query)
        if d < best_d:
            best_d = d
            best_id = r.id
    return best_id, best_d


def _time_action(name: str, iterations: int, action: Callable[[], Tuple[int, Optional[str]]]) -> Tuple[float, int, Optional[str]]:
    timings = []
    last_count = 0
    last_note: Optional[str] = None
    for _ in range(iterations):
      t0 = time.time()
      count, note = action()
      timings.append(time.time() - t0)
      last_count = count
      last_note = note
    avg_secs = sum(timings) / len(timings)
    print(f"{name}: avg={avg_secs:.3f}s over {iterations} runs, rows={last_count}, detail={last_note}")
    return avg_secs, last_count, last_note


def _parse_num_rows_list(default_rows: int) -> list[int]:
    if not NUM_ROWS_LIST_RAW.strip():
        return [default_rows]
    rows = []
    for token in NUM_ROWS_LIST_RAW.split(","):
        token = token.strip().lower()
        if token.endswith("m"):
            rows.append(int(float(token[:-1]) * 1_000_000))
        elif token.endswith("k"):
            rows.append(int(float(token[:-1]) * 1_000))
        else:
            rows.append(int(token))
    return rows


def _recommended_num_clusters(n_rows: int) -> int:
    # Conservative IVF sizing rule for smoke benchmarking.
    if n_rows <= 1_000_000:
        return 256
    if n_rows <= 10_000_000:
        return 1024
    return 4096


def _logical_index_name(index_name: str) -> str:
    return index_name.removeprefix("vector_index_")


def _rabitq_binary_column(index_name: str) -> str:
    return f"_hudi_vec_{_logical_index_name(index_name)}_binary_code"


def _rabitq_scalar_column(index_name: str) -> str:
    return f"_hudi_vec_{_logical_index_name(index_name)}_scalar"


def _register_rabitq_distance_udf(spark: SparkSession) -> bool:
    try:
        spark.udf.registerJavaFunction(
            RABITQ_APPROX_DISTANCE_FN, RABITQ_APPROX_DISTANCE_UDF_CLASS, "float"
        )
        print(
            "Registered Spark SQL RaBitQ UDF: "
            f"{RABITQ_APPROX_DISTANCE_FN} -> {RABITQ_APPROX_DISTANCE_UDF_CLASS}"
        )
        return True
    except Exception as exc:
        print(f"Skipping RaBitQ benchmark: failed to register Java UDF: {exc}")
        return False



def _exact_l2_sql(vector_col: str, query_vector: list[float]) -> str:
    query_array_sql = ", ".join(f"{float(value):.12g}D" for value in query_vector)
    return (
        "sqrt(aggregate("
        f"zip_with({vector_col}, array({query_array_sql}), "
        "(x, y) -> pow(cast(x as double) - y, 2D)), "
        "cast(0D as double), "
        "(acc, x) -> acc + x))"
    )


def _sql_quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def main() -> None:
    dim = int(os.getenv("HOODIE_VECTOR_TEST_DIM", "8"))
    n_rows_default = int(os.getenv("HOODIE_VECTOR_TEST_NUM_ROWS", "5"))
    n_rows_list = _parse_num_rows_list(n_rows_default)
    if len(n_rows_list) > 1:
        print(
            "INFO: HOODIE_VECTOR_TEST_NUM_ROWS_LIST provided; "
            "this script currently executes one size per run. "
            f"Using first size: {n_rows_list[0]}"
        )
    n_rows = n_rows_list[0]
    run_id = uuid.uuid4().hex[:12]
    table_path = f"{HOODIE_GCS_VECTOR_BASE}/vector_connect_it_{run_id}"
    catalog_table = f"{TABLE_NAME}_{run_id}"
    num_clusters = _recommended_num_clusters(n_rows) if VECTOR_NUM_CLUSTERS_AUTO else VECTOR_NUM_CLUSTERS

    print("Connecting to Spark Connect...")
    print(f"  SPARK_REMOTE={SPARK_REMOTE}")
    print(f"  table_path={table_path}")
    print(f"  vector_quantizer={VECTOR_QUANTIZER}")
    print(f"  rabitq_storage={RABITQ_STORAGE}")
    print(f"  materialize_on_create={MATERIALIZE_ON_CREATE}")
    print(f"  lock_provider={LOCK_PROVIDER}")
    print(f"  num_clusters={num_clusters}")
    print(f"  rabitq_posting_target_rows_per_shard={RABITQ_POSTING_TARGET_ROWS_PER_SHARD}")
    print(f"  rabitq_posting_max_shards_per_cluster={RABITQ_POSTING_MAX_SHARDS_PER_CLUSTER}")
    print(f"  spark.sql.shuffle.partitions={SPARK_SQL_SHUFFLE_PARTITIONS}")
    print(f"  hoodie.metadata.record.preparation.parallelism={MDT_RECORD_PREPARATION_PARALLELISM}")
    print(f"  hoodie.metadata.repartition.default.partitions={MDT_REPARTITION_DEFAULT_PARTITIONS}")

    spark = (
        SparkSession.builder.remote(SPARK_REMOTE)
        .config("spark.sql.parquet.enableVectorizedReader", PARQUET_VECTORIZED_READER)
        .config("spark.sql.shuffle.partitions", str(SPARK_SQL_SHUFFLE_PARTITIONS))
        .config("hoodie.write.lock.provider", LOCK_PROVIDER)
        .config("hoodie.metadata.record.preparation.parallelism", str(MDT_RECORD_PREPARATION_PARALLELISM))
        .config("hoodie.metadata.repartition.default.partitions", str(MDT_REPARTITION_DEFAULT_PARTITIONS))
        .getOrCreate()
    )
    print(f"Connected. Spark version: {spark.version}")
    print(f"  parquet_vectorized_reader={PARQUET_VECTORIZED_READER}")
    spark.sql(f"SET hoodie.write.lock.provider={LOCK_PROVIDER}")
    spark.sql(f"SET spark.sql.shuffle.partitions={SPARK_SQL_SHUFFLE_PARTITIONS}")
    spark.sql(
        f"SET hoodie.metadata.record.preparation.parallelism={MDT_RECORD_PREPARATION_PARALLELISM}"
    )
    spark.sql(
        f"SET hoodie.metadata.repartition.default.partitions={MDT_REPARTITION_DEFAULT_PARTITIONS}"
    )

    if USE_SPARK_RANGE_GENERATOR:
        print("Data generator: Spark range-based generator (distributed)")
        df = _build_spark_generated_df(spark, dim, n_rows)
    else:
        print("Data generator: Python local generator")
        rnd = random.Random(7)
        schema = _vector_schema(dim)
        rows_py = [
            (f"k{i}", [rnd.random() for _ in range(dim)], f"l{i}") for i in range(n_rows)
        ]
        df = spark.createDataFrame(rows_py, schema=schema)

    hudi_opts = {
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "id",
        "hoodie.datasource.write.table.name": TABLE_NAME,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.insert.shuffle.parallelism": str(INSERT_PARALLELISM),
        "hoodie.upsert.shuffle.parallelism": str(UPSERT_PARALLELISM),
        "hoodie.parquet.max.file.size": str(PARQUET_MAX_FILE_SIZE),
        "hoodie.metadata.record.preparation.parallelism": str(MDT_RECORD_PREPARATION_PARALLELISM),
        "hoodie.metadata.repartition.default.partitions": str(MDT_REPARTITION_DEFAULT_PARTITIONS),
    }
    if USE_WRITE_SCHEMA_OVERRIDE:
        hudi_opts["hoodie.write.schema"] = _vector_write_schema_json(dim)

    t0 = time.time()
    print(f"Writing {n_rows} rows (VECTOR({dim})) to {table_path} ...")
    (
        df.write.format("hudi")
        .mode("overwrite")
        .options(**hudi_opts)
        .option("path", table_path)
        .save()
    )
    print(f"Write finished in {time.time() - t0:.1f}s")

    read_df = spark.read.format("hudi").load(table_path)
    cnt = read_df.count()
    if STRICT_ROW_COUNT_CHECK:
        assert cnt == n_rows, f"expected {n_rows} rows, got {cnt}"
    elif cnt != n_rows:
        print(f"WARNING: row-count mismatch (expected {n_rows}, got {cnt}); continuing due to HOODIE_VECTOR_STRICT_ROW_COUNT_CHECK=false")

    emb_field = next(f for f in read_df.schema.fields if f.name == "embedding")
    assert isinstance(emb_field.dataType, ArrayType)
    md = emb_field.metadata or {}
    if HOODIE_TYPE_METADATA_KEY in md:
        hudi_type = md[HOODIE_TYPE_METADATA_KEY]
        assert hudi_type.startswith("VECTOR("), f"unexpected hudi_type: {hudi_type}"
        print(f"Schema OK: embedding metadata {HOODIE_TYPE_METADATA_KEY}={hudi_type!r}")
    else:
        print(
            "Read schema is missing hudi_type metadata; continuing because the explicit "
            "hoodie.write.schema override should still preserve VECTOR semantics for index creation."
        )

    # Distributed sanity check: collect one query row, then distributed exact top1.
    query_row = read_df.select("id", "embedding").limit(1).collect()[0]
    query = query_row.embedding
    nearest_row = (
        read_df.selectExpr("id", f"{_exact_l2_sql('embedding', query)} AS exact_distance")
        .orderBy("exact_distance")
        .limit(1)
        .collect()[0]
    )
    assert nearest_row.exact_distance < 1e-5, (
        f"expected ~0 distance to embedded query, got {nearest_row.exact_distance}"
    )
    print(f"Vector distance check OK: nearest id={nearest_row.id!r}, L2={nearest_row.exact_distance:.6g}")

    print(f"Registering catalog table {catalog_table} ...")
    spark.sql(f"DROP TABLE IF EXISTS {catalog_table}")
    spark.sql(f"CREATE TABLE {catalog_table} USING hudi LOCATION '{table_path}'")

    print(f"Creating vector index {INDEX_NAME} ...")
    t1 = time.time()
    index_options = [
        f"'vector.dimension' = '{dim}'",
        f"'vector.num_clusters' = '{num_clusters}'",
        "'vector.metric' = 'l2'",
        "'vector.max_iter' = '5'",
    ]
    if VECTOR_QUANTIZER:
        index_options.append(f"'vector.quantizer' = '{VECTOR_QUANTIZER}'")
    if RABITQ_STORAGE:
        index_options.append(f"'vector.rabitq.storage' = '{RABITQ_STORAGE}'")
        index_options.append(
            f"'vector.rabitq.posting.target_rows_per_shard' = '{RABITQ_POSTING_TARGET_ROWS_PER_SHARD}'"
        )
        index_options.append(
            f"'vector.rabitq.posting.max_shards_per_cluster' = '{RABITQ_POSTING_MAX_SHARDS_PER_CLUSTER}'"
        )
    if MATERIALIZE_ON_CREATE:
        index_options.append("'vector.rabitq.materialize.on.create' = 'true'")

    spark.sql(
        f"""
        CREATE INDEX {INDEX_NAME}
        ON {catalog_table}
        USING VECTOR (embedding)
        OPTIONS (
          {", ".join(index_options)}
        )
        """
    )
    print(f"Index build finished in {time.time() - t1:.1f}s")

    show_indexes = spark.sql(f"SHOW INDEXES FROM {catalog_table}").collect()
    print("SHOW INDEXES:")
    for row in show_indexes:
        print(f"  {row}")

    metadata_rows = spark.sql(
        f"""
        SELECT key
        FROM hudi_metadata('{table_path}')
        WHERE type = 8
        ORDER BY key
        """
    ).collect()
    metadata_keys = [row.key for row in metadata_rows]
    assert "__centroids__" in metadata_keys, (
        f"vector centroid record not found in MDT keys: {metadata_keys}"
    )
    assert VECTOR_QUANTIZER_KEY in metadata_keys, (
        f"vector quantizer record not found in MDT keys: {metadata_keys}"
    )
    assert len(metadata_keys) >= cnt + 1, (
        f"expected at least {cnt + 1} vector MDT records, got {len(metadata_keys)}"
    )
    print(f"Vector MDT records OK: {len(metadata_keys)} records, centroid + quantizer keys present")

    centroid_rows = spark.sql(
        f"""
        SELECT
          key,
          VectorIndexMetadata.clusterId AS cluster_id,
          VectorIndexMetadata.centroidBytes AS centroid_bytes_blob,
          length(VectorIndexMetadata.centroidBytes) AS centroid_bytes
        FROM hudi_metadata('{table_path}')
        WHERE type = {VECTOR_RECORD_TYPE}
          AND key = '{VECTOR_CENTROIDS_KEY}'
        """
    ).collect()
    print("Centroid metadata sample:")
    for row in centroid_rows:
        print(f"  {row}")
    centroid_blob = centroid_rows[0].centroid_bytes_blob if centroid_rows else None
    centroids = _deserialize_float_centroids(bytes(centroid_blob), dim) if centroid_blob is not None else []
    probed_clusters = _top_probe_clusters(query, centroids, VECTOR_QUERY_NPROBES) if centroids else []
    print(f"Probed clusters (nprobes={VECTOR_QUERY_NPROBES}): {probed_clusters}")

    quantizer_rows = spark.sql(
        f"""
        SELECT
          key,
          VectorIndexMetadata.quantizerType AS quantizer_type,
          VectorIndexMetadata.quantizedCodeBytes AS quantized_code_bytes,
          VectorIndexMetadata.randomSeed AS random_seed,
          VectorIndexMetadata.assumeNormalized AS assume_normalized
        FROM hudi_metadata('{table_path}')
        WHERE type = {VECTOR_RECORD_TYPE}
          AND key = '{VECTOR_QUANTIZER_KEY}'
        """
    ).collect()
    assert quantizer_rows, "expected vector quantizer row in MDT, found none"
    print("Quantizer metadata sample:")
    for row in quantizer_rows:
        print(f"  {row}")

    assignment_rows = spark.sql(
        f"""
        SELECT
          key,
          VectorIndexMetadata.clusterId AS cluster_id,
          VectorIndexMetadata.fileGroupId AS file_group_id,
          VectorIndexMetadata.partitionPath AS partition_path,
          VectorIndexMetadata.isDeleted AS is_deleted
        FROM hudi_metadata('{table_path}')
        WHERE type = {VECTOR_RECORD_TYPE}
          AND key LIKE '{VECTOR_ASSIGNMENT_KEY_PREFIX}%'
        ORDER BY key
        LIMIT 5
        """
    ).collect()
    assert assignment_rows, "expected vector assignment rows in MDT, found none"
    print("Assignment metadata samples:")
    for row in assignment_rows:
        print(f"  {row}")

    fg_mapping_rows = spark.sql(
        f"""
        SELECT
          key,
          VectorIndexMetadata.clusterId AS cluster_id,
          VectorIndexMetadata.partitionPath AS partition_path,
          VectorIndexMetadata.fileGroupIds AS file_group_ids,
          VectorIndexMetadata.vectorCount AS vector_count,
          VectorIndexMetadata.lastUpdatedTs AS last_updated_ts
        FROM hudi_metadata('{table_path}')
        WHERE type = {VECTOR_RECORD_TYPE}
          AND key LIKE '{VECTOR_FG_KEY_PREFIX}%'
        ORDER BY key
        LIMIT 5
        """
    ).collect()
    assert fg_mapping_rows, "expected vector fg_mapping rows in MDT, found none"
    print("fg_mapping metadata samples:")
    for row in fg_mapping_rows:
        print(f"  {row}")

    if RABITQ_STORAGE in ("mdt_lookup", "both"):
        manifest_rows = spark.sql(
            f"""
            SELECT
              key,
              VectorIndexMetadata.entryType AS entry_type,
              VectorIndexMetadata.generationId AS generation_id,
              VectorIndexMetadata.quantizerType AS quantizer_type,
              VectorIndexMetadata.quantizedCodeBytes AS quantized_code_bytes
            FROM hudi_metadata('{table_path}')
            WHERE type = {VECTOR_RECORD_TYPE}
              AND key = '{VECTOR_MANIFEST_KEY}'
            """
        ).collect()
        assert manifest_rows, "expected vector manifest row in MDT for mdt_lookup/both mode"
        print("Manifest metadata sample:")
        for row in manifest_rows:
            print(f"  {row}")

        cluster_manifest_rows = spark.sql(
            f"""
            SELECT
              key,
              VectorIndexMetadata.entryType AS entry_type,
              VectorIndexMetadata.generationId AS generation_id,
              VectorIndexMetadata.clusterId AS cluster_id,
              VectorIndexMetadata.shardCount AS shard_count,
              VectorIndexMetadata.vectorCount AS vector_count
            FROM hudi_metadata('{table_path}')
            WHERE type = {VECTOR_RECORD_TYPE}
              AND key LIKE '{VECTOR_CLUSTER_KEY_PREFIX}%'
            ORDER BY key
            LIMIT 5
            """
        ).collect()
        assert cluster_manifest_rows, "expected cluster manifest rows in MDT for sharded postings"
        print("Cluster manifest metadata samples:")
        for row in cluster_manifest_rows:
            print(f"  {row}")

        posting_df = spark.sql(
            f"""
            SELECT
              key,
              VectorIndexMetadata.entryType AS entry_type,
              VectorIndexMetadata.generationId AS generation_id,
              VectorIndexMetadata.clusterId AS cluster_id,
              VectorIndexMetadata.shardId AS shard_id,
              VectorIndexMetadata.fileGroupId AS file_group_id,
              length(VectorIndexMetadata.binaryCode) AS binary_code_bytes,
              VectorIndexMetadata.scalar AS scalar
            FROM hudi_metadata('{table_path}')
            WHERE type = {VECTOR_RECORD_TYPE}
              AND key LIKE '{VECTOR_POSTING_KEY_PREFIX}%'
            ORDER BY key
            LIMIT 5
            """
        )
        assert posting_df.count() > 0, "expected vector posting rows in MDT for mdt_lookup/both mode"
        print("Posting metadata samples:")
        posting_df.show(5, truncate=False)

    query_literal = "[" + ", ".join(f"{value:.6f}" for value in query) + "]"
    print("Exercising MDT-backed vector pruning read options ...")
    print(f"  {VECTOR_READ_INDEX_OPTION}={INDEX_NAME}")
    print(f"  {VECTOR_READ_QUERY_OPTION}={query_literal}")
    print(f"  {VECTOR_READ_NPROBES_OPTION}=1")
    pruned_read = (
        spark.read.format("hudi")
        .option(VECTOR_READ_INDEX_OPTION, INDEX_NAME)
        .option(VECTOR_READ_QUERY_OPTION, query_literal)
        .option(VECTOR_READ_NPROBES_OPTION, "1")
        .load(table_path)
    )
    pruned_sample = pruned_read.select("id").limit(3).collect()
    print(f"Vector-pruned read path executed; sample rows returned: {len(pruned_sample)}")

    rabitq_binary_col = _rabitq_binary_column(INDEX_NAME)
    rabitq_scalar_col = _rabitq_scalar_column(INDEX_NAME)
    available_columns = set(pruned_read.columns)
    print(f"Looking for RaBitQ hidden columns: {rabitq_binary_col}, {rabitq_scalar_col}")
    print(
        "RaBitQ hidden column presence: "
        f"binary_code={rabitq_binary_col in available_columns}, "
        f"scalar={rabitq_scalar_col in available_columns}"
    )

    print("\nRunning timing comparison ...")
    print(
        "Vector query settings: "
        f"topK={VECTOR_QUERY_TOPK}, "
        f"nprobes={VECTOR_QUERY_NPROBES}, "
        f"refine_factor={VECTOR_QUERY_REFINE_FACTOR}, "
        f"full_scan_baseline={ENABLE_FULL_SCAN_BASELINE}"
    )

    def full_scan_action() -> Tuple[int, Optional[str]]:
        exact_distance_sql = _exact_l2_sql("embedding", query)
        full_df = spark.read.format("hudi").load(table_path)
        full_file_count = (
            full_df.select("_hoodie_file_name").distinct().count()
        )
        rows = (
            full_df.selectExpr("id", f"{exact_distance_sql} AS exact_distance")
            .orderBy("exact_distance")
            .limit(VECTOR_QUERY_TOPK)
            .collect()
        )
        detail = None if not rows else (
            f"top1_id={rows[0].id}, exact_l2={rows[0].exact_distance:.6g}, files_touched={full_file_count}"
        )
        return len(rows), detail

    def pruned_scan_action() -> Tuple[int, Optional[str]]:
        pruned_df = (
            spark.read.format("hudi")
            .option(VECTOR_READ_INDEX_OPTION, INDEX_NAME)
            .option(VECTOR_READ_QUERY_OPTION, query_literal)
            .option(VECTOR_READ_NPROBES_OPTION, str(VECTOR_QUERY_NPROBES))
            .load(table_path)
        )
        # Keep this distributed: do not collect full candidate rows to driver.
        rows = pruned_df.count()
        file_count = pruned_df.select("_hoodie_file_name").distinct().count()
        return rows, f"files_touched={file_count}"

    if ENABLE_FULL_SCAN_BASELINE:
        full_scan_avg, full_scan_rows, _ = _time_action(
            "full scan + distributed exact topK", BENCHMARK_ITERATIONS, full_scan_action
        )
    else:
        full_scan_avg, full_scan_rows = float("nan"), -1
        print("Skipped full-scan baseline (HOODIE_VECTOR_ENABLE_FULL_SCAN_BASELINE=false)")

    pruned_scan_avg, pruned_scan_rows, _ = _time_action(
        "mdt-pruned candidate scan", BENCHMARK_ITERATIONS, pruned_scan_action
    )
    speedup = (full_scan_avg / pruned_scan_avg) if (ENABLE_FULL_SCAN_BASELINE and pruned_scan_avg > 0) else float("nan")
    print(
        "Benchmark summary: "
        f"full_scan_avg={(f'{full_scan_avg:.3f}s' if ENABLE_FULL_SCAN_BASELINE else 'skipped')}, "
        f"pruned_scan_avg={pruned_scan_avg:.3f}s, "
        f"speedup={(f'{speedup:.2f}x' if ENABLE_FULL_SCAN_BASELINE else 'n/a')}, "
        f"full_rows={(full_scan_rows if ENABLE_FULL_SCAN_BASELINE else 'n/a')}, "
        f"pruned_rows={pruned_scan_rows}"
    )

    quantizer_row = quantizer_rows[0]
    quantizer_type = quantizer_row.quantizer_type
    assume_normalized = bool(quantizer_row.assume_normalized)
    random_seed = int(quantizer_row.random_seed)

    if (
        quantizer_type == "IVF_RABITQ"
        and rabitq_binary_col in available_columns
        and _register_rabitq_distance_udf(spark)
    ):
        def rabitq_scan_action() -> Tuple[int, Optional[str]]:
            candidate_df = (
                spark.read.format("hudi")
                .option(VECTOR_READ_INDEX_OPTION, INDEX_NAME)
                .option(VECTOR_READ_QUERY_OPTION, query_literal)
                .option(VECTOR_READ_NPROBES_OPTION, str(VECTOR_QUERY_NPROBES))
                .load(table_path)
                .where(f"{rabitq_binary_col} IS NOT NULL")
                .select("id", "embedding", rabitq_binary_col, *([rabitq_scalar_col] if rabitq_scalar_col in available_columns else []))
                .cache()
            )
            candidate_count = candidate_df.count()
            scalar_sql = rabitq_scalar_col if rabitq_scalar_col in available_columns else "CAST(NULL AS FLOAT)"
            approx_distance_sql = (
                f"{RABITQ_APPROX_DISTANCE_FN}("
                f"{rabitq_binary_col}, "
                f"{scalar_sql}, "
                f"'{query_literal}', "
                f"{dim}, "
                f"{random_seed}L, "
                f"{str(assume_normalized).lower()})"
            )
            exact_distance_sql = _exact_l2_sql("embedding", query)
            rerank_limit = max(VECTOR_QUERY_TOPK, VECTOR_QUERY_TOPK * max(1, VECTOR_QUERY_REFINE_FACTOR))
            reranked = (
                candidate_df.selectExpr("id", "embedding", f"{approx_distance_sql} AS approx_distance")
                .orderBy("approx_distance")
                .limit(rerank_limit)
                .selectExpr("id", f"{exact_distance_sql} AS exact_distance")
                .orderBy("exact_distance")
                .limit(VECTOR_QUERY_TOPK)
                .collect()
            )
            candidate_df.unpersist()
            detail = None if not reranked else (
                f"top1_id={reranked[0].id}, exact_l2={reranked[0].exact_distance:.6g}, topk={len(reranked)}"
            )
            return candidate_count, detail

        rabitq_avg, rabitq_rows, rabitq_detail = _time_action(
            "mdt-pruned + rabitq approx + exact rerank", BENCHMARK_ITERATIONS, rabitq_scan_action
        )
        rabitq_speedup = (full_scan_avg / rabitq_avg) if (ENABLE_FULL_SCAN_BASELINE and rabitq_avg > 0) else float("nan")
        print(
            "RaBitQ benchmark summary: "
            f"rabitq_avg={rabitq_avg:.3f}s, "
            f"speedup_vs_full={(f'{rabitq_speedup:.2f}x' if ENABLE_FULL_SCAN_BASELINE else 'n/a')}, "
            f"candidate_rows={rabitq_rows}, "
            f"detail={rabitq_detail}"
        )
    else:
        print(
            "Skipping RaBitQ timing. "
            "This needs the new Spark jar plus materialized hidden code columns on the table."
        )

    if (
        quantizer_type == "IVF_RABITQ"
        and RABITQ_STORAGE in ("mdt_lookup", "both")
        and ENABLE_MDT_POSTING_BENCHMARK
        and _register_rabitq_distance_udf(spark)
    ):
        print("Running MDT posting benchmark ...")
        rerank_limit = max(VECTOR_QUERY_TOPK, VECTOR_QUERY_TOPK * max(1, VECTOR_QUERY_REFINE_FACTOR))
        manifest_generation_id = manifest_rows[0].generation_id if manifest_rows else None
        if not manifest_generation_id:
            raise AssertionError("Manifest generation_id is required for MDT posting benchmark")
        if not probed_clusters:
            raise AssertionError("No probed clusters available for MDT posting benchmark")
        probed_cluster_predicate = ", ".join(str(int(cluster_id)) for cluster_id in probed_clusters)
        selected_cluster_manifests = spark.sql(
            f"""
            SELECT
              VectorIndexMetadata.clusterId AS cluster_id,
              VectorIndexMetadata.shardCount AS shard_count,
              VectorIndexMetadata.vectorCount AS vector_count
            FROM hudi_metadata('{table_path}')
            WHERE type = {VECTOR_RECORD_TYPE}
              AND VectorIndexMetadata.entryType = 'CLUSTER'
              AND VectorIndexMetadata.generationId = '{manifest_generation_id}'
              AND VectorIndexMetadata.clusterId IN ({probed_cluster_predicate})
            ORDER BY cluster_id
            """
        ).collect()
        total_selected_shards = sum(int(row.shard_count or 0) for row in selected_cluster_manifests)
        total_selected_postings = sum(int(row.vector_count or 0) for row in selected_cluster_manifests)
        cluster_id_predicate = ", ".join(str(int(cluster_id)) for cluster_id in probed_clusters)
        approx_distance_sql = (
            f"{RABITQ_APPROX_DISTANCE_FN}("
            "VectorIndexMetadata.binaryCode, "
            "VectorIndexMetadata.scalar, "
            f"'{query_literal}', "
            f"{dim}, "
            f"{random_seed}L, "
            f"{str(assume_normalized).lower()})"
        )

        def mdt_posting_action() -> Tuple[int, Optional[str]]:
            approx_df = spark.sql(
                f"""
                SELECT
                  key AS posting_key,
                  {approx_distance_sql} AS approx_distance
                FROM hudi_metadata('{table_path}')
                WHERE type = {VECTOR_RECORD_TYPE}
                  AND VectorIndexMetadata.entryType = 'POSTING'
                  AND VectorIndexMetadata.generationId = '{manifest_generation_id}'
                  AND VectorIndexMetadata.clusterId IN ({cluster_id_predicate})
                ORDER BY approx_distance ASC
                LIMIT {rerank_limit}
                """
            ).selectExpr(
                r"regexp_extract(posting_key, '^P\\|[^|]+\\|[^|]+\\|[^|]+\\|(.*)$', 1) AS id",
                "approx_distance"
            ).where("id IS NOT NULL")

            candidate_count = approx_df.count()
            if candidate_count == 0:
                return 0, None

            exact_distance_sql = _exact_l2_sql("embedding", query)
            base_df = spark.read.format("hudi").load(table_path)
            rerank_df = (
                base_df.join(F.broadcast(approx_df.select("id")), on="id", how="inner")
            )
            exact_rows = (
                rerank_df.selectExpr("id", f"{exact_distance_sql} AS exact_distance")
                .orderBy("exact_distance")
                .limit(VECTOR_QUERY_TOPK)
                .collect()
            )
            exact_file_count = (
                rerank_df.select("_hoodie_file_name")
                .distinct()
                .count()
            )
            detail = None if not exact_rows else (
                f"top1_id={exact_rows[0].id}, exact_l2={exact_rows[0].exact_distance:.6g}, "
                f"topk={len(exact_rows)}, files_touched={exact_file_count}, "
                f"selected_clusters={len(selected_cluster_manifests)}, selected_shards={total_selected_shards}, "
                f"selected_postings={total_selected_postings}"
            )
            return candidate_count, detail

        mdt_avg, mdt_candidates, mdt_detail = _time_action(
            "mdt-posting approx + exact rerank", BENCHMARK_ITERATIONS, mdt_posting_action
        )
        print(
            "MDT posting benchmark summary: "
            f"mdt_avg={mdt_avg:.3f}s, "
            f"candidate_rows={mdt_candidates}, "
            f"detail={mdt_detail}"
        )

    print("\n" + "=" * 60)
    print("PASS — Hudi VECTOR round-trip + CREATE INDEX + MDT verification on GCS.")
    print(f"Table: {table_path}")
    print(f"Catalog table: {catalog_table}")
    print(f"Index name: {INDEX_NAME}")
    print("Verify manually, e.g.:")
    print(f"  gsutil ls {table_path}/.hoodie/")
    print(f"  spark.sql(\"SELECT key FROM hudi_metadata('{table_path}') WHERE type = 8 ORDER BY key\")")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
