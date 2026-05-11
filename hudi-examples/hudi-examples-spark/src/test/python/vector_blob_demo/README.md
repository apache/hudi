<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Hudi VECTOR + BLOB + Vector Search demo (PySpark + Lance)

End-to-end PySpark demo that exercises three Hudi 1.2.0 features together on
the Oxford-IIIT Pet dataset:

1. **VECTOR type** — embedding column is annotated with
   `hudi_type = "VECTOR(<dim>)"`.
2. **BLOB type (INLINE)** — image bytes are written as a Hudi BLOB struct
   tagged with `hudi_type = "BLOB"`.
3. **Vector search** — cosine similarity top-K via the
   `hudi_vector_search` SQL table-valued function, backed by Lance files.

## Three variants

The folder ships three scripts — each focused on a specific Hudi feature.
Run them independently or in sequence for a full walkthrough.

| File | Feature focus | Surface | Best for |
|---|---|---|---|
| [`hudi_blob_reader_demo.py`](hudi_blob_reader_demo.py) | **OUT_OF_LINE BLOBs + `read_blob()`** — Hudi table stores references to bytes living in a separate container file; `read_blob()` resolves them on demand | Spark SQL | Showing the "lakehouse that references unstructured data without copying" story — tiny Hudi table, bytes elsewhere |
| [`hudi_sql_vector_blob_demo.py`](hudi_sql_vector_blob_demo.py) | **INLINE BLOBs + VECTOR + `hudi_vector_search`** — bytes embedded in the Hudi base files, cosine similarity search via the TVF | Spark SQL — `CREATE TABLE ... (embedding VECTOR(N), image_bytes BLOB, ...) USING hudi`, `named_struct('type','INLINE', ...)`, `hudi_vector_search(...)` | Live demos; SQL-first users; showing the Hudi 1.2.0 DDL/DML surface the way it's documented |
| [`hudi_dataframe_vector_blob_demo.py`](hudi_dataframe_vector_blob_demo.py) | Same as the SQL demo, but via DataFrame | Python DataFrame API — `spark.createDataFrame(rows, explicit_schema)` with `containsNull=False` and `hudi_type` metadata declared upfront, then `df.write.format("hudi").save(path)` | Library-style integration; seeing how the Python DataFrame API composes the VECTOR/BLOB logical types under the hood |

All three share the same venv, jars, and env vars. They write to different
table paths (`/tmp/hudi_blob_reader_{format}_pets` vs `/tmp/hudi_sql_{format}_pets`
vs `/tmp/hudi_{format}_pets`) so you can run them back-to-back without
collision.

**Suggested demo order** when walking someone through: blob reader → SQL vector search → DataFrame variant (as "here's the lower-level view"). The first two cover the features; the third is reference material.

## Run as Jupyter notebooks

For live presentations and self-serve exploration, the demos are also
available as Jupyter notebooks under [`notebooks/`](notebooks/). Code,
narrative, and output (images, top-K panels) live in one scrollable artifact;
toggling a feature is a single Python variable edit + "Run All" — no shell
env vars.

**Start here:** [`notebooks/00_main_demo.ipynb`](notebooks/00_main_demo.ipynb)
— the canonical end-to-end story (Lance + INLINE BLOBs +
`hudi_vector_search` + `read_blob()`) in 13 cells. To run the same demo
on Parquet, flip one config flag in the DDL.

| Notebook | Role | `.py` cousin | Toggleable variables |
|---|---|---|---|
| [`notebooks/00_main_demo.ipynb`](notebooks/00_main_demo.ipynb) | **Start here** — canonical demo | — | `N_SAMPLES`, `TOP_K` |
| [`notebooks/01_blob_reader.ipynb`](notebooks/01_blob_reader.ipynb) | supplemental: OUT_OF_LINE deep-dive | [`hudi_blob_reader_demo.py`](hudi_blob_reader_demo.py) | `BASE_FILE_FORMAT`, `BLOB_MODE`, `INLINE_READ_MODE`, `N_SAMPLES` |
| [`notebooks/02_sql_vector_search.ipynb`](notebooks/02_sql_vector_search.ipynb) | supplemental: SQL DDL deep-dive | [`hudi_sql_vector_blob_demo.py`](hudi_sql_vector_blob_demo.py) | `BASE_FILE_FORMAT`, `N_SAMPLES` |
| [`notebooks/03_dataframe_vector_search.ipynb`](notebooks/03_dataframe_vector_search.ipynb) | supplemental: DataFrame API | [`hudi_dataframe_vector_blob_demo.py`](hudi_dataframe_vector_blob_demo.py) | `BASE_FILE_FORMAT`, `N_SAMPLES` |

See [`notebooks/README.md`](notebooks/README.md) for setup details.

## Prereqs

- Java 11
- Python **3.12** (PySpark 3.5 does NOT support Python 3.13/3.14)
- Hudi Spark bundle (Apache 1.2.0-rc1 staging jar, or build from source)
- Lance Spark bundle jar

## 1. Get the Hudi bundle

The scripts default `HUDI_BUNDLE_JAR` to
`~/Downloads/hudi-spark3.5-bundle_2.12-1.2.0-rc1.jar`, so you can drop the
Apache 1.2.0-rc1 staging jar there and skip exporting anything.

**Option A — Download the rc1 staging jar (recommended; no build required):**

```bash
curl -L -o ~/Downloads/hudi-spark3.5-bundle_2.12-1.2.0-rc1.jar \
  https://repository.apache.org/content/repositories/orgapachehudi-1176/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.2.0-rc1/hudi-spark3.5-bundle_2.12-1.2.0-rc1.jar
```

This is the exact jar published to Apache's Nexus staging repo for the
1.2.0-rc1 vote — running the demo against it doubles as smoke-testing the
release candidate. Note: the staging URL (`orgapachehudi-1176`) rolls forward
each RC; if you're reading this after rc1 closes, find the current staging
repo at <https://repository.apache.org/#stagingRepositories>.

**Option B — Build from source:**

```bash
mvn clean package -pl packaging/hudi-spark-bundle -am -DskipTests -Dspark3.5
export HUDI_BUNDLE_JAR=$(pwd)/packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.2.0-SNAPSHOT.jar
```

## 2. Grab the Lance bundle

The scripts default `LANCE_BUNDLE_JAR` to
`~/Downloads/lance-spark-bundle-3.5_2.12-0.4.0.jar`. Hudi's pom pins
`org.lance:lance-spark-3.5_2.12:0.4.0` (see `lance.spark.connector.version` in
the root `pom.xml`); the matching **bundle** jar is on Maven Central:

```bash
curl -L -o ~/Downloads/lance-spark-bundle-3.5_2.12-0.4.0.jar \
  https://repo1.maven.org/maven2/com/lancedb/lance-spark-bundle-3.5_2.12/0.4.0/lance-spark-bundle-3.5_2.12-0.4.0.jar
```

Browse: <https://central.sonatype.com/artifact/org.lance/lance-spark-bundle-3.5_2.12/0.4.0>.
Skip this step if you only intend to run with `HUDI_BASE_FILE_FORMAT=parquet`.

## 3. Create a Python 3.12 venv + install deps

```bash
cd hudi-examples/hudi-examples-spark/src/test/python/vector_blob_demo

# Homebrew: brew install python@3.12 if you don't have it yet
python3.12 -m venv .venv
source .venv/bin/activate

python --version     # sanity check: must be 3.12.x, not 3.13+
pip install --upgrade pip
pip install -r requirements.txt
```

`torch` + `torchvision` is the heaviest install (~800 MB); first run takes a
few minutes.

## 4. Run

```bash
# If both jars live in ~/Downloads/ at the default filenames, no exports needed.
# Override only if your jars live elsewhere:
#   export HUDI_BUNDLE_JAR=/abs/path/to/hudi-spark3.5-bundle_2.12-...jar
#   export LANCE_BUNDLE_JAR=/abs/path/to/lance-spark-bundle-3.5_2.12-0.4.0.jar

# Start small to verify correctness — 100 images runs in under a minute
export HUDI_LANCE_DEMO_N=100

# Blob reader variant — OUT_OF_LINE + read_blob()
python hudi_blob_reader_demo.py

# SQL variant — INLINE + vector search
python hudi_sql_vector_blob_demo.py

# DataFrame variant — same as SQL, but through PySpark DataFrame API
python hudi_dataframe_vector_blob_demo.py
```

Once it works, crank it up:

```bash
export HUDI_LANCE_DEMO_N=1000
python hudi_dataframe_vector_blob_demo.py        # or hudi_sql_vector_blob_demo.py
```

### Run the same demo against Parquet base files

The Hudi VECTOR + BLOB + vector search path is format-agnostic — flip the
base file format with one env var:

```bash
# Parquet runs do NOT need LANCE_BUNDLE_JAR — leave it unset
HUDI_BASE_FILE_FORMAT=parquet python hudi_dataframe_vector_blob_demo.py
```

Table path and panel filename auto-rename to `/tmp/hudi_parquet_pets` (or
`/tmp/hudi_sql_parquet_pets` for the SQL script) and the corresponding
`outputs/hudi_{...}_parquet_results.png` so you can diff runs side by side.
`LANCE_BUNDLE_JAR` is **only required when `HUDI_BASE_FILE_FORMAT=lance`** —
all three scripts skip the Lance jar entirely on Parquet runs.

### Open the result panel

```bash
open /Users/rahil/workplace/hudi/hudi-examples/hudi-examples-spark/src/test/python/vector_blob_demo/outputs/hudi_lance_results.png
```

An ideal run shows the query image on the left and the top-5 nearest
neighbors (by cosine similarity on the image embedding) to its right — for a
Sphynx query you should see other short-haired cats (Siamese, Russian Blue,
etc.) with similarity scores in the 0.3–0.5 range at N=100, tighter at N=1000.

## Environment variables

| Var | Default | Purpose |
|---|---|---|
| `HUDI_BUNDLE_JAR` | `~/Downloads/hudi-spark3.5-bundle_2.12-1.2.0-rc1.jar` (Apache 1.2.0-rc1 staging jar) | Hudi spark bundle. Override to point at a locally built `*-SNAPSHOT.jar` if you go the Option B route. |
| `LANCE_BUNDLE_JAR` | `~/Downloads/lance-spark-bundle-3.5_2.12-0.4.0.jar` (Maven Central) | Lance spark bundle. Used only when `HUDI_BASE_FILE_FORMAT=lance`; Parquet runs skip it entirely. |
| `HUDI_BASE_FILE_FORMAT` | `lance` | Set to `parquet` to write Parquet base files instead |
| `HUDI_BLOB_MODE` | `out_of_line` | Blob reader demo only. Set to `inline` to embed PNG bytes directly in the Hudi table (no external container file) |
| `HUDI_INLINE_READ_MODE` | `content` | Blob reader demo only, and only meaningful when `HUDI_BLOB_MODE=inline`. Set to `descriptor` to make `image_bytes.data` come back null and force `read_blob()` to materialize bytes lazily. |
| `HUDI_LANCE_DEMO_N` | `1000` (`100` for blob reader) | Number of images to sample |
| `PYSPARK_DRIVER_MEMORY` | `4g` | Driver JVM heap — bump to `8g`+ for N≥2000 |
| `HUDI_LANCE_DEMO_OUTDIR` | `./outputs` | Where query/top-K PNGs land |

## What the run produces

- Hudi table at `/tmp/hudi_lance_pets`
  - `.hoodie/` with one commit on the timeline
  - `<breed>/*.lance` files per partition (37 breed categories)
- `./outputs/query.png`, `./outputs/top1.png` … `top5.png`
- `./outputs/hudi_lance_results.png` — combined panel (the one to open)

## Verifying the logical-type tags landed

```python
spark.read.format("hudi").load("/tmp/hudi_lance_pets").schema.json()
```

Look for:
- `embedding`   → metadata `{"hudi_type": "VECTOR(1024)"}` (dim depends on the backbone)
- `image_bytes` → metadata `{"hudi_type": "BLOB"}`, struct fields `type`, `data`, `reference`

## Switching BLOB read mode (blob reader demo)

`hoodie.read.blob.inline.mode` controls how INLINE blobs come back:

- `CONTENT` (default) — `image_bytes.data` returns the raw bytes directly.
- `DESCRIPTOR` — `image_bytes.data` is null; `image_bytes.reference.*` is
  synthesized to point at the underlying base file (`.lance` for Lance
  base files), and `read_blob(image_bytes)` materializes bytes lazily.

The blob reader demo exposes this via `HUDI_INLINE_READ_MODE`:

```bash
# Default: bytes inline in the data column
HUDI_BLOB_MODE=inline python hudi_blob_reader_demo.py

# Lazy: data column is null, read_blob() resolves bytes via the synthesized reference
HUDI_BLOB_MODE=inline HUDI_INLINE_READ_MODE=descriptor python hudi_blob_reader_demo.py
```

**Important wiring detail (matches `TestLanceDataSource.testBlobInlineDescriptorMode`):**
the `DESCRIPTOR` option is scoped to a single per-load read in
`show_descriptors()`; `read_blob_and_save()` uses a separate default-mode
load so `read_blob()` can actually materialize bytes. Setting
`hoodie.read.blob.inline.mode=DESCRIPTOR` at the SparkSession level would
make every read return `data=null`, including the read backing `read_blob()`,
so it would also return null.

The setting is a no-op for `HUDI_BLOB_MODE=out_of_line` — those rows are
already descriptors (no inline bytes to suppress); `read_blob()` always
resolves them via the user-supplied reference.

## How the blob reader demo works

[`hudi_blob_reader_demo.py`](hudi_blob_reader_demo.py) is the focused
"Hudi references bytes instead of storing them" walkthrough. End-to-end flow:

### Step 1 — pack PNGs into one container file (pure Python)

```
/tmp/pets_blob_container.bin
├── [bytes for image 0]   offset=0,     length=L0
├── [bytes for image 1]   offset=L0,    length=L1
├── [bytes for image 2]   offset=L0+L1, length=L2
└── ...
```

The script packs every image's raw PNG bytes end-to-end into a single file
and records each image's `(offset, length)` slice. The Hudi table will
reference these slices — it will never hold the bytes itself.

### Step 2 — stage references as a Spark temp view

PyArrow writes a tiny Parquet with columns `(image_id, category,
external_path, offset, length)` — just metadata, no binary. Registered as
`staging_blob_refs`.

### Step 3 — `CREATE TABLE ... BLOB ... USING hudi`

```sql
CREATE TABLE pets_blob_reader_lance (
    image_id     STRING,
    category     STRING,
    image_bytes  BLOB
) USING hudi
TBLPROPERTIES (
    primaryKey = 'image_id',
    preCombineField = 'image_id',
    type = 'cow',
    'hoodie.table.base.file.format' = 'lance',
    'hoodie.write.record.merge.custom.implementation.classes' = 'org.apache.hudi.DefaultSparkRecordMerger'
)
```

### Step 4 — `INSERT INTO ... SELECT` building OUT_OF_LINE references

```sql
INSERT INTO pets_blob_reader_lance
SELECT
    image_id,
    category,
    named_struct(
        'type',      'OUT_OF_LINE',
        'data',      cast(null as binary),
        'reference', named_struct(
            'external_path', external_path,
            'offset',        offset,
            'length',        length,
            'managed',       false
        )
    ) AS image_bytes
FROM staging_blob_refs
```

Canonical shape from
[`TestDeleteFromTable.scala:151-167`](../../../../../../hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/spark/sql/hudi/dml/others/TestDeleteFromTable.scala:151)
and
[`TestUpdateTable.scala:545-558`](../../../../../../hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/spark/sql/hudi/dml/others/TestUpdateTable.scala:545).
`managed = false` means the user owns the external file's lifecycle — Hudi
will not delete it when rows are removed.

### Step 5 — inspect what's actually stored

The script does the inspection through a **scoped per-load read** that
honors `HUDI_INLINE_READ_MODE`:

```python
reader = spark.read.format("hudi")
if blob_mode == "inline":
    reader = reader.option("hoodie.read.blob.inline.mode", inline_read_mode.upper())
reader.load(path).createOrReplaceTempView("blob_descriptors_view")
```

```sql
SELECT image_id,
       image_bytes.type                    AS blob_type,
       length(image_bytes.data)            AS inline_bytes_len,
       image_bytes.reference.external_path AS ref_path,
       image_bytes.reference.offset        AS ref_offset,
       image_bytes.reference.length        AS ref_length,
       image_bytes.reference.managed       AS ref_managed
FROM blob_descriptors_view LIMIT 3
```

Output depends on the run:
- **OUT_OF_LINE** (default): `inline_bytes_len` is null; `ref_path/offset/length` point
  at the user-supplied container file. Hudi table is tiny — just pointers.
- **INLINE + CONTENT**: `inline_bytes_len > 0`; `ref_*` is null. Bytes live in the
  Hudi base files and are returned directly.
- **INLINE + DESCRIPTOR**: `inline_bytes_len` is null; `ref_path` ends in `.lance`,
  `ref_managed=true` — Lance synthesized a reference into the base file. The bytes
  are still resolvable via `read_blob()` against a separate default-mode load.

### Step 6 — `read_blob()` resolves the descriptor to bytes

`read_blob_and_save()` registers a **separate, default-mode load** so the
underlying read sees the bytes (`CONTENT` mode) regardless of what the
inspection step in Step 5 used:

```python
spark.read.format("hudi").load(path).createOrReplaceTempView("blob_resolve_view")
```

```sql
SELECT image_id, length(read_blob(image_bytes)) AS resolved_byte_count
FROM blob_resolve_view LIMIT 5
```

Canonical usage from
[`TestReadBlobSQL.scala:90-94`](../../../../../../hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/hudi/blob/TestReadBlobSQL.scala:90)
and the descriptor-mode pattern from
[`TestLanceDataSource.testBlobInlineDescriptorMode`](../../../../../../hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/hudi/functional/TestLanceDataSource.scala).
`read_blob(col)` is a scalar function (not a TVF) — returns `BINARY`. Works
in projections, WHERE clauses, joins — anywhere you'd use a column.

**Why two views?** If `read_blob()` ran against the same DESCRIPTOR-mode
view as Step 5, `BatchedBlobReader` would dispatch on `storage_type=INLINE`,
see `data=null`, and return null bytes — never consulting the synthesized
`reference`. The two-view pattern keeps the descriptor inspection scoped
while letting `read_blob()` see real bytes.

### Step 7 — pull bytes into the driver and re-save as PNGs

Final sanity check: `SELECT read_blob(image_bytes) FROM ... LIMIT 3`,
decode each result as a PNG, save to `./outputs/blob_reader_resolved/`.
Viewer opens those files to confirm the round-trip worked.

### Step 8 — compare footprints

Prints the sizes of the blob container vs. the Hudi table directory.
Typical ratio at 100 images: Hudi table is <1% of the container size — the
"we reference bytes without copying them" punchline, made concrete.

### Comparing INLINE vs OUT_OF_LINE

The same script can be re-run with `HUDI_BLOB_MODE=inline` to show the
other half of the story: bytes live **inside** the Hudi base files, not in
a separate container. Same CREATE TABLE, same `read_blob(image_bytes)` SQL
— the only difference is the INSERT builds `named_struct('type','INLINE',
'data', <bytes>, 'reference', cast(null as struct<...>))`. On the footprint
comparison the Hudi table size is the one that ballooned; there's no
container to reference.

```bash
# OUT_OF_LINE (default) — references a container file
python hudi_blob_reader_demo.py

# INLINE — bytes embedded in the Hudi table
HUDI_BLOB_MODE=inline python hudi_blob_reader_demo.py
```

Run both back-to-back and the viewer sees: same SQL, same `read_blob()`
call, radically different storage layouts. That's the "Hudi's BLOB type
abstracts over inline-vs-external" point.

---

## How the script works

The script is a single file —
[`hudi_dataframe_vector_blob_demo.py`](hudi_dataframe_vector_blob_demo.py) — organized
into numbered sections. Here's what each one does and why.

### Pre-JVM env setup (top of file, before any `pyspark` import)

```python
_driver_mem = os.getenv("PYSPARK_DRIVER_MEMORY", "4g")
os.environ.setdefault("PYSPARK_SUBMIT_ARGS",
    f"--driver-memory {_driver_mem} --conf spark.driver.maxResultSize=2g pyspark-shell",
)
```

In `local[*]` mode the driver JVM IS the executor. Driver heap is set **when
the JVM launches**, not via `SparkSession.config()` later — so
`PYSPARK_SUBMIT_ARGS` must be in `os.environ` before `import pyspark` triggers
JVM launch. 4 GB handles ~1000 rows with room to spare; bump to 8g for larger.

### `CONFIG` + Hudi schema constants

Knobs grouped at the top. The constants under `HUDI_TYPE_METADATA_KEY` and
`BLOB_*` mirror values from `HoodieSchema.java` in `hudi-common`:

| Python constant | Hudi source |
|---|---|
| `HUDI_TYPE_METADATA_KEY = "hudi_type"` | `HoodieSchema.TYPE_METADATA_FIELD` |
| `BLOB_TYPE_INLINE = "INLINE"` | `HoodieSchema.Blob.INLINE` |
| `BLOB_FIELD_TYPE/DATA/REFERENCE` | `HoodieSchema.Blob.TYPE/INLINE_DATA_FIELD/EXTERNAL_REFERENCE` |

If those change in Hudi, update these four constants.

### Section 1 — `create_spark()`

Every Spark config line has a purpose:

| Config | Why |
|---|---|
| `spark.jars` | Ships the Hudi + Lance bundles to the classpath |
| `spark.serializer = KryoSerializer` | Required by Hudi — bombs with default Java serializer |
| `spark.sql.extensions = HoodieSparkSessionExtension` | Registers Hudi's SQL rules, including the vector search TVF |
| `spark.sql.catalog.spark_catalog = HoodieCatalog` | Makes `CREATE TABLE` aware of Hudi |
| `spark.sql.session.timeZone = UTC` | Determinism |
| `spark.default.parallelism = 2`, `spark.sql.shuffle.partitions = 2` | Conservative parallelism keeps socket buffer pressure low on local-mode runs. The DataFrame demo relies on small N (100–256 rows) to keep `PythonRDD` traffic within socket budgets; the SQL + blob reader demos stage through PyArrow → Parquet (see "Why we stage through Parquet" below). Low parallelism keeps any incidental Python UDF path well within those budgets regardless |

`hoodie.read.blob.inline.mode` is intentionally **not** set on the session —
the blob reader demo scopes it per-load (see "Switching BLOB read mode"
above) so that `read_blob()` can run against a default-mode load and
materialize bytes.

### Section 2 — `load_dataset()`

Pulls N random images from torchvision's Oxford-IIIT Pet (37 dog/cat breeds).
Each row becomes a Python dict with the PNG bytes in a *staging* column called
`image_bytes_raw`. The BLOB struct shape is applied later — simpler to write
the dict with a flat binary field than to hand-build nested dicts.

### Section 3 — Embedding model

Uses `timm.create_model("mobilenetv3_small_100", pretrained=True, num_classes=0)`.
`num_classes=0` strips the classifier head so `model(x)` returns feature
vectors, not predictions. `sklearn.preprocessing.normalize` L2-normalizes the
embeddings so cosine distance = `1 - dot_product`. Returns
`(data, embedding_dim)` — the dim (1024 for this backbone) is data-driven and
becomes the `N` in `VECTOR(N)`.

### Section 4 — Writing to Hudi

Three pieces work together. The pattern mirrors
[`TestVectorDataSource.scala`](../../../../../../hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/hudi/functional/TestVectorDataSource.scala)
exactly: declare the target `StructType` (with `containsNull=False` on the
embedding's array elements and `hudi_type` metadata on the columns), build
positional `Row` tuples from the Python data, and let
`spark.createDataFrame(rows, schema)` treat the schema as authoritative.

**`build_target_schema(embedding_dim)`** returns the full `StructType` with:
- `embedding`: `ArrayType(FloatType(), containsNull=False)`, `nullable=False`,
  `metadata={"hudi_type": "VECTOR(<dim>)"}`
- `image_bytes`: the BLOB struct shape (`type`, `data`, `reference`),
  `metadata={"hudi_type": "BLOB"}`
- All other scalar fields (`image_id`, `category`, etc.)

**`rows_for_hudi(data)`** yields one positional `Row(...)` per input dict.
The BLOB inline value is constructed as a nested `Row("INLINE", bytes, None)`
— positional, matching the schema's field order.

**`write_to_hudi(spark, data, embedding_dim)`** is now three lines of real
work:
1. `schema = build_target_schema(embedding_dim)`
2. `df = spark.createDataFrame(list(rows_for_hudi(data)), schema)` — schema
   is authoritative, so `containsNull=False` and `hudi_type` metadata both
   land on the JVM-side DataFrame as declared.
3. `df.write.format("hudi").mode("overwrite").save(table_path)` with the same
   `hoodie.*` options the SQL demo uses:
   - `hoodie.table.base.file.format = lance` — the switch from Parquet to Lance
   - `hoodie.datasource.write.partitionpath.field = category_sanitized` — 37 breed partitions
   - `hoodie.write.record.merge.custom.implementation.classes = DefaultSparkRecordMerger` — required to merge Spark rows with Hudi's logical types

No PyArrow, no Parquet staging, no `withColumn`/`alias`/`stamp_blob_metadata`
gymnastics — the schema declares everything, and `createDataFrame` honors it.

### Section 5 — `find_similar()`

Builds a literal SQL string: `ARRAY(f1, f2, ...)` (1024 floats inlined into
the query). The `hudi_vector_search` TVF requires the query vector to be a
constant expression, so a scalar subquery won't work — the literal is the
documented pattern, matching `TestHoodieVectorSearchFunction.scala`.

Calls:
```sql
SELECT image_id, category, image_bytes, _hudi_distance
FROM hudi_vector_search('<path>', 'embedding', ARRAY(...), k+1, 'cosine')
ORDER BY _hudi_distance
```

Notes:
- Asks for `top_k + 1` because the query image is itself in the corpus
  (distance ≈ 0) and gets skipped in the result loop.
- Reads the image bytes out of the struct with `row["image_bytes"]["data"]`
  — in CONTENT mode the BLOB comes back as the full struct, and the inline
  bytes live in the `data` field.

### Section 6 — `visualize_and_save()`

Pure matplotlib. Saves `query.png`, `top1.png` … `top5.png`, and a combined
panel at `hudi_lance_results.png`. Uses the `Agg` backend so it runs headless.

### `main()`

Linear flow — no branches:

```
create_spark()
  → load_dataset()
  → create_embedding_model() + generate_embeddings()
  → write_to_hudi()
  → (pick random row as query)
  → find_similar()
  → visualize_and_save()
  → spark.stop()
```

## How the SQL script works

[`hudi_sql_vector_blob_demo.py`](hudi_sql_vector_blob_demo.py) reaches the
same end state — a partitioned Hudi table with a VECTOR embedding column, a
BLOB image column, and a vector similarity query — but every Hudi-touching
line is a SQL string rather than a DataFrame transform. The Python↔SQL
bridge is a Spark **temp view**.

### Steps 1–3 — identical to the DataFrame variant

Pre-JVM env setup, `create_spark()`, dataset loading, and embedding
generation are copied verbatim. The interesting divergence starts at step 4.

### Step 4 — stage via PyArrow, register the Parquet as a Spark temp view

```python
stage_to_parquet_with_pyarrow(data, embedding_dim, staging_path)   # Python → Parquet, no Spark
spark.read.parquet(staging_path).createOrReplaceTempView("staging_pets")
```

Why PyArrow instead of `spark.createDataFrame(...)`? Because the latter
builds a `PythonRDD` which blows macOS kernel socket buffers at 1000+ rows
— see "Why we stage through Parquet" below. No Hudi metadata is attached
on the view; the Hudi logical types come from the **target table's DDL**,
not the source.

### Step 5 — `CREATE TABLE ... USING hudi` (SQL)

```sql
CREATE TABLE pets_sql_lance (
    image_id            STRING,
    category            STRING,
    category_sanitized  STRING,
    label               INT,
    description         STRING,
    image_bytes         BLOB           COMMENT 'Pet image bytes (INLINE)',
    width               INT,
    height              INT,
    embedding           VECTOR(1024)   COMMENT 'Image embedding for ANN search'
) USING hudi
PARTITIONED BY (category_sanitized)
LOCATION '/tmp/hudi_sql_lance_pets'
TBLPROPERTIES (
    primaryKey = 'image_id',
    preCombineField = 'image_id',
    type = 'cow',
    'hoodie.table.base.file.format' = 'lance',
    'hoodie.write.record.merge.custom.implementation.classes' = 'org.apache.hudi.DefaultSparkRecordMerger'
)
```

The `DefaultSparkRecordMerger` TBLPROPERTY is mandatory for Lance: it flips
Hudi's writer factory from AVRO to SPARK, which is what
`HoodieSparkFileWriterFactory` → `HoodieSparkLanceWriter` dispatches on.
Without it, the write fails mid-commit with "Lance base file format is
currently only supported with the Spark engine". Harmless for Parquet base
files — safe to leave on always.

`VECTOR(1024)` and `BLOB` are first-class Hudi-extended SQL types. The
parser at
[`HoodieSpark3_5ExtendedSqlAstBuilder.scala:2612-2626`](../../../../../../hudi-spark-datasource/hudi-spark3.5.x/src/main/scala/org/apache/spark/sql/parser/HoodieSpark3_5ExtendedSqlAstBuilder.scala)
rewrites them into the right Spark types **and** stamps `hudi_type` metadata
automatically — so the DataFrame demo's explicit
`StructField(..., metadata={"hudi_type": ...})` declarations simply aren't
needed here.

### Step 6 — `INSERT INTO ... SELECT` with `named_struct` (SQL)

```sql
INSERT INTO pets_sql_lance
SELECT
    image_id, category, category_sanitized, label, description,
    named_struct(
        'type',      'INLINE',
        'data',      image_bytes_raw,
        'reference', cast(null as struct<external_path:string,
                                         offset:bigint,
                                         length:bigint,
                                         managed:boolean>)
    ) AS image_bytes,
    width, height,
    embedding
FROM staging_pets
```

`named_struct` constructs the BLOB INLINE value in SQL — same shape the
DataFrame demo builds with `struct(lit("INLINE").as("type"), ...)`. The
`embedding` column passes through as-is because Hudi accepts `ARRAY<FLOAT>`
for a `VECTOR(N)` column.

### Step 7 — `hudi_vector_search` (SQL)

Exactly the same TVF as in the DataFrame demo:

```sql
SELECT image_id, category, image_bytes, _hudi_distance
FROM hudi_vector_search(
    '/tmp/hudi_sql_lance_pets',
    'embedding',
    ARRAY(0.0123, 0.4567, ...),   -- 1024 floats inlined from the query embedding
    6,                             -- k + 1 (the query image itself is also in the corpus)
    'cosine'
)
ORDER BY _hudi_distance
```

### Step 8 — visualization

Reused verbatim from the DataFrame variant.

### `main()` flow

```
create_spark()
  → load_dataset()
  → create_embedding_model() + generate_embeddings()
  → register_staging_view()              # Python → Spark temp view
  → create_hudi_table_sql()              # CREATE TABLE ... VECTOR/BLOB
  → insert_into_hudi_sql()               # INSERT INTO ... SELECT named_struct(...)
  → spark.sql("SELECT ... LIMIT 5")      # preview
  → find_similar_sql()                   # hudi_vector_search TVF
  → visualize_and_save()
  → spark.stop()
```

All five Hudi-facing steps (CREATE, INSERT, preview SELECT, vector search,
and the COUNT validation inside the INSERT helper) are raw SQL strings the
viewer can read top-to-bottom.

## Why the SQL + blob reader demos stage through Parquet (written directly by PyArrow)

The **SQL** and **blob reader** demos write the generated images +
embeddings to a local Parquet file (`/tmp/staging_<table>_parquet.parquet`)
via **PyArrow**, then read that back with `spark.read.parquet(...)` as the
input to the Hudi write. It looks redundant — why not just pass a DataFrame
to Hudi directly?

**Short answer**: anything built with `spark.createDataFrame(python_list, ...)`
is a `PythonRDD`. Its rows are pickled Python bytes that require a Python
worker to materialize. Any downstream operation on that DataFrame —
`.write.parquet(...)` included — spawns Python workers and streams rows
through a localhost socket from the JVM. On some local environments (notably
macOS), kernel socket buffer resources are limited, and sustained
multi-hundred-MB loopback traffic can saturate them:

```
java.net.SocketException: No buffer space available (Write failed)
  at org.apache.spark.api.python.PythonRDD$.writeIteratorToStream(...)
```

This fires even on the **first** pass — so naive Spark-side Parquet staging
(`createDataFrame(...).write.parquet(...)`) doesn't actually fix it, because
that IS a PythonRDD write.

**The approach for SQL + blob reader**: skip Spark for the staging step.
PyArrow writes a proper Parquet file directly from Python, in-process, no
Spark involved. After `pq.write_table(table, path)`, we do
`spark.read.parquet(path)` and get a JVM-native DataFrame backing a temp
view. No `PythonRDD` ever exists in the lineage, so no localhost socket
traffic happens anywhere, and socket buffer pressure is avoided.

### Why the DataFrame demo doesn't need this

The DataFrame demo (`hudi_dataframe_vector_blob_demo.py`) deliberately keeps
a `PythonRDD` in its lineage — `spark.createDataFrame(list_of_rows, schema)`
is a one-shot construction, not sustained streaming. At demo sizes (100–256
rows) the loopback traffic from the single Hudi write stays well within the
socket buffer budget.

The reason `createDataFrame` is preferred there: it accepts the explicit
`StructType` as **authoritative**, so `containsNull=False` on the
embedding's array elements and the `hudi_type` metadata on each column
both survive into the JVM-side schema. `spark.read.parquet(...)` does the
opposite — its reader resets `containsNull` regardless of any
`read.schema()` hint, and Hudi's VECTOR validator then rejects the column.
That's the same pattern
[`TestVectorDataSource.scala`](../../../../../../hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/hudi/functional/TestVectorDataSource.scala)
uses for the canonical Hudi-side test, and it's the one to copy when
integrating Hudi VECTOR/BLOB writes from a Python DataFrame surface.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `RecursionError: Stack overflow` during `createDataFrame` | Python 3.13+ | Use Python 3.12 — PySpark 3.5 doesn't support 3.13+ |
| `java.lang.OutOfMemoryError: Java heap space` during write | Default driver heap too small for N>~300 | `export PYSPARK_DRIVER_MEMORY=8g` |
| `java.net.SocketException: No buffer space available` | PythonRDD → Python workers streaming rows through localhost sockets can saturate the local socket buffer pool on some environments | Both scripts already stage Python data via **PyArrow** directly to Parquet (bypassing Spark for the initial hop) so no `PythonRDD` ever exists — see "Why we stage through Parquet" below. If it still fires (e.g. with a Python UDF you added), drop `spark.default.parallelism` to `1` or raise kernel buffers: `sudo sysctl -w kern.ipc.maxsockbuf=16777216` |
| `ModuleNotFoundError: No module named 'pyarrow'` | Venv missing pyarrow | `pip install -r requirements.txt` — pyarrow is an explicit dependency used by the SQL + blob reader staging path (the DataFrame demo doesn't import it) |
| `ARRAY_ELEMENT NOT_NULL_CONSTRAINT_VIOLATION` or `cannot cast ARRAY<FLOAT> to ARRAY<FLOAT>` from the DataFrame demo | DataFrame built via `spark.read.parquet(...)` — the Parquet reader resets `containsNull` to true on round-trip, then Hudi's VECTOR validator rejects the column | Build the DataFrame via `spark.createDataFrame(rows, explicit_schema)` instead — schema is authoritative, `containsNull=False` is preserved. This is what the DataFrame demo now does, mirroring `TestVectorDataSource.scala` |
| `Lance base file format is currently only supported with the Spark engine` during INSERT | Hudi fell back to AVRO record type; its Lance writer is a stub that throws | SQL path: add `'hoodie.write.record.merge.custom.implementation.classes' = 'org.apache.hudi.DefaultSparkRecordMerger'` to the table's `TBLPROPERTIES` (already in the script). DataFrame path: pass the same key as a write option (already in the script) |
| `Lance batch column count N does not match expected Spark schema size 0` during read | `SELECT COUNT(*)` prunes to zero columns; Hudi's `LanceRecordIterator` has a strict column-count check that rejects the empty projection | Use `COUNT(<named_col>)` instead of `COUNT(*)` — script does this. Naming a column ensures the projection is non-empty |
| `query vector must be a constant expression` | Query vector passed as subquery | Use `ARRAY(f1, f2, …)` literal — script does this |
| Demo starts but hangs on `OxfordIIITPet` download | First-run 800 MB dataset download | Wait; lands in `~/.cache/torchvision/` and is cached for subsequent runs |
