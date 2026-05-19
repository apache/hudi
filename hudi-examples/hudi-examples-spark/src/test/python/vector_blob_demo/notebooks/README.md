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

# Hudi VECTOR + BLOB demos — Jupyter notebooks

Notebook variants of the three `.py` demos in the parent folder. Same end
state, same Spark / Hudi / Lance jars, but code + narrative + output panels
live inline in one scrollable artifact and feature toggles are plain Python
variables at the top of each notebook.

The original `.py` scripts in the parent folder are unchanged — they stay
the canonical scriptable references and `run_demos.sh` still drives them.

## Setup

Use the **same venv** as the `.py` scripts. From the parent folder:

```bash
cd ../    # back into vector_blob_demo/
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt    # adds jupyter + ipykernel for this folder
```

The notebooks default `HUDI_BUNDLE_JAR` to
`~/Downloads/hudi-spark3.5-bundle_2.12-1.2.0-rc1.jar` and `LANCE_BUNDLE_JAR`
to `~/Downloads/lance-spark-bundle-3.5_2.12-0.4.0.jar`, matching the `.py`
scripts. If you placed both jars in `~/Downloads/` per the parent
[`README.md`](../README.md) §1–2, you don't need to export anything. To
override (e.g. point at a locally built bundle):

```bash
export HUDI_BUNDLE_JAR=/abs/path/to/hudi-spark3.5-bundle_2.12-1.2.0-SNAPSHOT.jar
export LANCE_BUNDLE_JAR=/abs/path/to/lance-spark-bundle-3.5_2.12-0.4.0.jar
```

`LANCE_BUNDLE_JAR` is only consulted when `BASE_FILE_FORMAT = "lance"`.

Launch JupyterLab from this folder so the notebooks find their working dir:

```bash
cd notebooks/
jupyter lab
```

## Notebooks

### `00_main_demo.ipynb` — **Start here**

The canonical demo. Single notebook end-to-end: Lance base files with
**inline BLOBs**, then **`hudi_vector_search` + `read_blob()`** composed
in one SQL query so PNG bytes are materialized only for the top-K
nearest neighbors.

Toggle variables:

```python
N_SAMPLES = 250
TOP_K     = 5
```

Lance / INLINE are the defaults. To run the same demo on Parquet, change
`'hoodie.table.base.file.format' = 'lance'` to `'parquet'` in §4's DDL —
no other code changes needed. For the OUT_OF_LINE / DESCRIPTOR
storyboard, see [`01_blob_reader.ipynb`](#01_blob_readeripynb--supplemental-out_of_line-blobs).

### `01_blob_reader.ipynb` — supplemental: OUT_OF_LINE blobs

Demonstrates Hudi's BLOB type as a **reference** rather than as bytes —
how a tiny Hudi table can point at unstructured data sitting elsewhere, and
how `read_blob()` resolves the reference to bytes at query time.

Toggle variables (top of notebook):

```python
BASE_FILE_FORMAT  = "parquet"      # "parquet" or "lance"
BLOB_MODE         = "out_of_line"  # "out_of_line" or "inline"
INLINE_READ_MODE  = "content"      # "content" or "descriptor"
                                   # (only meaningful when BLOB_MODE == "inline")
N_SAMPLES         = 100
```

Storyboard:
- `BLOB_MODE = "out_of_line"` → Hudi table holds `(external_path, offset, length)`
  references; bytes live in `/tmp/pets_blob_container.bin`. Footprint ratio
  shows the Hudi table at <1% of the container.
- `BLOB_MODE = "inline"` + `INLINE_READ_MODE = "content"` → bytes embedded in
  the Hudi base files; `image_bytes.data` returns raw PNG bytes directly.
- `BLOB_MODE = "inline"` + `INLINE_READ_MODE = "descriptor"` → `image_bytes.data`
  is null, `image_bytes.reference.*` is synthesized to point at the underlying
  base file, and `read_blob()` materializes bytes lazily.

### `02_sql_vector_search.ipynb` — supplemental: SQL DDL deep-dive

Pure-SQL walkthrough of `CREATE TABLE ... (embedding VECTOR(N), image_bytes BLOB, ...) USING hudi`,
`INSERT INTO ... SELECT named_struct('type','INLINE', ...)`, and the
`hudi_vector_search` TVF for cosine similarity top-K.

Toggle variables:

```python
BASE_FILE_FORMAT  = "parquet"      # "parquet" or "lance"
N_SAMPLES         = 256
```

### `03_dataframe_vector_search.ipynb` — supplemental: DataFrame API

Same end state as `02`, but built through the PySpark DataFrame API rather
than SQL. Useful for seeing how `StructField(metadata={"hudi_type": "VECTOR(N)"})`
and the BLOB struct shape compose under the hood.

Toggle variables:

```python
BASE_FILE_FORMAT  = "parquet"      # "parquet" or "lance"
N_SAMPLES         = 256
```

## How toggles work

Each notebook starts with a small **toggles cell** containing plain Python
variables. Edit the values and `Run All`. The variables flow into the same
`CONFIG` dict the `.py` scripts build from `os.getenv(...)` — runtime
behavior is identical to the script with the matching env vars set.

`HUDI_BUNDLE_JAR` and `LANCE_BUNDLE_JAR` are read from the environment with
`~/Downloads/` defaults (host-specific file locations, not feature toggles).
The notebooks fail fast with a clear error if the resolved path doesn't exist
— see the parent [`README.md`](../README.md) §1–2 for the curl commands that
populate the defaults.

## Sharing `/tmp/` paths with the `.py` scripts

The notebooks write to the same `/tmp/hudi_*_pets/`, `/tmp/pets_blob_container.bin`,
and `/tmp/staging_pets_*.parquet` paths as the `.py` scripts. That's
intentional — every notebook starts with a **cleanup cell** that wipes
those paths (mirroring `clean()` in `run_demos.sh`), so running notebooks
back-to-back with `./run_demos.sh` is fine in either order.

If you ever skip the cleanup cell on a re-run, you may hit `EOFException` in
`BatchedBlobReader` because old Hudi commits reference offsets past EOF in
a freshly overwritten container file. Always Run All from the top, or at
minimum re-run the cleanup cell before re-running the rest.
