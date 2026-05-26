## MDT RaBitQ POC (MDT-Only)

This POC documents the MDT-native vector metadata implementation that is now reflected in RFC-104 split proposals.

## What gets written

Inside `vector_index_<name>` MDT partition:

- `__centroids__`
- `__quantizer__`
- `__manifest__`
- `M|<generation>`
- `C|<generation>|<cluster>`
- `P|<generation>|<cluster>|<shard>|<record_key>`
- assignment rows keyed as `A|<record_key>`
- `__fg__/<cluster>/<partition_path>` (legacy-compatible mapping rows)

## Current implementation highlights

- Bootstrap computes centroids and assignments in `SparkHoodieBackedTableMetadataWriter`.
- RaBitQ posting rows are generated in MDT with binary code and scalar payload.
- Query-side helpers exist in `VectorIndexSupport`, `VectorIndexPruner`, and `VectorIndexMdtSearchUtils`.
- Raw key classes (`Vector*RawKey`) and payload entry types (`POSTING`, `MANIFEST`, `CLUSTER`, etc.) are implemented in `hudi-common`.

## Scope statement

This POC and RFC split are aligned on MDT-only storage for RaBitQ payloads.
Hidden-column storage is not part of the accepted proposal direction.
