# DSv2 Aggregate Regression Report Between `98af4fe` and `973df90`

## Summary

The large regression is most likely not a general DSv2 read slowdown. The scan, projection,
filter, and limit benchmarks stay close to the earlier results. The big drop is isolated to
aggregate pushdown:

- `COUNT(*)` falls from about `0.2s` to about `3.6s`
- `MIN/MAX` falls from about `0.2s` to about `3.9s`

That pattern points to DSv2 losing the metadata-only aggregate path and falling back to a normal
file scan. In the newer tree, `HoodieScanBuilder` still tries to resolve aggregates from column
stats, but two later changes make that path much easier to miss.

## Main Findings

### 1. `COUNT(*)` most likely regressed because DSv2 changed which column it uses for row-count stats

The original aggregate implementation used the first table column for `COUNT(*)`. In the newer
tree, `COUNT(*)` now prefers a user-defined record key field:

- Earlier behavior in the initial aggregate pushdown implementation: first table column
- New behavior in `ee4e497b80f1`: record key if it exists in the user schema, else first column

Current code:

- [HoodieScanBuilder.scala](/home/geserdugarov/git/hudi-open-source/hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieScanBuilder.scala:299)

Why this matters in your benchmark:

- The benchmark only enables column stats with
  `hoodie.metadata.index.column.stats.enable = true`
- It does not set `hoodie.metadata.index.column.stats.column.list`
- If no explicit list is set, Hudi indexes only the first `n` supported columns
- The default `n` is `32`

Relevant defaults and selection logic:

- [HoodieMetadataConfig.java](/home/geserdugarov/git/hudi-open-source/hudi-common/src/main/java/org/apache/hudi/common/config/HoodieMetadataConfig.java:267)
- [HoodieTableMetadataUtil.java](/home/geserdugarov/git/hudi-open-source/hudi-common/src/main/java/org/apache/hudi/metadata/HoodieTableMetadataUtil.java:1671)

The aggregate fast path aborts if Hudi cannot fetch column stats for every base file:

- [HoodieScanBuilder.scala](/home/geserdugarov/git/hudi-open-source/hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieScanBuilder.scala:324)

So the likely failure mode is:

1. In the earlier commit, `COUNT(*)` asked for stats on the first schema column, which was indexed.
2. In the newer commit, `COUNT(*)` asks for stats on the configured record key column.
3. If that record key column is outside the default indexed first-32-column subset, `getColumnStats(...)`
   is incomplete.
4. DSv2 drops out of aggregate pushdown and runs a normal scan.

This matches the benchmark very well: `COUNT(*)` becomes DSv1-like while the non-aggregate scan
benchmarks remain mostly unchanged.

### 2. `MIN/MAX` likely regressed because DSv2 now intentionally disables `MAX` pushdown on floating-point columns

Commit `29623e1b74eb` changed aggregate support so that `MAX` is no longer pushed down for
`float` and `double` columns:

- [HoodieScanBuilder.scala](/home/geserdugarov/git/hudi-open-source/hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieScanBuilder.scala:118)

Reason from the code comment:

- Parquet/Hudi column stats do not preserve Spark SQL NaN semantics for `MAX`
- Spark treats `NaN` as greater than any non-NaN value
- Stats alone cannot safely distinguish the required cases

The benchmark runs `MIN/MAX` on `HUDI_BENCHMARK_FILTER_COL`:

- [benchmarks/.env-example at `973df90`](/home/geserdugarov/git/hudi-open-source/benchmarks/.env-example) sets `HUDI_BENCHMARK_FILTER_COL=col0`
- [benchmarks/hudi_benchmark.scala at `973df90`](/home/geserdugarov/git/hudi-open-source/benchmarks/hudi_benchmark.scala) runs `SELECT MIN($filterCol), MAX($filterCol) FROM $cowReadTable`

If your benchmark column is `float` or `double`, this change alone explains the `MIN/MAX`
collapse from `~0.2s` to `~3.9s`: DSv2 is intentionally falling back to a normal scan.

### 3. The DSv2 test suite itself was relaxed to allow aggregate fallback

Earlier aggregate tests only validated correctness. Later test updates explicitly accept
either:

- `LocalTableScan` / `HoodieLocalScan` for fully pushed aggregates
- `BatchScan` for fallback

Current test file:

- [TestDSv2Pushdowns.scala](/home/geserdugarov/git/hudi-open-source/hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/spark/sql/hudi/feature/v2/TestDSv2Pushdowns.scala:225)

That is important evidence: by the time of the newer commit, the code and tests no longer treat
"fully pushed aggregate" as guaranteed behavior.

### 4. Metadata and file-index changes are a secondary suspect, but they do not explain the pattern as well

The range also includes metadata and file-index changes such as:

- `3c53b91d9633` (`perf(metadata): avoid recursive calls for partition listing using catalog`)
- `9ddf58223922` (extra file-index timing and cache logging)

Those can affect the cost of `fileIndex.filterFileSlices(...)`, which is the first step in DSv2
aggregate pushdown:

- [HoodieScanBuilder.scala](/home/geserdugarov/git/hudi-open-source/hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieScanBuilder.scala:249)

But they are less likely to be the primary cause because:

- full scan and projection are still close to the earlier numbers
- the regression is sharply concentrated in `COUNT(*)` and `MIN/MAX`
- there are direct aggregate-specific logic changes in the same range that match the symptoms

The catalog-backed partition listing path is also off by default:

- `hoodie.datasource.read.file.index.list.partitions.from.catalog = false`

So it only matters if your benchmark environment explicitly enabled it.

## Most Likely Explanation For Your Benchmark

The most likely combined explanation is:

1. `COUNT(*)` lost pushdown because `ee4e497b80f1` switched DSv2 to use record-key stats, while
   the benchmark table only had the default first-32-column stats coverage.
2. `MIN/MAX` lost pushdown because `29623e1b74eb` disabled `MAX` pushdown for floating-point
   columns, and the benchmark column appears to be chosen from a generic primitive dataset where
   `col0` may be `float` or `double`.

That combination produces exactly the kind of result you observed:

- non-aggregate reads remain similar
- aggregate queries revert to DSv1-like timings

## How To Validate This Quickly

### Validate `COUNT(*)`

Run on both SHAs:

```sql
EXPLAIN SELECT COUNT(*) FROM <table>;
```

Expected outcome:

- fast path: `LocalTableScan` or `HoodieLocalScan`
- regressed path: `BatchScan`

Then check:

1. Which record key is configured for the benchmark table
2. Where that field sits in the schema order
3. Whether it is included in column stats coverage

If the record key is outside the first 32 indexed columns, that strongly confirms the `COUNT(*)`
root cause.

### Validate `MIN/MAX`

Check the benchmark column type:

```sql
DESCRIBE TABLE <table>;
```

or:

```sql
SELECT typeof(<filterCol>) FROM <table> LIMIT 1;
```

If the `MIN/MAX` column is `float` or `double`, the new `MAX` pushdown guard explains the
regression directly.

### Validate actual fallback behavior

Enable explain or inspect the executed plan in the benchmark run:

- older SHA should show the aggregate local path
- newer SHA should show a scan path for the regressed cases

## Mitigations

### For `COUNT(*)`

Make sure the record key column is indexed by column stats. The two simplest options are:

1. Set `hoodie.metadata.index.column.stats.column.list` explicitly and include the record key.
2. Increase `hoodie.metadata.index.column.stats.max.columns.to.index` so the record key is still
   within the indexed prefix.

This should restore the DSv2 `COUNT(*)` fast path if no other pushdown blocker is present.

### For `MIN/MAX`

If the benchmark column is floating-point:

1. Use a non-floating numeric column for the benchmark if the goal is to measure metadata-only
   pushdown performance.
2. Or revisit `29623e1b74eb` if you want to trade NaN correctness for speed on `MAX(float/double)`.

The current behavior is intentional, not an accidental slowdown.

## Final Assessment

The strongest root-cause candidates are commit-local and aggregate-specific:

- `ee4e497b80f1` for `COUNT(*)`
- `29623e1b74eb` for `MIN/MAX`

The `COUNT(*)` explanation is highly likely if the record key is not indexed by default column
stats. The `MIN/MAX` explanation is close to confirmed if the benchmark column is `float` or
`double`.
