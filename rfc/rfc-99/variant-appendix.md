### Spark 4.0 Write & Shredding Verification

Spark 4.0 successfully writes VariantType in both Shredded and Unshredded modes.

Physical Layout confirmed via `parquet-tools`: **Shredding** physically maps variant fields to native Parquet columns
(e.g., `v.typed_value.a.typed_value` as INT32) alongside the standard `metadata` and `value` binary columns.

#### Shredding Configuration

The relevant Spark configuration for shredding is:

```
spark.sql.variant.writeShredding.enabled=true
```

You can also force a specific shredding schema for testing:

```
spark.sql.variant.forceShreddingSchemaForTest=<schema>
```

#### Shredded Parquet Physical Layout

An example shredded variant column `v` with schema `a int, b string, c decimal(15, 1)` produces 8 physical columns:

```
v.metadata                       BYTE_ARRAY
v.value                          BYTE_ARRAY
v.typed_value.a.value            BYTE_ARRAY
v.typed_value.a.typed_value      INT32
v.typed_value.b.value            BYTE_ARRAY
v.typed_value.b.typed_value      BYTE_ARRAY (String/UTF8)
v.typed_value.c.value            BYTE_ARRAY
v.typed_value.c.typed_value      INT64 (Decimal(precision=15, scale=1))
```

#### Unshredded Parquet Physical Layout

An unshredded variant column `v` produces only 2 physical columns:

```
v.value      BYTE_ARRAY
v.metadata   BYTE_ARRAY
```

### Root-Level Shredding

Variants can also be shredded at the root level (i.e., when the variant value is a scalar, not an object).

Example: forcing `bigint` as the shredding schema with values like `100`, `"hello_world"`, and `{"A": 1, "c": 1.23}`
will shred the root-level integer into a typed column while falling back to the binary blob for non-matching types.

```python
spark.conf.set("spark.sql.variant.forceShreddingSchemaForTest", "bigint")
df = spark.sql("""
    select case
        when id = 0 then parse_json('100')
        when id = 1 then parse_json('"hello_world"')
        when id = 2 then parse_json('{"A": 1, "c": 1.23}')
    end as v
    from range(3)
""")
```

### Backward Compatibility (Spark 3.5 Reading VARIANT written by Spark 4.0)

1. **Logical Type Failure**: Spark 3.5 cannot interpret the `Variant` logical type from the Parquet footer, triggering
   warnings and ignoring the serialized Spark schema:
    ```
    WARN ParquetFileFormat: Failed to parse and ignored serialized Spark schema in Parquet key-value metadata:
    {"type":"struct","fields":[{"name":"v","type":"variant","nullable":true,"metadata":{}}]}
    ```

2. **Physical Read Success (Raw Binary)**: The data is still readable, but only as raw physical columns:
    - **Unshredded**: Reads as `Struct<value: Binary, metadata: Binary>`
    - **Shredded**: Reads as a complex `Struct` containing the binaries plus the nested typed columns:
      ```
      root
       |-- v: struct (nullable = true)
       |    |-- metadata: binary (nullable = true)
       |    |-- value: binary (nullable = true)
       |    |-- typed_value: struct (nullable = true)
       |    |    |-- a: struct (nullable = true)
       |    |    |    |-- value: binary (nullable = true)
       |    |    |    |-- typed_value: integer (nullable = true)
       |    |    |-- b: struct (nullable = true)
       |    |    |    |-- value: binary (nullable = true)
       |    |    |    |-- typed_value: string (nullable = true)
       |    |    |-- c: struct (nullable = true)
       |    |    |    |-- value: binary (nullable = true)
       |    |    |    |-- typed_value: decimal(15,1) (nullable = true)
      ```

3. **The Consequence**: Users on older engine versions can access the bytes, but they lose all JSON/Variant
   functionality.
   They would need to manually parse the internal binary encoding to make sense of the data.

### Shredding Read Compatibility Issue

Writing with shredding enabled and then reading with the shredded flag disabled (
`spark.sql.variant.allowReadingShredded=false`)
causes a read failure:

```
[INVALID_VARIANT_FROM_PARQUET.WRONG_NUM_FIELDS]
Invalid variant. Variant column must contain exactly two fields.
```

This occurs because the Parquet schema converter expects exactly two fields (`metadata` and `value`) for a variant
column,
but the shredded layout adds additional `typed_value` fields.
This means engines that do not support shredded variants cannot read shredded parquet files as variant types.

### Appendix

- Source issue: https://github.com/apache/hudi/issues/17744
- Parquet testing reference: https://github.com/apache/parquet-testing/issues/75#issue-2976445672
- Spark shredding config
  source: https://github.com/apache/spark/blob/b615f34aebd760a8b3f67d35e13dca2e78fa5765/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5678-L5684