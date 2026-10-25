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

# RFC-99: Hudi Type System

## Proposers

- @bvaradar
- @rahil-c
- @voonhous

## Approvers

- @vinothchandar
- @the-other-tim-brown

## Status

Umbrella ticket: [HUDI-9730](https://issues.apache.org/jira/browse/HUDI-9730)


## Abstract
The main goal is to propose a native Hudi type system as the authoritative representation for Hudi data types, making the system more extensible and the semantics of data types clear and unified. While Hudi currently uses Avro for schema representation, introducing a more comprehensive, Arrow-based type system will make it easier to provide consistent handling and implementation of data types across different engines and improve support for modern data paradigms like multi-modal and semi-structured data.

There is [earlier attempt](https://github.com/apache/hudi/pull/12795/files) to define a common schema but it was geared towards building more general abstractions. This RFC relooks at the specific need for defining a type system model for Hudi to become more extensible fnd also support non-traditional usecases.
   
## Background
Apache Hudi currently uses Apache Avro as the canonical representation for its schema. While this has served the project well, introducing a native, engine-agnostic type system offers a strategic opportunity to evolve Hudi's core abstractions for the future. The primary motivations for this evolution are:

- A common type system allows us to build richer functionalities and common interface across engines and non-JVM clients to interact with Hudi data directly and efficiently.
- A native type system provides a formal framework for introducing new, complex data types. This will accelerate Hudi's ability to offer first-class support for emerging use cases in AI/ML (vectors, tensors) and semi-structured data analysis (VARIANT), keeping Hudi at the forefront of data lakehouse technology
- By standardizing on an in-memory format, Hudi can eliminate costly serialization and deserialization steps when exchanging data with a growing number of Arrow-native tools and engines. This unlocks zero-copy data access, significantly boosting performance for both read and write paths.

## Design

The canonical in-memory representation for all types will be based on the Apache Arrow specification. The main reasons for this is that :

- Apache Arrow provides a standard in-memory format that eliminates the costly process of data serialization and deserialization when moving data across system boundaries. This enables "zero-copy" data exchange, which radically reduces computational overhead and query latency.
- This helps us more easily achieve seamless data exchange with ecosystem of Arrow-native tools.
- Query engines have good support for Arrow type systems which is multi-modal itself. This aligns with our goals of providing first-class multi-modal type system support.

The proposed type system will be implemented such that the in-memory layout is compatible with Apache Arrow to get the performance benefits.

 
### **Type Specification**

The below section defines the types that are going to be supported and finally how they map to other system's data types.
 
#### **3.1. Primitive Types**

These are the fundamental scalar types that form the basis of the type system.
This includes standard signed integers in 8, 16, 32, and 64-bit widths (TINYINT, SMALLINT, INTEGER, BIGINT), as well as floating-point numbers like FLOAT and DOUBLE. The system also provides types for BOOLEAN, DECIMAL, STRING, BINARY, FIXED, and UUID. A notable addition in the new proposal is the explicit support for unsigned integer types (UINT8, UINT16, UINT32, UINT64) to enhance data fidelity and accommodate a wider range of use cases. A half-precision FLOAT16 is also introduced to support AI/ML workloads.

| Logical Type | Description | Parameters |
| :---- | :---- | :---- |
| BOOLEAN | A logical boolean value (true/false). | None |
| TINYINT | An 8-bit signed integer. | None |
| UINT8 | An 8-bit **unsigned** integer. | None |
| SMALLINT | A 16-bit signed integer. | None |
| UINT16 | A 16-bit **unsigned** integer. | None |
| INTEGER | A 32-bit signed integer. | None |
| UINT32 | A 32-bit **unsigned** integer. | None |
| BIGINT | A 64-bit signed integer. | None |
| UINT64 | A 64-bit **unsigned** integer. | None |
| FLOAT16 | A 16-bit half-precision floating-point number. | None |
| FLOAT | A 32-bit single-precision floating-point number. | None |
| DOUBLE | A 64-bit double-precision floating-point number. | None |
| DECIMAL(p, s) | An exact numeric with specified precision/scale. | p, s |
| STRING | A variable-length UTF-8 character string, limited to 2GB per value. | None |
| LARGE\_STRING | A variable-length UTF-8 character string for values exceeding 2GB. | None |
| BINARY | A variable-length sequence of bytes, limited to 2GB per value. | None |
| LARGE\_BINARY | A variable-length sequence of bytes for values exceeding 2GB. | None |
| FIXED(n) | A fixed-length sequence of n bytes. | n |
| UUID | A 128-bit universally unique identifier. | None |

#### **3.2. Temporal Types**

These types handle date and time representations with high precision and timezone awareness.

| Logical Type | Description | Parameters |
| :---- | :---- | :---- |
| DATE | A calendar date (year, month, day). | None |
| DATE64 | A calendar date stored as milliseconds. | None |
| TIME(precision) | A time of day without a timezone. | s, ms, us, ns |
| TIMESTAMP(precision) | An instant in time without a timezone. | us or ns |
| TIMESTAMPTZ(precision) | An instant in time with a timezone, normalized and stored as UTC. | us or ns |
| DURATION(unit) | An exact physical time duration, independent of calendars. | s, ms, us, ns |
| INTERVAL | Represents a duration of time (e.g., months, days, milliseconds). | None |

#### **3.3. Composite Types**

These types allow for the creation of complex, nested data structures.

| Logical Type | Description | Parameters |
| :---- | :---- | :---- |
| STRUCT\<name: type, ...\> | An ordered collection of named fields. | Field list |
| LIST\<element\_type\> | An ordered list of elements of the same type. | Element type |
| MAP\<key\_type, value\_type\> | A collection of key-value pairs. Keys must be unique. | Key, Value types |
| UNION\<type1, type2, ...\> | A value that can be one of several specified types. | Type list |

#### **3.4. Specialized and Optimized Types**

These types provide advanced functionality for performance optimization and specific use cases.

| Logical Type | Description | Parameters |
| :---- | :---- | :---- |
| DICTIONARY\<K, V\> | A dictionary-encoded type for low-cardinality columns to improve performance and reduce storage. K is an integer index type, V is the value type. | K: Index Type, V: Value Type |


#### **3.4. Semi-Structured Type**

This type provides native support for flexible, schema-on-read data formats.

| Logical Type | Description | Parameters |
| :---- | :---- | :---- |
| VARIANT | A type that can store a value of any other Hudi type (e.g., JSON). | None |

#### **3.5. Multi-modal and AI Types**

These are first-class types designed for modern AI/ML workloads.

| Logical Type                     | Description                                        | Parameters              |
|:---------------------------------|:---------------------------------------------------|:------------------------|
| VECTOR(element\_type, dimension) | A dense, fixed-length vector of numeric values.    | Element type, dimension |
| SPARSE\_VECTOR(indices, values)  | A sparse vector represented by indices and values. | Index, Value types      |
| TENSOR(element\_type, shape)     | A multi-dimensional array (tensor).                | Element type, shape     |

#### **3.6. Unstructured Data Types**

These types are designed for large binary objects such as images, videos, audio, and documents.

| Logical Type | Description | Parameters |
| :---- | :---- | :---- |
| BLOB | A binary large object for unstructured data (e.g., images, video, audio, documents). Physically represented as a record with a type discriminator, inline data bytes, and an external reference (path, offset, length, managed). | None |


### **Interoperability Mapping**

The following table defines the canonical mapping from the proposed logical types to the types of key external systems.

| Logical Type | Apache Arrow Type | Apache Parquet Type (Physical \+ Logical) | Apache Avro Type | Apache Spark Type | Apache Flink Type |
| :---- | :---- | :---- | :---- | :---- | :---- |
| BOOLEAN | Boolean | BOOLEAN | boolean | BooleanType | BOOLEAN |
| TINYINT | Int8 | INT32 \+ INTEGER(8, signed=true) | int | ByteType | TINYINT |
| UINT8 | UInt8 | INT32 \+ INTEGER(8, signed=false) | int | ShortType | SMALLINT |
| SMALLINT | Int16 | INT32 \+ INTEGER(16, signed=true) | int | ShortType | SMALLINT |
| UINT16 | UInt16 | INT32 \+ INTEGER(16, signed=false) | int | IntegerType | INT |
| INTEGER | Int32 | INT32 | int | IntegerType | INT |
| UINT32 | UInt32 | INT64 \+ INTEGER(32, signed=false) | long | LongType | BIGINT |
| BIGINT | Int64 | INT64 | long | LongType | BIGINT |
| UINT64 | UInt64 | INT64 (lossy) or FIXED\_LEN\_BYTE\_ARRAY(8) | long (lossy) | DecimalType(20,0) | DECIMAL(20,0) |
| FLOAT16 | Float16 | FLOAT (promoted) | float (promoted) | FloatType (promoted) | FLOAT (promoted) |
| FLOAT | Float32 | FLOAT | float | FloatType | FLOAT |
| DOUBLE | Float64 | DOUBLE | double | DoubleType | DOUBLE |
| DECIMAL(p,s) | Decimal128(p,s) or Decimal256(p,s) | FIXED\_LEN\_BYTE\_ARRAY \+ DECIMAL | bytes \+ decimal | DecimalType(p,s) | DECIMAL(p,s) |
| STRING | Utf8 | BYTE\_ARRAY \+ STRING | string | StringType | STRING |
| **LARGE\_STRING** | **LargeUtf8** | BYTE\_ARRAY \+ STRING | string | StringType | STRING |
| BINARY | Binary | BYTE\_ARRAY | bytes | BinaryType | BYTES |
| **LARGE\_BINARY** | **LargeBinary** | BYTE\_ARRAY | bytes | BinaryType | BYTES |
| DATE | Date32 | INT32 \+ DATE | int \+ date | DateType | DATE |
| **DATE64** | **Date64** | INT64 \+ TIMESTAMP(isAdjustedToUTC=true, MILLIS) | long \+ timestamp-millis | TimestampType | TIMESTAMP(3) |
| TIME(ms) | Time32(ms) | INT32 \+ TIME(isAdjustedToUTC=false, MILLIS) | int \+ time-millis | LongType (as µs) | TIME(3) |
| TIMESTAMP(us) | Timestamp(us, null) | INT64 \+ TIMESTAMP(isAdjustedToUTC=false, MICROS) | long \+ timestamp-micros | TimestampNTZType | TIMESTAMP(6) |
| TIMESTAMPTZ(us) | Timestamp(us, 'UTC') | INT64 \+ TIMESTAMP(isAdjustedToUTC=true, MICROS) | long \+ timestamp-micros | TimestampType | TIMESTAMP(6) WITH LOCAL TIME ZONE |
| **DURATION(us)** | **Duration(us)** | INT64 | long | LongType | BIGINT |
| STRUCT\<...\> | Struct(...) | Group | record | StructType | ROW\<...\> |
| LIST\<T\> | LargeList\<T\> | Group \+ LIST | array | ArrayType | ARRAY\<T\> |
| MAP\<K,V\> | Map\<K,V\> | Group \+ MAP | map | MapType | MAP\<K,V\> |
| **DICTIONARY\<K,V\>** | **Dictionary** | Parquet Type for V (w/ Dictionary Encoding) | Avro Type for V (e.g., string, long) | Spark Type for V (e.g., StringType) | Flink Type for V (e.g., STRING) |
| VECTOR(FLOAT, d) | FixedSizeList\<Float32, d\> | FIXED\_LEN\_BYTE\_ARRAY or LIST | array\<float\> | ArrayType(FloatType) | ARRAY\<FLOAT\> |
| VARIANT | DenseUnion or LargeBinary | BYTE\_ARRAY \+ JSON | record of 2 byte fields + variant logical type | VariantType | JSON |
| BLOB | Struct\<type, data, reference\> | Group (BLOB logical type) | record \+ blob logical type | StructType (w/ BLOB metadata) | ROW\<type STRING, data BYTES, reference ROW\> |

 
## Implementation

A specific hudi core module "hudi-core-type" will define the above types. The translation layer to and from other type-systems such as Avro, Spark, Flink, Parquet,.. will reside in their own separate modules to keep the dependency clean. 

The table schema itself will need to be tracked in metadata table.
SQL Extensions needs to be added to define the table in a hudi type native way. 

TODO: There is an open question regarding the need to maintain type ids to track schema evolution and how it would interplay with NBCC. 

---

## Variant Type Implementation

This section documents the implementation of the VARIANT type in Hudi, which provides first-class support for
semi-structured data (e.g., JSON). The Variant type is implemented following Spark 4.0's native VariantType
specification.

### Overview

The Variant type enables Hudi to store and query semi-structured data efficiently. It is particularly useful for:

- Schema-on-read flexibility for evolving data structures
- Storing JSON-like data without requiring predefined schemas

### Motivation

Hudi readers and writers should be able to handle datasets with variant types.
This will allow users to work with semi-structured data more easily.

The variant type is now formally defined in Parquet and engines like Spark have full support for this type.
Users with semi-structured data are otherwise forced to use strings or byte arrays to store this data.

### What is the VARIANT Type?

The `VARIANT` type is a new data type designed to store semi-structured data (like JSON) efficiently.
Unlike storing JSON as a plain string, `VARIANT` uses an optimized binary encoding that allows for fast navigation
and element extraction without needing to parse the entire document.
It offers the flexibility of a schema-less design (like JSON) with performance closer to structured columns.

### Storage Modes: Shredded vs. Unshredded

- **Unshredded** (Binary Blob):
    - The entire JSON structure is encoded into binary metadata and value blobs.
    - **Pros**: Fast write speed; handles completely dynamic/random schemas easily.
    - **Cons**: To read a single field (e.g., `user.id`), the engine must load the entire binary blob.
- **Shredded** (Columnar Optimization):
    - The engine identifies common paths in the data (e.g., `v.a` or `v.c`) and extracts them into separate, native
      Parquet columns (e.g., Int32, Decimal).
    - **Pros**: Massive performance gain for queries. If you query `SELECT v:a`, the engine reads only the specific
      Int32 column and skips the rest of the binary data (Columnar Pruning).
    - **Cons**: Higher write overhead to analyze and split the data, prone to read jitter if there are large variation 
    - in shredding output.

### Architecture

Variant support is built on a **layered architecture** with version-specific adapters:

```
┌────────────────────────────────────────────────────┐
│            Application Layer (Spark SQL)           │
│    SELECT parse_json('{"a": 1}') as data           │
└────────────────────────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────┐
│              Spark Version Adapters                │
│  ┌──────────────────┐  ┌────────────────────────┐  │
│  │ BaseSpark3Adapter│  │   BaseSpark4Adapter    │  │
│  │ (No Variant)     │  │   (Full Variant)       │  │
│  └──────────────────┘  └────────────────────────┘  │
└────────────────────────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────┐
│             HoodieSchema.Variant                   │
│     (Avro Logical Type + Record Schema)            │
└────────────────────────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────┐
│              Parquet Storage                       │
│    GROUP { value: BINARY, metadata: BINARY }       │
└────────────────────────────────────────────────────┘
```

### Variant Schema Definition

The `HoodieSchema.Variant` class in `hudi-common` defines the Variant type:

```java
public static class Variant extends HoodieSchema {
  private static final String VARIANT_METADATA_FIELD = "metadata";
  private static final String VARIANT_VALUE_FIELD = "value";
  private static final String VARIANT_TYPED_VALUE_FIELD = "typed_value";

  private final boolean isShredded;
  private final Option<HoodieSchema> typedValueSchema;
}
```

#### Two Storage Modes

1. **Unshredded Variant** (Default):
    - Created with: `HoodieSchema.createVariant()`
    - Structure: Record with two REQUIRED binary fields
    - Fields: `metadata` (BYTES, REQUIRED), `value` (BYTES, REQUIRED)
    - Use case: Simple semi-structured data storage

2. **Shredded Variant**:
    - Created with: `HoodieSchema.createVariantShredded(typedValueSchema)`
    - Structure: Record with `typed_value` group containing extracted typed columns
    - Fields: `value` (BYTES, OPTIONAL), `metadata` (BYTES, REQUIRED), `typed_value` (GROUP, OPTIONAL)
    - Use case: Frequently accessed fields are extracted into native typed columns for columnar reads

#### How a Reader Distinguishes the Two Modes

Both modes use the same `VARIANT` logical type annotation in the Parquet footer — the annotation does not change.
The reader inspects the Parquet file schema to determine which mode was used:

- If the Variant group contains only `metadata` and `value`, it is **unshredded**.
- If the Variant group also contains a `typed_value` child group, it is **shredded**.

In the shredded case, `value` becomes OPTIONAL because when all fields are fully shredded, the binary blob
may be `null` — the content is entirely represented in the typed columns. The reader falls back to `value`
only for fields that were not shredded.

#### Custom Avro Logical Type

Variant uses a custom Avro logical type for identification:

```java
public static class VariantLogicalType extends LogicalType {
  private static final String VARIANT_LOGICAL_TYPE_NAME = "variant";
}
```

### On-Disk Representation (Parquet)

Hudi's on-disk Variant representation intentionally aligns with the
[Parquet Variant spec](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md).
The spec defines a Variant as a group annotated with the `VARIANT` logical type. Hudi writes Variant columns using
this exact layout in both modes:

#### Unshredded

```
optional group variant_column (VARIANT) {
  required binary metadata;
  required binary value;
}
```

The entire JSON value is encoded into the `value` blob. Both fields are REQUIRED — every row carries the full binary
representation.

#### Shredded

```
optional group variant_column (VARIANT) {
  required binary metadata;
  optional binary value;
  optional group typed_value {
    optional group a {
      optional binary value;
      optional int32 typed_value;
    }
    optional group b {
      optional binary value;
      optional binary typed_value (STRING);
    }
    optional group c {
      optional binary value;
      optional int64 typed_value;
    }
  }
}
```

Each child under `typed_value` corresponds to a shredded field. The child's `typed_value` column holds the extracted
native-typed value (e.g., `int32`, `string`, `int64`), while the child's `value` column is a fallback binary blob for
rows where the field's type does not match the shredded type. The top-level `value` becomes OPTIONAL — it is `null`
when all fields in the row are fully covered by shredded columns, and populated only for fields that were not shredded.

**How a reader uses this**: The Parquet file schema in the footer tells the reader which mode was used. If the Variant
group contains only `metadata` and `value`, it is unshredded. If a `typed_value` child group is present, the reader
knows which fields have been shredded and can read them as native typed columns — enabling column pruning, predicate
pushdown, and data skipping without touching the binary blob.

The `VARIANT` annotation is what allows readers to recognize the column as a Variant and expose semantic operations
(e.g., `variant_get`, JSON path access). Without it, the group is indistinguishable from an ordinary
`Struct<metadata: Binary, value: Binary>`.

**Alignment with the Parquet spec**: Hudi does not diverge from the Parquet Variant spec. The annotation, field names,
field types, and repetition levels all follow the spec exactly. This means any Parquet reader that implements the
Variant spec can read Hudi-written Variant columns natively, and vice versa.

**Reader compatibility**: Engines that do not yet implement the Parquet Variant spec (e.g., Spark 3.5) will ignore the
`VARIANT` annotation and fall back to reading the column as a plain struct of binary fields.
The data remains physically accessible, but users lose Variant query functions and must manually parse
the binary encoding. See [variant-appendix.md](variant-appendix.md) for detailed backward compatibility findings.

**Migration path**: If a future Parquet spec revision changes the Variant physical layout, Hudi can introduce a
table property (e.g., `hoodie.variant.parquet.format.version`) to control which format is written, and the reader
can inspect the Parquet footer to determine which layout to expect. (This is outside the scope of this RFC for now)

#### Binary Format

The Variant binary encoding follows the
[Parquet Variant Binary Encoding spec](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md):

| Component    | Description                                                         |
|--------------|---------------------------------------------------------------------|
| **metadata** | Dictionary of field names and type information for efficient access |
| **value**    | Binary encoding of the actual data (scalars, objects, arrays)       |

Example for `{"updated": true, "new_field": 123}`:

```
Metadata Bytes: [0x01, 0x02, 0x00, 0x07, 0x10, "updated", "new_field"]
Value Bytes:   [0x02, 0x02, 0x01, 0x00, 0x01, 0x00, 0x03, 0x04, 0x0C, 0x7B]
```

The metadata contains a dictionary of all field names, while the value contains references to these fields plus the
actual data values.

### Schema Evolution Support

Variant types provide **schema-on-read** flexibility:

| Aspect                   | Behavior                                                          |
|--------------------------|-------------------------------------------------------------------|
| Adding new fields        | ✅ Supported - New JSON fields can be added without schema changes |
| Removing fields          | ✅ Supported - Missing fields return null on read                  |
| Type changes within JSON | ✅ Supported - Variant can store any JSON-compatible type          |
| Table schema evolution   | ✅ Supported - Variant column can be added to existing tables      |
| Hudi schema evolution    | ✅ Supported - Works with Hudi's standard schema evolution         |

**Important**: The schema flexibility is within the Variant column itself. The table-level schema (including the Variant
column definition) still follows Hudi's standard schema evolution rules.

### Column Statistics and Indexing

Variant statistics and indexing capabilities depend on whether a field is accessed from the unshredded binary blob
or from a shredded (extracted) typed column. With shredding, extracted fields become regular typed Parquet columns
that automatically leverage all existing Hudi metadata infrastructure.

#### Unshredded Variant Column

| Feature            | Status | Notes                                                              |
|--------------------|--------|--------------------------------------------------------------------|
| Value/null counts  | ✅      | Standard column-level counts via MDT `column_stats` partition      |
| Min/max bounds     | ❌      | Binary blob has no meaningful sort order                           |
| Data skipping      | ❌      | Requires comparable min/max bounds                                 |
| Bloom filter       | ❌      | Bloom filters are used for record key lookups only                 |
| Partition stats    | ❌      | Variant columns are not used as partition keys                     |
| Predicate pushdown | ⚠️     | Structural predicates only (`IS NULL`, `IS NOT NULL`)              |

#### Shredded Variant Fields

| Feature            | Status | Notes                                                                                                                  |
|--------------------|--------|------------------------------------------------------------------------------------------------------------------------|
| Min/max bounds     | ✅      | Shredded columns are regular typed columns; `HoodieTableMetadataUtil.isColumnTypeSupported()` accepts all primitives   |
| Data skipping      | ✅      | `DataSkippingUtils` translates filters on shredded fields to min/max range checks                                      |
| Expression index   | ✅      | Index over `variant_get()` expressions enables stats collection without full shredding                                 |
| Bloom filter       | ❌      | Bloom filters are used for record key lookups only                                                                     |
| Partition stats    | ✅      | Applicable if a shredded field is used as a partition key                                                              |
| Predicate pushdown | ✅      | Full pushdown support: equality, range, and IN-list predicates                                                         |
| Column pruning     | ✅      | Read only the shredded columns needed; skip the binary blob entirely                                                   |

**Recommendation**: Enable shredding for frequently accessed fields to unlock full MDT column stats, data skipping,
and predicate pushdown. For lighter-weight optimization, use expression indexes over `variant_get()` expressions to
collect per-field statistics without materializing shredded columns.

### Usage Guide

#### Spark 4.0+ (Native Support)

```sql
-- Create table with Variant column
CREATE TABLE events
(
    id      STRING,
    ts      TIMESTAMP,
    payload VARIANT
) USING hudi
OPTIONS (
    primaryKey = 'id',
    preCombineField = 'ts'
);

-- Insert with parse_json
INSERT INTO events
VALUES ('1', current_timestamp(), parse_json('{"event": "click", "page": "/home"}')),
       ('2', current_timestamp(), parse_json('{"event": "purchase", "amount": 99.99}'));

-- Query Variant data
SELECT id, payload:event, payload:amount
FROM events;

-- Update Variant column
UPDATE events
SET payload = parse_json('{"event": "click", "page": "/products"}')
WHERE id = '1';

-- Works with both COW and MOR tables
CREATE TABLE events_mor
(
    id      STRING,
    ts      TIMESTAMP,
    payload VARIANT
) USING hudi
TBLPROPERTIES (
    'type' = 'mor',
    'primaryKey' = 'id',
    'preCombineField' = 'ts'
);
```

#### Spark 3.x (Backward Compatibility Read)

Spark 3.x does not support VariantType natively, but can read Variant tables as struct:

```sql
-- Reading Spark 4.0 Variant table in Spark 3.x
-- Variant column appears as: STRUCT<value: BINARY, metadata: BINARY>

SELECT id, cast(payload.value as string)
FROM events;
```

**Limitations in Spark 3.x**:

- Cannot write Variant data
- Variant column reads as raw struct with binary fields
- No helper functions like `parse_json()` available

### Cross-Engine Compatibility

Engines that do not support the Variant type can still read all non-Variant columns in the table. The Variant column's
on-disk layout is a plain Parquet struct (`value BINARY, metadata BINARY`), so engines that lack Variant-aware logic
can either project it as a struct of binary fields or simply exclude it from their column projection. This means adding
a Variant column to a table does not break reads from older or third-party engines — they continue to access every
other column as before.

#### Flink Integration

| Operation                            | Support                            |
|--------------------------------------|------------------------------------|
| Reading Spark-written Variant tables | ✅ Supported                        |
| Variant representation in Flink      | `ROW<value BYTES, metadata BYTES>` |
| Writing Variant from Flink           | ❌ Not yet implemented              |

Example Flink query reading Variant data:

```sql
-- Flink sees Variant as ROW type
SELECT id, variant_col.value, variant_col.metadata
FROM hudi_variant_table;
```

#### Avro Serialization (MOR Tables)

For MOR tables, Variant data is serialized to Avro for log files:

```
{
  "type": "record",
  "logicalType": "variant",
  "fields": [
    {"name": "value", "type": "bytes"},
    {"name": "metadata", "type": "bytes"}
  ]
}
```

### Backward Compatibility

The implementation ensures backward compatibility through:

1. **Storage Format**: On-disk layout is a plain Parquet struct (`value BINARY, metadata BINARY`), readable by any Parquet-compatible engine
2. **Logical Type Annotation**: Avro logical type allows newer versions to recognize Variant semantics
3. **Graceful Degradation**: Older readers see Variant as `STRUCT<value: BINARY, metadata: BINARY>`

| Scenario                          | Behavior                  |
|-----------------------------------|---------------------------|
| Spark 4.0 writes, Spark 4.0 reads | Full Variant support      |
| Spark 4.0 writes, Spark 3.x reads | Struct with binary fields |
| Spark 4.0 writes, Flink reads     | ROW with binary fields    |
| Spark 3.x writes Variant          | ❌ Not supported           |

### Limitations and Constraints

1. **Spark Version Dependency**:
    - Write support requires Spark 4.0+
    - Spark 3.x limited to read-only access with degraded experience

2. **Storage Overhead**:
    - Metadata stored redundantly per row
    - No column-level compression optimizations for Variant content

3. **Query Performance**:
    - Predicate pushdown on unshredded Variant content is limited to structural predicates (`IS NULL`, `IS NOT NULL`)
    - Accessing fields from the unshredded blob requires loading the full binary value
    - Shredded fields and expression indexes enable full predicate pushdown, data skipping, and column pruning

4. **Shredded Variant**:
    - `typed_value` field is defined for extracted typed columns within the Variant group
    - Shredded fields unlock full MDT column stats and data skipping via `DataSkippingUtils`

5. **Functions**:
    - `parse_json()` - Spark 4.0+ only
    - JSON path access (`payload:field`) - Spark 4.0+ only
    - No UDFs for Variant manipulation in Spark 3.x

### Key Implementation Files

| File                                                      | Description                                      |
|-----------------------------------------------------------|--------------------------------------------------|
| `hudi-common/.../HoodieSchema.java`                       | Core Variant schema definition with logical type |
| `hudi-spark3-common/.../BaseSpark3Adapter.scala`          | Spark 3 adapter (no Variant support)             |
| `hudi-spark4-common/.../BaseSpark4Adapter.scala`          | Spark 4 adapter with full Variant API            |
| `hudi-spark-client/.../HoodieRowParquetWriteSupport.java` | Variant Parquet writing                          |
| `hudi-spark-client/.../HoodieSparkSchemaConverters.scala` | Schema conversion for Variant                    |
| `hudi-spark4.0.x/.../AvroSerializer.scala`                | Spark to Avro Variant conversion                 |
| `hudi-spark4.0.x/.../AvroDeserializer.scala`              | Avro to Spark Variant conversion                 |

### Test Coverage

| Test                                      | Description                                          |
|-------------------------------------------|------------------------------------------------------|
| `TestHoodieRowParquetWriteSupportVariant` | Parquet write for unshredded/shredded Variants       |
| `TestVariantDataType`                     | INSERT, UPDATE, DELETE operations on Variant columns |
| `TestHoodieFileGroupReaderOnSparkVariant` | File group reader with Variant data                  |
| `ITTestVariantCrossEngineCompatibility`   | Flink reading Spark-written Variant tables           |

### Future Enhancements

1. **Shredded Variant Population**: Implement typed_value extraction for frequently accessed fields
2. **Flink Write Support**: Enable writing Variant data from Flink
3. **Variant Field Statistics**: Collect per-field column stats for shredded Variant fields and expression-indexed Variant extract expressions via the MDT `column_stats` partition
4. **Spark 3.x Write Support**: Provide UDFs for Variant creation in Spark 3.x

## Appendix

- For more details on VECTOR type design considerations, please see the
following: [rfc-99/vector-appendix.md](vector-appendix.md)

- For more details on VARIANT type design considerations, please see the
  following: [rfc-99/variant-appendix.md](variant-appendix.md)

