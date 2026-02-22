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

| Logical Type | Description | Parameters |
| :---- | :---- | :---- |
| VECTOR(element\_type, dimension) | A dense, fixed-length vector of numeric values. | Element type, dimension |
| SPARSE\_VECTOR(indices, values) | A sparse vector represented by indices and values. | Index, Value types |
| TENSOR(element\_type, shape) | A multi-dimensional array (tensor). | Element type, shape |

#### **3.7. Geospatial Types 🗺️**

These types are aligned with the Open Geospatial Consortium (OGC) standard and are designed for first-class integration with the **GeoArrow** extension for high-performance in-memory processing. The distinction between planar (**geometry**) and geodetic (**geography**) data is handled by the Coordinate Reference System (CRS) information stored as metadata with the schema.

| Logical Type | Description | Parameters |
| :---- | :---- | :---- |
| **GEOMETRY** | A generic container for a single geometry of any shape. | None |
| **GEOGRAPHY** | A generic container for a single geodetic shape. | None |
| **POINT** | An exact coordinate location. | dimensions: 'XY', 'XYZ', 'XYM', 'XYZM' |
| **LINESTRING** | A sequence of two or more points connected by straight line segments. | dimensions: 'XY', 'XYZ', 'XYM', 'XYZM' |
| **POLYGON** | A planar surface defined by one exterior ring and zero or more interior rings (holes). | dimensions: 'XY', 'XYZ', 'XYM', 'XYZM' |
| **MULTIPOINT** | A collection of one or more points. | dimensions: 'XY', 'XYZ', 'XYM', 'XYZM' |
| **MULTILINESTRING** | A collection of one or more LineStrings. | dimensions: 'XY', 'XYZ', 'XYM', 'XYZM' |
| **MULTIPOLYGON** | A collection of one or more Polygons. | dimensions: 'XY', 'XYZ', 'XYM', 'XYZM' |
| **GEOMETRY\_COLLECTION** | A collection of one or more geometry objects, which can be of different types. | dimensions: 'XY', 'XYZ', 'XYM', 'XYZM' |


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
| **GEOMETRY** | **GeoArrow: WKB (in LargeBinary)** | BYTE\_ARRAY | bytes | BinaryType | BYTES |
| **GEOGRAPHY** | **GeoArrow: WKB (in LargeBinary)** | BYTE\_ARRAY | bytes | BinaryType | BYTES |
| **POINT** | **GeoArrow: Point (Struct layout)** | Group | record | StructType | ROW |
| **LINESTRING** | **GeoArrow: LineString (List layout)** | Group \+ LIST | array of record | ArrayType(StructType) | ARRAY\<ROW\> |
| **POLYGON** | **GeoArrow: Polygon (List layout)** | Group \+ LIST (nested) | array of array of record | ArrayType(ArrayType(StructType)) | ARRAY\<ARRAY\<ROW\>\> |
| **MULTIPOINT** | **GeoArrow: MultiPoint (List layout)** | Group \+ LIST | array of record | ArrayType(StructType) | ARRAY\<ROW\> |
| **MULTILINESTRING** | **GeoArrow: MultiLineString (List layout)** | Group \+ LIST (nested) | array of array of record | ArrayType(ArrayType(StructType)) | ARRAY\<ARRAY\<ROW\>\> |
| **MULTIPOLYGON** | **GeoArrow: MultiPolygon (List layout)** | Group \+ LIST (nested) | array of array of array of record | ArrayType(ArrayType(ArrayType(StructType))) | ARRAY\<ARRAY\<ARRAY\<ROW\>\>\> |
| **GEOMETRY\_COLLECTION** | **GeoArrow: Union or WKB** | Group (Union) or BYTE\_ARRAY | union or bytes | BinaryType | BYTES |
| VARIANT | DenseUnion or LargeBinary | BYTE\_ARRAY \+ JSON | string or union | VariantType | JSON |

 
## Implementation

A specific hudi core module "hudi-core-type" will define the above types. The translation layer to and from other type-systems such as Avro, Spark, Flink, Parquet,.. will reside in their own separate modules to keep the dependency clean. 

The table schema itself will need to be tracked in metadata table.
SQL Extensions needs to be added to define the table in a hudi type native way. 

TODO: There is an open question regarding the need to maintain type ids to track schema evolution and how it would interplay with NBCC. 

The main implementation change would require replacing the Avro schema references with the new type system. 


## Supporting VECTOR type in Hudi
This section captures additional research and design notes for supporting a VECTOR logical type in Hudi. See appendix for more details on research sources.

### Initial scope

The intial use case we are targeting for `VECTOR` within Hudi, 
is to enable KNN style vector search functionality to be performed on blobs(large text, images, audio, video) alongside their generated vector embeddings.
Typically vector search is popular for Retrieval-Augmented Generation (RAG) applications
which provide relevant context to an LLM in order to improve its accuracy when answering user queries. 
The vector embeddings generated by frontier models are usually in the form of an array of floating point values.

### Dense vectors vs sparse vectors

***Dense vector***
* Has a value for every dimension.
* Stored as a full length-D sequence: v = [0.12, -0.03, 0.44, ...] (length = D)
* Even if some entries are 0, you still store them.

***Sparse vector***
* Most entries are 0 / absent, so you store only the non-zero positions.
* Stored as pairs (index, value), sometimes also with a separate nnz count:
* [(3, 0.44), (107, 1.2), (9012, -0.7)]
* The “dimension” is still D, but the stored length is nnz (number of non-zeros), typically nnz << D.

Sparse vectors become important for other types of hybrid/lexical-style retrieval which is not targeted for the intial scope, 
as that requires running different algorithms such as (TF-IDF or BM25) which is different from the intial use case of KNN style search.  
Hence this RFC has seperated both into two distinct types one for VECTOR (dense) and one for SPARSE_VECTOR, we will for now spend time on VECTOR dense case. 


### Vector Schema constraints

**Logical level requirements:**
- All values within the VECTOR column must have the same **dimension** i.e (number of elements within the vector), as this is needed to perform cosine/L2/dot-product correctly. 
- There should be no null elements within the vector at write time.
- VECTOR must have an "element type" which can be one of `FLOAT`, `DOUBLE` or `INT8`.
- We also want to keep a property around such as `storageBacking` which lets the writers know how to serialize the vector to disk. For an intial approach we will start with a fixed bytes approach covered below. 

See the following avro schema model as a general example:
```
{
  "type" : "fixed",
  "name" : "vector",
  "size" : 3072,
  "logicalType" : "vector",
  "dimension" : 768,
  "elementType" : "FLOAT",
  "storageBacking" : "FIXED_BYTES"
}

```


**Physical level requirements:**

For now we will support a fixed-size packed byte representation for storing vectors on disk as this yields optimal performance(see parquet tuning section below for more details): 
- FLOAT32 vector of dimension D stored as exactly `D * 4` bytes (IEEE-754 float32, little-endian)
- Map to Parquet `FIXED_LEN_BYTE_ARRAY(D * 4)` with VECTOR metadata.
- For Lance, vectors are typically represented using Arrow's `FixedSizedList`

## Optimal Parquet tuning for vectors:
Vector data is typically high-cardinality and not dictionary-friendly. Therefore we will be disabling dictionary encoding and column stats for vector columns. 
Also based on findings from the parquet community, encodings such as `PLAIN` or `BYTE_STREAM_SPLIT` are useful when dealing with vectors, as well as disabling compression
as this would yield best write/read performance.

***Benchmark experiment with vectors***

* The results below was from an experiment writing `10,000` vectors (where each vector dimension is 1,536 and the element type is FLOAT(4 bytes), around 6KB per record).
* We performed a full round trip for writing all vectors to a file and then read it back, using PARQUET and LANCE's java file writers/readers.
* For PARQUET we tried several combination of writing with different types,as well as tried different encodings, compressions, etc to handle vectors.
* For LANCE we opted to use vanilla settings based on it claims already toward already handling vectors optimally.
* We performed 5 warmup rounds and 10 measurement rounds and collected averages below.


***Physical backings tested***
* Parquet LIST: Vectors stored as Parquet's LIST<FLOAT> type (variable-length array)
* Parquet FIXED: Vectors stored as Parquet's FIXED_LEN_BYTE_ARRAY (fixed 6,144 bytes for 1,536 floats)
* Lance: Vectors stored in Lance format using FixedSizeList<Float32>

***Summary of Results***
```
Winner (most compact file size): Parquet LIST (byte-stream-split, ZSTD)

Currently parquet list is only a couple of MB more compact then the other parquet fixed tests.

Performance Winner (Write): Lance
Performance Winner (Read):  Parquet FIXED (byte-stream-split, UNCOMPRESSED)

*Note* Parquet FIXED and Lance are close in write perf
```

***Detailed comparison table***
```
======================================================================
COMPARISON SUMMARY
======================================================================

Representation                 |     File Size |    Write Speed |     Read Speed |     Bytes/Rec |    vs Raw |   vs Base
------------------------------------------------------------------------------------------------------------------------
Parquet LIST (plain, UNCOMPRESSED) |      58.86 MB |    124.93 MB/s |    233.44 MB/s |       6,172 B |     1.00x |     1.00x
------------------------------------------------------------------------------------------------------------------------
Parquet LIST (plain, SNAPPY)   |      58.69 MB |    125.20 MB/s |    232.51 MB/s |       6,154 B |     1.00x |     1.00x
------------------------------------------------------------------------------------------------------------------------
Parquet LIST (plain, ZSTD)     |      54.35 MB |    117.66 MB/s |    206.32 MB/s |       5,698 B |     1.08x |     0.92x
------------------------------------------------------------------------------------------------------------------------
Parquet LIST (byte-stream-split, UNCOMPRESSED) |      58.86 MB |    118.61 MB/s |    210.77 MB/s |       6,172 B |     1.00x |     1.00x
------------------------------------------------------------------------------------------------------------------------
Parquet LIST (byte-stream-split, SNAPPY) |      53.60 MB |    111.18 MB/s |    200.66 MB/s |       5,620 B |     1.09x |     0.91x
------------------------------------------------------------------------------------------------------------------------
Parquet LIST (byte-stream-split, ZSTD) |      50.27 MB |    101.90 MB/s |    194.02 MB/s |       5,270 B |     1.17x |     0.85x
------------------------------------------------------------------------------------------------------------------------
Parquet FIXED (plain, UNCOMPRESSED) |      58.82 MB |    527.87 MB/s |   2253.61 MB/s |       6,167 B |     1.00x |     1.00x
------------------------------------------------------------------------------------------------------------------------
Parquet FIXED (plain, SNAPPY)  |      58.69 MB |    496.56 MB/s |   2092.63 MB/s |       6,154 B |     1.00x |     1.00x
------------------------------------------------------------------------------------------------------------------------
Parquet FIXED (plain, ZSTD)    |      54.35 MB |    430.84 MB/s |    760.96 MB/s |       5,699 B |     1.08x |     0.92x
------------------------------------------------------------------------------------------------------------------------
Parquet FIXED (byte-stream-split, UNCOMPRESSED) |      58.82 MB |    480.28 MB/s |   2343.75 MB/s |       6,167 B |     1.00x |     1.00x
------------------------------------------------------------------------------------------------------------------------
Parquet FIXED (byte-stream-split, SNAPPY) |      58.69 MB |    327.34 MB/s |   2020.47 MB/s |       6,154 B |     1.00x |     1.00x
------------------------------------------------------------------------------------------------------------------------
Parquet FIXED (byte-stream-split, ZSTD) |      54.35 MB |    415.56 MB/s |    802.65 MB/s |       5,699 B |     1.08x |     0.92x
------------------------------------------------------------------------------------------------------------------------
Lance                          |      58.85 MB |    665.84 MB/s |   1395.09 MB/s |       6,170 B |     1.00x |         -
------------------------------------------------------------------------------------------------------------------------

Winner (most compact): Parquet LIST (byte-stream-split, ZSTD)

======================================================================
PERFORMANCE SUMMARY
======================================================================

Representation                      |        Write Time |      Write Speed |         Read Time |       Read Speed
-------------------------------------------------------------------------------------------------------------------
Parquet LIST (plain, UNCOMPRESSED)  |            469 ms |      124.93 MB/s |            251 ms |      233.44 MB/s
-------------------------------------------------------------------------------------------------------------------
Parquet LIST (plain, SNAPPY)        |            468 ms |      125.20 MB/s |            252 ms |      232.51 MB/s
-------------------------------------------------------------------------------------------------------------------
Parquet LIST (plain, ZSTD)          |            498 ms |      117.66 MB/s |            284 ms |      206.32 MB/s
-------------------------------------------------------------------------------------------------------------------
Parquet LIST (byte-stream-split, UNCOMPRESSED) |            494 ms |      118.61 MB/s |            278 ms |      210.77 MB/s
-------------------------------------------------------------------------------------------------------------------
Parquet LIST (byte-stream-split, SNAPPY) |            527 ms |      111.18 MB/s |            292 ms |      200.66 MB/s
-------------------------------------------------------------------------------------------------------------------
Parquet LIST (byte-stream-split, ZSTD) |            575 ms |      101.90 MB/s |            302 ms |      194.02 MB/s
-------------------------------------------------------------------------------------------------------------------
Parquet FIXED (plain, UNCOMPRESSED) |            111 ms |      527.87 MB/s |             26 ms |     2253.61 MB/s
-------------------------------------------------------------------------------------------------------------------
Parquet FIXED (plain, SNAPPY)       |            118 ms |      496.56 MB/s |             28 ms |     2092.63 MB/s
-------------------------------------------------------------------------------------------------------------------
Parquet FIXED (plain, ZSTD)         |            136 ms |      430.84 MB/s |             77 ms |      760.96 MB/s
-------------------------------------------------------------------------------------------------------------------
Parquet FIXED (byte-stream-split, UNCOMPRESSED) |            122 ms |      480.28 MB/s |             25 ms |     2343.75 MB/s
-------------------------------------------------------------------------------------------------------------------
Parquet FIXED (byte-stream-split, SNAPPY) |            179 ms |      327.34 MB/s |             29 ms |     2020.47 MB/s
-------------------------------------------------------------------------------------------------------------------
Parquet FIXED (byte-stream-split, ZSTD) |            141 ms |      415.56 MB/s |             73 ms |      802.65 MB/s
-------------------------------------------------------------------------------------------------------------------
Lance                               |             88 ms |      665.84 MB/s |             42 ms |     1395.09 MB/s
-------------------------------------------------------------------------------------------------------------------

Performance Winner (Write): Lance
Performance Winner (Read):  Parquet FIXED (byte-stream-split, UNCOMPRESSED)

======================================================================
COMPRESSION CODEC ANALYSIS
======================================================================

Parquet LIST - Compression Comparison:
----------------------------------------------------------------------
Compression                    |     File Size |        vs Raw |     Write Time |      Read Time |   Write MB/s
--------------------------------------------------------------------------------------------------------------
plain, UNCOMPRESSED            |      58.86 MB |         1.00x |         469 ms |         251 ms |       124.93
--------------------------------------------------------------------------------------------------------------
plain, SNAPPY                  |      58.69 MB |         1.00x |         468 ms |         252 ms |       125.20
--------------------------------------------------------------------------------------------------------------
plain, ZSTD                    |      54.35 MB |         1.08x |         498 ms |         284 ms |       117.66
--------------------------------------------------------------------------------------------------------------
byte-stream-split, UNCOMPRESSED |      58.86 MB |         1.00x |         494 ms |         278 ms |       118.61
--------------------------------------------------------------------------------------------------------------
byte-stream-split, SNAPPY      |      53.60 MB |         1.09x |         527 ms |         292 ms |       111.18
--------------------------------------------------------------------------------------------------------------
byte-stream-split, ZSTD        |      50.27 MB |         1.17x |         575 ms |         302 ms |       101.90
--------------------------------------------------------------------------------------------------------------

Best compression ratio: byte-stream-split, ZSTD
Fastest write: plain, SNAPPY
Fastest read: plain, UNCOMPRESSED

Parquet FIXED - Compression Comparison:
----------------------------------------------------------------------
Compression                    |     File Size |        vs Raw |     Write Time |      Read Time |   Write MB/s
--------------------------------------------------------------------------------------------------------------
plain, UNCOMPRESSED            |      58.82 MB |         1.00x |         111 ms |          26 ms |       527.87
--------------------------------------------------------------------------------------------------------------
plain, SNAPPY                  |      58.69 MB |         1.00x |         118 ms |          28 ms |       496.56
--------------------------------------------------------------------------------------------------------------
plain, ZSTD                    |      54.35 MB |         1.08x |         136 ms |          77 ms |       430.84
--------------------------------------------------------------------------------------------------------------
byte-stream-split, UNCOMPRESSED |      58.82 MB |         1.00x |         122 ms |          25 ms |       480.28
--------------------------------------------------------------------------------------------------------------
byte-stream-split, SNAPPY      |      58.69 MB |         1.00x |         179 ms |          29 ms |       327.34
--------------------------------------------------------------------------------------------------------------
byte-stream-split, ZSTD        |      54.35 MB |         1.08x |         141 ms |          73 ms |       415.56
--------------------------------------------------------------------------------------------------------------

Best compression ratio: plain, ZSTD
Fastest write: plain, UNCOMPRESSED
Fastest read: byte-stream-split, UNCOMPRESSED


Lance Format - Default Compression:
----------------------------------------------------------------------
Default                  58.85 MB         1.00x         88 ms         42 ms     665.84

Note: Lance uses default compression settings (no variations tested)


======================================================================
ENCODING STRATEGY ANALYSIS
======================================================================

Parquet LIST - Encoding Strategy Comparison:
----------------------------------------------------------------------
plain                    : Avg size     57.30 MB (1.02x), Avg write    478 ms, Avg read    262 ms
  Breakdown by compression:
    UNCOMPRESSED   :     58.86 MB (1.00x)
    SNAPPY         :     58.69 MB (1.00x)
    ZSTD           :     54.35 MB (1.08x)

byte-stream-split        : Avg size     54.24 MB (1.08x), Avg write    532 ms, Avg read    290 ms
  Breakdown by compression:
    UNCOMPRESSED   :     58.86 MB (1.00x)
    SNAPPY         :     53.60 MB (1.09x)
    ZSTD           :     50.27 MB (1.17x)


Parquet FIXED - Encoding Strategy Comparison:
----------------------------------------------------------------------
plain                    : Avg size     57.29 MB (1.02x), Avg write    121 ms, Avg read     43 ms
  Breakdown by compression:
    UNCOMPRESSED   :     58.82 MB (1.00x)
    SNAPPY         :     58.69 MB (1.00x)
    ZSTD           :     54.35 MB (1.08x)

byte-stream-split        : Avg size     57.29 MB (1.02x), Avg write    147 ms, Avg read     42 ms
  Breakdown by compression:
    UNCOMPRESSED   :     58.82 MB (1.00x)
    SNAPPY         :     58.69 MB (1.00x)
    ZSTD           :     54.35 MB (1.08x)



Lance Format - Default Encoding:
----------------------------------------------------------------------
Default (Arrow IPC)      : Avg size     58.85 MB (1.00x), Avg write     88 ms, Avg read     42 ms

Note: Lance uses Apache Arrow IPC encoding (no variations tested)
```

#### Vector definition in HoodieSchema:
* Refer to the following PR: https://github.com/apache/hudi/pull/18146


#### Appendix:
- https://milvus.io/ai-quick-reference/what-is-the-difference-between-sparse-and-dense-retrieval
- https://aws.amazon.com/blogs/big-data/integrate-sparse-and-dense-vectors-to-enhance-knowledge-retrieval-in-rag-using-amazon-opensearch-service/
- https://developers.openai.com/api/docs/guides/embeddings
- https://docs.cloud.google.com/vertex-ai/generative-ai/docs/embeddings
- https://lance.org/integrations/spark/operations/ddl/create-table/?utm_source=chatgpt.com#vector-columns
- https://lucene.apache.org/core/9_0_0/core/org/apache/lucene/document/KnnVectorField.html
- (Parquet community ALP Doc) https://docs.google.com/document/d/1PlyUSfqCqPVwNt8XA-CfRqsbc0NKRG0Kk1FigEm3JOg/edit?tab=t.0#heading=h.5xf60mx6q7xk
- https://docs.snowflake.com/en/sql-reference/data-types-vector
- https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/create-tables-using-vector-data-type.html
