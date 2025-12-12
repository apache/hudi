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

## Approvers

- @vinothchandar


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




 


