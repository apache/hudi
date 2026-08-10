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

# RFC-84: Optimized SerDe of `DataStream` in Flink operators

## Proposers

- @geserdugarov

## Approvers

- @danny0405
- @cshuo

## Status

Main part of implementation is done:
https://github.com/apache/hudi/pull/12796

Umbrella ticket: [HUDI-8920](https://issues.apache.org/jira/browse/HUDI-8920)

## Abstract

Currently, in the majority of scenarios when Flink writes into Hudi, the first step is row data conversion into Avro record, which is used for key generation, 
and passed to the following operators as part of `HoodieRecord`. Kryo serializer is used to serialize/deserialize those records. 
And as it mentioned in the [claim for this RFC](https://github.com/apache/hudi/pull/12550), Kryo serde costs are huge, which is unacceptable for stream processing.

This RFC suggests to implement data processing with keeping focus on performance for Flink, and considering Flink's internal data types and serialization.

## Background

Currently, `HoodieRecord` is chosen as standardized API for interacting with a single record, see [RFC-46](../rfc-46/rfc-46.md). 
But `HoodieRecord` complex structure leads to high serialization/deserialization costs if it needed to be sent.
So for Flink main use case scenario of stream processing, when we handle each record separately on different operators, 
current approach with initial conversion into `HoodieRecord` becomes unacceptable.

Conversion into `HoodieRecord` should be done only in operators, which actually perform write into HDFS, S3, etc., to prevent excessive costs. 
And also it allows to implement future optimizations with direct write of Flink `RowData` to parquet files if it is needed without any intermediate conversions.
In Flink pipelines we could keep internal `RowData` together with only necessary Hudi metadata.
And these metadata should be added considering Flink data types.

## Implementation

To prepare implementation plan we should start from current state review.

### Write

All scenarios of Flink writes into Hudi could be presented in a schema below:

![`DataStream` for `HoodieTableSink`](datastream_hoodietablesink.png)

There are two special cases for processing: bulk insert, and append mode (insert operation into MOR and COW without inline clustering), which are seen in the lower part.
For both of these cases there is no conversion of `RowData` into `HoodieRecord`.
Also, Flink could automatically chain operators together if it's possible, which means that one operator combines multiple transformations,
and there are no serialization/deserialization between these transformations.
Therefore, our main focus should be on the upper part of the presented schema due to the fact that for bulk insert and append mode chained Flink operators are used.

Transformations separated by some partitioners, like 
[`keyBy()`](https://github.com/apache/flink/blob/b1fe7b4099497f02b4658df7c3de8e45b62b7e21/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/datastream/DataStream.java#L304-L308), 
which uses 
[`KeyGroupStreamPartitioner`](https://github.com/apache/flink/blob/b1fe7b4099497f02b4658df7c3de8e45b62b7e21/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/partitioner/KeyGroupStreamPartitioner.java), 
or 
[`partitionCustom()`](https://github.com/apache/flink/blob/b1fe7b4099497f02b4658df7c3de8e45b62b7e21/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/datastream/DataStream.java#L398-L402), 
which expects user defined custom partitioner, couldn't been chained.
Those partitioners are marked as purple blocks in the schema above, and at those places we will face high serialization/deserialization costs.

### Read

As for reading of Hudi table by Flink, at the first blush there is no need to implement such optimizations there.

### Metadata

We start writing into Hudi table from `DataStream<RowData>`.
Hudi metadata, which is necessary for processing in different operators, is marked by red color on the schema above.
We could use 
[`map()`](https://github.com/apache/flink/blob/b1fe7b4099497f02b4658df7c3de8e45b62b7e21/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/datastream/DataStream.java#L611-L614)
transformation to convert incoming `RowData` into a new object `HoodieFlinkInternalRow`.
We don't use `HoodieFlinkRecord` name to prevent confusion, because `HoodieFlinkInternalRow` doesn't extend `HoodieRecord`.

Structure of `HoodieFlinkInternalRow`:
```Java
public class HoodieFlinkInternalRow implements Serializable {

  private static final long serialVersionUID = 1L;

  // the number of fields without nesting
  protected static final int ARITY = 7;

  // recordKey, partitionPath, isIndexRecord and rowData are final
  private final StringData recordKey;
  private final StringData partitionPath;
  private StringData fileId;
  private StringData instantTime;
  private StringData operationType;
  // there is no rowData for index record
  private final BooleanValue isIndexRecord;
  private final RowData rowData;

  public HoodieFlinkInternalRow(String recordKey, String partitionPath, RowData rowData) {
    this(recordKey, partitionPath, "", "", "", false, rowData);
  }

  // constructor for index records without row data
  public HoodieFlinkInternalRow(String recordKey, String partitionPath, String fileId, String instantTime) {
    this(recordKey, partitionPath, fileId, instantTime, "", true, null);
  }

  public HoodieFlinkInternalRow(String recordKey,
                                String partitionPath,
                                String fileId,
                                String instantTime,
                                String operationType,
                                boolean isIndexRecord,
                                RowData rowData) {
    this.recordKey = StringData.fromString(recordKey);
    this.partitionPath = StringData.fromString(partitionPath);
    this.fileId = StringData.fromString(fileId);
    this.instantTime = StringData.fromString(instantTime);
    this.operationType = StringData.fromString(operationType);
    this.isIndexRecord = new BooleanValue(isIndexRecord);
    this.rowData = rowData;
  }

  public String getRecordKey() {}

  public String getPartitionPath() {}

  public void setFileId(String fileId) {}
  public String getFileId() {}

  public void setInstantTime(String instantTime) {}
  public String getInstantTime() {}

  public void setOperationType(String operationType) {}
  public String getOperationType() {}

  public boolean isIndexRecord() {}

  public RowData getRowData() {}

  public HoodieFlinkInternalRow copy(RowDataSerializer rowDataSerializer) {
    return new HoodieFlinkInternalRow(
        this.recordKey.toString(),
        this.partitionPath.toString(),
        this.fileId.toString(),
        this.instantTime.toString(),
        this.operationType.toString(),
        this.isIndexRecord.getValue(),
        rowDataSerializer.copy(this.rowData));
  }
}
```

To describe how to serialize and deserialize it properly, we also need to implement `HoodieFlinkInternalRowTypeInfo` and `HoodieFlinkInternalRowSerializer`.

```Java
public class HoodieFlinkInternalRowTypeInfo extends TypeInformation<HoodieFlinkInternalRow> {

  private static final long serialVersionUID = 1L;

  private final RowType rowType;

  public HoodieFlinkInternalRowTypeInfo(RowType rowType) {
    this.rowType = rowType;
  }

  @Override
  public boolean isBasicType() {}

  @Override
  public boolean isTupleType() {}

  @Override
  public int getArity() {
    return HoodieFlinkInternalRow.ARITY;
  }

  /**
   * Used only in Flink `CompositeType`, not used in this type
   */
  @Override
  public int getTotalFields() {
    return HoodieFlinkInternalRow.ARITY;
  }

  @Override
  public Class<HoodieFlinkInternalRow> getTypeClass() {}

  @Override
  public boolean isKeyType() {}

  @Override
  public TypeSerializer<HoodieFlinkInternalRow> createSerializer(ExecutionConfig config) {
    return new HoodieFlinkInternalRowSerializer(this.rowType);
  }

  @Override
  public String toString() {}

  @Override
  public boolean equals(Object obj) {}

  @Override
  public int hashCode() {}

  @Override
  public boolean canEqual(Object obj) {}
}
```

```Java
public class HoodieFlinkInternalRowSerializer extends TypeSerializer<HoodieFlinkInternalRow> {

  private static final long serialVersionUID = 1L;

  protected RowType rowType;

  protected RowDataSerializer rowDataSerializer;

  protected StringDataSerializer stringDataSerializer;

  public HoodieFlinkInternalRowSerializer(RowType rowType) {
    this.rowType = rowType;
    this.rowDataSerializer = new RowDataSerializer(rowType);
    this.stringDataSerializer = StringDataSerializer.INSTANCE;
  }

  @Override
  public boolean isImmutableType() {}

  @Override
  public TypeSerializer<HoodieFlinkInternalRow> duplicate() {}

  @Override
  public HoodieFlinkInternalRow createInstance() {
    throw new UnsupportedOperationException("HoodieFlinkInternalRow doesn't allow creation with some defaults.");
  }

  @Override
  public HoodieFlinkInternalRow copy(HoodieFlinkInternalRow from) {
    return from.copy(rowDataSerializer);
  }

  @Override
  public HoodieFlinkInternalRow copy(HoodieFlinkInternalRow from, HoodieFlinkInternalRow reuse) {
    throw new UnsupportedOperationException("HoodieFlinkInternalRow doesn't allow reusing.");
  }

  @Override
  public int getLength() {
    // -1 for variable length data types
    return -1;
  }

  @Override
  public void serialize(HoodieFlinkInternalRow record, DataOutputView target) throws IOException {
    boolean isIndexRecord = record.isIndexRecord();
    target.writeBoolean(isIndexRecord);
    stringDataSerializer.serialize(StringData.fromString(record.getRecordKey()), target);
    stringDataSerializer.serialize(StringData.fromString(record.getPartitionPath()), target);
    stringDataSerializer.serialize(StringData.fromString(record.getFileId()), target);
    stringDataSerializer.serialize(StringData.fromString(record.getInstantTime()), target);
    stringDataSerializer.serialize(StringData.fromString(record.getOperationType()), target);
    if (!isIndexRecord) {
      rowDataSerializer.serialize(record.getRowData(), target);
    }
  }

  @Override
  public HoodieFlinkInternalRow deserialize(DataInputView source) throws IOException {
    boolean isIndexRecord = source.readBoolean();
    StringData recordKey = stringDataSerializer.deserialize(source);
    StringData partition = stringDataSerializer.deserialize(source);
    StringData fileId = stringDataSerializer.deserialize(source);
    StringData instantTime = stringDataSerializer.deserialize(source);
    StringData operationType = stringDataSerializer.deserialize(source);
    HoodieFlinkInternalRow record;
    if (!isIndexRecord) {
      RowData rowData = rowDataSerializer.deserialize(source);
      record = new HoodieFlinkInternalRow(
          recordKey.toString(),
          partition.toString(),
          fileId.toString(),
          instantTime.toString(),
          operationType.toString(),
          isIndexRecord,
          rowData);
    } else {
      record = new HoodieFlinkInternalRow(
          recordKey.toString(),
          partition.toString(),
          fileId.toString(),
          instantTime.toString());
    }
    return record;
  }

  @Override
  public HoodieFlinkInternalRow deserialize(HoodieFlinkInternalRow reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    boolean isIndexRecord = source.readBoolean();
    target.writeBoolean(isIndexRecord);
    stringDataSerializer.copy(source, target);
    stringDataSerializer.copy(source, target);
    stringDataSerializer.copy(source, target);
    stringDataSerializer.copy(source, target);
    stringDataSerializer.copy(source, target);
    if (!isIndexRecord) {
      rowDataSerializer.copy(source, target);
    }
  }

  @Override
  public boolean equals(Object obj) {}

  @Override
  public int hashCode() {}

  @Override
  public TypeSerializerSnapshot<HoodieFlinkInternalRow> snapshotConfiguration() {
    throw new UnsupportedOperationException();
  }
}
```

### Potential problems

1. We will switch to use of `RowDataKeyGen`, which allows to generate keys directly on `RowData`, and avoid excessive unnecessary conversions.
   All main cases of already implemented other key generators are supported by `RowDataKeyGen`.
2. Payloads creation is hardly coupled with Avro `GenericRecord`. 
   Similar to the previous point, there would be undesirable intermediate conversion into Avro.
   It is a huge work, and should be done under a separate RFC. 

## Rollout/Adoption Plan

- What impact (if any) will there be on existing users?
  - Better performance of Flink write into Hudi for main scenarios. 
    Total write time [decreased up to 25%](https://github.com/apache/hudi/pull/12796).
- If we are changing behavior how will we phase out the older behavior?
  - It is an internal processing, and there is no plan to rollback older behavior without reverting changes.
- If we need special migration tools, describe them here.
  - For such kind of changes, there is no need in special migration tools.
- When will we remove the existing behavior
  - Previous behavior will be removed with proposed changes. 

## Test Plan

This RFC will be tested by running of TPC-H benchmark. 
Existing set of test cases will allow to check that nothing is broken due to change of default behavior.
