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

# RFC-87: Avro elimination in Flink writer

## Proposers

- @alowator

## Approvers

- @danny0405
- @cshuo

## Status: Claim

Umbrella ticket: [HUDI-9075](https://issues.apache.org/jira/browse/HUDI-9075)

## Abstract

Building on RFC-84, which removed Avro from Flink’s pre-write operators, RFC-87 eliminates Avro from the write path to improve performance.
Current writes suffer from excessive Avro serialization/deserialization and in-memory storage of List<HoodieRecord>, causing high GC overhead.
This RFC replaces DataBucket’s list storage with Flink’s BinaryInMemorySortBuffer, enabling efficient sorting and iterator-based writes.
HoodieLogBlock is also refactored to separate deserialization from buffering.
Precombine field deduplication will now occur after sorting.

By removing Avro, this RFC reduces GC pressure, minimizes serialization overhead, as a result it improves Flink write performance in Hudi.

## Background

Currently, Flink writes in Hudi rely heavily on Avro for serialization, deserialization, and in-memory storage, particularly in DataBucket and AppendHandle.
This approach introduces unnecessary GC pressure, frequent avroToBytes/bytesToAvro conversions, and overall performance bottlenecks.

With RFC-84, Avro was eliminated from Flink’s pre-write operators. However, the write path still suffers from excessive Avro-based processing, leading to inefficiencies.
StreamWriteFunction now receives HoodieFlinkInternalRow, which already encapsulates Flink’s native RowData, making it possible to remove Avro entirely from the write phase.

## Implementation

The keys ideas of Flink native implementation is considered in rework of DataBucket and Flink write handles.
This implementation doesn't touch neither bulk_insert nor append mode and works for all index types.
It changes only StreamWriteFunction logic.

### DataBucket

To reduce GC pressure and eliminate Avro usage DataBucket could store RowData records in BinaryInMemorySortBuffer.
Instead of getRecords() DataBucket could provide getIterator() method, which will be passed to Flink write handles.
MutableObjectIterator is Flink's @Internal class, so it's better to create Hudi internal Iterator which will incapsulate MutableObjectIterator's logic.  

```Java
protected static class DataBucket {
  private final BinaryInMemorySortBuffer sortBuffer;
  private final BufferSizeDetector detector;

  private DataBucket(Double batchSize, RowType rowType) {
    this.sortBuffer = SortBufferFactory.createInMemorySortBuffer(rowType);
    this.detector = new BufferSizeDetector(batchSize);
  }
  
  public MutableObjectIterator<BinaryRowData> getIterator() {
    return this.sortBuffer.getIterator();
  }
  
  public boolean isEmpty() {
    return sortBuffer.isEmpty();
  }
  
  public void reset() {
    this.sortBuffer.reset();
    this.detector.reset();
  }

  public void add(HoodieFlinkInternalRow record) throws IOException {
    this.sortBuffer.write(record.getRowDataWithMetadata());
  }
}
```

BinaryInMemorySortBuffer could ve provided by SortBufferFactory implementation.

```Java
class SortBufferFactory {

  private static final int DEFAULT_PAGE_SIZE = 65536;

  static BinaryInMemorySortBuffer createInMemorySortBuffer(RowType rowType) {
    NormalizedKeyComputer normalizedKeyComputer = new NormalizedRecordKeyComputer();
    RecordComparator keyComparator = new NormalizedRecordKeyComputer();

    RowDataSerializer serializer = new RowDataSerializer(rowType);
    BinaryRowDataSerializer binarySerializer = new BinaryRowDataSerializer(rowType.getFieldCount());

    MemorySegmentPool unlimitedMemoryPool = new UnlimitedHeapMemorySegmentPool(DEFAULT_PAGE_SIZE);

    return BinaryInMemorySortBuffer.createBuffer(normalizedKeyComputer, serializer, binarySerializer,
        keyComparator, unlimitedMemoryPool);
  }
}

```

## NormalizedRecordKeyComputer

There is no requirements to generate NormalizedRecordKeyComputer, because _hoodie_record_key could be used always.

```Java
/**
 * Computes normalized keys for records, used in sorting operations.
 * It uses {@link FastRecordKeyComparator} for comparing and transforming record keys
 * into normalized byte representations.
 */
public class NormalizedRecordKeyComputer implements NormalizedKeyComputer, RecordComparator {

  private final FastRecordKeyComparator comparator = new FastRecordKeyComparator();

  /** Number of bytes allocated for the normalized key. */
  private static final int NUM_KEY_BYTES = 8;

  private static final int RECORD_KEY_POS = HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD.ordinal();

  @Override
  public void putKey(RowData record, MemorySegment target, int offset) {
    if (record.isNullAt(RECORD_KEY_POS)) {
      SortUtil.minNormalizedKey(target, offset, NUM_KEY_BYTES);
    } else {
      byte[] hoodieKeyBytes = comparator.getRecordKeyBytes(record.getString(RECORD_KEY_POS).toString());
      int limit = offset + NUM_KEY_BYTES;

      int i;
      for (i = 0; i < hoodieKeyBytes.length && offset < limit; ++i) {
        target.put(offset++, hoodieKeyBytes[i]);
      }
      for (i = offset; i < limit; ++i) {
        target.put(i, (byte) 0);
      }
    }
  }

  @Override
  public int compareKey(MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ) {
    byte[] a = new byte[NUM_KEY_BYTES];
    byte[] b = new byte[NUM_KEY_BYTES];
    segI.get(offsetI, a, 0, NUM_KEY_BYTES);
    segJ.get(offsetJ, b, 0, NUM_KEY_BYTES);
    return comparator.compareBytesWithoutColumnName(a, b);
  }

  /**
   * This method implements RecordComparator logic due to use common {@link FastRecordKeyComparator}
   * for both serialized and deserialized representations of record key.
   */
  @Override
  public int compare(RowData a, RowData b) {
    return comparator.compare(
        a.getString(RECORD_KEY_POS).toString(),
        b.getString(RECORD_KEY_POS).toString());
  }

  @Override
  public void swapKey(MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ) {
    byte[] temp = new byte[NUM_KEY_BYTES];
    segI.swapBytes(temp, segJ, offsetI, offsetJ, NUM_KEY_BYTES);
  }

  @Override
  public int getNumKeyBytes() {
    return NUM_KEY_BYTES;
  }

  @Override
  public boolean isKeyFullyDetermines() {
    return false;
  }

  @Override
  public boolean invertKey() {
    return false;
  }
}
```

Comparing record using _hoodie_record_key has a problem, for ComplexKeyGenerator _hoodie_record_key starts with column name.
It makes BinaryInMemorySortBuffer usage inefficient, because BinaryInMemorySortBuffer builds index with first 8 bytes from key to make sorting faster (avoid records serialization/deserialization). 
Here is possible to use something like FastRecordKeyComparator to trim column name from key values.

```Java
/**
 * Comparator for record keys used in sorting operations before writing and during data compaction.
 * Utilizes {@link SignedBytes#lexicographicalComparator()} for high-performance lexicographical comparison.
 *
 * <p>Writer uses this class for sorting data before writing, while the Compaction service uses it
 * for determining the order of records.
 */
public class FastRecordKeyComparator implements Comparator<String> {
  Comparator<byte[]> comparator = SignedBytes.lexicographicalComparator();

  /**
   * Compares two string representing full record keys.
   *
   * @return Negative if a < b, positive if a > b, 0 if equal.
   */
  @Override
  public int compare(String recordKeyA, String recordKeyB) {
    return compareBytesWithoutColumnName(
        getRecordKeyBytes(recordKeyA),
        getRecordKeyBytes(recordKeyB));
  }

  /**
   * Compares two byte arrays representing record keys without column names and delimiters.
   * 
   * <p>These byte arrays should be a result of {@link #getRecordKeyBytes}
   *
   * @return Negative if a < b, positive if a > b, 0 if equal.
   */
  public int compareBytesWithoutColumnName(byte[] a, byte[] b) {
    return comparator.compare(a, b);
  }

  /**
   * Converts a record key into a byte array by extracting only values (without column name)
   * and encoding them in UTF-8.
   *
   * @return Byte array representing record key without column names and delimiters, these byte arrays
   * should only be compared using {@link #compareBytesWithoutColumnName(byte[], byte[])} method.  
   */
  public byte[] getRecordKeyBytes(String recordKey) {
    return String.join("", KeyGenUtils.extractRecordKeys(recordKey))
        .getBytes(StandardCharsets.UTF_8);
  }
}
```

### FlinkAppendHandle / FlinkCreateHandle

The key idea here is to use Iterator provided by DataBucket instead of List<HoodieRecrod>.

For FlinkAppendHandle, flushToDiskIfRequired looks redundant here, because storage and size calculation could be provided by DataBucket.
write.batch.size and hoodie.logfile.data.block.max.size properties conflicts here.
We could support division batch in a few log blocks.
This question is open.

### HoodieRowDataFileWriter

To generate parquet log blocks content from RowData here is possible to reuse HoodieRowDataParquetWriter.
Problem here is HoodieRowDataParquetWriter should support writing not only in files provided by StoragePath, it also could support writing to OutputStream for FlinkAppendHandle log blocks generation.
Then LogBlock should be created using this content and then be sent to appendBlocks().
In general, HoodieRowDataFileWriterFactory should use more abstract entity than File, it also should support writing into in-memory logblock content.

Here is also possible to support not only parquet format.

```Java
/**
 * Factory to assist in instantiating a new {@link RowDataWriter}.
 */
public class RowDataWriterFactory {

  /**
   * Factory method to assist in instantiating an instance of {@link RowDataWriter}.
   *
   * @param outputStream  data output stream written in provided format.
   * @param format        log block format of data written to outputStream.
   * @param hoodieTable   instance of {@link HoodieTable} in use.
   * @param config        instance of {@link HoodieWriteConfig} to use.
   * @param schema        schema of the dataset in use.
   * @return the instantiated {@link RowDataWriter}.
   * @throws IOException if format is not supported or if any exception during instantiating the RowFileWriter.
   */
  public static RowDataWriter getLogBlockRowDataWriter(OutputStream outputStream,
                                                       HoodieLogBlock.HoodieLogBlockType format,
                                                       HoodieTable hoodieTable,
                                                       HoodieWriteConfig config,
                                                       RowType schema) throws IOException {
    switch (format) {
      case PARQUET_DATA_BLOCK:
        return newParquetInternalRowFileWriter(outputStream, config, schema, hoodieTable);
      default:
        throw new UnsupportedOperationException(format + " format for writing RowData not supported yet. " 
            + "Only " + PARQUET.getFileExtensionWithoutDot() + " What impact (if any) will there be on existing users?is supported.");
    }
  }

  private static RowDataParquetWriter newParquetInternalRowFileWriter(
      OutputStream outputStream, HoodieWriteConfig writeConfig, RowType rowType, HoodieTable table)
      throws IOException {
    OutputFile file = new OutputStreamBackedOutputFile(new FSDataOutputStream(outputStream, null));
    BloomFilter filter = BloomFilterFactory.createBloomFilter(
        writeConfig.getBloomFilterNumEntries(),
        writeConfig.getBloomFilterFPP(),
        writeConfig.getDynamicBloomFilterMaxNumEntries(),
        writeConfig.getBloomFilterType());
    HoodieRowDataParquetWriteSupport writeSupport =
        new HoodieRowDataParquetWriteSupport((Configuration) table.getStorageConf().unwrap(), rowType, filter, writeConfig.isWriteUtcTimezone());
    return new RowDataParquetWriter(
        file, new HoodieParquetConfig<>(
        writeSupport,
        getCompressionCodecName(writeConfig.getParquetCompressionCodec()),
        writeConfig.getParquetBlockSize(),
        writeConfig.getParquetPageSize(),
        writeConfig.getParquetMaxFileSize(),
        new HadoopStorageConfiguration(writeSupport.getHadoopConf()),
        writeConfig.getParquetCompressionRatio(),
        writeConfig.parquetDictionaryEnabled()));
  }
}
```

### Metadata population

Metadata population could be performed before writing into sort buffer and could be performed before writing to log block content.
Mandatory requirement here is to generate hoodie record key before writing record to sort buffer.
The way generating metadata before inserting into sort buffer is HoodieFlinkInternalRow's method:

```Java
public RowData getRowDataWithMetadata(...) {
  return HoodieRowDataCreation.create(
      commitTime,
      commitSeqNo,
      recordKey.toString(),
      partitionPath.toString(),
      fileName,
      rowData,
      withOperation,
      false);
}
```

Problem here is generating metadata filed now is WriteHandle's responsibility. This question os open.

## Rollout/Adoption Plan

- Default log block format will be switched to parquet for existing tables.
- Result table should be read by existing readers. 

## Test Plan

This RFC will be tested by running of TPC-H benchmark's Lineitem table.
Current POC version shows 4x times write performance improvement.
Existing set of test cases will allow to check that nothing is broken due to change of default behavior.
