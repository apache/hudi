/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.schema.HoodieSchemaUtils.METADATA_FIELD_SCHEMA;
import static org.apache.hudi.metadata.HoodieIndexVersion.V1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests parquet utils.
 */
public class TestParquetUtils extends HoodieCommonTestHarness {

  private ParquetUtils parquetUtils;

  public static List<Arguments> bloomFilterTypeCodes() {
    return Arrays.asList(
        Arguments.of(BloomFilterTypeCode.SIMPLE.name()),
        Arguments.of(BloomFilterTypeCode.DYNAMIC_V0.name())
    );
  }

  @BeforeEach
  public void setup() {
    initPath();
    parquetUtils = new ParquetUtils();
  }

  @ParameterizedTest
  @MethodSource("bloomFilterTypeCodes")
  public void testHoodieWriteSupport(String typeCode) throws Exception {
    List<String> rowKeys = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      rowKeys.add(UUID.randomUUID().toString());
    }

    String filePath = Paths.get(basePath, "test.parquet").toUri().toString();
    writeParquetFile(typeCode, filePath, rowKeys);

    // Read and verify
    List<String> rowKeysInFile = new ArrayList<>(parquetUtils.readRowKeys(
        HoodieTestUtils.getStorage(filePath), new StoragePath(filePath)));
    Collections.sort(rowKeysInFile);
    Collections.sort(rowKeys);

    assertEquals(rowKeys, rowKeysInFile, "Did not read back the expected list of keys");
    BloomFilter filterInFile = parquetUtils.readBloomFilterFromMetadata(
        HoodieTestUtils.getStorage(filePath), new StoragePath(filePath));
    for (String rowKey : rowKeys) {
      assertTrue(filterInFile.mightContain(rowKey), "key should be found in bloom filter");
    }
  }

  @ParameterizedTest
  @MethodSource("bloomFilterTypeCodes")
  public void testFilterParquetRowKeys(String typeCode) throws Exception {
    List<String> rowKeys = new ArrayList<>();
    Set<String> filter = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      String rowKey = UUID.randomUUID().toString();
      rowKeys.add(rowKey);
      if (i % 100 == 0) {
        filter.add(rowKey);
      }
    }

    String filePath = Paths.get(basePath, "test.parquet").toUri().toString();
    writeParquetFile(typeCode, filePath, rowKeys);

    // Read and verify
    Set<Pair<String, Long>> filtered = parquetUtils.filterRowKeys(
        HoodieTestUtils.getStorage(filePath), new StoragePath(filePath), filter);

    assertEquals(filter.size(), filtered.size(), "Filtered count does not match");

    for (Pair<String, Long> rowKeyAndPosition : filtered) {
      assertTrue(filter.contains(rowKeyAndPosition.getLeft()), "filtered key must be in the given filter");
    }
  }

  @ParameterizedTest
  @MethodSource("bloomFilterTypeCodes")
  public void testFetchRecordKeyPartitionPathFromParquet(String typeCode) throws Exception {
    List<String> rowKeys = new ArrayList<>();
    List<HoodieKey> expected = new ArrayList<>();
    String partitionPath = "path1";
    for (int i = 0; i < 1000; i++) {
      String rowKey = UUID.randomUUID().toString();
      rowKeys.add(rowKey);
      expected.add(new HoodieKey(rowKey, partitionPath));
    }

    String filePath = Paths.get(basePath, "test.parquet").toUri().toString();
    HoodieSchema schema = HoodieSchemaUtils.getRecordKeyPartitionPathSchema();
    writeParquetFile(typeCode, filePath, rowKeys, schema, true, partitionPath);

    // Read and verify
    List<Pair<HoodieKey, Long>> fetchedRows = new ArrayList<>();
    try (ClosableIterator<Pair<HoodieKey, Long>> iter = parquetUtils.fetchRecordKeysWithPositions(
        HoodieTestUtils.getStorage(filePath), new StoragePath(filePath))) {
      while (iter.hasNext()) {
        fetchedRows.add(iter.next());
      }
    }
    assertEquals(rowKeys.size(), fetchedRows.size(), "Total count does not match");

    for (Pair<HoodieKey, Long> entry : fetchedRows) {
      assertTrue(expected.contains(entry.getLeft()), "Record key must be in the given filter");
    }
  }

  @Test
  public void testFetchRecordKeyPartitionPathVirtualKeysFromParquet() throws Exception {
    List<String> rowKeys = new ArrayList<>();
    List<HoodieKey> expected = new ArrayList<>();
    String partitionPath = "path1";
    for (int i = 0; i < 1000; i++) {
      String rowKey = UUID.randomUUID().toString();
      rowKeys.add(rowKey);
      expected.add(new HoodieKey(rowKey, partitionPath));
    }

    String filePath = Paths.get(basePath, "test.parquet").toUri().toString();
    HoodieSchema schema = getSchemaWithFields(Arrays.asList(new String[] {"abc", "def"}));
    writeParquetFile(BloomFilterTypeCode.SIMPLE.name(), filePath, rowKeys, schema, true, partitionPath,
        false, "abc", "def");

    // Read and verify
    List<Pair<HoodieKey, Long>> fetchedRows = new ArrayList<>();
    try (ClosableIterator<Pair<HoodieKey, Long>> iter = parquetUtils.fetchRecordKeysWithPositions(
        HoodieTestUtils.getStorage(filePath), new StoragePath(filePath),
        Option.of(new TestBaseKeyGen("abc", "def")), Option.empty())) {
      while (iter.hasNext()) {
        fetchedRows.add(iter.next());
      }
    }
    assertEquals(rowKeys.size(), fetchedRows.size(), "Total count does not match");

    for (Pair<HoodieKey, Long> entry : fetchedRows) {
      assertTrue(expected.contains(entry.getLeft()), "Record key must be in the given filter");
    }
  }

  @Test
  public void testReadCounts() throws Exception {
    String filePath = Paths.get(basePath, "test.parquet").toUri().toString();
    List<String> rowKeys = new ArrayList<>();
    for (int i = 0; i < 123; i++) {
      rowKeys.add(UUID.randomUUID().toString());
    }
    writeParquetFile(BloomFilterTypeCode.SIMPLE.name(), filePath, rowKeys);

    assertEquals(123, parquetUtils.getRowCount(
        HoodieTestUtils.getStorage(filePath), new StoragePath(filePath)));
  }

  @Test
  public void testReadColumnStatsFromMetadata() throws Exception {
    List<Pair<Pair<String, String>, Boolean>> valueList = new ArrayList<>();
    String minKey = "z";
    String maxKey = "0";
    String minValue = "z";
    String maxValue = "0";
    int nullValueCount = 0;
    int totalCount = 1000;
    String partitionPath = "path1";
    for (int i = 0; i < totalCount; i++) {
      boolean nullifyData = i % 3 == 0;
      String rowKey = UUID.randomUUID().toString();
      String value = String.valueOf(i);
      valueList.add(Pair.of(Pair.of(rowKey, value), nullifyData));
      minKey = (minKey.compareTo(rowKey) > 0) ? rowKey : minKey;
      maxKey = (maxKey.compareTo(rowKey) < 0) ? rowKey : maxKey;

      if (nullifyData) {
        nullValueCount++;
      } else {
        minValue = (minValue.compareTo(value) > 0) ? value : minValue;
        maxValue = (maxValue.compareTo(value) < 0) ? value : maxValue;
      }
    }

    String fileName = "test.parquet";
    String filePath = new StoragePath(basePath, fileName).toString();
    String recordKeyField = "id";
    String partitionPathField = "partition";
    String dataField = "data";
    HoodieSchema schema = getSchema(recordKeyField, partitionPathField, dataField);

    BloomFilter filter = BloomFilterFactory
        .createBloomFilter(1000, 0.0001, 10000, BloomFilterTypeCode.SIMPLE.name());
    HoodieAvroWriteSupport writeSupport =
        new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema.toAvroSchema()), schema, Option.of(filter), new Properties());
    try (ParquetWriter writer = new ParquetWriter(new Path(filePath), writeSupport, CompressionCodecName.GZIP,
        120 * 1024 * 1024, ParquetWriter.DEFAULT_PAGE_SIZE)) {
      valueList.forEach(entry -> {
        GenericRecord rec = new GenericData.Record(schema.toAvroSchema());
        rec.put(recordKeyField, entry.getLeft().getLeft());
        rec.put(partitionPathField, partitionPath);
        if (entry.getRight()) {
          rec.put(dataField, null);
        } else {
          rec.put(dataField, entry.getLeft().getRight());
        }
        try {
          writer.write(rec);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        writeSupport.add(entry.getLeft().getLeft());
      });
    }

    List<String> columnList = new ArrayList<>();
    columnList.add(recordKeyField);
    columnList.add(partitionPathField);
    columnList.add(dataField);

    List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList = parquetUtils.readColumnStatsFromMetadata(
            HoodieTestUtils.getStorage(filePath), new StoragePath(filePath), columnList, V1)
        .stream()
        .sorted(Comparator.comparing(HoodieColumnRangeMetadata::getColumnName))
        .collect(Collectors.toList());
    assertEquals(3, columnRangeMetadataList.size(), "Should return column stats of 3 columns");
    validateColumnRangeMetadata(columnRangeMetadataList.get(0),
        fileName, dataField, minValue, maxValue, nullValueCount, totalCount);
    validateColumnRangeMetadata(columnRangeMetadataList.get(1),
        fileName, recordKeyField, minKey, maxKey, 0, totalCount);
    validateColumnRangeMetadata(columnRangeMetadataList.get(2),
        fileName, partitionPathField, partitionPath, partitionPath, 0, totalCount);
  }

  /**
   * Tests that for a FLOAT/DOUBLE column containing any NaN, ParquetUtils reports
   * null min/max — because parquet-mr clears its stats (hasNonNullValue=false) but
   * leaves min/max at their default 0.0. Persisting 0.0/0.0 as if they were valid
   * stats was the writer-side half of #18754; downstream data-skipping would then
   * silently prune the file. The fix is gated on Statistics.hasNonNullValue().
   */
  @Test
  public void testReadColumnStatsFromMetadata_nanTaintedDoubleColumn() throws Exception {
    String recordKeyField = "id";
    String partitionPathField = "partition";
    String dataField = "data_double";
    HoodieSchema schema = getSchemaWithDoubleData(recordKeyField, partitionPathField, dataField);

    String fileName = "test_nan.parquet";
    String filePath = new StoragePath(basePath, fileName).toString();
    String partitionPath = "p";

    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.0001, 10000, BloomFilterTypeCode.SIMPLE.name());
    HoodieAvroWriteSupport writeSupport =
        new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema.toAvroSchema()), schema, Option.of(filter), new Properties());
    int totalCount = 11;
    try (ParquetWriter writer = new ParquetWriter(new Path(filePath), writeSupport, CompressionCodecName.GZIP,
        120 * 1024 * 1024, ParquetWriter.DEFAULT_PAGE_SIZE)) {
      for (int i = 0; i < 10; i++) {
        GenericRecord rec = new GenericData.Record(schema.toAvroSchema());
        rec.put(recordKeyField, UUID.randomUUID().toString());
        rec.put(partitionPathField, partitionPath);
        rec.put(dataField, 300.0 + i);
        writer.write(rec);
        writeSupport.add(rec.get(recordKeyField).toString());
      }
      // Trailing NaN row — poisons parquet's stats
      GenericRecord nanRec = new GenericData.Record(schema.toAvroSchema());
      nanRec.put(recordKeyField, UUID.randomUUID().toString());
      nanRec.put(partitionPathField, partitionPath);
      nanRec.put(dataField, Double.NaN);
      writer.write(nanRec);
      writeSupport.add(nanRec.get(recordKeyField).toString());
    }

    List<HoodieColumnRangeMetadata<Comparable>> rangeList = parquetUtils.readColumnStatsFromMetadata(
            HoodieTestUtils.getStorage(filePath), new StoragePath(filePath),
            Collections.singletonList(dataField), V1);
    assertEquals(1, rangeList.size());
    HoodieColumnRangeMetadata<Comparable> data = rangeList.get(0);
    // The pre-fix behavior persisted min=0.0, max=0.0 here. With the fix, we recognize
    // hasNonNullValue=false and report null min/max — the right signal for "stats
    // unreliable, must scan file at query time".
    assertNull(data.getMinValue(), "Expected null min for NaN-tainted FLOAT/DOUBLE column");
    assertNull(data.getMaxValue(), "Expected null max for NaN-tainted FLOAT/DOUBLE column");
    // NaN is counted as a non-null value (parquet treats it that way too).
    assertEquals(0, data.getNullCount());
    assertEquals(totalCount, data.getValueCount());
  }

  /**
   * Tests that when a Parquet column omits stats entirely (one value exceeds
   * parquet-mr's stats truncation threshold, ~1KB for binary/string), ParquetUtils
   * reports nullCount=0 rather than nullCount=valueCount. The pre-fix behavior of
   * "if isEmpty then valueCount else getNumNulls" was correct only for genuinely
   * all-null columns; for truncated stats it incorrectly claimed all values were
   * null, causing data-skipping to silently skip the file. #18755.
   */
  @Test
  public void testReadColumnStatsFromMetadata_statsAbsentForLongValues() throws Exception {
    String recordKeyField = "id";
    String partitionPathField = "partition";
    String dataField = "data";
    HoodieSchema schema = getSchema(recordKeyField, partitionPathField, dataField);

    String fileName = "test_truncation.parquet";
    String filePath = new StoragePath(basePath, fileName).toString();
    String partitionPath = "p";

    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.0001, 10000, BloomFilterTypeCode.SIMPLE.name());
    HoodieAvroWriteSupport writeSupport =
        new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema.toAvroSchema()), schema, Option.of(filter), new Properties());
    // Build a string longer than parquet's default stats truncation threshold (~1KB).
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 4096; i++) {
      sb.append('Y');
    }
    String longValue = sb.toString();

    try (ParquetWriter writer = new ParquetWriter(new Path(filePath), writeSupport, CompressionCodecName.GZIP,
        120 * 1024 * 1024, ParquetWriter.DEFAULT_PAGE_SIZE)) {
      GenericRecord shortRec = new GenericData.Record(schema.toAvroSchema());
      shortRec.put(recordKeyField, UUID.randomUUID().toString());
      shortRec.put(partitionPathField, partitionPath);
      shortRec.put(dataField, "Y");
      writer.write(shortRec);
      writeSupport.add(shortRec.get(recordKeyField).toString());

      GenericRecord longRec = new GenericData.Record(schema.toAvroSchema());
      longRec.put(recordKeyField, UUID.randomUUID().toString());
      longRec.put(partitionPathField, partitionPath);
      longRec.put(dataField, longValue);
      writer.write(longRec);
      writeSupport.add(longRec.get(recordKeyField).toString());
    }

    List<HoodieColumnRangeMetadata<Comparable>> rangeList = parquetUtils.readColumnStatsFromMetadata(
            HoodieTestUtils.getStorage(filePath), new StoragePath(filePath),
            Collections.singletonList(dataField), V1);
    assertEquals(1, rangeList.size());
    HoodieColumnRangeMetadata<Comparable> data = rangeList.get(0);
    // Stats are absent entirely — both min/max null AND nullCount must be 0,
    // not the pre-fix valueCount which signaled "all rows are null".
    assertNull(data.getMinValue(), "Expected null min when parquet stats are absent");
    assertNull(data.getMaxValue(), "Expected null max when parquet stats are absent");
    assertEquals(0, data.getNullCount(),
        "Expected nullCount=0 (unknown) when stats are absent; valueCount would incorrectly imply all-null");
    assertEquals(2, data.getValueCount());
  }

  /**
   * Tests that a genuinely all-null column still produces nullCount=valueCount — the
   * existing convention. This is the case the original "isEmpty ? valueCount" logic was
   * trying to handle; the fix in #18755 distinguishes it from the stats-truly-absent
   * case via Statistics.isNumNullsSet().
   */
  @Test
  public void testReadColumnStatsFromMetadata_allNullColumnRetainsValueCountAsNullCount() throws Exception {
    String recordKeyField = "id";
    String partitionPathField = "partition";
    String dataField = "data";
    HoodieSchema schema = getSchema(recordKeyField, partitionPathField, dataField);

    String fileName = "test_allnull.parquet";
    String filePath = new StoragePath(basePath, fileName).toString();
    String partitionPath = "p";
    int totalCount = 10;

    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.0001, 10000, BloomFilterTypeCode.SIMPLE.name());
    HoodieAvroWriteSupport writeSupport =
        new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema.toAvroSchema()), schema, Option.of(filter), new Properties());
    try (ParquetWriter writer = new ParquetWriter(new Path(filePath), writeSupport, CompressionCodecName.GZIP,
        120 * 1024 * 1024, ParquetWriter.DEFAULT_PAGE_SIZE)) {
      for (int i = 0; i < totalCount; i++) {
        GenericRecord rec = new GenericData.Record(schema.toAvroSchema());
        rec.put(recordKeyField, UUID.randomUUID().toString());
        rec.put(partitionPathField, partitionPath);
        rec.put(dataField, null);
        writer.write(rec);
        writeSupport.add(rec.get(recordKeyField).toString());
      }
    }

    List<HoodieColumnRangeMetadata<Comparable>> rangeList = parquetUtils.readColumnStatsFromMetadata(
            HoodieTestUtils.getStorage(filePath), new StoragePath(filePath),
            Collections.singletonList(dataField), V1);
    assertEquals(1, rangeList.size());
    HoodieColumnRangeMetadata<Comparable> data = rangeList.get(0);
    assertNull(data.getMinValue());
    assertNull(data.getMaxValue());
    // All values null → nullCount must equal valueCount (existing convention).
    assertEquals(totalCount, data.getNullCount());
    assertEquals(totalCount, data.getValueCount());
  }

  private HoodieSchema getSchemaWithDoubleData(String recordKeyField, String partitionPathField, String dataField) {
    List<HoodieSchemaField> toBeAddedFields = new ArrayList<>(3);
    toBeAddedFields.add(HoodieSchemaField.of(recordKeyField,
        HoodieSchema.createNullable(HoodieSchemaType.STRING), "", HoodieSchema.NULL_VALUE));
    toBeAddedFields.add(HoodieSchemaField.of(partitionPathField,
        HoodieSchema.createNullable(HoodieSchemaType.STRING), "", HoodieSchema.NULL_VALUE));
    toBeAddedFields.add(HoodieSchemaField.of(dataField,
        HoodieSchema.createNullable(HoodieSchemaType.DOUBLE), "", HoodieSchema.NULL_VALUE));
    return HoodieSchema.createRecord("HoodieRecord", "", "", false, toBeAddedFields);
  }

  private HoodieSchema getSchema(String recordKeyField, String partitionPathField, String dataField) {
    List<HoodieSchemaField> toBeAddedFields = new ArrayList<>(3);

    HoodieSchemaField recordKeySchemaField =
        HoodieSchemaField.of(recordKeyField, HoodieSchema.createNullable(HoodieSchemaType.STRING), "", HoodieSchema.NULL_VALUE);
    HoodieSchemaField partitionPathSchemaField =
        HoodieSchemaField.of(partitionPathField, HoodieSchema.createNullable(HoodieSchemaType.STRING), "", HoodieSchema.NULL_VALUE);
    HoodieSchemaField dataSchemaField =
        HoodieSchemaField.of(dataField, HoodieSchema.createNullable(HoodieSchemaType.STRING), "", HoodieSchema.NULL_VALUE);

    toBeAddedFields.add(recordKeySchemaField);
    toBeAddedFields.add(partitionPathSchemaField);
    toBeAddedFields.add(dataSchemaField);
    return HoodieSchema.createRecord("HoodieRecord", "", "", false, toBeAddedFields);
  }

  private void validateColumnRangeMetadata(HoodieColumnRangeMetadata metadata,
                                           String filePath,
                                           String columnName,
                                           String minValue,
                                           String maxValue,
                                           long nullCount,
                                           long valueCount) {
    assertEquals(filePath, metadata.getFilePath(), "File path does not match");
    assertEquals(columnName, metadata.getColumnName(), "Column name does not match");
    assertEquals(minValue, metadata.getMinValue(), "Min value does not match");
    assertEquals(maxValue, metadata.getMaxValue(), "Max value does not match");
    assertEquals(nullCount, metadata.getNullCount(), "Null count does not match");
    assertEquals(valueCount, metadata.getValueCount(), "Value count does not match");
  }

  private void writeParquetFile(String typeCode, String filePath, List<String> rowKeys) throws Exception {
    writeParquetFile(typeCode, filePath, rowKeys, HoodieSchemaUtils.getRecordKeySchema(), false, "");
  }

  private void writeParquetFile(String typeCode, String filePath, List<String> rowKeys, HoodieSchema schema, boolean addPartitionPathField, String partitionPath) throws Exception {
    writeParquetFile(typeCode, filePath, rowKeys, schema, addPartitionPathField, partitionPath,
        true, null, null);
  }

  private void writeParquetFile(String typeCode, String filePath, List<String> rowKeys, HoodieSchema schema, boolean addPartitionPathField, String partitionPathValue,
                                boolean useMetaFields, String recordFieldName, String partitionFieldName) throws Exception {
    // Write out a parquet file
    BloomFilter filter = BloomFilterFactory
        .createBloomFilter(1000, 0.0001, 10000, typeCode);
    HoodieAvroWriteSupport writeSupport =
        new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema.toAvroSchema()), schema, Option.of(filter), new Properties());
    ParquetWriter writer = new ParquetWriter(new Path(filePath), writeSupport, CompressionCodecName.GZIP,
        120 * 1024 * 1024, ParquetWriter.DEFAULT_PAGE_SIZE);
    for (String rowKey : rowKeys) {
      GenericRecord rec = new GenericData.Record(schema.toAvroSchema());
      rec.put(useMetaFields ? HoodieRecord.RECORD_KEY_METADATA_FIELD : recordFieldName, rowKey);
      if (addPartitionPathField) {
        rec.put(useMetaFields ? HoodieRecord.PARTITION_PATH_METADATA_FIELD : partitionFieldName, partitionPathValue);
      }
      writer.write(rec);
      writeSupport.add(rowKey);
    }
    writer.close();
  }

  private static HoodieSchema getSchemaWithFields(List<String> fields) {
    List<HoodieSchemaField> toBeAddedFields = new ArrayList<>(fields.size());
    for (String field: fields) {
      HoodieSchemaField schemaField =
          HoodieSchemaField.of(field, METADATA_FIELD_SCHEMA, "", HoodieSchema.NULL_VALUE);
      toBeAddedFields.add(schemaField);
    }
    return HoodieSchema.createRecord("HoodieRecordKey", "", "", false, toBeAddedFields);
  }

  class TestBaseKeyGen extends BaseKeyGenerator {

    private String recordKeyField;
    private String partitionField;

    public TestBaseKeyGen(String recordKeyField, String partitionField) {
      super(new TypedProperties());
      this.recordKeyField = recordKeyField;
      this.partitionField = partitionField;
    }

    @Override
    public String getRecordKey(GenericRecord record) {
      return record.get(recordKeyField).toString();
    }

    @Override
    public String getPartitionPath(GenericRecord record) {
      return record.get(partitionField).toString();
    }

    @Override
    public List<String> getRecordKeyFieldNames() {
      return Arrays.asList(new String[]{recordKeyField});
    }

    @Override
    public List<String> getPartitionPathFields() {
      return Arrays.asList(new String[]{partitionField});
    }
  }

  @Test
  public void testReadMessageTypeHash() throws Exception {
    // Given: Create a parquet file with a specific schema
    List<String> rowKeys = Arrays.asList("row1", "row2", "row3");
    String filePath = Paths.get(basePath, "test_schema_hash.parquet").toUri().toString();
    writeParquetFile(BloomFilterTypeCode.SIMPLE.name(), filePath, rowKeys);
    
    StoragePath storagePath = new StoragePath(filePath);
    
    // When: Reading schema hash
    Integer schemaHash = ParquetUtils.readSchemaHash(HoodieTestUtils.getStorage(filePath), storagePath);
    
    // Then: Should return a valid hash
    assertTrue(schemaHash != null, "Schema hash should not be null");
    assertTrue(schemaHash != 0, "Schema hash should not be zero (default error value)");
    
    // Verify consistency - reading same file should return same hash
    Integer secondRead = ParquetUtils.readSchemaHash(HoodieTestUtils.getStorage(filePath), storagePath);
    assertEquals(schemaHash, secondRead, "Schema hash should be consistent across reads");
  }

  @Test
  public void testReadMessageTypeHash_DifferentSchemas() throws Exception {
    // Given: Create two parquet files with different schemas
    List<String> rowKeys = Arrays.asList("row1", "row2");
    
    // File 1 with original schema
    String filePath1 = Paths.get(basePath, "test_schema1.parquet").toUri().toString();
    writeParquetFile(BloomFilterTypeCode.SIMPLE.name(), filePath1, rowKeys);
    
    // File 2 with extended schema (add a field)
    String filePath2 = Paths.get(basePath, "test_schema2.parquet").toUri().toString();
    writeParquetFileWithExtendedSchema(filePath2, rowKeys);
    
    // When: Reading schema hashes from both files
    Integer hash1 = ParquetUtils.readSchemaHash(HoodieTestUtils.getStorage(filePath1), new StoragePath(filePath1));
    Integer hash2 = ParquetUtils.readSchemaHash(HoodieTestUtils.getStorage(filePath2), new StoragePath(filePath2));
    
    // Then: Should have different hashes for different schemas
    assertTrue(hash1 != null && hash2 != null, "Both schema hashes should be valid");
    assertTrue(!hash1.equals(hash2), "Different schemas should have different hash codes");
  }

  @Test
  public void testReadMessageTypeHash_NonExistentFile() throws Exception {
    // Given: Non-existent file path
    StoragePath nonExistentPath = new StoragePath("/non/existent/file.parquet");
    
    // When: Reading schema hash from non-existent file
    Integer schemaHash = ParquetUtils.readSchemaHash(HoodieTestUtils.getStorage(basePath), nonExistentPath);
    
    // Then: Should return 0 (error default value)
    assertEquals(Integer.valueOf(0), schemaHash, "Non-existent file should return default error value 0");
  }

  @Test
  public void testReadSchemaHash_MatchesDirectSchemaRead() throws Exception {
    // Given: Create a parquet file
    List<String> rowKeys = Arrays.asList("row1", "row2", "row3");
    String filePath = Paths.get(basePath, "test_direct_schema.parquet").toUri().toString();
    writeParquetFile(BloomFilterTypeCode.SIMPLE.name(), filePath, rowKeys);
    
    StoragePath storagePath = new StoragePath(filePath);
    
    // When: Reading schema hash vs direct schema read
    Integer schemaHashFromUtils = ParquetUtils.readSchemaHash(HoodieTestUtils.getStorage(filePath), storagePath);
    MessageType directSchema = parquetUtils.readMessageType(HoodieTestUtils.getStorage(filePath), storagePath);
    Integer directSchemaHash = directSchema.hashCode();
    
    // Then: Hash from utility method should match direct schema hash
    assertEquals(directSchemaHash, schemaHashFromUtils, 
        "Schema hash from utility should match direct schema.hashCode()");
  }

  private void writeParquetFileWithExtendedSchema(String filePath, List<String> rowKeys) throws Exception {
    // Create an extended schema with an additional field
    List<HoodieSchemaField> fields = new ArrayList<>(4);
    fields.add(HoodieSchemaField.of("_row_key", HoodieSchema.create(HoodieSchemaType.STRING), "", (Object) null));
    fields.add(HoodieSchemaField.of("time", HoodieSchema.create(HoodieSchemaType.LONG), "", (Object) null));
    fields.add(HoodieSchemaField.of("number", HoodieSchema.create(HoodieSchemaType.LONG), "", (Object) null));
    fields.add(HoodieSchemaField.of("extra_field", HoodieSchema.createNullable(HoodieSchemaType.STRING), "", HoodieSchema.NULL_VALUE)); // Additional field
    HoodieSchema extendedSchema = HoodieSchema.createRecord("record", "", "", false, fields);

    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.0001, -1, BloomFilterTypeCode.SIMPLE.name());

    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
        new AvroSchemaConverter().convert(extendedSchema.toAvroSchema()), extendedSchema, Option.of(filter), new Properties());

    ParquetWriter writer = new ParquetWriter(new Path(filePath), writeSupport, CompressionCodecName.GZIP,
        120 * 1024 * 1024, ParquetWriter.DEFAULT_PAGE_SIZE);

    for (String rowKey : rowKeys) {
      GenericRecord record = new GenericData.Record(extendedSchema.toAvroSchema());
      record.put("_row_key", rowKey);
      record.put("time", 1234567L);
      record.put("number", 12345L);
      record.put("extra_field", "extra_value"); // Set the extra field
      writer.write(record);
      writeSupport.add(rowKey);
    }
    writer.close();
  }

  @Test
  public void testReadParquetMetadata() throws Exception {
    List<String> rowKeys = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      rowKeys.add(UUID.randomUUID().toString());
    }

    String filePath = Paths.get(basePath, "test.parquet").toUri().toString();
    writeParquetFile(BloomFilterTypeCode.SIMPLE.name(), filePath, rowKeys);

    ParquetMetadata metadata = parquetUtils.readMetadata(HoodieTestUtils.getStorage(filePath), new StoragePath(filePath));
    ParquetMetadata metadataWithSkipRowGroups = parquetUtils.readFileMetadataOnly(HoodieTestUtils.getStorage(filePath), new StoragePath(filePath));
    assertTrue(metadata.getFileMetaData() != null);
    assertTrue(metadataWithSkipRowGroups.getFileMetaData() != null);
    assertFalse(metadata.getBlocks().isEmpty());
    assertTrue(metadataWithSkipRowGroups.getBlocks().isEmpty());
  }
}
