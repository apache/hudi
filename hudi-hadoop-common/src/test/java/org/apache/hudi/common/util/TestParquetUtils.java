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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
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

import static org.apache.hudi.avro.AvroSchemaUtils.createNullableSchema;
import static org.apache.hudi.avro.HoodieAvroUtils.METADATA_FIELD_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    Set<String> filtered =
        parquetUtils.filterRowKeys(HoodieTestUtils.getStorage(filePath), new StoragePath(filePath), filter);

    assertEquals(filter.size(), filtered.size(), "Filtered count does not match");

    for (String rowKey : filtered) {
      assertTrue(filter.contains(rowKey), "filtered key must be in the given filter");
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
    Schema schema = HoodieAvroUtils.getRecordKeyPartitionPathSchema();
    writeParquetFile(typeCode, filePath, rowKeys, schema, true, partitionPath);

    // Read and verify
    List<HoodieKey> fetchedRows =
        parquetUtils.fetchHoodieKeys(HoodieTestUtils.getStorage(filePath), new StoragePath(filePath));
    assertEquals(rowKeys.size(), fetchedRows.size(), "Total count does not match");

    for (HoodieKey entry : fetchedRows) {
      assertTrue(expected.contains(entry), "Record key must be in the given filter");
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
    Schema schema = getSchemaWithFields(Arrays.asList(new String[]{"abc", "def"}));
    writeParquetFile(BloomFilterTypeCode.SIMPLE.name(), filePath, rowKeys, schema, true, partitionPath,
        false, "abc", "def");

    // Read and verify
    List<HoodieKey> fetchedRows =
        parquetUtils.fetchHoodieKeys(HoodieTestUtils.getStorage(filePath), new StoragePath(filePath),
            Option.of(new TestBaseKeyGen("abc","def")));
    assertEquals(rowKeys.size(), fetchedRows.size(), "Total count does not match");

    for (HoodieKey entry : fetchedRows) {
      assertTrue(expected.contains(entry), "Record key must be in the given filter");
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
    Schema schema = getSchema(recordKeyField, partitionPathField, dataField);

    BloomFilter filter = BloomFilterFactory
        .createBloomFilter(1000, 0.0001, 10000, BloomFilterTypeCode.SIMPLE.name());
    HoodieAvroWriteSupport writeSupport =
        new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema), schema, Option.of(filter), new Properties());
    try (ParquetWriter writer = new ParquetWriter(new Path(filePath), writeSupport, CompressionCodecName.GZIP,
        120 * 1024 * 1024, ParquetWriter.DEFAULT_PAGE_SIZE)) {
      valueList.forEach(entry -> {
        GenericRecord rec = new GenericData.Record(schema);
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
            HoodieTestUtils.getStorage(filePath), new StoragePath(filePath), columnList)
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

  private Schema getSchema(String recordKeyField, String partitionPathField, String dataField) {
    List<Schema.Field> toBeAddedFields = new ArrayList<>();
    Schema recordSchema = Schema.createRecord("HoodieRecord", "", "", false);

    Schema.Field recordKeySchemaField =
        new Schema.Field(recordKeyField, createNullableSchema(Schema.Type.STRING), "", JsonProperties.NULL_VALUE);
    Schema.Field partitionPathSchemaField =
        new Schema.Field(partitionPathField, createNullableSchema(Schema.Type.STRING), "", JsonProperties.NULL_VALUE);
    Schema.Field dataSchemaField =
        new Schema.Field(dataField, createNullableSchema(Schema.Type.STRING), "", JsonProperties.NULL_VALUE);

    toBeAddedFields.add(recordKeySchemaField);
    toBeAddedFields.add(partitionPathSchemaField);
    toBeAddedFields.add(dataSchemaField);
    recordSchema.setFields(toBeAddedFields);
    return recordSchema;
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
    writeParquetFile(typeCode, filePath, rowKeys, HoodieAvroUtils.getRecordKeySchema(), false, "");
  }

  private void writeParquetFile(String typeCode, String filePath, List<String> rowKeys, Schema schema, boolean addPartitionPathField, String partitionPath) throws Exception {
    writeParquetFile(typeCode, filePath, rowKeys, schema, addPartitionPathField, partitionPath,
        true, null, null);
  }

  private void writeParquetFile(String typeCode, String filePath, List<String> rowKeys, Schema schema, boolean addPartitionPathField, String partitionPathValue,
                                boolean useMetaFields, String recordFieldName, String partitionFieldName) throws Exception {
    // Write out a parquet file
    BloomFilter filter = BloomFilterFactory
        .createBloomFilter(1000, 0.0001, 10000, typeCode);
    HoodieAvroWriteSupport writeSupport =
        new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema), schema, Option.of(filter), new Properties());
    ParquetWriter writer = new ParquetWriter(new Path(filePath), writeSupport, CompressionCodecName.GZIP,
        120 * 1024 * 1024, ParquetWriter.DEFAULT_PAGE_SIZE);
    for (String rowKey : rowKeys) {
      GenericRecord rec = new GenericData.Record(schema);
      rec.put(useMetaFields ? HoodieRecord.RECORD_KEY_METADATA_FIELD : recordFieldName, rowKey);
      if (addPartitionPathField) {
        rec.put(useMetaFields ? HoodieRecord.PARTITION_PATH_METADATA_FIELD : partitionFieldName, partitionPathValue);
      }
      writer.write(rec);
      writeSupport.add(rowKey);
    }
    writer.close();
  }

  private static Schema getSchemaWithFields(List<String> fields) {
    List<Schema.Field> toBeAddedFields = new ArrayList<>();
    Schema recordSchema = Schema.createRecord("HoodieRecordKey", "", "", false);

    for (String field: fields) {
      Schema.Field schemaField =
          new Schema.Field(field, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
      toBeAddedFields.add(schemaField);
    }
    recordSchema.setFields(toBeAddedFields);
    return recordSchema;
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
}
