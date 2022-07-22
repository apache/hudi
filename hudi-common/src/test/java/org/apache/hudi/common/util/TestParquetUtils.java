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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.keygen.BaseKeyGenerator;

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

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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
    List<String> rowKeysInFile = new ArrayList<>(
        parquetUtils.readRowKeys(HoodieTestUtils.getDefaultHadoopConf(), new Path(filePath)));
    Collections.sort(rowKeysInFile);
    Collections.sort(rowKeys);

    assertEquals(rowKeys, rowKeysInFile, "Did not read back the expected list of keys");
    BloomFilter filterInFile =
        parquetUtils.readBloomFilterFromMetadata(HoodieTestUtils.getDefaultHadoopConf(), new Path(filePath));
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
        parquetUtils.filterRowKeys(HoodieTestUtils.getDefaultHadoopConf(), new Path(filePath), filter);

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
        parquetUtils.fetchHoodieKeys(HoodieTestUtils.getDefaultHadoopConf(), new Path(filePath));
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
        parquetUtils.fetchHoodieKeys(HoodieTestUtils.getDefaultHadoopConf(), new Path(filePath),
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

    assertEquals(123, parquetUtils.getRowCount(HoodieTestUtils.getDefaultHadoopConf(), new Path(filePath)));
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
        new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema), schema, Option.of(filter));
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
