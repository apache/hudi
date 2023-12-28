/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.hudi.avro.HoodieAvroUtils.METADATA_FIELD_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests base file utils.
 */
public abstract class TestBaseFileUtilsBase extends HoodieCommonTestHarness {

  protected BaseFileUtils baseFileUtils;
  protected String fileName;

  /**
   * Initializes the {@link BaseFileUtils} instance.
   */
  public abstract void initBaseFileUtils();

  /**
   * Writes a file in particular format for testing.
   *
   * @param typeCode              bloom filter type code.
   * @param filePath              file path.
   * @param rowKeys               a list of record keys.
   * @param schema                schema of the records.
   * @param addPartitionPathField whether to add partition path field.
   * @param partitionPathValue    partition path value.
   * @param useMetaFields         whether to use meta fields.
   * @param recordFieldName       record key field name.
   * @param partitionFieldName    partition path field name.
   * @throws Exception upon errors.
   */
  protected abstract void writeFileForTesting(String typeCode, String filePath,
                                              List<String> rowKeys, Schema schema,
                                              boolean addPartitionPathField,
                                              String partitionPathValue, boolean useMetaFields,
                                              String recordFieldName,
                                              String partitionFieldName) throws Exception;

  @BeforeEach
  public void setup() {
    initPath();
    initBaseFileUtils();
  }

  public static List<Arguments> bloomFilterTypeCodes() {
    return Arrays.asList(
        Arguments.of(BloomFilterTypeCode.SIMPLE.name()),
        Arguments.of(BloomFilterTypeCode.DYNAMIC_V0.name())
    );
  }

  @ParameterizedTest
  @MethodSource("bloomFilterTypeCodes")
  public void testHoodieWriteSupport(String typeCode) throws Exception {
    List<String> rowKeys = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      rowKeys.add(UUID.randomUUID().toString());
    }

    String filePath = Paths.get(basePath, fileName).toUri().toString();
    writeFileForTesting(typeCode, filePath, rowKeys);

    // Read and verify
    List<String> rowKeysInFile = new ArrayList<>(
        baseFileUtils.readRowKeys(HoodieTestUtils.getDefaultHadoopConf(), new Path(filePath)));
    Collections.sort(rowKeysInFile);
    Collections.sort(rowKeys);

    assertEquals(rowKeys, rowKeysInFile, "Did not read back the expected list of keys");
    BloomFilter filterInFile =
        baseFileUtils.readBloomFilterFromMetadata(HoodieTestUtils.getDefaultHadoopConf(), new Path(filePath));
    for (String rowKey : rowKeys) {
      assertTrue(filterInFile.mightContain(rowKey), "key should be found in bloom filter");
    }
  }

  @ParameterizedTest
  @MethodSource("bloomFilterTypeCodes")
  public void testFilterRowKeys(String typeCode) throws Exception {
    List<String> rowKeys = new ArrayList<>();
    Set<String> filter = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      String rowKey = UUID.randomUUID().toString();
      rowKeys.add(rowKey);
      if (i % 100 == 0) {
        filter.add(rowKey);
      }
    }

    String filePath = Paths.get(basePath, fileName).toUri().toString();
    writeFileForTesting(typeCode, filePath, rowKeys);

    // Read and verify
    Set<Pair<String, Long>> filtered =
        baseFileUtils.filterRowKeys(HoodieTestUtils.getDefaultHadoopConf(), new Path(filePath), filter);

    assertEquals(filter.size(), filtered.size(), "Filtered count does not match");

    for (Pair<String, Long> rowKeyAndPosition : filtered) {
      assertTrue(filter.contains(rowKeyAndPosition.getLeft()), "filtered key must be in the given filter");
    }
  }

  @ParameterizedTest
  @MethodSource("bloomFilterTypeCodes")
  public void testFetchRecordKeyPartitionPath(String typeCode) throws Exception {
    List<String> rowKeys = new ArrayList<>();
    Map<HoodieKey, Long> expectedPositionMap = new HashMap<>();
    String partitionPath = "path1";
    for (int i = 0; i < 1000; i++) {
      String rowKey = UUID.randomUUID().toString();
      rowKeys.add(rowKey);
      expectedPositionMap.put(new HoodieKey(rowKey, partitionPath), (long) i);
    }

    String filePath = Paths.get(basePath, fileName).toUri().toString();
    Schema schema = HoodieAvroUtils.getRecordKeyPartitionPathSchema();
    writeFileForTesting(typeCode, filePath, rowKeys, schema, true, partitionPath);

    // Read and verify
    List<Pair<HoodieKey, Long>> fetchedRows = baseFileUtils.fetchRecordKeysWithPositions(
        HoodieTestUtils.getDefaultHadoopConf(), new Path(filePath));
    assertEquals(rowKeys.size(), fetchedRows.size(), "Total count does not match");

    for (Pair<HoodieKey, Long> entry : fetchedRows) {
      assertTrue(expectedPositionMap.containsKey(entry.getLeft()),
          "Record key must be in the given filter");
      assertEquals(expectedPositionMap.get(entry.getLeft()), entry.getRight(),
          "Row position does not match");
    }
  }

  @Test
  public void testFetchRecordKeyPartitionPathVirtualKeys() throws Exception {
    List<String> rowKeys = new ArrayList<>();
    Map<HoodieKey, Long> expectedPositionMap = new HashMap<>();
    String partitionPath = "path1";
    for (int i = 0; i < 1000; i++) {
      String rowKey = UUID.randomUUID().toString();
      rowKeys.add(rowKey);
      expectedPositionMap.put(new HoodieKey(rowKey, partitionPath), (long) i);
    }

    String filePath = Paths.get(basePath, fileName).toUri().toString();
    Schema schema = getSchemaWithFields(Arrays.asList(new String[] {"abc", "def"}));
    writeFileForTesting(
        BloomFilterTypeCode.SIMPLE.name(), filePath, rowKeys, schema, true,
        partitionPath, false, "abc", "def");

    // Read and verify
    List<Pair<HoodieKey, Long>> fetchedRows =
        baseFileUtils.fetchRecordKeysWithPositions(HoodieTestUtils.getDefaultHadoopConf(),
            new Path(filePath),
            Option.of(new TestParquetUtils.TestBaseKeyGen("abc", "def")));
    assertEquals(rowKeys.size(), fetchedRows.size(), "Total count does not match");

    for (Pair<HoodieKey, Long> entry : fetchedRows) {
      assertTrue(expectedPositionMap.containsKey(entry.getLeft()),
          "Record key must be in the given filter");
      assertEquals(expectedPositionMap.get(entry.getLeft()), entry.getRight(),
          "Row position does not match");
    }
  }

  @Test
  public void testReadCounts() throws Exception {
    String filePath = Paths.get(basePath, "test.parquet").toUri().toString();
    List<String> rowKeys = new ArrayList<>();
    for (int i = 0; i < 123; i++) {
      rowKeys.add(UUID.randomUUID().toString());
    }
    writeFileForTesting(BloomFilterTypeCode.SIMPLE.name(), filePath, rowKeys);

    assertEquals(123,
        baseFileUtils.getRowCount(HoodieTestUtils.getDefaultHadoopConf(), new Path(filePath)));
  }

  private void writeFileForTesting(String typeCode, String filePath, List<String> rowKeys) throws Exception {
    writeFileForTesting(
        typeCode, filePath, rowKeys, HoodieAvroUtils.getRecordKeySchema(), false, "");
  }

  private void writeFileForTesting(String typeCode, String filePath, List<String> rowKeys, Schema schema, boolean addPartitionPathField, String partitionPath) throws Exception {
    writeFileForTesting(
        typeCode, filePath, rowKeys, schema, addPartitionPathField, partitionPath, true, null, null);
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
