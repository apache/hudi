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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests parquet utils.
 */
public class TestParquetUtils {

  private ParquetUtils parquetUtils;
  private String testFilePath;
  private Configuration hadoopConf;

  public static List<Arguments> bloomFilterTypeCodes() {
    return Arrays.asList(
        Arguments.of(BloomFilterTypeCode.SIMPLE.name()),
        Arguments.of(BloomFilterTypeCode.DYNAMIC_V0.name())
    );
  }

  @BeforeEach
  public void setup(@TempDir java.nio.file.Path tempDir) {
    parquetUtils = new ParquetUtils();
    testFilePath = tempDir.resolve("test.parquet").toUri().toString();
    hadoopConf = new Configuration();
  }

  @ParameterizedTest
  @MethodSource("bloomFilterTypeCodes")
  public void testHoodieWriteSupport(String typeCode) throws Exception {
    List<String> rowKeys = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      rowKeys.add(UUID.randomUUID().toString());
    }

    writeParquetFile(typeCode, testFilePath, rowKeys);

    // Read and verify
    List<String> rowKeysInFile = new ArrayList<>(
        parquetUtils.readRowKeys(hadoopConf, new Path(testFilePath)));
    Collections.sort(rowKeysInFile);
    Collections.sort(rowKeys);

    assertEquals(rowKeys, rowKeysInFile, "Did not read back the expected list of keys");
    BloomFilter filterInFile =
        parquetUtils.readBloomFilterFromMetadata(hadoopConf, new Path(testFilePath));
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

    writeParquetFile(typeCode, testFilePath, rowKeys);

    // Read and verify
    Set<String> filtered =
        parquetUtils.filterRowKeys(hadoopConf, new Path(testFilePath), filter);

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

    Schema schema = HoodieAvroUtils.getRecordKeyPartitionPathSchema();
    writeParquetFile(typeCode, testFilePath, rowKeys, schema, true, partitionPath);

    // Read and verify
    List<HoodieKey> fetchedRows =
        parquetUtils.fetchRecordKeyPartitionPath(hadoopConf, new Path(testFilePath));
    assertEquals(rowKeys.size(), fetchedRows.size(), "Total count does not match");

    for (HoodieKey entry : fetchedRows) {
      assertTrue(expected.contains(entry), "Record key must be in the given filter");
    }
  }

  @ParameterizedTest
  @MethodSource("bloomFilterTypeCodes")
  public void testReadCounts(String typeCode) throws Exception {
    List<String> rowKeys = new ArrayList<>();
    for (int i = 0; i < 123; i++) {
      rowKeys.add(UUID.randomUUID().toString());
    }
    writeParquetFile(typeCode, testFilePath, rowKeys);

    assertEquals(123, parquetUtils.getRowCount(hadoopConf, new Path(testFilePath)));
  }

  private void writeParquetFile(String typeCode, String filePath, List<String> rowKeys) throws Exception {
    writeParquetFile(typeCode, filePath, rowKeys, HoodieAvroUtils.getRecordKeySchema(), false, "");
  }

  private void writeParquetFile(String typeCode, String filePath, List<String> rowKeys, Schema schema, boolean addPartitionPathField, String partitionPath) throws Exception {
    // Write out a parquet file
    BloomFilter filter = BloomFilterFactory
        .createBloomFilter(1000, 0.0001, 10000, typeCode);
    HoodieAvroWriteSupport writeSupport =
        new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema), schema, filter);
    ParquetWriter writer = new ParquetWriter(new Path(filePath), writeSupport, CompressionCodecName.GZIP,
        120 * 1024 * 1024, ParquetWriter.DEFAULT_PAGE_SIZE);
    for (String rowKey : rowKeys) {
      GenericRecord rec = new GenericData.Record(schema);
      rec.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, rowKey);
      if (addPartitionPathField) {
        rec.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, partitionPath);
      }
      writer.write(rec);
      writeSupport.add(rowKey);
    }
    writer.close();
  }
}
