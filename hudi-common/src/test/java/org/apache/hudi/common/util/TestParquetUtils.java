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
import org.apache.hudi.common.BloomFilter;
import org.apache.hudi.common.HoodieCommonTestHarness;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTestUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests parquet utils.
 */
public class TestParquetUtils extends HoodieCommonTestHarness {

  @Before
  public void setup() {
    initPath();
  }

  @Test
  public void testHoodieWriteSupport() throws Exception {
    List<String> rowKeys = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      rowKeys.add(UUID.randomUUID().toString());
    }

    String filePath = basePath + "/test.parquet";
    writeParquetFile(filePath, rowKeys);

    // Read and verify
    List<String> rowKeysInFile = new ArrayList<>(
        ParquetUtils.readRowKeysFromParquet(HoodieTestUtils.getDefaultHadoopConf(), new Path(filePath)));
    Collections.sort(rowKeysInFile);
    Collections.sort(rowKeys);

    assertEquals("Did not read back the expected list of keys", rowKeys, rowKeysInFile);
    BloomFilter filterInFile =
        ParquetUtils.readBloomFilterFromParquetMetadata(HoodieTestUtils.getDefaultHadoopConf(), new Path(filePath));
    for (String rowKey : rowKeys) {
      assertTrue("key should be found in bloom filter", filterInFile.mightContain(rowKey));
    }
  }

  @Test
  public void testFilterParquetRowKeys() throws Exception {
    List<String> rowKeys = new ArrayList<>();
    Set<String> filter = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      String rowKey = UUID.randomUUID().toString();
      rowKeys.add(rowKey);
      if (i % 100 == 0) {
        filter.add(rowKey);
      }
    }

    String filePath = basePath + "/test.parquet";
    writeParquetFile(filePath, rowKeys);

    // Read and verify
    Set<String> filtered =
        ParquetUtils.filterParquetRowKeys(HoodieTestUtils.getDefaultHadoopConf(), new Path(filePath), filter);

    assertEquals("Filtered count does not match", filter.size(), filtered.size());

    for (String rowKey : filtered) {
      assertTrue("filtered key must be in the given filter", filter.contains(rowKey));
    }
  }

  private void writeParquetFile(String filePath, List<String> rowKeys) throws Exception {
    // Write out a parquet file
    Schema schema = HoodieAvroUtils.getRecordKeySchema();
    BloomFilter filter = new BloomFilter(1000, 0.0001);
    HoodieAvroWriteSupport writeSupport =
        new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema), schema, filter);
    ParquetWriter writer = new ParquetWriter(new Path(filePath), writeSupport, CompressionCodecName.GZIP,
        120 * 1024 * 1024, ParquetWriter.DEFAULT_PAGE_SIZE);
    for (String rowKey : rowKeys) {
      GenericRecord rec = new GenericData.Record(schema);
      rec.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, rowKey);
      writer.write(rec);
      filter.add(rowKey);
    }
    writer.close();
  }
}
