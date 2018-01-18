/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.uber.hoodie.avro.HoodieAvroWriteSupport;
import com.uber.hoodie.common.BloomFilter;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTestUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestParquetUtils {


  private String basePath;

  @Before
  public void setup() throws IOException {
    // Create a temp folder as the base path
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    basePath = folder.getRoot().getAbsolutePath();
  }

  @Test
  public void testHoodieWriteSupport() throws Exception {

    List<String> rowKeys = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      rowKeys.add(UUID.randomUUID().toString());
    }

    // Write out a parquet file
    Schema schema = HoodieAvroUtils.getRecordKeySchema();
    BloomFilter filter = new BloomFilter(1000, 0.0001);
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
        new AvroSchemaConverter().convert(schema), schema, filter);

    String filePath = basePath + "/test.parquet";
    ParquetWriter writer = new ParquetWriter(new Path(filePath),
        writeSupport, CompressionCodecName.GZIP, 120 * 1024 * 1024,
        ParquetWriter.DEFAULT_PAGE_SIZE);
    for (String rowKey : rowKeys) {
      GenericRecord rec = new GenericData.Record(schema);
      rec.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, rowKey);
      writer.write(rec);
      filter.add(rowKey);
    }
    writer.close();

    // Read and verify
    List<String> rowKeysInFile = new ArrayList<>(
        ParquetUtils
            .readRowKeysFromParquet(HoodieTestUtils.getDefaultHadoopConf(), new Path(filePath)));
    Collections.sort(rowKeysInFile);
    Collections.sort(rowKeys);

    assertEquals("Did not read back the expected list of keys", rowKeys, rowKeysInFile);
    BloomFilter filterInFile = ParquetUtils
        .readBloomFilterFromParquetMetadata(HoodieTestUtils.getDefaultHadoopConf(),
            new Path(filePath));
    for (String rowKey : rowKeys) {
      assertTrue("key should be found in bloom filter", filterInFile.mightContain(rowKey));
    }
  }
}
