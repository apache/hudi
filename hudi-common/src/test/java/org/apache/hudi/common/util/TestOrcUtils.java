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

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.io.storage.HoodieAvroOrcWriter;
import org.apache.hudi.io.storage.HoodieOrcConfig;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.mockito.Mockito;

import java.util.List;
import java.util.function.Supplier;

import static org.mockito.Mockito.when;

/**
 * Tests ORC utils.
 */
public class TestOrcUtils extends TestBaseFileUtilsBase {
  @Override
  public void initBaseFileUtils() {
    baseFileUtils = new OrcUtils();
    fileName = "test.orc";
  }

  @Override
  protected void writeFileForTesting(String typeCode, String filePath, List<String> rowKeys,
                                     Schema schema, boolean addPartitionPathField,
                                     String partitionPathValue, boolean useMetaFields,
                                     String recordFieldName, String partitionFieldName)
      throws Exception {
    // Write out a ORC file
    BloomFilter filter = BloomFilterFactory
        .createBloomFilter(1000, 0.0001, 10000, typeCode);
    Configuration conf = new Configuration();
    int orcStripSize = Integer.parseInt(HoodieStorageConfig.ORC_STRIPE_SIZE.defaultValue());
    int orcBlockSize = Integer.parseInt(HoodieStorageConfig.ORC_BLOCK_SIZE.defaultValue());
    int maxFileSize = Integer.parseInt(HoodieStorageConfig.ORC_FILE_MAX_SIZE.defaultValue());
    HoodieOrcConfig config = new HoodieOrcConfig(
        conf, CompressionKind.ZLIB, orcStripSize, orcBlockSize, maxFileSize, filter);
    TaskContextSupplier mockTaskContextSupplier = Mockito.mock(TaskContextSupplier.class);
    Supplier<Integer> partitionSupplier = Mockito.mock(Supplier.class);
    when(mockTaskContextSupplier.getPartitionIdSupplier()).thenReturn(partitionSupplier);
    when(partitionSupplier.get()).thenReturn(10);
    String instantTime = "000";
    try (HoodieAvroOrcWriter writer = new HoodieAvroOrcWriter(
        instantTime, new Path(filePath), config, schema, mockTaskContextSupplier)) {
      for (String rowKey : rowKeys) {
        GenericRecord rec = new GenericData.Record(schema);
        rec.put(useMetaFields ? HoodieRecord.RECORD_KEY_METADATA_FIELD : recordFieldName, rowKey);
        if (addPartitionPathField) {
          rec.put(useMetaFields ? HoodieRecord.PARTITION_PATH_METADATA_FIELD : partitionFieldName,
              partitionPathValue);
        }
        writer.writeAvro(rowKey, rec);
        filter.add(rowKey);
      }
    }
  }
}
