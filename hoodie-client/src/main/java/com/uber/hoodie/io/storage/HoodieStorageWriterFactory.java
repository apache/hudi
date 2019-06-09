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

package com.uber.hoodie.io.storage;

import com.uber.hoodie.avro.HoodieAvroWriteSupport;
import com.uber.hoodie.common.BloomFilter;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.table.HoodieTable;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;

public class HoodieStorageWriterFactory {

  public static <T extends HoodieRecordPayload, R extends IndexedRecord> HoodieStorageWriter<R> getStorageWriter(
      String commitTime, Path path, HoodieTable<T> hoodieTable,
      HoodieWriteConfig config, Schema schema) throws IOException {
    //TODO - based on the metadata choose the implementation of HoodieStorageWriter
    // Currently only parquet is supported
    return newParquetStorageWriter(commitTime, path, config, schema, hoodieTable);
  }

  private static <T extends HoodieRecordPayload,
      R extends IndexedRecord> HoodieStorageWriter<R> newParquetStorageWriter(String commitTime, Path path,
      HoodieWriteConfig config, Schema schema, HoodieTable hoodieTable) throws IOException {
    BloomFilter filter = new BloomFilter(config.getBloomFilterNumEntries(),
        config.getBloomFilterFPP());
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
        new AvroSchemaConverter().convert(schema), schema, filter);

    HoodieParquetConfig parquetConfig =
        new HoodieParquetConfig(writeSupport, config.getParquetCompressionCodec(),
            config.getParquetBlockSize(), config.getParquetPageSize(),
            config.getParquetMaxFileSize(), hoodieTable.getHadoopConf(),
            config.getParquetCompressionRatio());

    return new HoodieParquetWriter<>(commitTime, path, parquetConfig, schema);
  }
}
