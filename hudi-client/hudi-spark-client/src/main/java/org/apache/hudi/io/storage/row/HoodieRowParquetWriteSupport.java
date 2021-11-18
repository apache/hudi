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

package org.apache.hudi.io.storage.row;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.HoodieDynamicBoundedBloomFilter;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.HashMap;

import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER;

/**
 * Hoodie Write Support for directly writing Row to Parquet.
 */
public class HoodieRowParquetWriteSupport extends ParquetWriteSupport {

  private final Configuration hadoopConf;
  private final BloomFilter bloomFilter;

  private UTF8String minRecordKey;
  private UTF8String maxRecordKey;

  public HoodieRowParquetWriteSupport(Configuration conf, StructType structType, Option<BloomFilter> bloomFilterOpt, HoodieWriteConfig writeConfig) {
    Configuration hadoopConf = new Configuration(conf);
    hadoopConf.set("spark.sql.parquet.writeLegacyFormat", writeConfig.parquetWriteLegacyFormatEnabled());
    hadoopConf.set("spark.sql.parquet.outputTimestampType", writeConfig.parquetOutputTimestampType());
    hadoopConf.set("spark.sql.parquet.fieldId.write.enabled", writeConfig.parquetFieldIdWriteEnabled());
    this.hadoopConf = hadoopConf;
    setSchema(structType, hadoopConf);
    this.bloomFilter = bloomFilterOpt.orElse(null);
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  @Override
  public WriteSupport.FinalizedWriteContext finalizeWrite() {
    HashMap<String, String> extraMetaData = new HashMap<>();
    if (bloomFilter != null) {
      extraMetaData.put(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY, bloomFilter.serializeToString());
      if (minRecordKey != null && maxRecordKey != null) {
        extraMetaData.put(HOODIE_MIN_RECORD_KEY_FOOTER, minRecordKey.toString());
        extraMetaData.put(HOODIE_MAX_RECORD_KEY_FOOTER, maxRecordKey.toString());
      }
      if (bloomFilter.getBloomFilterTypeCode().name().contains(HoodieDynamicBoundedBloomFilter.TYPE_CODE_PREFIX)) {
        extraMetaData.put(HOODIE_BLOOM_FILTER_TYPE_CODE, bloomFilter.getBloomFilterTypeCode().name());
      }
    }
    return new WriteSupport.FinalizedWriteContext(extraMetaData);
  }

  public void add(UTF8String recordKey) {
    if (recordKey == null) {
      return;
    }
    this.bloomFilter.add(recordKey.getBytes());

    if (minRecordKey == null || minRecordKey.compareTo(recordKey) < 0) {
      // NOTE: [[clone]] is performed here (rather than [[copy]]) to only copy underlying buffer in
      //       cases when [[UTF8String]] is pointing into a buffer storing the whole containing record,
      //       and simply do a pass over when it holds a (immutable) buffer holding just the string
      minRecordKey = recordKey.clone();
    }
  }
}
