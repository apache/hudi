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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;

import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER;

/**
 * Hoodie Write Support for directly writing Row to Parquet.
 */
public class HoodieRowParquetWriteSupport extends ParquetWriteSupport {

  private Configuration hadoopConf;
  private BloomFilter bloomFilter;
  private String minRecordKey;
  private String maxRecordKey;

  public HoodieRowParquetWriteSupport(Configuration conf, StructType structType, BloomFilter bloomFilter, HoodieWriteConfig writeConfig) {
    super();
    Configuration hadoopConf = new Configuration(conf);

    //Sets spark.sql.parquet.writeLegacyFormat. If true, data will be written in a way of Spark 1.4 and earlier.
    //For example, decimal values will be written in Parquet's fixed-length byte array format which other systems such as Apache Hive and Apache Impala use.
    //If false, the newer format in Parquet will be used. For example, decimals will be written in int-based format.
    String legacyFormatEnabled = conf.get("spark.sql.parquet.writeLegacyFormat");

    //spark.sql.parquet.outputTimestampType. Parquet timestamp type to use when Spark writes data to Parquet files. Default, TIMESTAMP_MICROS
    String timestampType = conf.get("spark.sql.parquet.outputTimestampType");

    if (StringUtils.isNullOrEmpty(legacyFormatEnabled)) {
      hadoopConf.set("spark.sql.parquet.writeLegacyFormat", "false");
    }

    if (StringUtils.isNullOrEmpty(timestampType)) {
      hadoopConf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS");
    }

    this.hadoopConf = hadoopConf;
    setSchema(structType, hadoopConf);
    this.bloomFilter = bloomFilter;
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
        extraMetaData.put(HOODIE_MIN_RECORD_KEY_FOOTER, minRecordKey);
        extraMetaData.put(HOODIE_MAX_RECORD_KEY_FOOTER, maxRecordKey);
      }
      if (bloomFilter.getBloomFilterTypeCode().name().contains(HoodieDynamicBoundedBloomFilter.TYPE_CODE_PREFIX)) {
        extraMetaData.put(HOODIE_BLOOM_FILTER_TYPE_CODE, bloomFilter.getBloomFilterTypeCode().name());
      }
    }
    return new WriteSupport.FinalizedWriteContext(extraMetaData);
  }

  public void add(String recordKey) {
    this.bloomFilter.add(recordKey);
    if (minRecordKey != null) {
      minRecordKey = minRecordKey.compareTo(recordKey) <= 0 ? minRecordKey : recordKey;
    } else {
      minRecordKey = recordKey;
    }

    if (maxRecordKey != null) {
      maxRecordKey = maxRecordKey.compareTo(recordKey) >= 0 ? maxRecordKey : recordKey;
    } else {
      maxRecordKey = recordKey;
    }
  }
}
