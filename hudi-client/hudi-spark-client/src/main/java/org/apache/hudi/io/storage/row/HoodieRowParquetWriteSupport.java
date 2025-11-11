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

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;

import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_FIELD_ID_WRITE_ENABLED;

/**
 * Hoodie Write Support for directly writing Row to Parquet.
 */
public class HoodieRowParquetWriteSupport extends ParquetWriteSupport {

  private final Configuration hadoopConf;
  private final Option<HoodieBloomFilterWriteSupport<UTF8String>> bloomFilterWriteSupportOpt;

  public HoodieRowParquetWriteSupport(Configuration conf, StructType structType, Option<BloomFilter> bloomFilterOpt, HoodieConfig config) {
    Configuration hadoopConf = new Configuration(conf);
    hadoopConf.set("spark.sql.parquet.writeLegacyFormat", config.getStringOrDefault(HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED));
    hadoopConf.set("spark.sql.parquet.outputTimestampType", config.getStringOrDefault(HoodieStorageConfig.PARQUET_OUTPUT_TIMESTAMP_TYPE));
    hadoopConf.set("spark.sql.parquet.fieldId.write.enabled", config.getStringOrDefault(PARQUET_FIELD_ID_WRITE_ENABLED));
    setSchema(structType, hadoopConf);

    this.hadoopConf = hadoopConf;
    this.bloomFilterWriteSupportOpt = bloomFilterOpt.map(HoodieBloomFilterRowWriteSupport::new);
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  @Override
  public WriteSupport.FinalizedWriteContext finalizeWrite() {
    Map<String, String> extraMetadata =
        bloomFilterWriteSupportOpt.map(HoodieBloomFilterWriteSupport::finalizeMetadata)
            .orElse(Collections.emptyMap());

    return new WriteSupport.FinalizedWriteContext(extraMetadata);
  }

  public void add(UTF8String recordKey) {
    this.bloomFilterWriteSupportOpt.ifPresent(bloomFilterWriteSupport ->
        bloomFilterWriteSupport.addKey(recordKey));
  }

  private static class HoodieBloomFilterRowWriteSupport extends HoodieBloomFilterWriteSupport<UTF8String> {
    public HoodieBloomFilterRowWriteSupport(BloomFilter bloomFilter) {
      super(bloomFilter);
    }

    @Override
    protected byte[] getUTF8Bytes(UTF8String key) {
      return key.getBytes();
    }

    @Override
    protected UTF8String dereference(UTF8String key) {
      // NOTE: [[clone]] is performed here (rather than [[copy]]) to only copy underlying buffer in
      //       cases when [[UTF8String]] is pointing into a buffer storing the whole containing record,
      //       and simply do a pass over when it holds a (immutable) buffer holding just the string
      return key.clone();
    }
  }

  public static HoodieRowParquetWriteSupport getHoodieRowParquetWriteSupport(Configuration conf, StructType structType,
                                                                             Option<BloomFilter> bloomFilterOpt, HoodieConfig config) {
    return (HoodieRowParquetWriteSupport) ReflectionUtils.loadClass(
        config.getStringOrDefault(HoodieStorageConfig.HOODIE_PARQUET_SPARK_ROW_WRITE_SUPPORT_CLASS),
        new Class<?>[] {Configuration.class, StructType.class, Option.class, HoodieConfig.class},
        conf, structType, bloomFilterOpt, config);
  }
}
