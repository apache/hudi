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
import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.datasketch.DataSketchWriteSupport;

import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Collections;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_FIELD_ID_WRITE_ENABLED;

/**
 * Hoodie Write Support for directly writing Row to Parquet.
 */
public class HoodieRowParquetWriteSupport extends ParquetWriteSupport {

  private final Configuration hadoopConf;
  private final Option<HoodieBloomFilterWriteSupport<UTF8String>> bloomFilterWriteSupportOpt;
  private final Option<HoodieDataSketchRowWriteSupport> dataSketchRowWriteSupportOpt;

  public HoodieRowParquetWriteSupport(Configuration conf, StructType structType, Option<BloomFilter> bloomFilterOpt, HoodieStorageConfig config, boolean dataSketchEnable) {
    Configuration hadoopConf = new Configuration(conf);
    hadoopConf.set("spark.sql.parquet.writeLegacyFormat", config.getString(HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED));
    hadoopConf.set("spark.sql.parquet.outputTimestampType", config.getString(HoodieStorageConfig.PARQUET_OUTPUT_TIMESTAMP_TYPE));
    hadoopConf.set("spark.sql.parquet.fieldId.write.enabled", config.getString(PARQUET_FIELD_ID_WRITE_ENABLED));
    setSchema(structType, hadoopConf);

    this.hadoopConf = hadoopConf;
    this.bloomFilterWriteSupportOpt = bloomFilterOpt.map(HoodieBloomFilterRowWriteSupport::new);
    this.dataSketchRowWriteSupportOpt = dataSketchEnable ? Option.of(new HoodieDataSketchRowWriteSupport()) : Option.empty();
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  @Override
  public WriteSupport.FinalizedWriteContext finalizeWrite() {
    Map<String, String> extraMetadata =
        bloomFilterWriteSupportOpt.map(HoodieBloomFilterWriteSupport::finalizeMetadata)
            .orElse(Collections.emptyMap());

    extraMetadata = CollectionUtils.combine(extraMetadata,
        dataSketchRowWriteSupportOpt.map(DataSketchWriteSupport::finalizeMetadata)
            .orElse(Collections.emptyMap()));
    return new WriteSupport.FinalizedWriteContext(extraMetadata);
  }

  public void add(UTF8String recordKey) {
    this.bloomFilterWriteSupportOpt.ifPresent(bloomFilterWriteSupport ->
        bloomFilterWriteSupport.addKey(recordKey));
    this.dataSketchRowWriteSupportOpt.ifPresent(sketch -> sketch.addKey(recordKey));
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

  private static class HoodieDataSketchRowWriteSupport extends DataSketchWriteSupport<UTF8String> {
    @Override
    protected String getUTF8String(UTF8String key) {
      return key.toString();
    }

    @Override
    protected UTF8String dereference(UTF8String key) {
      // NOTE: [[clone]] is performed here (rather than [[copy]]) to only copy underlying buffer in
      //       cases when [[UTF8String]] is pointing into a buffer storing the whole containing record,
      //       and simply do a pass over when it holds a (immutable) buffer holding just the string
      return key.clone();
    }
  }

}
