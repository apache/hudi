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

import org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;

import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Hoodie Write Support for directly writing {@link RowData} to Parquet.
 */
public class HoodieRowDataParquetWriteSupport extends RowDataParquetWriteSupport {

  private final Map<String, String> footerMetadata = new HashMap<>();
  private final Option<HoodieBloomFilterWriteSupport<String>> bloomFilterWriteSupportOpt;

  public HoodieRowDataParquetWriteSupport(Configuration conf, HoodieSchema schema, BloomFilter bloomFilter) {
    super(schema, conf);
    this.bloomFilterWriteSupportOpt = Option.ofNullable(bloomFilter)
        .map(HoodieBloomFilterRowDataWriteSupport::new);
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  @Override
  public WriteSupport.FinalizedWriteContext finalizeWrite() {
    Map<String, String> extraMetadata =
        bloomFilterWriteSupportOpt.map(HoodieBloomFilterWriteSupport::finalizeMetadata)
            .orElse(Collections.emptyMap());
    String vectorColumnsMetadata = HoodieSchema.buildVectorColumnsMetadataValue(schema);
    if (!vectorColumnsMetadata.isEmpty()) {
      extraMetadata = new HashMap<>(extraMetadata);
      extraMetadata.put(HoodieSchema.VECTOR_COLUMNS_METADATA_KEY, vectorColumnsMetadata);
    }
    if (!footerMetadata.isEmpty()) {
      extraMetadata = new HashMap<>(extraMetadata);
      extraMetadata.putAll(footerMetadata);
    }

    return new WriteSupport.FinalizedWriteContext(extraMetadata);
  }

  public void addFooterMetadata(Map<String, String> footerMetadata) {
    this.footerMetadata.putAll(footerMetadata);
  }

  public void add(String recordKey) {
    this.bloomFilterWriteSupportOpt.ifPresent(bloomFilterWriteSupport ->
        bloomFilterWriteSupport.addKey(recordKey));
  }
}
