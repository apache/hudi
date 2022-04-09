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

package org.apache.hudi.avro;

import org.apache.avro.Schema;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.HoodieDynamicBoundedBloomFilter;
import org.apache.hudi.common.util.Option;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.Map;

/**
 * Wrap AvroWriterSupport for plugging in the bloom filter.
 */
public class HoodieAvroWriteSupport extends AvroWriteSupport {

  private Option<BloomFilter> bloomFilterOpt;
  private String minRecordKey;
  private String maxRecordKey;
  private Map<String, String> footerMetadata = new HashMap<>();

  public static final String OLD_HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY = "com.uber.hoodie.bloomfilter";
  public static final String HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY = "org.apache.hudi.bloomfilter";
  public static final String HOODIE_MIN_RECORD_KEY_FOOTER = "hoodie_min_record_key";
  public static final String HOODIE_MAX_RECORD_KEY_FOOTER = "hoodie_max_record_key";
  public static final String HOODIE_BLOOM_FILTER_TYPE_CODE = "hoodie_bloom_filter_type_code";

  public HoodieAvroWriteSupport(MessageType schema, Schema avroSchema, Option<BloomFilter> bloomFilterOpt) {
    super(schema, avroSchema, ConvertingGenericData.INSTANCE);
    this.bloomFilterOpt = bloomFilterOpt;
  }

  @Override
  public WriteSupport.FinalizedWriteContext finalizeWrite() {
    if (bloomFilterOpt.isPresent()) {
      footerMetadata.put(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY, bloomFilterOpt.get().serializeToString());
      if (minRecordKey != null && maxRecordKey != null) {
        footerMetadata.put(HOODIE_MIN_RECORD_KEY_FOOTER, minRecordKey);
        footerMetadata.put(HOODIE_MAX_RECORD_KEY_FOOTER, maxRecordKey);
      }
      if (bloomFilterOpt.get().getBloomFilterTypeCode().name().contains(HoodieDynamicBoundedBloomFilter.TYPE_CODE_PREFIX)) {
        footerMetadata.put(HOODIE_BLOOM_FILTER_TYPE_CODE, bloomFilterOpt.get().getBloomFilterTypeCode().name());
      }
    }
    return new WriteSupport.FinalizedWriteContext(footerMetadata);
  }

  public void add(String recordKey) {
    if (bloomFilterOpt.isPresent()) {
      this.bloomFilterOpt.get().add(recordKey);
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

  public void addFooterMetadata(String key, String value) {
    footerMetadata.put(key, value);
  }
}
