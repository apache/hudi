/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.avro;

import com.uber.hoodie.common.BloomFilter;

import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;

import java.io.*;
import java.util.HashMap;

/**
 * Wrap AvroWriterSupport for plugging in the bloom filter.
 */
public class HoodieAvroWriteSupport extends AvroWriteSupport {
    private BloomFilter bloomFilter;
    public final static String HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY =
        "com.uber.hoodie.bloomfilter";

    public HoodieAvroWriteSupport(MessageType schema, Schema avroSchema, BloomFilter bloomFilter) {
        super(schema, avroSchema);
        this.bloomFilter = bloomFilter;
    }

    @Override public WriteSupport.FinalizedWriteContext finalizeWrite() {
        HashMap<String, String> extraMetaData = new HashMap<>();
        if (bloomFilter != null) {
            extraMetaData
                .put(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY, bloomFilter.serializeToString());
        }
        return new WriteSupport.FinalizedWriteContext(extraMetaData);
    }

    public void add(String recordKey) {
        this.bloomFilter.add(recordKey);
    }
}
