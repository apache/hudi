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
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.core.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.ByteBufferBackedInputStream;
import org.apache.hudi.io.compress.CompressionCodec;
import org.apache.hudi.io.hfile.HFileReader;
import org.apache.hudi.io.hfile.HFileReaderImpl;
import org.apache.hudi.io.hfile.UTF8StringKey;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_DYNAMIC_MAX_ENTRIES;
import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_FPP_VALUE;
import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_NUM_ENTRIES_VALUE;
import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_TYPE;
import static org.apache.hudi.common.config.HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM_NAME;
import static org.apache.hudi.common.config.HoodieStorageConfig.HFILE_WITH_BLOOM_FILTER_ENABLED;
import static org.apache.hudi.common.util.HFileUtils.getHFileCompressionAlgorithm;
import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HFileUtils}
 */
public class TestHFileUtils {
  private static final String KEY_FIELD_NAME = "key";
  private static final HoodieSchema SCHEMA = HoodieSchema.parse("{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"HFileLogBlockRecord\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"" + KEY_FIELD_NAME + "\", \"type\": \"string\"},\n"
      + "    {\"name\": \"value\", \"type\": \"string\"}\n"
      + "  ]\n"
      + "}");

  @ParameterizedTest
  @EnumSource(CompressionCodec.class)
  public void testGetHFileCompressionAlgorithm(CompressionCodec algo) {
    for (boolean upperCase : new boolean[] {true, false}) {
      Map<String, String> paramsMap = Collections.singletonMap(
          HFILE_COMPRESSION_ALGORITHM_NAME.key(),
          upperCase ? algo.getName().toUpperCase() : algo.getName().toLowerCase());
      assertEquals(algo, getHFileCompressionAlgorithm(paramsMap));
    }
  }

  @Test
  public void testGetHFileCompressionAlgorithmWithEmptyString() {
    assertEquals(CompressionCodec.GZIP, getHFileCompressionAlgorithm(
        Collections.singletonMap(HFILE_COMPRESSION_ALGORITHM_NAME.key(), "")));
  }

  @Test
  public void testGetDefaultHFileCompressionAlgorithm() {
    assertEquals(CompressionCodec.GZIP, getHFileCompressionAlgorithm(Collections.emptyMap()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSerializeRecordsToLogBlockControlsBloomFilterMetadata(boolean hfileBloomFilterEnabled) throws Exception {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put(HFILE_COMPRESSION_ALGORITHM_NAME.key(), HFILE_COMPRESSION_ALGORITHM_NAME.defaultValue());
    paramsMap.put(HFILE_WITH_BLOOM_FILTER_ENABLED.key(), Boolean.toString(hfileBloomFilterEnabled));
    paramsMap.put(BLOOM_FILTER_NUM_ENTRIES_VALUE.key(), BLOOM_FILTER_NUM_ENTRIES_VALUE.defaultValue());
    paramsMap.put(BLOOM_FILTER_FPP_VALUE.key(), BLOOM_FILTER_FPP_VALUE.defaultValue());
    paramsMap.put(BLOOM_FILTER_DYNAMIC_MAX_ENTRIES.key(), BLOOM_FILTER_DYNAMIC_MAX_ENTRIES.defaultValue());
    paramsMap.put(BLOOM_FILTER_TYPE.key(), BLOOM_FILTER_TYPE.defaultValue());

    ByteArrayOutputStream outputStream = new HFileUtils().serializeRecordsToLogBlock(
        null, createRecords(), SCHEMA, SCHEMA, KEY_FIELD_NAME, paramsMap);

    try (HFileReader reader = new HFileReaderImpl(
        new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(outputStream.toByteArray())),
        outputStream.size())) {
      reader.initializeMetadata();

      assertEquals(hfileBloomFilterEnabled,
          reader.getMetaBlock(HoodieAvroHFileReaderImplBase.KEY_BLOOM_FILTER_META_BLOCK).isPresent());
      assertEquals(hfileBloomFilterEnabled,
          reader.getMetaInfo(new UTF8StringKey(HoodieAvroHFileReaderImplBase.KEY_BLOOM_FILTER_TYPE_CODE)).isPresent());
      assertEquals(hfileBloomFilterEnabled,
          reader.getMetaInfo(new UTF8StringKey(HoodieAvroHFileReaderImplBase.KEY_MIN_RECORD)).isPresent());
      assertEquals(hfileBloomFilterEnabled,
          reader.getMetaInfo(new UTF8StringKey(HoodieAvroHFileReaderImplBase.KEY_MAX_RECORD)).isPresent());

      if (hfileBloomFilterEnabled) {
        ByteBuffer bloomFilterBuffer = reader.getMetaBlock(HoodieAvroHFileReaderImplBase.KEY_BLOOM_FILTER_META_BLOCK).get();
        BloomFilter bloomFilter = BloomFilterFactory.fromByteBuffer(
            bloomFilterBuffer,
            fromUTF8Bytes(reader.getMetaInfo(new UTF8StringKey(HoodieAvroHFileReaderImplBase.KEY_BLOOM_FILTER_TYPE_CODE)).get()));

        assertTrue(bloomFilter.mightContain("key00"));
        assertTrue(bloomFilter.mightContain("key02"));
        assertFalse(bloomFilter.mightContain("missing-key"));
      }
    }
  }

  private static List<HoodieRecord> createRecords() {
    List<HoodieRecord> records = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      String key = "key" + String.format("%02d", i);
      GenericRecord record = new GenericData.Record(SCHEMA.toAvroSchema());
      record.put(KEY_FIELD_NAME, key);
      record.put("value", "value" + i);
      records.add(new HoodieAvroIndexedRecord(new HoodieKey(key, ""), record));
    }
    return records;
  }
}
