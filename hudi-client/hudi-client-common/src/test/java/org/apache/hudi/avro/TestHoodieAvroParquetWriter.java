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
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.DummyTaskContextSupplier;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.io.storage.HoodieAvroParquetWriter;
import org.apache.hudi.io.storage.HoodieParquetConfig;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieAvroParquetWriter {

  @TempDir java.nio.file.Path tmpDir;

  @Test
  public void testReadingAndWriting() throws IOException {
    Configuration hadoopConf = new Configuration();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEED);
    List<GenericRecord> records = dataGen.generateGenericRecords(10);

    Schema schema = records.get(0).getSchema();

    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.0001, 10000,
        BloomFilterTypeCode.DYNAMIC_V0.name());
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema),
        schema, Option.of(filter));

    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig =
        new HoodieParquetConfig(writeSupport, CompressionCodecName.GZIP, ParquetWriter.DEFAULT_BLOCK_SIZE,
            ParquetWriter.DEFAULT_PAGE_SIZE, 1024 * 1024 * 1024, hadoopConf, 0.1);

    Path filePath = new Path(tmpDir.resolve("test.parquet").toAbsolutePath().toString());

    try (HoodieAvroParquetWriter<GenericRecord> writer =
        new HoodieAvroParquetWriter<>(filePath, parquetConfig, "001", new DummyTaskContextSupplier(), true)) {
      for (GenericRecord record : records) {
        writer.writeAvro((String) record.get("_row_key"), record);
      }
    }

    ParquetUtils utils = new ParquetUtils();

    // Step 1: Make sure records are written appropriately
    List<GenericRecord> readRecords = utils.readAvroRecords(hadoopConf, filePath);

    assertEquals(toJson(records), toJson(readRecords));

    // Step 2: Assert Parquet metadata was written appropriately
    List<String> recordKeys = records.stream().map(r -> (String) r.get("_row_key")).collect(Collectors.toList());

    String minKey = recordKeys.stream().min(Comparator.naturalOrder()).get();
    String maxKey = recordKeys.stream().max(Comparator.naturalOrder()).get();

    FileMetaData parquetMetadata = ParquetUtils.readMetadata(hadoopConf, filePath).getFileMetaData();

    Map<String, String> extraMetadata = parquetMetadata.getKeyValueMetaData();

    assertEquals(extraMetadata.get(HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER), minKey);
    assertEquals(extraMetadata.get(HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER), maxKey);
    assertEquals(extraMetadata.get(HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE), BloomFilterTypeCode.DYNAMIC_V0.name());

    // Step 3: Make sure Bloom Filter contains all the record keys
    BloomFilter bloomFilter = utils.readBloomFilterFromMetadata(hadoopConf, filePath);
    recordKeys.forEach(recordKey -> {
      assertTrue(bloomFilter.mightContain(recordKey));
    });
  }

  private static List<String> toJson(List<GenericRecord> records) {
    return records.stream().map(r -> {
      try {
        return new String(HoodieAvroUtils.avroToJson(r, true));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }
}
