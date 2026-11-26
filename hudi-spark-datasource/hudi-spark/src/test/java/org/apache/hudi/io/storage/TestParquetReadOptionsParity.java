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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.testutils.FooterKeyDecryptionFactory;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.HoodieAvroParquetReaderBuilder;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests {@link ParquetUtils#withHadoopReadOptions} against the parquet version on the classpath. It lives in
 * this module, rather than next to {@link ParquetUtils}, because the parquet version differs per Spark
 * profile and CI runs this module's tests under each one.
 */
class TestParquetReadOptionsParity {

  @TempDir
  java.nio.file.Path tempDir;

  /**
   * A reader built with {@link ParquetUtils#withHadoopReadOptions} must get the read options parquet builds
   * from the configuration and the file path, field by field, and read the same rows as a reader built from
   * the path with {@code withConf}, both with the read options left at their defaults and with them set.
   */
  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void testWithHadoopReadOptionsMatchesPathBasedReader(boolean encrypted, boolean setReadOptions) throws Exception {
    Configuration conf = new Configuration();
    Path path = new Path(tempDir.resolve("options.parquet").toUri());
    FooterKeyDecryptionFactory.writeFile(conf, path, 10, encrypted);
    if (encrypted) {
      conf.set(FooterKeyDecryptionFactory.CRYPTO_FACTORY_CLASS, FooterKeyDecryptionFactory.class.getName());
    }
    if (setReadOptions) {
      // Non-default values, so options that ignore the configuration differ
      conf.setBoolean(ParquetInputFormat.STATS_FILTERING_ENABLED, false);
      conf.setBoolean(ParquetInputFormat.DICTIONARY_FILTERING_ENABLED, false);
      conf.setBoolean(ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED, false);
      conf.setBoolean(ParquetInputFormat.BLOOM_FILTERING_ENABLED, false);
      conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, true);
      conf.setInt("parquet.read.allocation.size", 4 * 1024 * 1024);
      ParquetInputFormat.setFilterPredicate(conf, FilterApi.eq(FilterApi.longColumn("ts"), 5L));
    }

    HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);
    ParquetReadOptions expectedOptions = HadoopReadOptions.builder(conf, inputFile.getPath()).build();
    FooterKeyDecryptionFactory.drainRequestedPaths();
    try (ParquetReader<IndexedRecord> reader = ParquetUtils.withHadoopReadOptions(
        new HoodieAvroParquetReaderBuilder<IndexedRecord>(inputFile), inputFile).build()) {
      assertEquals(encrypted ? Collections.singletonList(inputFile.getPath()) : Collections.emptyList(),
          FooterKeyDecryptionFactory.drainRequestedPaths());
      Field options = ParquetReader.class.getDeclaredField("options");
      options.setAccessible(true);
      assertSameFields(expectedOptions, options.get(reader), "options");

      List<String> keys = readKeys(reader);
      assertEquals(setReadOptions ? Collections.singletonList("key5")
          : IntStream.range(0, 10).mapToObj(i -> "key" + i).collect(Collectors.toList()), keys);
      try (ParquetReader<IndexedRecord> pathReader = new HoodieAvroParquetReaderBuilder<IndexedRecord>(
          new StoragePath(path.toUri())).withConf(conf).build()) {
        assertEquals(readKeys(pathReader), keys);
      }
    }
  }

  private static void assertSameFields(Object expected, Object actual, String name) throws ReflectiveOperationException {
    assertEquals(expected.getClass(), actual.getClass(), name);
    for (Class<?> clazz = expected.getClass(); clazz != Object.class; clazz = clazz.getSuperclass()) {
      for (Field field : clazz.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers())) {
          continue;
        }
        field.setAccessible(true);
        Object expectedValue = field.get(expected);
        Object actualValue = field.get(actual);
        String fieldName = name + "." + field.getName();
        if (expectedValue == null || actualValue == null || expectedValue instanceof Configuration) {
          assertSame(expectedValue, actualValue, fieldName);
        } else if (expectedValue instanceof byte[]) {
          assertArrayEquals((byte[]) expectedValue, (byte[]) actualValue, fieldName);
        } else if (expectedValue instanceof FileDecryptionProperties) {
          assertSameFields(expectedValue, actualValue, fieldName);
        } else if (expectedValue instanceof FilterCompat.FilterPredicateCompat) {
          assertEquals(((FilterCompat.FilterPredicateCompat) expectedValue).getFilterPredicate(),
              ((FilterCompat.FilterPredicateCompat) actualValue).getFilterPredicate(), fieldName);
        } else if (expectedValue.getClass().getMethod("equals", Object.class).getDeclaringClass() != Object.class) {
          assertEquals(expectedValue, actualValue, fieldName);
        } else {
          // Created per build without value equality (codec factory, allocator, configuration wrapper)
          assertEquals(expectedValue.getClass(), actualValue.getClass(), fieldName);
        }
      }
    }
  }

  private static List<String> readKeys(ParquetReader<IndexedRecord> reader) throws IOException {
    List<String> keys = new ArrayList<>();
    for (IndexedRecord record = reader.read(); record != null; record = reader.read()) {
      keys.add(((GenericRecord) record).get("_row_key").toString());
    }
    return keys;
  }
}
