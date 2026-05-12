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

package org.apache.hudi.common.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.config.HoodieParquetConfig.PARQUET_COMPRESS_ZSTD_LEVEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class TestHoodieParquetConfig {

  @Test
  void applyZstdLevelStampsClonedConfWhenCodecIsZstd() {
    Configuration original = new Configuration(false);
    Configuration result = HoodieParquetConfig.applyZstdLevel(original, CompressionCodecName.ZSTD, 10);

    assertNotSame(original, result, "applyZstdLevel must not mutate the shared conf");
    assertEquals(10, result.getInt(PARQUET_COMPRESS_ZSTD_LEVEL, -1));
    assertNull(original.get(PARQUET_COMPRESS_ZSTD_LEVEL), "original conf must be untouched");
  }

  @Test
  void applyZstdLevelReturnsOriginalForNonZstdCodec() {
    Configuration original = new Configuration(false);
    Configuration result = HoodieParquetConfig.applyZstdLevel(original, CompressionCodecName.GZIP, 10);

    assertSame(original, result);
    assertNull(original.get(PARQUET_COMPRESS_ZSTD_LEVEL),
        "non-ZSTD codec must not stamp the zstd level on the conf");
  }

  @Test
  void applyZstdLevelOverridesAnyPreexistingValue() {
    Configuration original = new Configuration(false);
    original.setInt(PARQUET_COMPRESS_ZSTD_LEVEL, 7);
    Configuration result = HoodieParquetConfig.applyZstdLevel(original, CompressionCodecName.ZSTD, 22);

    assertEquals(22, result.getInt(PARQUET_COMPRESS_ZSTD_LEVEL, -1));
    assertEquals(7, original.getInt(PARQUET_COMPRESS_ZSTD_LEVEL, -1),
        "original conf must be untouched even when it already had the key");
  }
}
