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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.common.config.TypedProperties;

import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The configured inline threshold through {@link BlobTableConfig#inlineMaxBytes}: kept as set, and capped on a Parquet without
 * per-column statistics.
 */
class TestBlobLegacyParquetCap {

  @Test
  void smallConfiguredThresholdRemainsSixteenBytes() {
    TypedProperties props = new TypedProperties();
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), "16");
    assertEquals(16L, BlobTableConfig.inlineMaxBytes(props));
  }

  @Test
  void legacyParquetCapsConfiguredHundredThousandBytesAtEightyKibibytes() {
    boolean statisticsSwitch = hasColumnStatisticsSwitch();
    System.out.println("PARQUET_COLUMN_STATISTICS_SWITCH=" + statisticsSwitch);
    TypedProperties props = new TypedProperties();
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), "100000");
    assertEquals(statisticsSwitch ? 100000L : 81920L, BlobTableConfig.inlineMaxBytes(props));
  }

  /** Independent runtime probe: expected byte counts never come from a production helper. */
  private static boolean hasColumnStatisticsSwitch() {
    try {
      ParquetWriter.Builder.class.getMethod("withStatisticsEnabled", String.class, boolean.class);
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }
}
