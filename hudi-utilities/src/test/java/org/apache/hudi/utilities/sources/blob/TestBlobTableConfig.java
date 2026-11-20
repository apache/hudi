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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBlobTableConfig {

  @Test
  public void defaultsSelectPdfAndOneMiBInline() {
    TypedProperties props = new TypedProperties();
    assertEquals(hasColumnStatisticsSwitch() ? 1048576L : 10240L, BlobTableConfig.inlineMaxBytes(props));
    assertTrue(BlobTableConfig.fileExtensions(props).contains("pdf"));
  }

  @Test
  public void configuredThresholdAndSelectorsAreUsed() {
    TypedProperties props = new TypedProperties();
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), "4096");
    props.setProperty(BlobTableConfig.FILE_EXTENSIONS.key(), " pdf, , PDF ");
    assertEquals(4096L, BlobTableConfig.inlineMaxBytes(props));
    assertEquals(Arrays.asList("pdf", "PDF"), BlobTableConfig.fileExtensions(props));
  }

  @Test
  public void largestByteArrayThresholdIsAccepted() {
    TypedProperties props = new TypedProperties();
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), "2147483639");
    assertEquals(2147483639L, BlobTableConfig.inlineMaxBytes(props, true));
    assertEquals(81920L, BlobTableConfig.inlineMaxBytes(props, false));
  }

  @ParameterizedTest
  @ValueSource(longs = {2147483640L, Integer.MAX_VALUE, 4294967296L, Long.MAX_VALUE})
  public void thresholdBeyondTheLargestByteArrayIsRefused(long threshold) {
    TypedProperties props = new TypedProperties();
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), String.valueOf(threshold));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> BlobTableConfig.inlineMaxBytes(props));
    org.junit.jupiter.api.Assertions.assertTrue(e.getMessage().contains("hoodie.blob.table.inline.max.bytes"));
  }

  private static boolean hasColumnStatisticsSwitch() {
    try {
      org.apache.parquet.hadoop.ParquetWriter.Builder.class.getMethod("withStatisticsEnabled", String.class, boolean.class);
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }
}
