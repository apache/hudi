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

package org.apache.hudi.utilities.unstructured.storage;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.sources.blob.BlobTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.LogCapture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The inline threshold of the blob table ({@code hoodie.blob.table.inline.max.bytes}) on both Parquet capabilities: without
 * the per-column statistics switch the default is 10 KiB and a larger configured value is capped at 80 KiB; with it the
 * configured value or the 1 MiB default is used as is.
 */
public class TestParquetInlineCap {

  private static final String BLOB_KEY = "hoodie.blob.table.inline.max.bytes";
  private static final String[] KEYS = {BLOB_KEY};

  /** The threshold {@code key}'s reader resolves from {@code props} on a Parquet with or without the statistics switch. */
  private static long resolve(String key, TypedProperties props, boolean statisticsSwitch) {
    switch (key) {
      case BLOB_KEY:
        return BlobTableConfig.inlineMaxBytes(props, statisticsSwitch);
      default:
        throw new IllegalArgumentException(key);
    }
  }

  /** Key, configured value (null: unset), statistics switch, effective threshold. */
  private static Stream<Arguments> thresholds() {
    Stream.Builder<Arguments> cases = Stream.builder();
    for (String key : KEYS) {
      cases.add(Arguments.of(key, null, false, 10240L));
      cases.add(Arguments.of(key, "0", false, 0L));
      cases.add(Arguments.of(key, "10240", false, 10240L));
      cases.add(Arguments.of(key, "81919", false, 81919L));
      cases.add(Arguments.of(key, "81920", false, 81920L));
      cases.add(Arguments.of(key, "81921", false, 81920L));
      cases.add(Arguments.of(key, "1048576", false, 81920L));
      cases.add(Arguments.of(key, null, true, 1048576L));
      cases.add(Arguments.of(key, "81919", true, 81919L));
      cases.add(Arguments.of(key, "81920", true, 81920L));
      cases.add(Arguments.of(key, "81921", true, 81921L));
      cases.add(Arguments.of(key, "4194304", true, 4194304L));
    }
    return cases.build();
  }

  @ParameterizedTest
  @MethodSource("thresholds")
  public void effectiveThresholdOnBothParquetCapabilities(String key, String configured, boolean statisticsSwitch, long expected) {
    TypedProperties props = new TypedProperties();
    if (configured != null) {
      props.setProperty(key, configured);
    }
    assertEquals(expected, resolve(key, props, statisticsSwitch));
  }

  /** A value outside [0, 2^31-9] is refused before the cap, on both Parquet capabilities. */
  @ParameterizedTest
  @MethodSource("outOfRange")
  public void outOfRangeValueIsRefusedOnBothCapabilities(String key, String configured, boolean statisticsSwitch) {
    TypedProperties props = new TypedProperties();
    props.setProperty(key, configured);
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> resolve(key, props, statisticsSwitch));
    assertTrue(refused.getMessage().contains(key), refused.getMessage());
  }

  private static Stream<Arguments> outOfRange() {
    Stream.Builder<Arguments> cases = Stream.builder();
    for (String key : KEYS) {
      for (String configured : new String[] {"-1", "3000000000"}) {
        cases.add(Arguments.of(key, configured, false));
        cases.add(Arguments.of(key, configured, true));
      }
    }
    return cases.build();
  }

  @ParameterizedTest
  @ValueSource(strings = {BLOB_KEY})
  public void warningNamesTheKeyTheValueAndTheCap(String key) {
    assertEquals(key + "=81921 is above 81920 bytes, the largest inline threshold on a Parquet without per-column statistics "
        + "(before 1.14, e.g. the Spark 3.5 builds): 81920 is used", ParquetInlineCap.warning(key, 81921L));
  }

  /** The WARN is logged once per resolution above the cap, and only without the switch. */
  @ParameterizedTest
  @ValueSource(strings = {BLOB_KEY})
  public void warnIsLoggedOnlyAboveTheCapWithoutTheSwitch(String key) {
    try (LogCapture logs = LogCapture.of(ParquetInlineCap.class)) {
      for (String configured : new String[] {"81920", "81921"}) {
        for (boolean statisticsSwitch : new boolean[] {true, false}) {
          TypedProperties props = new TypedProperties();
          props.setProperty(key, configured);
          resolve(key, props, statisticsSwitch);
        }
      }
      assertEquals(Collections.singletonList("WARN " + ParquetInlineCap.warning(key, 81921L)), logs.warnings());
    }
  }

  @Test
  public void defaultAndCapAreBinaryKibibytes() {
    assertEquals(10240L, ParquetInlineCap.DEFAULT_BYTES);
    assertEquals(81920L, ParquetInlineCap.MAX_BYTES);
  }
}
