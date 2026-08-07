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

package org.apache.hudi.utils;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.util.CommonClientUtils;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exhaustiveness tests for the per-{@link HoodieFileFormat} dispatch sites on the log write
 * path in {@link CommonClientUtils}.
 *
 * <p>Per-format dispatch is duplicated across the codebase (reader/writer factories in
 * hudi-common/hudi-hadoop-common, log block type selection here). When a new
 * {@link HoodieFileFormat} value is added as a base file format, it is easy to wire the
 * reader/writer factories but miss {@link CommonClientUtils#getLogBlockType}, in which case
 * MOR log writes throw at runtime; that is exactly what happened with VORTEX (missing case
 * threw HoodieException until apache/hudi#19252). These tests iterate
 * {@code HoodieFileFormat.values()} (never a hardcoded list, excluding only HOODIE_LOG which
 * is the log format itself and can never be a base file format) so they fail mechanically
 * when a future format value misses this dispatch point.
 */
class TestFileFormatDispatchCoverage {

  /**
   * Every format the write path can select as a base file format must map to a log block
   * type; a missing case in the getLogBlockType switch fails MOR upserts at runtime.
   */
  // TODO: include VORTEX here (remove the exclusion) once apache/hudi#19252 is merged; on
  //  current master getLogBlockType has no VORTEX case and throws HoodieException, which is
  //  exactly the gap this test exists to catch (see testGetLogBlockTypeForVortex).
  @ParameterizedTest
  @EnumSource(value = HoodieFileFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"HOODIE_LOG", "VORTEX"})
  void testGetLogBlockTypeMapsEveryBaseFileFormat(HoodieFileFormat format) {
    assertNotNull(
        CommonClientUtils.getLogBlockType(writeConfigWithoutExplicitLogFormat(), tableConfigWithBaseFormat(format)),
        () -> "getLogBlockType must return a log block type for base file format " + format
            + "; add the missing case to the switch in CommonClientUtils#getLogBlockType");
  }

  /**
   * Same check as {@link #testGetLogBlockTypeMapsEveryBaseFileFormat} for VORTEX, asserting
   * the post-fix mapping of apache/hudi#19252 (VORTEX -> AVRO_DATA_BLOCK).
   */
  @Disabled("Depends on apache/hudi#19252: on current master getLogBlockType has no VORTEX case and throws "
      + "HoodieException. Enable once #19252 is merged.")
  @Test
  void testGetLogBlockTypeForVortex() {
    assertEquals(HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK,
        CommonClientUtils.getLogBlockType(
            writeConfigWithoutExplicitLogFormat(), tableConfigWithBaseFormat(HoodieFileFormat.VORTEX)));
  }

  /**
   * shouldWriteNativeLogs must produce a decision (never throw) for every base file format,
   * and must never select native logs below writer version TEN regardless of format.
   * (Which formats opt out of native logs at version TEN and above is format-specific policy,
   * asserted case-by-case in TestCommonClientUtils.)
   */
  @ParameterizedTest
  @EnumSource(value = HoodieFileFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"HOODIE_LOG"})
  void testShouldWriteNativeLogsHandlesEveryBaseFileFormat(HoodieFileFormat format) {
    HoodieWriteConfig writeConfig = writeConfigWithoutExplicitLogFormat();
    HoodieTableConfig tableConfig = tableConfigWithBaseFormat(format);

    when(writeConfig.getWriteVersion()).thenReturn(HoodieTableVersion.SIX);
    assertFalse(CommonClientUtils.shouldWriteNativeLogs(writeConfig, tableConfig),
        () -> "Native logs must never be selected below writer version TEN (base file format " + format + ")");

    when(writeConfig.getWriteVersion()).thenReturn(HoodieTableVersion.TEN);
    // Must not throw for any base file format; the boolean policy per format is covered elsewhere.
    CommonClientUtils.shouldWriteNativeLogs(writeConfig, tableConfig);
  }

  /**
   * Vortex has no native log writer yet, so like Lance it must stay on the legacy inline log
   * format even at writer version TEN; asserts the post-fix behavior of apache/hudi#19252
   * (shouldWriteNativeLogs originally special-cased only LANCE).
   */
  @Disabled("Depends on apache/hudi#19252: on current master shouldWriteNativeLogs only special-cases LANCE and "
      + "returns true for VORTEX at version TEN. Enable once #19252 is merged.")
  @Test
  void testShouldWriteNativeLogsForVortex() {
    HoodieWriteConfig writeConfig = writeConfigWithoutExplicitLogFormat();
    when(writeConfig.getWriteVersion()).thenReturn(HoodieTableVersion.TEN);
    assertFalse(CommonClientUtils.shouldWriteNativeLogs(
        writeConfig, tableConfigWithBaseFormat(HoodieFileFormat.VORTEX)));
  }

  private static HoodieWriteConfig writeConfigWithoutExplicitLogFormat() {
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeConfig.getLogDataBlockFormat()).thenReturn(Option.empty());
    return writeConfig;
  }

  private static HoodieTableConfig tableConfigWithBaseFormat(HoodieFileFormat format) {
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getBaseFileFormat()).thenReturn(format);
    return tableConfig;
  }
}
