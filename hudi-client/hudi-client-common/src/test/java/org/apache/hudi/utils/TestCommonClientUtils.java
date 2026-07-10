/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi.utils;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.util.CommonClientUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.hudi.util.CommonClientUtils.areTableVersionsCompatible;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestCommonClientUtils {

  @Test
  void testDisallowPartialUpdatesPreVersion8() {
    // given:
    HoodieWriteConfig wConfig = mock(HoodieWriteConfig.class);
    HoodieTableConfig tConfig = mock(HoodieTableConfig.class);
    when(wConfig.getWriteVersion()).thenReturn(HoodieTableVersion.SIX);
    when(tConfig.getTableVersion()).thenReturn(HoodieTableVersion.SIX);
    when(wConfig.shouldWritePartialUpdates()).thenReturn(true);

    // when-then:
    assertThrows(HoodieNotSupportedException.class, () -> CommonClientUtils.validateTableVersion(tConfig, wConfig));
  }

  @Test
  void testDisallowNBCCPreVersion8() {
    // given:
    HoodieWriteConfig wConfig = mock(HoodieWriteConfig.class);
    HoodieTableConfig tConfig = mock(HoodieTableConfig.class);
    when(wConfig.getWriteVersion()).thenReturn(HoodieTableVersion.SIX);
    when(tConfig.getTableVersion()).thenReturn(HoodieTableVersion.SIX);
    when(wConfig.isNonBlockingConcurrencyControl()).thenReturn(true);

    // when-then:
    assertThrows(HoodieNotSupportedException.class, () -> CommonClientUtils.validateTableVersion(tConfig, wConfig));
  }

  @ParameterizedTest(name = "LSM-tree layout, writer version {0} should throw: {1}")
  @MethodSource("provideLSMTreeVersionExpectations")
  void testLSMTreeStorageLayoutRequiresVersionTen(HoodieTableVersion writeVersion, boolean expectThrow) {
    HoodieWriteConfig wConfig = mock(HoodieWriteConfig.class);
    HoodieTableConfig tConfig = mock(HoodieTableConfig.class);
    when(wConfig.getWriteVersion()).thenReturn(writeVersion);
    when(tConfig.getTableVersion()).thenReturn(writeVersion);
    when(tConfig.isLSMTreeStorageLayout()).thenReturn(true);

    if (expectThrow) {
      assertThrows(HoodieNotSupportedException.class, () -> CommonClientUtils.validateTableVersion(tConfig, wConfig));
    } else {
      CommonClientUtils.validateTableVersion(tConfig, wConfig);
    }
  }

  private static Stream<Arguments> provideLSMTreeVersionExpectations() {
    return Stream.of(
        Arguments.of(HoodieTableVersion.EIGHT, true),
        Arguments.of(HoodieTableVersion.NINE, true),
        Arguments.of(HoodieTableVersion.TEN, false)
    );
  }

  @Test
  void testGenerateTokenOnError() {
    // given: a task context supplies that throws errors.
    TaskContextSupplier taskContextSupplier = mock(TaskContextSupplier.class);
    when(taskContextSupplier.getPartitionIdSupplier()).thenThrow(new RuntimeException("generated under testing"));

    // when:
    assertEquals("0-0-0", CommonClientUtils.generateWriteToken(taskContextSupplier));
  }

  @ParameterizedTest(name = "Write version {0} with base file format {1} should write native log format: {2}")
  @MethodSource("provideWriteVersionNativeLogExpectations")
  void testShouldWriteNativeLogs(HoodieTableVersion writeVersion, HoodieFileFormat baseFileFormat, boolean expected) {
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(writeConfig.getWriteVersion()).thenReturn(writeVersion);
    when(tableConfig.getBaseFileFormat()).thenReturn(baseFileFormat);

    assertEquals(expected, CommonClientUtils.shouldWriteNativeLogs(writeConfig, tableConfig));
  }

  private static Stream<Arguments> provideWriteVersionNativeLogExpectations() {
    // Native log format is the default for write version >= TEN, except for Lance base files.
    return Stream.of(
        Arguments.of(HoodieTableVersion.SIX, HoodieFileFormat.PARQUET, false),
        Arguments.of(HoodieTableVersion.EIGHT, HoodieFileFormat.PARQUET, false),
        Arguments.of(HoodieTableVersion.NINE, HoodieFileFormat.PARQUET, false),
        Arguments.of(HoodieTableVersion.TEN, HoodieFileFormat.PARQUET, true),
        Arguments.of(HoodieTableVersion.TEN, HoodieFileFormat.ORC, true),
        Arguments.of(HoodieTableVersion.TEN, HoodieFileFormat.LANCE, false)
    );
  }

  @Test
  void testShouldWriteInlineLogFormatForMultiFormatLanceWrites() {
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(writeConfig.getWriteVersion()).thenReturn(HoodieTableVersion.TEN);
    when(writeConfig.contains(HoodieWriteConfig.BASE_FILE_FORMAT)).thenReturn(true);
    when(writeConfig.getBaseFileFormat()).thenReturn(HoodieFileFormat.LANCE);
    when(tableConfig.isMultipleBaseFileFormatsEnabled()).thenReturn(true);
    when(tableConfig.getBaseFileFormat()).thenReturn(HoodieFileFormat.PARQUET);

    assertFalse(CommonClientUtils.shouldWriteNativeLogs(writeConfig, tableConfig));
  }

  @ParameterizedTest(name = "Table version {0} with write version {1} should be valid: {2}")
  @MethodSource("provideValidTableVersionWriteVersionPairs")
  void testValidTableVersionWriteVersionPairs(
      HoodieTableVersion tableVersion, HoodieTableVersion writeVersion, boolean expectedResult) throws Exception {
    boolean result = areTableVersionsCompatible(tableVersion, writeVersion);
    assertEquals(expectedResult, result);
  }

  private static Stream<Arguments> provideValidTableVersionWriteVersionPairs() {
    return Stream.concat(
        Stream.concat(
            // Rule 1: writer > table version - always allowed
            generateWriterGreaterThanTableCases(),
            // Rule 2: same versions - always allowed  
            generateSameVersionCases()
        ),
        Stream.of(
            // Rule 3: downgrade scenarios
            Arguments.of(HoodieTableVersion.SEVEN, HoodieTableVersion.SIX, true),
            Arguments.of(HoodieTableVersion.EIGHT, HoodieTableVersion.SIX, true),
            Arguments.of(HoodieTableVersion.NINE, HoodieTableVersion.SIX, true),
            Arguments.of(HoodieTableVersion.NINE, HoodieTableVersion.EIGHT, true),
            Arguments.of(HoodieTableVersion.EIGHT, HoodieTableVersion.SEVEN, true),
            Arguments.of(HoodieTableVersion.SEVEN, HoodieTableVersion.SIX, true),
            // Rule 4: disallowed scenarios
            Arguments.of(HoodieTableVersion.NINE, HoodieTableVersion.FIVE, false),
            Arguments.of(HoodieTableVersion.EIGHT, HoodieTableVersion.FIVE, false),
            Arguments.of(HoodieTableVersion.SEVEN, HoodieTableVersion.FIVE, false),
            Arguments.of(HoodieTableVersion.SIX, HoodieTableVersion.FIVE, false)
        )
    );
  }

  private static Stream<Arguments> generateWriterGreaterThanTableCases() {
    HoodieTableVersion[] allVersions = HoodieTableVersion.values();
    return Stream.of(allVersions)
        .flatMap(tableVersion -> 
            Stream.of(allVersions)
                .filter(writeVersion -> writeVersion.greaterThan(tableVersion))
                .map(writeVersion -> Arguments.of(tableVersion, writeVersion, true))
        );
  }

  private static Stream<Arguments> generateSameVersionCases() {
    return Stream.of(HoodieTableVersion.values())
        .map(version -> Arguments.of(version, version, true));
  }
}
