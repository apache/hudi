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

import static org.apache.hudi.util.CommonClientUtils.isValidTableVersionWriteVersionPair;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCommonClientUtils {

  @Test
  public void testDisallowPartialUpdatesPreVersion8() {
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
  public void testDisallowNBCCPreVersion8() {
    // given:
    HoodieWriteConfig wConfig = mock(HoodieWriteConfig.class);
    HoodieTableConfig tConfig = mock(HoodieTableConfig.class);
    when(wConfig.getWriteVersion()).thenReturn(HoodieTableVersion.SIX);
    when(tConfig.getTableVersion()).thenReturn(HoodieTableVersion.SIX);
    when(wConfig.isNonBlockingConcurrencyControl()).thenReturn(true);

    // when-then:
    assertThrows(HoodieNotSupportedException.class, () -> CommonClientUtils.validateTableVersion(tConfig, wConfig));
  }

  @Test
  public void testGenerateTokenOnError() {
    // given: a task context supplies that throws errors.
    TaskContextSupplier taskContextSupplier = mock(TaskContextSupplier.class);
    when(taskContextSupplier.getPartitionIdSupplier()).thenThrow(new RuntimeException("generated under testing"));

    // when:
    assertEquals("0-0-0", CommonClientUtils.generateWriteToken(taskContextSupplier));
  }

  @ParameterizedTest(name = "Table version {0} with write version {1} should be valid: {2}")
  @MethodSource("provideValidTableVersionWriteVersionPairs")
  public void testValidTableVersionWriteVersionPairs(
      HoodieTableVersion tableVersion, HoodieTableVersion writeVersion, boolean expectedResult) throws Exception {
    boolean result = isValidTableVersionWriteVersionPair(tableVersion, writeVersion);
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
            // Rule 3: special case - upgrade scenario (table > 6, table < 9, writer = 6)
            Arguments.of(HoodieTableVersion.SEVEN, HoodieTableVersion.SIX, true),
            Arguments.of(HoodieTableVersion.EIGHT, HoodieTableVersion.SIX, true),

            // Rule 4: otherwise disallowed - table > writer (except special case above)
            Arguments.of(HoodieTableVersion.NINE, HoodieTableVersion.SIX, false),
            Arguments.of(HoodieTableVersion.NINE, HoodieTableVersion.EIGHT, false),
            Arguments.of(HoodieTableVersion.EIGHT, HoodieTableVersion.SEVEN, false)
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
