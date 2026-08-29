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

package org.apache.hudi.common.model;

import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link WriteConcurrencyMode}, focused on the shared insert-overwrite guard that every
 * engine entry point routes through.
 */
class TestWriteConcurrencyMode {

  @Test
  void checkInsertOverwriteSupportedRejectsOverwriteUnderNonBlocking() {
    // The exact combination that silently loses data: NB-CC + insert overwrite.
    HoodieException exception = assertThrows(HoodieException.class,
        () -> WriteConcurrencyMode.checkInsertOverwriteSupported(true, true));
    assertEquals(WriteConcurrencyMode.INSERT_OVERWRITE_NOT_SUPPORTED_ERROR, exception.getMessage());
  }

  @Test
  void checkInsertOverwriteSupportedAllowsOtherCombinations() {
    // NB-CC without overwrite (e.g. insert into / upsert) must pass — the guard must not over-block.
    assertDoesNotThrow(() -> WriteConcurrencyMode.checkInsertOverwriteSupported(true, false));
    // Insert overwrite is fine when NB-CC is off.
    assertDoesNotThrow(() -> WriteConcurrencyMode.checkInsertOverwriteSupported(false, true));
    // Neither.
    assertDoesNotThrow(() -> WriteConcurrencyMode.checkInsertOverwriteSupported(false, false));
  }

  /**
   * Ties the boolean guard to the {@link WriteOperationType#isOverwrite} classification, so it is
   * clear which operations are actually rejected under non-blocking concurrency control.
   */
  @ParameterizedTest
  @EnumSource(WriteOperationType.class)
  void checkInsertOverwriteSupportedMatchesIsOverwrite(WriteOperationType operation) {
    boolean isOverwrite = WriteOperationType.isOverwrite(operation);
    if (isOverwrite) {
      assertThrows(HoodieException.class,
          () -> WriteConcurrencyMode.checkInsertOverwriteSupported(true, isOverwrite));
    } else {
      assertDoesNotThrow(
          () -> WriteConcurrencyMode.checkInsertOverwriteSupported(true, isOverwrite));
    }
  }
}
