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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestCleanerUtils {
  private final Functions.Function0<Boolean> rollbackFunction = mock(Functions.Function0.class);

  @Test
  void rollbackFailedWrites_CleanWithEagerPolicy() {
    assertFalse(CleanerUtils.rollbackFailedWrites(HoodieFailedWritesCleaningPolicy.EAGER, HoodieActiveTimeline.CLEAN_ACTION, rollbackFunction));
    verify(rollbackFunction, never()).apply();
  }

  @Test
  void rollbackFailedWrites_CleanWithLazyPolicy() {
    when(rollbackFunction.apply()).thenReturn(true);
    assertTrue(CleanerUtils.rollbackFailedWrites(HoodieFailedWritesCleaningPolicy.LAZY, HoodieActiveTimeline.CLEAN_ACTION, rollbackFunction));
  }

  @Test
  void rollbackFailedWrites_CommitWithEagerPolicy() {
    when(rollbackFunction.apply()).thenReturn(true);
    assertTrue(CleanerUtils.rollbackFailedWrites(HoodieFailedWritesCleaningPolicy.EAGER, HoodieActiveTimeline.COMMIT_ACTION, rollbackFunction));
  }

  @Test
  void rollbackFailedWrites_CommitWithLazyPolicy() {
    assertFalse(CleanerUtils.rollbackFailedWrites(HoodieFailedWritesCleaningPolicy.LAZY, HoodieActiveTimeline.COMMIT_ACTION, rollbackFunction));
    verify(rollbackFunction, never()).apply();
  }
}
