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
}
