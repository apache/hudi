/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.config;

import org.apache.hudi.common.model.ActionType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieTableServiceManagerConfig {
  @Test
  void parsesWhitespaceEmptyTokensAndExactActionNames() {
    assertEquals(Collections.emptySet(), HoodieTableServiceManagerConfig.parseTableServiceActions(null));
    assertEquals(Collections.emptySet(), HoodieTableServiceManagerConfig.parseTableServiceActions(" , , "));
    assertEquals(Collections.singleton("compaction"),
        HoodieTableServiceManagerConfig.parseTableServiceActions(" compaction, ,compaction, "));
    assertFalse(HoodieTableServiceManagerConfig.parseTableServiceActions("logcompaction,").contains("compaction"));
  }

  @Test
  void schedulingDelegationDefaultsToEmpty() {
    HoodieTableServiceManagerConfig config = HoodieTableServiceManagerConfig.newBuilder().build();

    assertEquals("hoodie.table.service.manager.schedule.actions",
        HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_SCHEDULE_ACTIONS.key());
    assertEquals("", config.getTableServiceManagerScheduleActions());
    assertFalse(config.isTableServiceManagerEnabled());
  }

  @Test
  void schedulingAndExecutionActionsAreIndependent() {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_ENABLED.key(), "true");
    properties.setProperty(HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_ACTIONS.key(), "clean");
    properties.setProperty(HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_SCHEDULE_ACTIONS.key(), "compaction,logcompaction");
    HoodieTableServiceManagerConfig config = HoodieTableServiceManagerConfig.newBuilder().fromProperties(properties).build();

    assertEquals("compaction,logcompaction", config.getTableServiceManagerScheduleActions());
    assertEquals("clean", config.getTableServiceManagerActions());
    assertTrue(config.isEnabledAndActionSupported(ActionType.clean));
    assertFalse(config.isEnabledAndActionSupported(ActionType.compaction));
    assertFalse(config.isEnabledAndActionSupported(ActionType.logcompaction));
  }
}
