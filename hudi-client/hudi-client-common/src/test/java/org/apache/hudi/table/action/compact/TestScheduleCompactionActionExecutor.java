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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseTableServicePlanActionExecutor;
import org.apache.hudi.table.action.compact.plan.generators.HoodieCompactionPlanGenerator;
import org.apache.hudi.table.action.compact.strategy.LogFileNumBasedCompactionStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestScheduleCompactionActionExecutor extends HoodieCommonTestHarness {

  private final HoodieEngineContext context = new HoodieLocalEngineContext(getStorageConf());
  @Mock
  private HoodieWriteConfig config;
  @Mock
  private HoodieTable table;

  @Test
  void testInitPlanGenerator() throws IOException {
    initMetaClient();
    TypedProperties properties = new TypedProperties();
    properties.put(HoodieCompactionConfig.COMPACTION_PLAN_GENERATOR.key(), TestCompactionPlanGenerator.class.getName());
    when(config.getProps()).thenReturn(properties);
    when(config.getCompactionStrategy()).thenReturn(new LogFileNumBasedCompactionStrategy());
    Assertions.assertDoesNotThrow(() -> new ScheduleCompactionActionExecutor(
        context, config, table, "001", Option.empty(), WriteOperationType.COMPACT
    ));
    Assertions.assertEquals(1, TestCompactionPlanGenerator.getCount());
  }

  public static class TestCompactionPlanGenerator<T extends HoodieRecordPayload, I, K, O> extends HoodieCompactionPlanGenerator<T, I, K, O> {
    private static int count = 0;

    public TestCompactionPlanGenerator(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig, BaseTableServicePlanActionExecutor executor) {
      super(table, engineContext, writeConfig, executor);
      count++;
    }

    public static int getCount() {
      return count;
    }
  }
}