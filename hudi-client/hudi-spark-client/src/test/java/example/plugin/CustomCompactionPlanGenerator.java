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

package example.plugin;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseTableServicePlanActionExecutor;
import org.apache.hudi.table.action.compact.plan.generators.HoodieCompactionPlanGenerator;

/**
 * A compaction plan generator outside the Hudi packages, as a user would plug in.
 */
public class CustomCompactionPlanGenerator<T extends HoodieRecordPayload, I, K, O> extends HoodieCompactionPlanGenerator<T, I, K, O> {

  public CustomCompactionPlanGenerator(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig,
                                       BaseTableServicePlanActionExecutor executor) {
    super(table, engineContext, writeConfig, executor);
  }
}
