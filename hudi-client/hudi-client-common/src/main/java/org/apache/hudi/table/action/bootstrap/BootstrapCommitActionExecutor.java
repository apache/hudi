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

package org.apache.hudi.table.action.bootstrap;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.BaseCommitActionExecutor;
import org.apache.hudi.table.action.commit.BaseCommitHelper;

import java.util.Map;

public class BootstrapCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends BaseCommitActionExecutor<T, HoodieBootstrapWriteMetadata<HoodieData<WriteStatus>>> {

  private final BaseBootstrapHelper<T> bootstrapHelper;

  public BootstrapCommitActionExecutor(
      HoodieEngineContext context, HoodieWriteConfig config, HoodieTable table,
      Option<Map<String, String>> extraMetadata,
      BaseCommitHelper<T> commitHelper, BaseBootstrapHelper<T> bootstrapHelper) {
    super(context, config, table, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS,
        WriteOperationType.BOOTSTRAP, extraMetadata, commitHelper);
    this.bootstrapHelper = bootstrapHelper;
  }

  @Override
  public HoodieBootstrapWriteMetadata<HoodieData<WriteStatus>> execute() {
    return bootstrapHelper.execute(context, table, config, extraMetadata, commitHelper);
  }
}
