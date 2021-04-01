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

package org.apache.hudi.table;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.io.FlinkAppendHandle;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.delta.FlinkUpsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.compact.BaseScheduleCompactionActionExecutor;
import org.apache.hudi.table.action.compact.FlinkScheduleCompactionActionExecutor;

import java.util.List;
import java.util.Map;

public class HoodieFlinkMergeOnReadTable<T extends HoodieRecordPayload>
    extends HoodieFlinkCopyOnWriteTable<T> {

  HoodieFlinkMergeOnReadTable(
      HoodieWriteConfig config,
      HoodieEngineContext context,
      HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> upsert(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieRecord<T>> hoodieRecords) {
    ValidationUtils.checkArgument(writeHandle instanceof FlinkAppendHandle,
        "MOR write handle should always be a FlinkAppendHandle");
    FlinkAppendHandle<?, ?, ?, ?> appendHandle = (FlinkAppendHandle<?, ?, ?, ?>) writeHandle;
    return new FlinkUpsertDeltaCommitActionExecutor<>(context, appendHandle, config, this, instantTime, hoodieRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insert(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieRecord<T>> hoodieRecords) {
    ValidationUtils.checkArgument(writeHandle instanceof FlinkAppendHandle,
        "MOR write handle should always be a FlinkAppendHandle");
    FlinkAppendHandle<?, ?, ?, ?> appendHandle = (FlinkAppendHandle<?, ?, ?, ?>) writeHandle;
    return new FlinkUpsertDeltaCommitActionExecutor<>(context, appendHandle, config, this, instantTime, hoodieRecords).execute();
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(
      HoodieEngineContext context,
      String instantTime,
      Option<Map<String, String>> extraMetadata) {
    BaseScheduleCompactionActionExecutor scheduleCompactionExecutor = new FlinkScheduleCompactionActionExecutor(
        context, config, this, instantTime, extraMetadata);
    return scheduleCompactionExecutor.execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> compact(HoodieEngineContext context, String compactionInstantTime) {
    throw new HoodieNotSupportedException("Compaction is supported as a separate pipeline, "
        + "should not invoke directly through HoodieFlinkMergeOnReadTable");
  }
}

