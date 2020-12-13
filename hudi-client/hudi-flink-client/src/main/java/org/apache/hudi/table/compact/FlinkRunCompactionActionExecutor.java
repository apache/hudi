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

package org.apache.hudi.table.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
//import org.apache.hudi.common.model.*;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.io.IOException;
import java.util.List;

@SuppressWarnings("checkstyle:LineLength")
public class FlinkRunCompactionActionExecutor<T extends HoodieRecordPayload> extends
    BaseActionExecutor<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>, HoodieWriteMetadata<List<WriteStatus>>> {

  public FlinkRunCompactionActionExecutor(HoodieFlinkEngineContext context,
                                          HoodieWriteConfig config,
                                          HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
                                          String instantTime) {
    super(context, config, table, instantTime);
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> execute() {
    HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(instantTime);
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    if (!pendingCompactionTimeline.containsInstant(instant)) {
      throw new IllegalStateException(
          "No Compaction request available at " + instantTime + " to run compaction");
    }

    HoodieWriteMetadata<List<WriteStatus>> compactionMetadata = new HoodieWriteMetadata<>();
    try {
      HoodieActiveTimeline timeline = table.getActiveTimeline();
      HoodieCompactionPlan compactionPlan =
          CompactionUtils.getCompactionPlan(table.getMetaClient(), instantTime);
      // Mark instant as compaction inflight
      timeline.transitionCompactionRequestedToInflight(instant);
      table.getMetaClient().reloadActiveTimeline();

      HoodieFlinkMergeOnReadTableCompactor compactor = new HoodieFlinkMergeOnReadTableCompactor();
      List<WriteStatus> statuses = compactor.compact(context, compactionPlan, table, config, instantTime);


      List<HoodieWriteStat> updateStatusMap = context.map(statuses, WriteStatus::getStat, 0);
      HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
      for (HoodieWriteStat stat : updateStatusMap) {
        metadata.addWriteStat(stat.getPartitionPath(), stat);
      }
      metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, config.getSchema());

      compactionMetadata.setWriteStatuses(statuses);
      compactionMetadata.setCommitted(false);
      compactionMetadata.setCommitMetadata(Option.of(metadata));
    } catch (IOException e) {
      throw new HoodieCompactionException("Could not compact " + config.getBasePath(), e);
    }

    return compactionMetadata;
  }
}
