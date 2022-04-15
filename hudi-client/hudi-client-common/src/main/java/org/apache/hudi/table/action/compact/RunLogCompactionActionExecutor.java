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

import org.apache.avro.Schema;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieCompactionStrategy;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.table.HoodieCompactionHandler;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class RunLogCompactionActionExecutor<T extends HoodieRecordPayload> extends
    BaseActionExecutor<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>, HoodieWriteMetadata<HoodieData<WriteStatus>>> implements Serializable {

  private static final Logger LOG = LogManager.getLogger(RunLogCompactionActionExecutor.class);
  private final HoodieCompactionHandler compactionHandler;

  public RunLogCompactionActionExecutor(HoodieEngineContext engineContext,
                                        HoodieWriteConfig writeConfig,
                                        HoodieTable table,
                                        String instantTime,
                                        HoodieCompactionHandler compactionHandler) {
    super(engineContext, writeConfig, table, instantTime);
    this.compactionHandler = compactionHandler;
  }

  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute() {
    try {
      HoodieInstant requestedInstant = HoodieTimeline.getLogCompactionRequestedInstant(instantTime);
      // Mark instant as log compaction inflight
      table.getActiveTimeline().transitionLogCompactionRequestedToInflight(requestedInstant);
      table.getMetaClient().reloadActiveTimeline();

      // Get log compaction action plan
      HoodieCompactionPlan compactionPlan =
          CompactionUtils.getLogCompactionPlan(table.getMetaClient(), instantTime);

      if (compactionPlan == null) {
        throw new HoodieCompactionException("Log compaction action cannot be performed since there is log compaction plan.");
      }

      // Get the log compaction action executor.
      HoodieCompactionStrategy compactionStrategy = compactionPlan.getStrategy();
      if (compactionStrategy == null || StringUtils.isNullOrEmpty(compactionStrategy.getCompactorClassName())) {
        throw new HoodieCompactionException("Log compaction action cannot be performed since there is no strategy class.");
      }

      final Schema schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));

      HoodieData<WriteStatus> writeStatus =
          ((LogCompactionExecutionStrategy<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>>)
              ReflectionUtils.loadClass(compactionStrategy.getCompactorClassName(), new Class<?>[]
                  {HoodieTable.class, HoodieEngineContext.class, HoodieWriteConfig.class, HoodieCompactionHandler.class},
                  table, context, config, compactionHandler))
              .performCompaction(compactionPlan, schema, instantTime);

      context.setJobStatus(this.getClass().getSimpleName(), "Preparing log compaction metadata");
      List<HoodieWriteStat> updateStatusMap = writeStatus.map(WriteStatus::getStat).collectAsList();
      HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
      for (HoodieWriteStat stat : updateStatusMap) {
        metadata.addWriteStat(stat.getPartitionPath(), stat);
      }
      metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, config.getSchema());
      HoodieWriteMetadata<HoodieData<WriteStatus>> compactionMetadata = new HoodieWriteMetadata<>();
      compactionMetadata.setWriteStatuses(writeStatus);
      compactionMetadata.setCommitted(false);
      compactionMetadata.setCommitMetadata(Option.of(metadata));
      return compactionMetadata;
    } catch (IOException e) {
      throw new HoodieCompactionException("Could not compact " + config.getBasePath(), e);
    }
  }

}
