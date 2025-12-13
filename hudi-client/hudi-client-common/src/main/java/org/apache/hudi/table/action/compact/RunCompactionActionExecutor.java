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

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import lombok.extern.slf4j.Slf4j;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

@Slf4j
@SuppressWarnings("checkstyle:LineLength")
public class RunCompactionActionExecutor<T> extends
    BaseActionExecutor<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>, HoodieWriteMetadata<HoodieData<WriteStatus>>> {

  private final HoodieCompactor compactor;
  private final WriteOperationType operationType;

  private final HoodieMetrics metrics;

  public RunCompactionActionExecutor(HoodieEngineContext context,
                                     HoodieWriteConfig config,
                                     HoodieTable table,
                                     String instantTime,
                                     HoodieCompactor compactor,
                                     WriteOperationType operationType) {
    super(context, config, table, instantTime);
    this.compactor = compactor;
    this.operationType = operationType;
    checkArgument(operationType == WriteOperationType.COMPACT || operationType == WriteOperationType.LOG_COMPACT,
        "Only COMPACT and LOG_COMPACT is supported");
    metrics = new HoodieMetrics(config, table.getStorage());
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute() {
    log.info("Compaction requested. Instant time: {}.", instantTime);
    metrics.emitCompactionRequested();

    HoodieTimeline pendingMajorOrMinorCompactionTimeline = WriteOperationType.COMPACT.equals(operationType)
        ? table.getActiveTimeline().filterPendingCompactionTimeline()
        : table.getActiveTimeline().filterPendingLogCompactionTimeline();
    compactor.preCompact(table, pendingMajorOrMinorCompactionTimeline, this.operationType, instantTime);

    HoodieWriteMetadata<HoodieData<WriteStatus>> compactionWriteMetadata = new HoodieWriteMetadata<>();
    try {
      // generate compaction plan
      // should support configurable commit metadata
      HoodieCompactionPlan compactionPlan = operationType.equals(WriteOperationType.COMPACT)
          ? CompactionUtils.getCompactionPlan(table.getMetaClient(), instantTime)
          : CompactionUtils.getLogCompactionPlan(table.getMetaClient(), instantTime);

      // try to load internalSchema to support schema Evolution
      HoodieWriteConfig configCopy = config;
      Pair<Option<String>, Option<String>> schemaPair = InternalSchemaCache
          .getInternalSchemaAndAvroSchemaForClusteringAndCompaction(table.getMetaClient(), instantTime);
      if (schemaPair.getLeft().isPresent() && schemaPair.getRight().isPresent()) {
        // should not influence the original config, just copy it
        configCopy = HoodieWriteConfig.newBuilder().withProperties(config.getProps()).build();
        configCopy.setInternalSchemaString(schemaPair.getLeft().get());
        configCopy.setSchema(schemaPair.getRight().get());
      }

      HoodieData<WriteStatus> statuses = compactor.compact(
          context, operationType, compactionPlan, table, configCopy, instantTime);

      compactor.maybePersist(statuses, context, config, instantTime);
      context.setJobStatus(this.getClass().getSimpleName(), "Preparing compaction metadata: " + config.getTableName());

      HoodieCommitMetadata metadata = new HoodieCommitMetadata(false);
      metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, config.getSchema());
      if (schemaPair.getLeft().isPresent()) {
        metadata.addMetadata(SerDeHelper.LATEST_SCHEMA, schemaPair.getLeft().get());
        metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, schemaPair.getRight().get());
      }
      metadata.setOperationType(operationType);
      compactionWriteMetadata.setCommitMetadata(Option.of(metadata));
      compactionWriteMetadata.setWriteStatuses(statuses);
      compactionWriteMetadata.setCommitted(false);
    } catch (Exception e) {
      throw new HoodieCompactionException("Could not compact " + config.getBasePath(), e);
    }

    return compactionWriteMetadata;
  }
}
