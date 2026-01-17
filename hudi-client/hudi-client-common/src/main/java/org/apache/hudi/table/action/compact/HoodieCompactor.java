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

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.FileGroupReaderBasedAppendHandle;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.HoodieMergeHandleFactory;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * A HoodieCompactor runs compaction on a hoodie table.
 */
@Slf4j
public abstract class HoodieCompactor<T, I, K, O> implements Serializable {

  /**
   * Handles the compaction timeline based on the compaction instant before actual compaction.
   *
   * @param table                     {@link HoodieTable} instance to use.
   * @param pendingCompactionTimeline pending compaction timeline.
   * @param instantTime     compaction instant
   */
  public abstract void preCompact(
      HoodieTable table, HoodieTimeline pendingCompactionTimeline, WriteOperationType operationType, String instantTime);

  /**
   * Maybe persist write status.
   *
   * @param writeStatus {@link HoodieData} of {@link WriteStatus}.
   */
  public abstract void maybePersist(HoodieData<WriteStatus> writeStatus, HoodieEngineContext context, HoodieWriteConfig config, String instantTime);

  /**
   * Execute compaction operations and report back status.
   */
  public HoodieData<WriteStatus> compact(
      HoodieEngineContext context, WriteOperationType operationType,
      HoodieCompactionPlan compactionPlan,
      HoodieTable table, HoodieWriteConfig config, String compactionInstantTime) {
    if (compactionPlan == null || (compactionPlan.getOperations() == null)
        || (compactionPlan.getOperations().isEmpty())) {
      return context.emptyHoodieData();
    }

    // Transition requested to inflight file.
    HoodieActiveTimeline timeline = table.getActiveTimeline();
    if (operationType == WriteOperationType.LOG_COMPACT) {
      HoodieInstant instant = table.getMetaClient().getInstantGenerator().getLogCompactionRequestedInstant(compactionInstantTime);
      // Mark instant as compaction inflight
      timeline.transitionLogCompactionRequestedToInflight(instant);
    } else {
      HoodieInstant instant = table.getMetaClient().getInstantGenerator().getCompactionRequestedInstant(compactionInstantTime);
      timeline.transitionCompactionRequestedToInflight(instant);
    }
    table.getMetaClient().reloadActiveTimeline();

    HoodieTableMetaClient metaClient = table.getMetaClient();
    TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);

    // Here we firstly use the table schema as the reader schema to read
    // log file.That is because in the case of MergeInto, the config.getSchema may not
    // the same with the table schema.
    try {
      if (StringUtils.isNullOrEmpty(config.getInternalSchema())) {
        HoodieSchema readerSchema = schemaResolver.getTableSchema(false);
        config.setSchema(readerSchema.toString());
      }
    } catch (Exception e) {
      // If there is no commit in the table, just ignore the exception.
    }

    // Compacting is very similar to applying updates to existing file
    List<CompactionOperation> operations = compactionPlan.getOperations().stream()
        .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
    log.info("Compactor compacting {} fileGroups", operations.size());

    String maxInstantTime = getMaxInstantTime(metaClient);

    context.setJobStatus(this.getClass().getSimpleName(), "Compacting file slices: " + config.getTableName());
    TaskContextSupplier taskContextSupplier = table.getTaskContextSupplier();
    // if this is a MDT, set up the instant range of log reader just like regular MDT snapshot reader.
    Option<InstantRange> instantRange = CompactHelpers.getInstance().getInstantRange(metaClient);

    if (operationType == WriteOperationType.LOG_COMPACT) {
      return context.parallelize(operations).map(
              operation -> logCompact(config, operation, compactionInstantTime, instantRange, table, taskContextSupplier))
          .flatMap(List::iterator);
    } else {
      ReaderContextFactory<T> readerContextFactory;
      if (!metaClient.isMetadataTable()) {
        readerContextFactory = context.getReaderContextFactory(metaClient);
      } else {
        // Payload and HFile caching props are required here
        readerContextFactory = (ReaderContextFactory<T>) context.getReaderContextFactoryForWrite(metaClient, HoodieRecordType.AVRO, config.getProps());
      }
      return context.parallelize(operations).map(
              operation -> compact(config, operation, compactionInstantTime, readerContextFactory.getContext(), table, maxInstantTime, taskContextSupplier))
          .flatMap(List::iterator);
    }
  }

  /**
   * Execute a single compaction operation and report back status.
   */
  public List<WriteStatus> compact(HoodieWriteConfig writeConfig,
                                   CompactionOperation operation,
                                   String instantTime,
                                   HoodieReaderContext hoodieReaderContext,
                                   HoodieTable table,
                                   String maxInstantTime,
                                   TaskContextSupplier taskContextSupplier) throws IOException {
    HoodieMergeHandle<T, ?, ?, ?> mergeHandle = HoodieMergeHandleFactory.create(writeConfig,
        instantTime, table, operation, taskContextSupplier, hoodieReaderContext, maxInstantTime, getEngineRecordType());
    mergeHandle.doMerge();
    return mergeHandle.close();
  }

  public List<WriteStatus> logCompact(HoodieWriteConfig writeConfig,
                                      CompactionOperation operation,
                                      String instantTime,
                                      Option<InstantRange> instantRange,
                                      HoodieTable table,
                                      TaskContextSupplier taskContextSupplier) throws IOException {
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(
        table.getStorageConf(), table.getMetaClient().getTableConfig(), instantRange, Option.empty(), writeConfig.getProps());
    FileGroupReaderBasedAppendHandle<IndexedRecord, ?, ?, ?> appendHandle = new FileGroupReaderBasedAppendHandle<>(writeConfig, instantTime, table, operation,  taskContextSupplier, readerContext);
    appendHandle.doAppend();
    return appendHandle.close();
  }

  public String getMaxInstantTime(HoodieTableMetaClient metaClient) {
    String maxInstantTime = metaClient
        .getActiveTimeline().getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.COMMIT_ACTION,
            HoodieTimeline.ROLLBACK_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION))
        .filterCompletedInstants().lastInstant().get().requestedTime();
    return maxInstantTime;
  }

  protected abstract HoodieRecord.HoodieRecordType getEngineRecordType();
}
