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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
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
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * A HoodieCompactor runs compaction on a hoodie table.
 */
public abstract class HoodieCompactor<T, I, K, O> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieCompactor.class);

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
        Schema readerSchema = schemaResolver.getTableAvroSchema(false);
        config.setSchema(readerSchema.toString());
      }
    } catch (Exception e) {
      // If there is no commit in the table, just ignore the exception.
    }

    // Compacting is very similar to applying updates to existing file
    List<CompactionOperation> operations = compactionPlan.getOperations().stream()
        .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
    LOG.info("Compactor compacting {} fileGroups", operations.size());

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
      ReaderContextFactory<T> readerContextFactory = context.getReaderContextFactory(metaClient);
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
        instantTime, table, getFileSliceFromOperation(operation, writeConfig.getBasePath()), operation, taskContextSupplier, hoodieReaderContext, maxInstantTime, getEngineRecordType());
    mergeHandle.doMerge();
    return mergeHandle.close();
  }

  public List<WriteStatus> logCompact(HoodieWriteConfig writeConfig,
                                      CompactionOperation operation,
                                      String instantTime,
                                      Option<InstantRange> instantRange,
                                      HoodieTable table,
                                      TaskContextSupplier taskContextSupplier) throws IOException {
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(table.getStorageConf(), table.getMetaClient().getTableConfig(), instantRange, Option.empty());
    FileGroupReaderBasedAppendHandle<IndexedRecord, ?, ?, ?> appendHandle = new FileGroupReaderBasedAppendHandle<>(writeConfig, instantTime, table, getFileSliceFromOperation(operation,
        writeConfig.getBasePath()), operation,  taskContextSupplier, readerContext);
    appendHandle.doAppend();
    return appendHandle.close();
  }

  private FileSlice getFileSliceFromOperation(CompactionOperation operation, String basePath) {
    Option<HoodieBaseFile> baseFileOpt =
        operation.getBaseFile(basePath, operation.getPartitionPath());
    List<HoodieLogFile> logFiles = operation.getDeltaFileNames().stream().map(p ->
            new HoodieLogFile(new StoragePath(FSUtils.constructAbsolutePath(
                basePath, operation.getPartitionPath()), p)))
        .collect(Collectors.toList());
    return new FileSlice(
        operation.getFileGroupId(),
        operation.getBaseInstantTime(),
        baseFileOpt.isPresent() ? baseFileOpt.get() : null,
        logFiles);
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
