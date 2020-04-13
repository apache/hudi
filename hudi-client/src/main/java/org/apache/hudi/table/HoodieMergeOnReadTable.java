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
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.action.commit.HoodieWriteMetadata;
import org.apache.hudi.table.action.deltacommit.BulkInsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.BulkInsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.DeleteDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.InsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.InsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.UpsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.UpsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.restore.MergeOnReadRestoreActionExecutor;
import org.apache.hudi.table.action.rollback.MergeOnReadRollbackActionExecutor;
import org.apache.hudi.table.compact.HoodieMergeOnReadTableCompactor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of a more real-time Hoodie Table the provides tradeoffs on read and write cost/amplification.
 *
 * <p>
 * INSERTS - Same as HoodieCopyOnWriteTable - Produce new files, block aligned to desired size (or) Merge with the
 * smallest existing file, to expand it
 * </p>
 * <p>
 * UPDATES - Appends the changes to a rolling log file maintained per file Id. Compaction merges the log file into the
 * base file.
 * </p>
 * <p>
 * WARNING - MOR table type does not support nested rollbacks, every rollback must be followed by an attempted commit
 * action
 * </p>
 */
public class HoodieMergeOnReadTable<T extends HoodieRecordPayload> extends HoodieCopyOnWriteTable<T> {

  private static final Logger LOG = LogManager.getLogger(HoodieMergeOnReadTable.class);

  HoodieMergeOnReadTable(HoodieWriteConfig config, JavaSparkContext jsc, HoodieTableMetaClient metaClient) {
    super(config, jsc, metaClient);
  }

  @Override
  public HoodieWriteMetadata upsert(JavaSparkContext jsc, String instantTime, JavaRDD<HoodieRecord<T>> records) {
    return new UpsertDeltaCommitActionExecutor<>(jsc, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata insert(JavaSparkContext jsc, String instantTime, JavaRDD<HoodieRecord<T>> records) {
    return new InsertDeltaCommitActionExecutor<>(jsc, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata bulkInsert(JavaSparkContext jsc, String instantTime, JavaRDD<HoodieRecord<T>> records,
      Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    return new BulkInsertDeltaCommitActionExecutor<>(jsc, config,
        this, instantTime, records, bulkInsertPartitioner).execute();
  }

  @Override
  public HoodieWriteMetadata delete(JavaSparkContext jsc, String instantTime, JavaRDD<HoodieKey> keys) {
    return new DeleteDeltaCommitActionExecutor<>(jsc, config, this, instantTime, keys).execute();
  }

  @Override
  public HoodieWriteMetadata upsertPrepped(JavaSparkContext jsc, String instantTime,
      JavaRDD<HoodieRecord<T>> preppedRecords) {
    return new UpsertPreppedDeltaCommitActionExecutor<>(jsc, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata insertPrepped(JavaSparkContext jsc, String instantTime,
      JavaRDD<HoodieRecord<T>> preppedRecords) {
    return new InsertPreppedDeltaCommitActionExecutor<>(jsc, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata bulkInsertPrepped(JavaSparkContext jsc, String instantTime,
      JavaRDD<HoodieRecord<T>> preppedRecords,  Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    return new BulkInsertPreppedDeltaCommitActionExecutor<>(jsc, config,
        this, instantTime, preppedRecords, bulkInsertPartitioner).execute();
  }

  @Override
  public HoodieCompactionPlan scheduleCompaction(JavaSparkContext jsc, String instantTime) {
    LOG.info("Checking if compaction needs to be run on " + config.getBasePath());
    Option<HoodieInstant> lastCompaction =
        getActiveTimeline().getCommitTimeline().filterCompletedInstants().lastInstant();
    String deltaCommitsSinceTs = "0";
    if (lastCompaction.isPresent()) {
      deltaCommitsSinceTs = lastCompaction.get().getTimestamp();
    }

    int deltaCommitsSinceLastCompaction = getActiveTimeline().getDeltaCommitTimeline()
        .findInstantsAfter(deltaCommitsSinceTs, Integer.MAX_VALUE).countInstants();
    if (config.getInlineCompactDeltaCommitMax() > deltaCommitsSinceLastCompaction) {
      LOG.info("Not running compaction as only " + deltaCommitsSinceLastCompaction
          + " delta commits was found since last compaction " + deltaCommitsSinceTs + ". Waiting for "
          + config.getInlineCompactDeltaCommitMax());
      return new HoodieCompactionPlan();
    }

    LOG.info("Compacting merge on read table " + config.getBasePath());
    HoodieMergeOnReadTableCompactor compactor = new HoodieMergeOnReadTableCompactor();
    try {
      return compactor.generateCompactionPlan(jsc, this, config, instantTime,
          ((SyncableFileSystemView) getSliceView()).getPendingCompactionOperations()
              .map(instantTimeCompactionopPair -> instantTimeCompactionopPair.getValue().getFileGroupId())
              .collect(Collectors.toSet()));

    } catch (IOException e) {
      throw new HoodieCompactionException("Could not schedule compaction " + config.getBasePath(), e);
    }
  }

  @Override
  public JavaRDD<WriteStatus> compact(JavaSparkContext jsc, String compactionInstantTime,
      HoodieCompactionPlan compactionPlan) {
    HoodieMergeOnReadTableCompactor compactor = new HoodieMergeOnReadTableCompactor();
    try {
      return compactor.compact(jsc, compactionPlan, this, config, compactionInstantTime);
    } catch (IOException e) {
      throw new HoodieCompactionException("Could not compact " + config.getBasePath(), e);
    }
  }

  @Override
  public HoodieRollbackMetadata rollback(JavaSparkContext jsc,
                                         String rollbackInstantTime,
                                         HoodieInstant commitInstant,
                                         boolean deleteInstants) {
    return new MergeOnReadRollbackActionExecutor(jsc, config, this, rollbackInstantTime, commitInstant, deleteInstants).execute();
  }

  public HoodieRestoreMetadata restore(JavaSparkContext jsc, String restoreInstantTime, String instantToRestore) {
    return new MergeOnReadRestoreActionExecutor(jsc, config, this, restoreInstantTime, instantToRestore).execute();
  }

  @Override
  public void finalizeWrite(JavaSparkContext jsc, String instantTs, List<HoodieWriteStat> stats)
      throws HoodieIOException {
    // delegate to base class for MOR tables
    super.finalizeWrite(jsc, instantTs, stats);
  }
}
