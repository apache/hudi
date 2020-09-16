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

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.BootstrapDeltaCommitActionExecutor;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.hudi.table.action.compact.RunCompactionActionExecutor;
import org.apache.hudi.table.action.deltacommit.BulkInsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.BulkInsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.DeleteDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.InsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.InsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.UpsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.UpsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.compact.ScheduleCompactionActionExecutor;
import org.apache.hudi.table.action.restore.MergeOnReadRestoreActionExecutor;
import org.apache.hudi.table.action.rollback.MergeOnReadRollbackActionExecutor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Map;

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

  HoodieMergeOnReadTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
    super(config, hadoopConf, metaClient);
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
      Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    return new BulkInsertDeltaCommitActionExecutor(jsc, config,
        this, instantTime, records, userDefinedBulkInsertPartitioner).execute();
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
      JavaRDD<HoodieRecord<T>> preppedRecords,  Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    return new BulkInsertPreppedDeltaCommitActionExecutor(jsc, config,
        this, instantTime, preppedRecords, userDefinedBulkInsertPartitioner).execute();
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(JavaSparkContext jsc, String instantTime, Option<Map<String, String>> extraMetadata) {
    ScheduleCompactionActionExecutor scheduleCompactionExecutor = new ScheduleCompactionActionExecutor(
        jsc, config, this, instantTime, extraMetadata);
    return scheduleCompactionExecutor.execute();
  }

  @Override
  public HoodieWriteMetadata compact(JavaSparkContext jsc, String compactionInstantTime) {
    RunCompactionActionExecutor compactionExecutor = new RunCompactionActionExecutor(jsc, config, this, compactionInstantTime);
    return compactionExecutor.execute();
  }

  @Override
  public HoodieBootstrapWriteMetadata bootstrap(JavaSparkContext jsc, Option<Map<String, String>> extraMetadata) {
    return new BootstrapDeltaCommitActionExecutor(jsc, config, this, extraMetadata).execute();
  }

  @Override
  public void rollbackBootstrap(JavaSparkContext jsc, String instantTime) {
    new MergeOnReadRestoreActionExecutor(jsc, config, this, instantTime, HoodieTimeline.INIT_INSTANT_TS).execute();
  }

  @Override
  public HoodieRollbackMetadata rollback(JavaSparkContext jsc,
                                         String rollbackInstantTime,
                                         HoodieInstant commitInstant,
                                         boolean deleteInstants) {
    return new MergeOnReadRollbackActionExecutor(jsc, config, this, rollbackInstantTime, commitInstant, deleteInstants).execute();
  }

  @Override
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
