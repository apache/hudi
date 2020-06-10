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
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.SparkRunCompactionActionExecutor;
import org.apache.hudi.table.action.compact.SparkScheduleCompactionActionExecutor;
import org.apache.hudi.table.action.delatacommit.SparkBulkInsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.delatacommit.SparkDeleteDeltaCommitActionExecutor;
import org.apache.hudi.table.action.delatacommit.SparkInsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.delatacommit.SparkUpsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.restore.SparkMergeOnReadRestoreActionExecutor;
import org.apache.hudi.table.action.rollback.SparkMergeOnReadRollbackActionExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.Map;

public class HoodieSparkMergeOnReadTable<T extends HoodieRecordPayload> extends HoodieSparkCopyOnWriteTable<T> {
  public HoodieSparkMergeOnReadTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
    super(config, hadoopConf, metaClient);
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> upsert(HoodieEngineContext context, String instantTime, JavaRDD<HoodieRecord<T>> records) {
    return new SparkUpsertDeltaCommitActionExecutor((HoodieSparkEngineContext) context, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> insert(HoodieEngineContext context, String instantTime, JavaRDD<HoodieRecord<T>> records) {
    return new SparkInsertDeltaCommitActionExecutor((HoodieSparkEngineContext) context, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> bulkInsert(HoodieEngineContext context, String instantTime, JavaRDD<HoodieRecord<T>> records, Option<UserDefinedBulkInsertPartitioner<JavaRDD<HoodieRecord<T>>>> bulkInsertPartitioner) {
    return new SparkBulkInsertDeltaCommitActionExecutor((HoodieSparkEngineContext) context, config, this, instantTime, records, bulkInsertPartitioner).execute();
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> delete(HoodieEngineContext context, String instantTime, JavaRDD<HoodieKey> keys) {
    return new SparkDeleteDeltaCommitActionExecutor((HoodieSparkEngineContext) context, config, this, instantTime, keys).execute();
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> upsertPrepped(HoodieEngineContext context, String instantTime, JavaRDD<HoodieRecord<T>> preppedRecords) {
    return new SparkUpsertDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> insertPrepped(HoodieEngineContext context, String instantTime, JavaRDD<HoodieRecord<T>> preppedRecords) {
    return new SparkInsertDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    SparkScheduleCompactionActionExecutor scheduleCompactionExecutor = new SparkScheduleCompactionActionExecutor(
        context, config, this, instantTime, extraMetadata);
    return scheduleCompactionExecutor.execute();
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> compact(HoodieEngineContext context, String compactionInstantTime) {
    SparkRunCompactionActionExecutor compactionExecutor = new SparkRunCompactionActionExecutor(
        context, config, this, compactionInstantTime);
    return compactionExecutor.execute();
  }

  @Override
  public HoodieRollbackMetadata rollback(HoodieEngineContext context, String rollbackInstantTime, HoodieInstant commitInstant, boolean deleteInstants) {
    return new SparkMergeOnReadRollbackActionExecutor(context, config, this, rollbackInstantTime, commitInstant, deleteInstants).execute();
  }

  @Override
  public HoodieRestoreMetadata restore(HoodieEngineContext context, String restoreInstantTime, String instantToRestore) {
    return new SparkMergeOnReadRestoreActionExecutor<>(context,config,this,restoreInstantTime,instantToRestore).execute();
  }

  @Override
  public void finalizeWrite(HoodieEngineContext context, String instantTs, List<HoodieWriteStat> stats)
      throws HoodieIOException {
    // delegate to base class for MOR tables
    super.finalizeWrite(context, instantTs, stats);
  }

}
