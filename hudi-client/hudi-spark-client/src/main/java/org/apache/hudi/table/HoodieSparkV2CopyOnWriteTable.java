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

package org.apache.hudi.table;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.client.HoodieRowWriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.hudi.table.action.commit.SparkV2UpsertCommitActionExecutor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;

public class HoodieSparkV2CopyOnWriteTable extends HoodieSparkV2Table<Dataset<Row>, Dataset<HoodieKey>, Dataset<HoodieRowWriteStatus>> {

  protected HoodieSparkV2CopyOnWriteTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  @Override
  public HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> upsert(HoodieEngineContext context, String instantTime, Dataset<Row> records) {
    return new SparkV2UpsertCommitActionExecutor((HoodieSparkEngineContext) context,
        config, this, records, instantTime, WriteOperationType.UPSERT, Option.empty()).execute();
  }

  @Override
  public HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> insert(HoodieEngineContext context, String instantTime, Dataset<Row> records) {
    return null;
  }

  @Override
  public HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> bulkInsert(HoodieEngineContext context, String instantTime, Dataset<Row> records,
      Option<BulkInsertPartitioner<Dataset<Row>>> bulkInsertPartitioner) {
    return null;
  }

  @Override
  public HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> delete(HoodieEngineContext context, String instantTime, Dataset<HoodieKey> keys) {
    return null;
  }

  @Override
  public HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> deletePartitions(HoodieEngineContext context, String instantTime, List<String> partitions) {
    return null;
  }

  @Override
  public HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> upsertPrepped(HoodieEngineContext context, String instantTime, Dataset<Row> preppedRecords) {
    return null;
  }

  @Override
  public HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> insertPrepped(HoodieEngineContext context, String instantTime, Dataset<Row> preppedRecords) {
    return null;
  }

  @Override
  public HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> bulkInsertPrepped(HoodieEngineContext context, String instantTime, Dataset<Row> preppedRecords,
      Option<BulkInsertPartitioner<Dataset<Row>>> bulkInsertPartitioner) {
    return null;
  }

  @Override
  public HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> insertOverwrite(HoodieEngineContext context, String instantTime, Dataset<Row> records) {
    return null;
  }

  @Override
  public HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> insertOverwriteTable(HoodieEngineContext context, String instantTime, Dataset<Row> records) {
    return null;
  }

  @Override
  public void updateMetadataIndexes(@NotNull HoodieEngineContext context, @NotNull List<HoodieWriteStat> stats, @NotNull String instantTime) throws Exception {

  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    return null;
  }

  @Override
  public HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> compact(HoodieEngineContext context, String compactionInstantTime) {
    return null;
  }

  @Override
  public Option<HoodieClusteringPlan> scheduleClustering(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    return null;
  }

  @Override
  public HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> cluster(HoodieEngineContext context, String clusteringInstantTime) {
    return null;
  }

  @Override
  public HoodieBootstrapWriteMetadata<Dataset<HoodieRowWriteStatus>> bootstrap(HoodieEngineContext context, Option<Map<String, String>> extraMetadata) {
    return null;
  }

  @Override
  public void rollbackBootstrap(HoodieEngineContext context, String instantTime) {

  }

  @Override
  public Option<HoodieCleanerPlan> scheduleCleaning(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    return null;
  }

  @Override
  public HoodieCleanMetadata clean(HoodieEngineContext context, String cleanInstantTime, boolean skipLocking) {
    return null;
  }

  @Override
  public Option<HoodieRollbackPlan> scheduleRollback(HoodieEngineContext context, String instantTime, HoodieInstant instantToRollback, boolean skipTimelinePublish,
      boolean shouldRollbackUsingMarkers) {
    return null;
  }

  @Override
  public HoodieRollbackMetadata rollback(HoodieEngineContext context, String rollbackInstantTime, HoodieInstant commitInstant, boolean deleteInstants, boolean skipLocking) {
    return null;
  }

  @Override
  public HoodieSavepointMetadata savepoint(HoodieEngineContext context, String instantToSavepoint, String user, String comment) {
    return null;
  }

  @Override
  public HoodieRestoreMetadata restore(HoodieEngineContext context, String restoreInstantTime, String instantToRestore) {
    return null;
  }

  @Override
  public boolean isTableServiceAction(String actionType) {
    return false;
  }
}
