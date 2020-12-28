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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieEngineContext;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.hudi.table.action.clean.JavaCleanActionExecutor;
import org.apache.hudi.table.action.commit.JavaInsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.JavaInsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.JavaUpsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.JavaUpsertPreppedCommitActionExecutor;

import java.util.List;
import java.util.Map;

public class HoodieJavaCopyOnWriteTable<T extends HoodieRecordPayload> extends HoodieJavaTable<T> {
  protected HoodieJavaCopyOnWriteTable(HoodieWriteConfig config,
                                       HoodieEngineContext context,
                                       HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> upsert(HoodieEngineContext context,
                                                       String instantTime,
                                                       List<HoodieRecord<T>> records) {
    return new JavaUpsertCommitActionExecutor<>(context, config,
        this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insert(HoodieEngineContext context,
                                                       String instantTime,
                                                       List<HoodieRecord<T>> records) {
    return new JavaInsertCommitActionExecutor<>(context, config,
        this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> bulkInsert(HoodieEngineContext context,
                                                           String instantTime,
                                                           List<HoodieRecord<T>> records,
                                                           Option<BulkInsertPartitioner<List<HoodieRecord<T>>>> bulkInsertPartitioner) {
    throw new HoodieNotSupportedException("BulkInsert is not supported yet");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> delete(HoodieEngineContext context,
                                                       String instantTime,
                                                       List<HoodieKey> keys) {
    throw new HoodieNotSupportedException("Delete is not supported yet");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> upsertPrepped(HoodieEngineContext context,
                                                              String instantTime,
                                                              List<HoodieRecord<T>> preppedRecords) {
    return new JavaUpsertPreppedCommitActionExecutor<>((HoodieJavaEngineContext) context, config,
        this, instantTime, preppedRecords).execute();

  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insertPrepped(HoodieEngineContext context,
                                                              String instantTime,
                                                              List<HoodieRecord<T>> preppedRecords) {
    return new JavaInsertPreppedCommitActionExecutor<>((HoodieJavaEngineContext) context, config,
        this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> bulkInsertPrepped(HoodieEngineContext context,
                                                                  String instantTime,
                                                                  List<HoodieRecord<T>> preppedRecords,
                                                                  Option<BulkInsertPartitioner<List<HoodieRecord<T>>>> bulkInsertPartitioner) {
    throw new HoodieNotSupportedException("BulkInsertPrepped is not supported yet");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insertOverwrite(HoodieEngineContext context,
                                                                String instantTime,
                                                                List<HoodieRecord<T>> records) {
    throw new HoodieNotSupportedException("InsertOverwrite is not supported yet");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insertOverwriteTable(HoodieEngineContext context,
                                                                     String instantTime,
                                                                     List<HoodieRecord<T>> records) {
    throw new HoodieNotSupportedException("InsertOverwrite is not supported yet");
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context,
                                                         String instantTime,
                                                         Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("ScheduleCompaction is not supported yet");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> compact(HoodieEngineContext context,
                                                        String compactionInstantTime) {
    throw new HoodieNotSupportedException("Compact is not supported yet");
  }

  @Override
  public Option<HoodieClusteringPlan> scheduleClustering(final HoodieEngineContext context, final String instantTime, final Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("Clustering is not supported yet");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> cluster(final HoodieEngineContext context, final String clusteringInstantTime) {
    throw new HoodieNotSupportedException("Clustering is not supported yet");
  }

  @Override
  public HoodieBootstrapWriteMetadata<List<WriteStatus>> bootstrap(HoodieEngineContext context,
                                                                   Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("Bootstrap is not supported yet");
  }

  @Override
  public void rollbackBootstrap(HoodieEngineContext context,
                                String instantTime) {
    throw new HoodieNotSupportedException("RollbackBootstrap is not supported yet");
  }

  @Override
  public HoodieCleanMetadata clean(HoodieEngineContext context,
                                   String cleanInstantTime) {
    return new JavaCleanActionExecutor(context, config, this, cleanInstantTime).execute();
  }

  @Override
  public HoodieRollbackMetadata rollback(HoodieEngineContext context,
                                         String rollbackInstantTime,
                                         HoodieInstant commitInstant,
                                         boolean deleteInstants) {
    throw new HoodieNotSupportedException("Rollback is not supported yet");
  }

  @Override
  public HoodieSavepointMetadata savepoint(HoodieEngineContext context,
                                           String instantToSavepoint,
                                           String user,
                                           String comment) {
    throw new HoodieNotSupportedException("Savepoint is not supported yet");
  }

  @Override
  public HoodieRestoreMetadata restore(HoodieEngineContext context,
                                       String restoreInstantTime,
                                       String instantToRestore) {
    throw new HoodieNotSupportedException("Restore is not supported yet");
  }
}
