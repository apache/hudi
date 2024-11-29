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

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.SparkFileFormatInternalRowReaderContext;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.io.HoodieAppendHandle;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.hudi.table.action.bootstrap.SparkBootstrapDeltaCommitActionExecutor;
import org.apache.hudi.table.action.compact.HoodieSparkMergeOnReadTableCompactor;
import org.apache.hudi.table.action.compact.RunCompactionActionExecutor;
import org.apache.hudi.table.action.compact.ScheduleCompactionActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkBulkInsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkBulkInsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkDeleteDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkDeletePreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkInsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkInsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkUpsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkUpsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.restore.MergeOnReadRestoreActionExecutor;
import org.apache.hudi.table.action.rollback.BaseRollbackPlanActionExecutor;
import org.apache.hudi.table.action.rollback.MergeOnReadRollbackActionExecutor;
import org.apache.hudi.table.action.rollback.RestorePlanActionExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.apache.spark.sql.execution.datasources.parquet.SparkParquetReader;
import org.apache.spark.sql.hudi.SparkAdapter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import scala.Tuple2;
import scala.collection.JavaConverters;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.deleteMetadataTable;

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
public class HoodieSparkMergeOnReadTable<T> extends HoodieSparkCopyOnWriteTable<T> implements HoodieCompactionHandler<T> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkMergeOnReadTable.class);

  HoodieSparkMergeOnReadTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> upsert(HoodieEngineContext context, String instantTime, HoodieData<HoodieRecord<T>> records) {
    return new SparkUpsertDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> insert(HoodieEngineContext context, String instantTime, HoodieData<HoodieRecord<T>> records) {
    return new SparkInsertDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> bulkInsert(HoodieEngineContext context, String instantTime, HoodieData<HoodieRecord<T>> records,
      Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    return new SparkBulkInsertDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config,
        this, instantTime, records, userDefinedBulkInsertPartitioner).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> delete(HoodieEngineContext context, String instantTime, HoodieData<HoodieKey> keys) {
    return new SparkDeleteDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, keys).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> deletePrepped(HoodieEngineContext context, String instantTime, HoodieData<HoodieRecord<T>> preppedRecords) {
    return new SparkDeletePreppedDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> upsertPrepped(HoodieEngineContext context, String instantTime,
      HoodieData<HoodieRecord<T>> preppedRecords) {
    return new SparkUpsertPreppedDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> insertPrepped(HoodieEngineContext context, String instantTime,
      HoodieData<HoodieRecord<T>> preppedRecords) {
    return new SparkInsertPreppedDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> bulkInsertPrepped(HoodieEngineContext context, String instantTime,
      HoodieData<HoodieRecord<T>> preppedRecords,  Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    return new SparkBulkInsertPreppedDeltaCommitActionExecutor((HoodieSparkEngineContext) context, config,
        this, instantTime, preppedRecords, userDefinedBulkInsertPartitioner).execute();
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    ScheduleCompactionActionExecutor scheduleCompactionExecutor = new ScheduleCompactionActionExecutor(
        context, config, this, instantTime, extraMetadata, WriteOperationType.COMPACT);
    return scheduleCompactionExecutor.execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> compact(
      HoodieEngineContext context, String compactionInstantTime) {
    RunCompactionActionExecutor<T> compactionExecutor = new RunCompactionActionExecutor<>(
        context, config, this, compactionInstantTime, new HoodieSparkMergeOnReadTableCompactor<>(),
        new HoodieSparkMergeOnReadTable(config, context, getMetaClient()), WriteOperationType.COMPACT);
    return compactionExecutor.execute();
  }

  @Override
  public HoodieBootstrapWriteMetadata<HoodieData<WriteStatus>> bootstrap(HoodieEngineContext context, Option<Map<String, String>> extraMetadata) {
    return new SparkBootstrapDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, extraMetadata).execute();
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleLogCompaction(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    ScheduleCompactionActionExecutor scheduleLogCompactionExecutor = new ScheduleCompactionActionExecutor(
        context, config, this, instantTime, extraMetadata, WriteOperationType.LOG_COMPACT);
    return scheduleLogCompactionExecutor.execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> logCompact(
      HoodieEngineContext context, String logCompactionInstantTime) {
    RunCompactionActionExecutor logCompactionExecutor = new RunCompactionActionExecutor(context, config, this,
        logCompactionInstantTime, new HoodieSparkMergeOnReadTableCompactor<>(), this, WriteOperationType.LOG_COMPACT);
    return logCompactionExecutor.execute();
  }

  @Override
  public void rollbackBootstrap(HoodieEngineContext context, String instantTime) {
    // Delete metadata table to rollback a failed bootstrap. re-attempt of bootstrap will re-initialize the mdt.
    try {
      LOG.info("Deleting metadata table because we are rolling back failed bootstrap. ");
      deleteMetadataTable(config.getBasePath(), context);
    } catch (HoodieMetadataException e) {
      throw new HoodieException("Failed to delete metadata table.", e);
    }

    new RestorePlanActionExecutor<>(context, config, this, instantTime, HoodieTimeline.INIT_INSTANT_TS).execute();
    new MergeOnReadRestoreActionExecutor<>(context, config, this, instantTime, HoodieTimeline.INIT_INSTANT_TS).execute();
  }

  @Override
  public Option<HoodieRollbackPlan> scheduleRollback(HoodieEngineContext context,
                                                     String instantTime,
                                                     HoodieInstant instantToRollback, boolean skipTimelinePublish, boolean shouldRollbackUsingMarkers,
                                                     boolean isRestore) {
    return new BaseRollbackPlanActionExecutor<>(context, config, this, instantTime, instantToRollback, skipTimelinePublish,
        shouldRollbackUsingMarkers, isRestore).execute();
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsertsForLogCompaction(String instantTime, String partitionPath, String fileId,
                                                          Map<String, HoodieRecord<?>> recordMap,
                                                          Map<HoodieLogBlock.HeaderMetadataType, String> header) {
    HoodieAppendHandle appendHandle = new HoodieAppendHandle(config, instantTime, this,
        partitionPath, fileId, recordMap.values().iterator(), taskContextSupplier, header);
    appendHandle.write(recordMap);
    List<WriteStatus> writeStatuses = appendHandle.close();
    return Collections.singletonList(writeStatuses).iterator();
  }

  @Override
  public HoodieRollbackMetadata rollback(HoodieEngineContext context,
                                         String rollbackInstantTime,
                                         HoodieInstant commitInstant,
                                         boolean deleteInstants,
                                         boolean skipLocking) {
    return new MergeOnReadRollbackActionExecutor<>(context, config, this, rollbackInstantTime, commitInstant, deleteInstants, skipLocking).execute();
  }

  @Override
  public HoodieRestoreMetadata restore(HoodieEngineContext context, String restoreInstantTimestamp, String savepointToRestoreTimestamp) {
    return new MergeOnReadRestoreActionExecutor<>(context, config, this, restoreInstantTimestamp, savepointToRestoreTimestamp).execute();
  }

  @Override
  public void finalizeWrite(HoodieEngineContext context, String instantTs, List<HoodieWriteStat> stats)
      throws HoodieIOException {
    // delegate to base class for MOR tables
    super.finalizeWrite(context, instantTs, stats);
  }

  public void prepareBroadcastVariables() {
    if ((!config.useFileGroupReaderWithinTableService())
        || !(context instanceof HoodieSparkEngineContext)) {
      LOG.warn("Did not prepare ParquetFileReader");
      return;
    }

    HoodieSparkEngineContext hoodieSparkEngineContext = (HoodieSparkEngineContext) context;
    SQLConf sqlConf = hoodieSparkEngineContext.getSqlContext().sessionState().conf();
    JavaSparkContext jsc = hoodieSparkEngineContext.jsc();

    scala.collection.immutable.Map<String, String> options =
        scala.collection.immutable.Map$.MODULE$.<String, String>empty()
            .$plus(new Tuple2<>(FileFormat.OPTION_RETURNING_BATCH(), "false"));

    SparkAdapter sparkAdapter = SparkAdapterSupport$.MODULE$.sparkAdapter();
    Configuration mergedConf = addSparkConfigurations(sqlConf, new HashMap<>());
    mergeConfigurations(jsc.hadoopConfiguration(), mergedConf);

    configurationBroadcast = jsc.broadcast(new SerializableConfiguration(mergedConf));
    parquetReaderOpt = Option.of(sparkAdapter.createParquetFileReader(
        false, sqlConf, options, configurationBroadcast.getValue().value()));
    parquetReaderBroadcast = jsc.broadcast(parquetReaderOpt.get());
    LOG.info("ParquetFileReader object is broadcast");
  }

  @Override
  public Option<HoodieReaderContext> getReaderContext(StoragePath basePath) {
    if (parquetReaderBroadcast == null) {
      LOG.warn("ParquetReader is not broadcast; cannot use file group reader");
      return Option.empty();
    }
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(new HadoopStorageConfiguration(configurationBroadcast.getValue().value())).build();
    SparkParquetReader sparkParquetReader = parquetReaderBroadcast.getValue();
    if (sparkParquetReader != null) {
      List<Filter> filters = new ArrayList<>();
      return Option.of(new SparkFileFormatInternalRowReaderContext(
          sparkParquetReader,
          // Need to verify this logic.
          metaClient.getTableConfig().getRecordKeyFields().get()[0],
          JavaConverters.asScalaBufferConverter(filters).asScala().toSeq(),
          JavaConverters.asScalaBufferConverter(filters).asScala().toSeq()));
    } else {
      LOG.warn("ParquetFileReader is null");
      return Option.empty();
    }
  }

  @Override
  public Option<Configuration> getStorageConfig() {
    return Option.of(configurationBroadcast.getValue().value());
  }

  private Configuration addSparkConfigurations(SQLConf sqlConf, Map<String, String> options) {
    Configuration hadoopConf = new Configuration(false);

    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, ParquetReadSupport.class.getName());
    // Assuming `sqlConf` is an instance of SQLConf
    hadoopConf.set(SQLConf.SESSION_LOCAL_TIMEZONE().key(), sqlConf.sessionLocalTimeZone());
    hadoopConf.setBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED().key(), sqlConf.nestedSchemaPruningEnabled());
    hadoopConf.setBoolean(SQLConf.CASE_SENSITIVE().key(), sqlConf.caseSensitiveAnalysis());
    hadoopConf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING().key(), sqlConf.isParquetBinaryAsString());
    hadoopConf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), sqlConf.isParquetINT96AsTimestamp());
    // Using string value of this configuration to preserve compatibility across Spark versions
    hadoopConf.setBoolean(
        SQLConf.LEGACY_PARQUET_NANOS_AS_LONG().key(),
        Boolean.parseBoolean(sqlConf.getConfString(
            SQLConf.LEGACY_PARQUET_NANOS_AS_LONG().key(),
            SQLConf.LEGACY_PARQUET_NANOS_AS_LONG().defaultValueString()
        ))
    );

    hadoopConf.setBoolean(SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED().key(), sqlConf.parquetInferTimestampNTZEnabled());
    // Assuming `options` is a Map<String, String>
    boolean returningBatch = sqlConf.parquetVectorizedReaderEnabled()
        && "true".equals(options.getOrDefault(FileFormat.OPTION_RETURNING_BATCH(), "false"));
    hadoopConf.setBoolean(FileFormat.OPTION_RETURNING_BATCH(), returningBatch);

    return hadoopConf;
  }

  private void mergeConfigurations(Configuration source, Configuration target) {
    for (Map.Entry<String, String> entry : source) {
      target.set(entry.getKey(), entry.getValue());
    }
  }
}
