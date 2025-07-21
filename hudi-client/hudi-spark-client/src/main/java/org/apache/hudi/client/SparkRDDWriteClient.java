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

package org.apache.hudi.client;

import org.apache.hudi.callback.common.WriteStatusValidator;
import org.apache.hudi.index.HoodieSparkIndexClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.utils.SparkReleaseResources;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HoodieWrapperFileSystem;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.SparkHoodieIndexFactory;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.SparkMetadataWriterFactory;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.upgrade.SparkUpgradeDowngradeHelper;

import com.codahale.metrics.Timer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:LineLength")
public class SparkRDDWriteClient<T> extends
    BaseHoodieWriteClient<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRDDWriteClient.class);
  private final StreamingMetadataWriteHandler streamingMetadataWriteHandler = new StreamingMetadataWriteHandler();

  public SparkRDDWriteClient(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    this(context, clientConfig, Option.empty());
  }

  public SparkRDDWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig,
                             Option<EmbeddedTimelineService> timelineService) {
    super(context, writeConfig, timelineService, SparkUpgradeDowngradeHelper.getInstance());
    this.tableServiceClient = new SparkRDDTableServiceClient<T>(context, writeConfig, getTimelineServer(), getTransactionManager());
  }

  @Override
  protected HoodieIndex createIndex(HoodieWriteConfig writeConfig) {
    return SparkHoodieIndexFactory.createIndex(config);
  }

  public boolean commit(String instantTime, JavaRDD<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata,
                        String commitActionType, Map<String, List<String>> partitionToReplacedFileIds,
                        Option<BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata>> extraPreCommitFunc) {
    return commit(instantTime, writeStatuses, extraMetadata, commitActionType, partitionToReplacedFileIds, extraPreCommitFunc, Option.empty());
  }

  /**
   * Complete changes performed at the given instantTime marker with specified action.
   */
  @Override
  public boolean commit(String instantTime, JavaRDD<WriteStatus> rawWriteStatuses, Option<Map<String, String>> extraMetadata,
                        String commitActionType, Map<String, List<String>> partitionToReplacedFileIds,
                        Option<BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata>> extraPreCommitFunc,
                        Option<WriteStatusValidator> writeStatusValidatorOpt) {
    context.setJobStatus(this.getClass().getSimpleName(), "Committing stats: " + config.getTableName());

    // write to metadata table if streaming writes are enabled.
    HoodieTable table = createTable(config);
    final JavaRDD<WriteStatus> writeStatuses;
    if (WriteOperationType.streamingWritesToMetadataSupported((getOperationType())) && isStreamingWriteToMetadataEnabled(table)) {
      // this code block is expected to create a new Metadata Writer, start a new commit in metadata table and trigger streaming write to metadata table.
      writeStatuses = HoodieJavaRDD.getJavaRDD(streamingMetadataWriteHandler.streamWriteToMetadataTable(table, HoodieJavaRDD.of(rawWriteStatuses), instantTime));
    } else {
      writeStatuses = rawWriteStatuses;
    }

    // Triggering the dag for writes.
    //
    // 1. If streaming writes are enabled, writes to both data table and metadata table gets triggered at this juncture;
    // 2. If not, writes to data table gets triggered here.
    //
    // When streaming writes are enabled, data table's WriteStatus is expected to contain all stats required to generate metadata table records and so each object will be larger.
    // Here all additional stats and error records are dropped to retain only the required information and prevent collecting large objects on the driver.
    List<SlimWriteStats> slimWriteStatsList = SlimWriteStats.from(writeStatuses);
    // Compute stats for the data table writes and invoke callback
    AtomicLong totalRecords = new AtomicLong(0);
    AtomicLong totalErrorRecords = new AtomicLong(0);
    // collect record stats for data table
    slimWriteStatsList.stream().filter(slimWriteStats -> !slimWriteStats.isMetadataTable())
        .forEach(slimWriteStats -> {
          totalRecords.getAndAdd(slimWriteStats.getTotalRecords());
          totalErrorRecords.getAndAdd(slimWriteStats.getTotalErrorRecords());
        });
    // Why passing RDD<WriteStatus> to the WriteStatus validator:
    // At the beginning of this method, we drop all index stats and error records before collecting in the driver.
    // Just in case if there are errors, caller might be interested to fetch error records in the validator where
    // a complete collection of RDD<WriteStatus> is required.
    boolean canProceed = writeStatusValidatorOpt.map(callback -> callback.validate(totalRecords.get(), totalErrorRecords.get(),
            totalErrorRecords.get() > 0 ? Option.of(HoodieJavaRDD.of(writeStatuses.filter(status -> !status.isMetadataTable()).map(WriteStatus::removeMetadataStats))) : Option.empty()))
        .orElse(true);

    // Proceeds only if validator returns true, otherwise bails out.
    if (canProceed) {
      // when streaming writes are enabled, writeStatuses is a mix of data table write status and mdt write status
      List<HoodieWriteStat> dataTableHoodieWriteStats = slimWriteStatsList.stream().filter(entry -> !entry.isMetadataTable()).map(SlimWriteStats::getWriteStat).collect(Collectors.toList());
      List<HoodieWriteStat> partialMetadataTableWriteStats = slimWriteStatsList.stream().filter(entry -> entry.isMetadataTable).map(SlimWriteStats::getWriteStat).collect(Collectors.toList());
      return commitStats(instantTime, new TableWriteStats(dataTableHoodieWriteStats, partialMetadataTableWriteStats), extraMetadata, commitActionType, partitionToReplacedFileIds, extraPreCommitFunc,
          false, Option.of(table));
    } else {
      LOG.error("Exiting early due to errors with write operation ");
      return false;
    }
  }

  @Override
  protected void writeToMetadataTable(boolean skipStreamingWritesToMetadataTable,
                                      HoodieTable table,
                                      String instantTime,
                                      List<HoodieWriteStat> partialMetadataTableWriteStats,
                                      HoodieCommitMetadata metadata) {
    if (!skipStreamingWritesToMetadataTable
        && isStreamingWriteToMetadataEnabled(table)
        && WriteOperationType.streamingWritesToMetadataSupported(getOperationType())) {
      streamingMetadataWriteHandler.commitToMetadataTable(table, instantTime, metadata, partialMetadataTableWriteStats);
    } else {
      writeTableMetadata(table, instantTime, metadata);
    }
  }

  @Override
  protected HoodieTable createTable(HoodieWriteConfig config) {
    return createTableAndValidate(config, HoodieSparkTable::create);
  }

  @Override
  protected HoodieTable createTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    return createTableAndValidate(config, metaClient, HoodieSparkTable::create);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> filterExists(JavaRDD<HoodieRecord<T>> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, context);
    Timer.Context indexTimer = metrics.getIndexCtx();
    JavaRDD<HoodieRecord<T>> recordsWithLocation = HoodieJavaRDD.getJavaRDD(
        getIndex().tagLocation(HoodieJavaRDD.of(hoodieRecords), context, table));
    metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
    return recordsWithLocation.filter(v1 -> !v1.isCurrentLocationKnown());
  }

  /**
   * Main API to run bootstrap to hudi.
   */
  @Override
  public void bootstrap(Option<Map<String, String>> extraMetadata) {
    initTable(WriteOperationType.UPSERT, Option.ofNullable(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS)).bootstrap(context, extraMetadata);
  }

  @Override
  public JavaRDD<WriteStatus> upsert(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table =
        initTable(WriteOperationType.UPSERT, Option.ofNullable(instantTime));
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.UPSERT, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.upsert(context, instantTime, HoodieJavaRDD.of(records));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    if (result.getSourceReadAndIndexDurationMs().isPresent()) {
      metrics.updateSourceReadAndIndexMetrics(HoodieMetrics.DURATION_STR, result.getSourceReadAndIndexDurationMs().get());
    }
    return postWrite(resultRDD, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> upsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table =
        initTable(WriteOperationType.UPSERT_PREPPED, Option.ofNullable(instantTime));
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.UPSERT_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.upsertPrepped(context, instantTime, HoodieJavaRDD.of(preppedRecords));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> insert(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table =
        initTable(WriteOperationType.INSERT, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.insert(context, instantTime, HoodieJavaRDD.of(records));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> insertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table =
        initTable(WriteOperationType.INSERT_PREPPED, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.insertPrepped(context, instantTime, HoodieJavaRDD.of(preppedRecords));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }

  /**
   * Removes all existing records from the partitions affected and inserts the given HoodieRecords, into the table.
   *
   * @param records     HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public HoodieWriteResult insertOverwrite(JavaRDD<HoodieRecord<T>> records, final String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table = initTable(WriteOperationType.INSERT_OVERWRITE, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT_OVERWRITE, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.insertOverwrite(context, instantTime, HoodieJavaRDD.of(records));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return new HoodieWriteResult(postWrite(resultRDD, instantTime, table), result.getPartitionToReplaceFileIds());
  }

  /**
   * Removes all existing records of the Hoodie table and inserts the given HoodieRecords, into the table.
   *
   * @param records     HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public HoodieWriteResult insertOverwriteTable(JavaRDD<HoodieRecord<T>> records, final String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table = initTable(WriteOperationType.INSERT_OVERWRITE_TABLE, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT_OVERWRITE_TABLE, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.insertOverwriteTable(context, instantTime, HoodieJavaRDD.of(records));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return new HoodieWriteResult(postWrite(resultRDD, instantTime, table), result.getPartitionToReplaceFileIds());
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    return bulkInsert(records, instantTime, Option.empty());
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records, String instantTime, Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table =
        initTable(WriteOperationType.BULK_INSERT, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.BULK_INSERT, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.bulkInsert(context, instantTime, HoodieJavaRDD.of(records), userDefinedBulkInsertPartitioner);
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime, Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table =
        initTable(WriteOperationType.BULK_INSERT_PREPPED, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.BULK_INSERT_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.bulkInsertPrepped(context, instantTime, HoodieJavaRDD.of(preppedRecords), bulkInsertPartitioner);
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> delete(JavaRDD<HoodieKey> keys, String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table = initTable(WriteOperationType.DELETE, Option.ofNullable(instantTime));
    preWrite(instantTime, WriteOperationType.DELETE, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.delete(context, instantTime, HoodieJavaRDD.of(keys));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> deletePrepped(JavaRDD<HoodieRecord<T>> preppedRecord, String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table = initTable(WriteOperationType.DELETE_PREPPED, Option.ofNullable(instantTime));
    preWrite(instantTime, WriteOperationType.DELETE_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.deletePrepped(context, instantTime, HoodieJavaRDD.of(preppedRecord));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }

  public HoodieWriteResult deletePartitions(List<String> partitions, String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table = initTable(WriteOperationType.DELETE_PARTITION, Option.ofNullable(instantTime));
    preWrite(instantTime, WriteOperationType.DELETE_PARTITION, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.deletePartitions(context, instantTime, partitions);
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return new HoodieWriteResult(postWrite(resultRDD, instantTime, table), result.getPartitionToReplaceFileIds());
  }

  public HoodieWriteResult managePartitionTTL(String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table = initTable(WriteOperationType.DELETE_PARTITION, Option.ofNullable(instantTime));
    preWrite(instantTime, WriteOperationType.DELETE_PARTITION, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.managePartitionTTL(context, instantTime);
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return new HoodieWriteResult(postWrite(resultRDD, instantTime, table), result.getPartitionToReplaceFileIds());
  }

  @Override
  protected void initMetadataTable(Option<String> instantTime, HoodieTableMetaClient metaClient) {
    // Initialize Metadata Table to make sure it's bootstrapped _before_ the operation,
    // if it didn't exist before
    // See https://issues.apache.org/jira/browse/HUDI-3343 for more details
    initializeMetadataTable(instantTime, metaClient);
  }

  /**
   * Initialize the metadata table if needed. Creating the metadata table writer
   * will trigger the initial bootstrapping from the data table.
   *
   * @param inFlightInstantTimestamp - The in-flight action responsible for the metadata table initialization
   */
  private void initializeMetadataTable(Option<String> inFlightInstantTimestamp, HoodieTableMetaClient metaClient) {
    if (!config.isMetadataTableEnabled()) {
      return;
    }
    // if metadata table is enabled, emit enablement metrics
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    if (tableConfig.isMetadataTableAvailable()) {
      // if metadata table is available, lets emit partitions of interest
      boolean isMetadataColStatsAvailable = false;
      boolean isMetadataBloomFilterAvailable = false;
      boolean isMetadataRliAvailable = false;
      if (tableConfig.getMetadataPartitions().contains(MetadataPartitionType.COLUMN_STATS.getPartitionPath())) {
        isMetadataColStatsAvailable = true;
      }
      if (tableConfig.getMetadataPartitions().contains(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath())) {
        isMetadataBloomFilterAvailable = true;
      }
      if (tableConfig.getMetadataPartitions().contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath())) {
        isMetadataRliAvailable = true;
      }
      metrics.emitMetadataEnablementMetrics(true, isMetadataColStatsAvailable, isMetadataBloomFilterAvailable, isMetadataRliAvailable);
    }

    try (HoodieTableMetadataWriter writer = SparkMetadataWriterFactory.create(
        context.getStorageConf(), config, context, inFlightInstantTimestamp, tableConfig)) {
      if (writer.isInitialized()) {
        writer.performTableServices(inFlightInstantTimestamp);
      }
    } catch (Exception e) {
      throw new HoodieException("Failed to instantiate Metadata table ", e);
    }
  }

  @Override
  protected void initWrapperFSMetrics() {
    if (config.isMetricsOn()) {
      Registry registry;
      Registry registryMeta;
      JavaSparkContext jsc = ((HoodieSparkEngineContext) context).getJavaSparkContext();

      if (config.isExecutorMetricsEnabled()) {
        // Create a distributed registry for HoodieWrapperFileSystem
        registry = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName(),
            DistributedRegistry.class.getName());
        ((DistributedRegistry) registry).register(jsc);
        registryMeta = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName() + "MetaFolder",
            DistributedRegistry.class.getName());
        ((DistributedRegistry) registryMeta).register(jsc);
      } else {
        registry = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName());
        registryMeta = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName() + "MetaFolder");
      }

      HoodieWrapperFileSystem.setMetricsRegistry(registry, registryMeta);
    }
  }

  @Override
  protected void updateColumnsToIndexWithColStats(HoodieTableMetaClient metaClient, List<String> columnsToIndex) {
    new HoodieSparkIndexClient(config, getEngineContext()).createOrUpdateColumnStatsIndexDefinition(metaClient, columnsToIndex);
  }

  @Override
  public void releaseResources(String instantTime) {
    super.releaseResources(instantTime);
    SparkReleaseResources.releaseCachedData(context, config, basePath, instantTime);
  }

  /**
   * Slim WriteStatus to hold info like total records, total record records,
   * HoodieWriteStat and whether the writeStatus is referring to metadata table or not.
   */
  static class SlimWriteStats implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean isMetadataTable;
    private long totalRecords;
    private long totalErrorRecords;
    private HoodieWriteStat writeStat;

    private SlimWriteStats(boolean isMetadataTable, long totalRecords, long totalErrorRecords, HoodieWriteStat writeStat) {
      this.isMetadataTable = isMetadataTable;
      this.totalRecords = totalRecords;
      this.totalErrorRecords = totalErrorRecords;
      this.writeStat = writeStat;
    }

    public static List<SlimWriteStats> from(JavaRDD<WriteStatus> writeStatuses) {
      return writeStatuses.map(writeStatus -> new SlimWriteStats(writeStatus.isMetadataTable(), writeStatus.getTotalRecords(), writeStatus.getTotalErrorRecords(),
          writeStatus.getStat())).collect();
    }

    public boolean isMetadataTable() {
      return isMetadataTable;
    }

    public long getTotalRecords() {
      return totalRecords;
    }

    public long getTotalErrorRecords() {
      return totalErrorRecords;
    }

    public HoodieWriteStat getWriteStat() {
      return writeStat;
    }

    // setter for efficient serialization,
    // please do not remove it even if it is not used.
    public void setMetadataTable(boolean metadataTable) {
      isMetadataTable = metadataTable;
    }

    // setter for efficient serialization,
    // please do not remove it even if it is not used.
    public void setTotalRecords(long totalRecords) {
      this.totalRecords = totalRecords;
    }

    // setter for efficient serialization,
    // please do not remove it even if it is not used.
    public void setTotalErrorRecords(long totalErrorRecords) {
      this.totalErrorRecords = totalErrorRecords;
    }

    // setter for efficient serialization,
    // please do not remove it even if it is not used.
    public void setWriteStat(HoodieWriteStat writeStat) {
      this.writeStat = writeStat;
    }
  }
}
