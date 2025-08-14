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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieWriterUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.async.AsyncClusteringService;
import org.apache.hudi.async.AsyncCompactService;
import org.apache.hudi.async.SparkAsyncClusteringService;
import org.apache.hudi.async.SparkAsyncCompactService;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.utils.OperationConverter;
import org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieClusteringUpdateException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.utilities.HiveIncrementalPuller;
import org.apache.hudi.utilities.IdentitySplitter;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.checkpointing.InitialCheckPointProvider;
import org.apache.hudi.utilities.ingestion.HoodieIngestionException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.ingestion.HoodieIngestionService;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.JsonDFSSource;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.String.format;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1.STREAMER_CHECKPOINT_RESET_KEY_V1;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.utilities.UtilHelpers.buildProperties;
import static org.apache.hudi.utilities.UtilHelpers.readConfig;

/**
 * An Utility which can incrementally take the output from {@link HiveIncrementalPuller} and apply it to the target
 * table. Does not maintain any state, queries at runtime to see how far behind the target table is from the source
 * table. This can be overridden to force sync from a timestamp.
 * <p>
 * In continuous mode, DeltaStreamer runs in loop-mode going through the below operations (a) pull-from-source (b)
 * write-to-sink (c) Schedule Compactions if needed (d) Conditionally Sync to Hive each cycle. For MOR table with
 * continuous mode enabled, a separate compactor thread is allocated to execute compactions
 */
public class HoodieStreamer implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(HoodieStreamer.class);
  private static final List<String> DEFAULT_SENSITIVE_CONFIG_KEYS = Arrays.asList(
      HoodieWriteConfig.SENSITIVE_CONFIG_KEYS_FILTER.defaultValue().split(","));
  private static final String SENSITIVE_VALUES_MASKED = "SENSITIVE_INFO_MASKED";

  public static final String CHECKPOINT_KEY = HoodieWriteConfig.STREAMER_CHECKPOINT_KEY;
  public static final String CHECKPOINT_RESET_KEY = STREAMER_CHECKPOINT_RESET_KEY_V1;

  protected transient Config cfg;

  /**
   * NOTE: These properties are already consolidated w/ CLI provided config-overrides.
   */
  private final TypedProperties properties;

  protected transient Option<HoodieIngestionService> ingestionService;

  private final Option<BootstrapExecutor> bootstrapExecutor;

  public static final String STREAMSYNC_POOL_NAME = "hoodiedeltasync";

  public HoodieStreamer(Config cfg, JavaSparkContext jssc) throws IOException {
    this(cfg, jssc, HadoopFSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration()),
        jssc.hadoopConfiguration(), Option.empty());
  }

  public HoodieStreamer(Config cfg, JavaSparkContext jssc, Option<TypedProperties> props) throws IOException {
    this(cfg, jssc, HadoopFSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration()),
        jssc.hadoopConfiguration(), props);
  }

  public HoodieStreamer(Config cfg, JavaSparkContext jssc, FileSystem fs, Configuration conf) throws IOException {
    this(cfg, jssc, fs, conf, Option.empty());
  }

  public HoodieStreamer(Config cfg, JavaSparkContext jssc, FileSystem fs, Configuration conf, Option<TypedProperties> propsOverride) throws IOException {
    this(cfg, jssc, fs, conf, propsOverride, Option.empty());
  }

  public HoodieStreamer(Config cfg, JavaSparkContext jssc, FileSystem fs, Configuration conf,
                        Option<TypedProperties> propsOverride, Option<SourceProfileSupplier> sourceProfileSupplier) throws IOException {
    this.properties = combineProperties(cfg, propsOverride, jssc.hadoopConfiguration());
    Triple<RecordMergeMode, String, String> mergingConfigs =
        HoodieTableConfig.inferCorrectMergingBehavior(
            cfg.recordMergeMode, cfg.payloadClassName, cfg.recordMergeStrategyId, cfg.sourceOrderingFields,
            HoodieTableVersion.fromVersionCode(ConfigUtils.getIntWithAltKeys(this.properties, HoodieWriteConfig.WRITE_TABLE_VERSION)), true);
    cfg.recordMergeMode = mergingConfigs.getLeft();
    cfg.payloadClassName = mergingConfigs.getMiddle();
    cfg.recordMergeStrategyId = mergingConfigs.getRight();
    if (cfg.initialCheckpointProvider != null && cfg.checkpoint == null) {
      InitialCheckPointProvider checkPointProvider =
          UtilHelpers.createInitialCheckpointProvider(cfg.initialCheckpointProvider, this.properties);
      checkPointProvider.init(conf);
      cfg.checkpoint = checkPointProvider.getCheckpoint();
    }

    this.cfg = cfg;
    this.bootstrapExecutor = Option.ofNullable(
        cfg.runBootstrap ? new BootstrapExecutor(cfg, jssc, fs, conf, this.properties) : null);
    HoodieSparkEngineContext sparkEngineContext = new HoodieSparkEngineContext(jssc);
    this.ingestionService = Option.ofNullable(
        cfg.runBootstrap ? null : new StreamSyncService(cfg, sparkEngineContext, fs, conf, Option.ofNullable(this.properties), sourceProfileSupplier));
  }

  @VisibleForTesting
  public static TypedProperties combineProperties(Config cfg, Option<TypedProperties> propsOverride, Configuration hadoopConf) {
    HoodieConfig hoodieConfig = new HoodieConfig();
    // Resolving the properties in a consistent way:
    //   1. Properties override always takes precedence
    //   2. Otherwise, check if there's no props file specified (merging in CLI overrides)
    //   3. Otherwise, parse provided specified props file (merging in CLI overrides)
    if (propsOverride.isPresent()) {
      hoodieConfig.setAll(propsOverride.get());
    } else if (cfg.propsFilePath.equals(Config.DEFAULT_DFS_SOURCE_PROPERTIES)) {
      hoodieConfig.setAll(UtilHelpers.getConfig(cfg.configs).getProps());
    } else {
      hoodieConfig.setAll(readConfig(hadoopConf, new Path(cfg.propsFilePath), cfg.configs).getProps());
    }

    // set any configs that Deltastreamer has to override explicitly
    hoodieConfig.setDefaultValue(DataSourceWriteOptions.RECONCILE_SCHEMA());
    // we need auto adjustment enabled for deltastreamer since async table services are feasible within the same JVM.
    hoodieConfig.setValue(HoodieWriteConfig.AUTO_ADJUST_LOCK_CONFIGS.key(), "true");
    if (cfg.tableType.equals(HoodieTableType.MERGE_ON_READ.name())) {
      // Explicitly set the table type
      hoodieConfig.setValue(HoodieTableConfig.TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    }
    if (!hoodieConfig.contains(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES)
        && !StringUtils.isNullOrEmpty(cfg.recordMergeImplClasses)) {
      hoodieConfig.setValue(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), cfg.recordMergeImplClasses);
    }

    return hoodieConfig.getProps(true);
  }

  public void shutdownGracefully() {
    ingestionService.ifPresent(ds -> {
      LOG.info("Shutting down DeltaStreamer");
      ds.shutdown(false);
      LOG.info("Async service shutdown complete. Closing DeltaSync ");
      ds.close();
    });
  }

  /**
   * Main method to start syncing.
   */
  public void sync() throws Exception {
    if (bootstrapExecutor.isPresent()) {
      LOG.info("Performing bootstrap. Source=" + bootstrapExecutor.get().getBootstrapConfig().getBootstrapSourceBasePath());
      bootstrapExecutor.get().execute();
    } else {
      ingestionService.ifPresent(HoodieIngestionService::startIngestion);
    }
  }

  public Config getConfig() {
    return cfg;
  }

  public static class Config implements Serializable {
    public static final String DEFAULT_DFS_SOURCE_PROPERTIES = "file://" + System.getProperty("user.dir")
        + "/src/test/resources/streamer-config/dfs-source.properties";

    @Parameter(names = {"--target-base-path"},
        description = "base path for the target hoodie table. "
            + "(Will be created if did not exist first time around. If exists, expected to be a hoodie table)",
        required = true)
    public String targetBasePath;

    // TODO: How to obtain hive configs to register?
    @Parameter(names = {"--target-table"}, description = "name of the target table", required = true)
    public String targetTableName;

    @Parameter(names = {"--table-type"}, description = "Type of table. COPY_ON_WRITE (or) MERGE_ON_READ", required = true)
    public String tableType;

    @Parameter(names = {"--base-file-format"}, description = "File format for the base files. PARQUET (or) HFILE", required = false)
    public String baseFileFormat = "PARQUET";

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client, schema provider, key generator and data source. For hoodie client props, sane defaults are "
        + "used, but recommend use to provide basic things like metrics endpoints, hive configs etc. For sources, refer"
        + "to individual classes, for supported properties."
        + " Properties in this file can be overridden by \"--hoodie-conf\"")
    public String propsFilePath = DEFAULT_DFS_SOURCE_PROPERTIES;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--source-class"},
        description = "Subclass of org.apache.hudi.utilities.sources to read data. "
            + "Built-in options: org.apache.hudi.utilities.sources.{JsonDFSSource (default), AvroDFSSource, "
            + "JsonKafkaSource, AvroKafkaSource, HiveIncrPullSource}")
    public String sourceClassName = JsonDFSSource.class.getName();

    @Parameter(names = {"--source-ordering-field"}, description = "Comma separated list of fields within source record to decide how"
        + " to break ties between records with same key in input data.")
    public String sourceOrderingFields = null;

    @Parameter(names = {"--payload-class"}, description = "Deprecated. "
        + "Use --merge-mode for commit time or event time merging. "
        + "Subclass of HoodieRecordPayload, that works off a GenericRecord. Implement your own, if you want to do something "
        + "other than overwriting existing value")
    public String payloadClassName = null;

    @Parameter(names = {"--merge-mode", "--record-merge-mode"}, description = "mode to merge records with")
    public RecordMergeMode recordMergeMode = null;
    
    @Parameter(names = {"--merge-strategy-id", "--record-merge-strategy-id"}, description = "only set this if you are using custom merge mode")
    public String recordMergeStrategyId = null;

    @Parameter(names = {"--merge-impl-classes", "--record-merge-custom-implementation-classes"},
        description = "Comma separated list of classes that implement the record merger strategy")
    public String recordMergeImplClasses = null;

    @Parameter(names = {"--schemaprovider-class"}, description = "subclass of org.apache.hudi.utilities.schema"
        + ".SchemaProvider to attach schemas to input & target table data, built in options: "
        + "org.apache.hudi.utilities.schema.FilebasedSchemaProvider."
        + "Source (See org.apache.hudi.utilities.sources.Source) implementation can implement their own SchemaProvider."
        + " For Sources that return Dataset<Row>, the schema is obtained implicitly. "
        + "However, this CLI option allows overriding the schemaprovider returned by Source.")
    public String schemaProviderClassName = null;

    @Parameter(names = {"--transformer-class"},
        description = "A subclass or a list of subclasses of org.apache.hudi.utilities.transform.Transformer"
            + ". Allows transforming raw source Dataset to a target Dataset (conforming to target schema) before "
            + "writing. Default : Not set. E:g - org.apache.hudi.utilities.transform.SqlQueryBasedTransformer (which "
            + "allows a SQL query templated to be passed as a transformation function). "
            + "Pass a comma-separated list of subclass names to chain the transformations. If there are two or more "
            + "transformers using the same config keys and expect different values for those keys, then transformer can include "
            + "an identifier. E:g - tr1:org.apache.hudi.utilities.transform.SqlQueryBasedTransformer. Here the identifier tr1 "
            + "can be used along with property key like `hoodie.streamer.transformer.sql.tr1` to identify properties related "
            + "to the transformer. So effective value for `hoodie.streamer.transformer.sql` is determined by key "
            + "`hoodie.streamer.transformer.sql.tr1` for this transformer. If identifier is used, it should "
            + "be specified for all the transformers. Further the order in which transformer is applied is determined by the occurrence "
            + "of transformer irrespective of the identifier used for the transformer. For example: In the configured value below "
            + "tr2:org.apache.hudi.utilities.transform.SqlQueryBasedTransformer,tr1:org.apache.hudi.utilities.transform.SqlQueryBasedTransformer "
            + ", tr2 is applied before tr1 based on order of occurrence."
    )
    public List<String> transformerClassNames = null;

    @Parameter(names = {"--source-limit"}, description = "Maximum amount of data to read from source. "
        + "Default: No limit, e.g: DFS-Source => max bytes to read, Kafka-Source => max events to read")
    public long sourceLimit = Long.MAX_VALUE;

    @Parameter(names = {"--op"}, description = "Takes one of these values : UPSERT (default), INSERT, "
        + "BULK_INSERT, INSERT_OVERWRITE, INSERT_OVERWRITE_TABLE, DELETE_PARTITION",
        converter = OperationConverter.class)
    public WriteOperationType operation = WriteOperationType.UPSERT;

    @Parameter(names = {"--filter-dupes"},
        description = "Should duplicate records from source be dropped/filtered out before insert/bulk-insert")
    public Boolean filterDupes = false;

    //will abandon in the future version, recommended use --enable-sync
    @Parameter(names = {"--enable-hive-sync"}, description = "Enable syncing to hive")
    public Boolean enableHiveSync = false;

    @Parameter(names = {"--enable-sync"}, description = "Enable syncing meta")
    public Boolean enableMetaSync = false;

    @Parameter(names = {"--force-empty-sync"}, description = "Force syncing meta even on empty commit")
    public Boolean forceEmptyMetaSync = false;

    @Parameter(names = {"--sync-tool-classes"}, description = "Meta sync client tool, using comma to separate multi tools")
    public String syncClientToolClassNames = HiveSyncTool.class.getName();

    @Parameter(names = {"--max-pending-compactions"},
        description = "Maximum number of outstanding inflight/requested compactions. Delta Sync will not happen unless"
            + "outstanding compactions is less than this number")
    public Integer maxPendingCompactions = 5;

    @Parameter(names = {"--max-pending-clustering"},
        description = "Maximum number of outstanding inflight/requested clustering. Delta Sync will not happen unless"
            + "outstanding clustering is less than this number")
    public Integer maxPendingClustering = 5;

    @Parameter(names = {"--continuous"}, description = "Hudi Streamer runs in continuous mode running"
        + " source-fetch -> Transform -> Hudi Write in loop")
    public Boolean continuousMode = false;

    @Parameter(names = {"--min-sync-interval-seconds"},
        description = "the min sync interval of each sync in continuous mode")
    public Integer minSyncIntervalSeconds = 0;

    @Parameter(names = {"--spark-master"},
        description = "spark master to use, if not defined inherits from your environment taking into "
            + "account Spark Configuration priority rules (e.g. not using spark-submit command).")
    public String sparkMaster = "";

    @Parameter(names = {"--commit-on-errors"}, description = "Commit even when some records failed to be written")
    public Boolean commitOnErrors = false;

    @Parameter(names = {"--delta-sync-scheduling-weight"},
        description = "Scheduling weight for delta sync as defined in "
            + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer deltaSyncSchedulingWeight = 1;

    @Parameter(names = {"--compact-scheduling-weight"}, description = "Scheduling weight for compaction as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer compactSchedulingWeight = 1;

    @Parameter(names = {"--delta-sync-scheduling-minshare"}, description = "Minshare for delta sync as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer deltaSyncSchedulingMinShare = 0;

    @Parameter(names = {"--compact-scheduling-minshare"}, description = "Minshare for compaction as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer compactSchedulingMinShare = 0;

    /**
     * Compaction is enabled for MoR table by default. This flag disables it
     */
    @Parameter(names = {"--disable-compaction"},
        description = "Compaction is enabled for MoR table by default. This flag disables it ")
    public Boolean forceDisableCompaction = false;

    /**
     * Resume Hudi Streamer from this checkpoint.
     */
    @Parameter(names = {"--checkpoint"}, description = "Resume Hudi Streamer from this checkpoint. \nIf --source-class specifies a class "
        + "that is a child class of org.apache.hudi.utilities.sources.HoodieIncrSource and the hoodie.table.version is 8 or higher, the "
        + "format is expected to be either `resumeFromInstantRequestTime: <checkpoint>` or `resumeFromInstantCompletionTime: <checkpoint>`. "
        + "In table version 8 and above internally we only support completion time based commit ordering. If we use resumeFromInstantCompletionTime "
        + "mode, the checkpoint will be reset to the instant with the corresponding completion time and resume. If we use resumeFromInstantRequestTime "
        + "hudi first finds the instant, fetch the completion time of it and resume from the completion time.")
    public String checkpoint = null;

    @Parameter(names = {"--initial-checkpoint-provider"}, description = "subclass of "
        + "org.apache.hudi.utilities.checkpointing.InitialCheckpointProvider. Generate check point for Hudi Streamer "
        + "for the first run. This field will override the checkpoint of last commit using the checkpoint field. "
        + "Use this field only when switching source, for example, from DFS source to Kafka Source.")
    public String initialCheckpointProvider = null;

    @Parameter(names = {"--run-bootstrap"}, description = "Run bootstrap if bootstrap index is not found")
    public Boolean runBootstrap = false;

    @Parameter(names = {"--bootstrap-overwrite"}, description = "Overwrite existing target table, default false")
    public Boolean bootstrapOverwrite = false;

    @Parameter(names = {"--bootstrap-index-class"}, description = "subclass of BootstrapIndex")
    public String bootstrapIndexClass = HFileBootstrapIndex.class.getName();

    @Parameter(names = {"--retry-on-source-failures"}, description = "Retry on any source failures")
    public Boolean retryOnSourceFailures = false;

    @Parameter(names = {"--retry-interval-seconds"}, description = "the retry interval for source failures if --retry-on-source-failures is enabled")
    public Integer retryIntervalSecs = 30;

    @Parameter(names = {"--max-retry-count"}, description = "the max retry count if --retry-on-source-failures is enabled")
    public Integer maxRetryCount = 3;

    @Parameter(names = {"--allow-commit-on-no-checkpoint-change"}, description = "allow commits even if checkpoint has not changed before and after fetch data"
        + "from source. This might be useful in sources like SqlSource where there is not checkpoint. And is not recommended to enable in continuous mode.")
    public Boolean allowCommitOnNoCheckpointChange = false;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Parameter(names = {"--retry-last-pending-inline-clustering", "-rc"}, description = "Retry last pending inline clustering plan before writing to sink.")
    public Boolean retryLastPendingInlineClusteringJob = false;

    @Parameter(names = {"--retry-last-pending-inline-compaction"}, description = "Retry last pending inline compaction plan before writing to sink.")
    public Boolean retryLastPendingInlineCompactionJob = false;

    @Parameter(names = {"--cluster-scheduling-weight"}, description = "Scheduling weight for clustering as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer clusterSchedulingWeight = 1;

    @Parameter(names = {"--cluster-scheduling-minshare"}, description = "Minshare for clustering as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer clusterSchedulingMinShare = 0;

    @Parameter(names = {"--post-write-termination-strategy-class"}, description = "Post writer termination strategy class to gracefully shutdown deltastreamer in continuous mode")
    public String postWriteTerminationStrategyClass = "";

    @Parameter(names = {"--ingestion-metrics-class"}, description = "Ingestion metrics class for reporting metrics during ingestion lifecycles.")
    public String ingestionMetricsClass = HoodieStreamerMetrics.class.getCanonicalName();

    @Parameter(names = {"--config-hot-update-strategy-class"}, description = "Configuration hot update in continuous mode")
    public String configHotUpdateStrategyClass = "";

    @Parameter(names = {"--ignore-checkpoint"}, description = "Set this config with a unique value, recommend using a timestamp value or UUID."
        + " Setting this config indicates that the subsequent sync should ignore the last committed checkpoint for the source. The config value is stored"
        + " in the commit history, so setting the config with same values would not have any affect. This config can be used in scenarios like kafka topic change,"
        + " where we would want to start ingesting from the latest or earliest offset after switching the topic (in this case we would want to ignore the previously"
        + " committed checkpoint, and rely on other configs to pick the starting offsets).")
    public String ignoreCheckpoint = null;

    @Parameter(names = {"--spark-app-name"},
            description = "spark app name to use while creating spark context."
                    + " If not defined then defaults to streamer-{cfg.targetTableName}.")
    public String sparkAppName = "";

    public boolean isAsyncCompactionEnabled() {
      return continuousMode && !forceDisableCompaction
          && HoodieTableType.MERGE_ON_READ.equals(HoodieTableType.valueOf(tableType));
    }

    public boolean isInlineCompactionEnabled() {
      // Inline compaction is disabled for continuous mode, otherwise enabled for MOR
      return !continuousMode && !forceDisableCompaction
          && HoodieTableType.MERGE_ON_READ.equals(HoodieTableType.valueOf(tableType));
    }

    public static TypedProperties getProps(Configuration conf, Config cfg) {
      return cfg.propsFilePath.isEmpty() || cfg.propsFilePath.equals(DEFAULT_DFS_SOURCE_PROPERTIES)
          ? buildProperties(cfg.configs)
          : readConfig(conf, new Path(cfg.propsFilePath), cfg.configs).getProps();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Config config = (Config) o;
      return sourceLimit == config.sourceLimit
          && Objects.equals(targetBasePath, config.targetBasePath)
          && Objects.equals(targetTableName, config.targetTableName)
          && Objects.equals(tableType, config.tableType)
          && Objects.equals(baseFileFormat, config.baseFileFormat)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs)
          && Objects.equals(sourceClassName, config.sourceClassName)
          && Objects.equals(sourceOrderingFields, config.sourceOrderingFields)
          && Objects.equals(payloadClassName, config.payloadClassName)
          && Objects.equals(schemaProviderClassName, config.schemaProviderClassName)
          && Objects.equals(transformerClassNames, config.transformerClassNames)
          && operation == config.operation
          && Objects.equals(filterDupes, config.filterDupes)
          && Objects.equals(enableHiveSync, config.enableHiveSync)
          && Objects.equals(enableMetaSync, config.enableMetaSync)
          && Objects.equals(forceEmptyMetaSync, config.forceEmptyMetaSync)
          && Objects.equals(syncClientToolClassNames, config.syncClientToolClassNames)
          && Objects.equals(maxPendingCompactions, config.maxPendingCompactions)
          && Objects.equals(maxPendingClustering, config.maxPendingClustering)
          && Objects.equals(continuousMode, config.continuousMode)
          && Objects.equals(minSyncIntervalSeconds, config.minSyncIntervalSeconds)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(commitOnErrors, config.commitOnErrors)
          && Objects.equals(deltaSyncSchedulingWeight, config.deltaSyncSchedulingWeight)
          && Objects.equals(compactSchedulingWeight, config.compactSchedulingWeight)
          && Objects.equals(clusterSchedulingWeight, config.clusterSchedulingWeight)
          && Objects.equals(deltaSyncSchedulingMinShare, config.deltaSyncSchedulingMinShare)
          && Objects.equals(compactSchedulingMinShare, config.compactSchedulingMinShare)
          && Objects.equals(clusterSchedulingMinShare, config.clusterSchedulingMinShare)
          && Objects.equals(forceDisableCompaction, config.forceDisableCompaction)
          && Objects.equals(checkpoint, config.checkpoint)
          && Objects.equals(initialCheckpointProvider, config.initialCheckpointProvider)
          && Objects.equals(ingestionMetricsClass, config.ingestionMetricsClass)
          && Objects.equals(help, config.help);
    }

    @Override
    public int hashCode() {
      return Objects.hash(targetBasePath, targetTableName, tableType,
          baseFileFormat, propsFilePath, configs, sourceClassName,
          sourceOrderingFields, payloadClassName, schemaProviderClassName,
          transformerClassNames, sourceLimit, operation, filterDupes,
          enableHiveSync, enableMetaSync, forceEmptyMetaSync, syncClientToolClassNames, maxPendingCompactions, maxPendingClustering,
          continuousMode, minSyncIntervalSeconds, sparkMaster, commitOnErrors,
          deltaSyncSchedulingWeight, compactSchedulingWeight, clusterSchedulingWeight, deltaSyncSchedulingMinShare,
          compactSchedulingMinShare, clusterSchedulingMinShare, forceDisableCompaction, checkpoint,
          initialCheckpointProvider, ingestionMetricsClass, help);
    }

    @Override
    public String toString() {
      return "Config{"
          + "targetBasePath='" + targetBasePath + '\''
          + ", targetTableName='" + targetTableName + '\''
          + ", tableType='" + tableType + '\''
          + ", baseFileFormat='" + baseFileFormat + '\''
          + ", propsFilePath='" + propsFilePath + '\''
          + ", configs=" + configs
          + ", sourceClassName='" + sourceClassName + '\''
          + ", sourceOrderingField='" + sourceOrderingFields + '\''
          + ", payloadClassName='" + payloadClassName + '\''
          + ", schemaProviderClassName='" + schemaProviderClassName + '\''
          + ", transformerClassNames=" + transformerClassNames
          + ", sourceLimit=" + sourceLimit
          + ", operation=" + operation
          + ", filterDupes=" + filterDupes
          + ", enableHiveSync=" + enableHiveSync
          + ", enableMetaSync=" + enableMetaSync
          + ", forceEmptyMetaSync=" + forceEmptyMetaSync
          + ", syncClientToolClassNames=" + syncClientToolClassNames
          + ", maxPendingCompactions=" + maxPendingCompactions
          + ", maxPendingClustering=" + maxPendingClustering
          + ", continuousMode=" + continuousMode
          + ", minSyncIntervalSeconds=" + minSyncIntervalSeconds
          + ", sparkMaster='" + sparkMaster + '\''
          + ", commitOnErrors=" + commitOnErrors
          + ", deltaSyncSchedulingWeight=" + deltaSyncSchedulingWeight
          + ", compactSchedulingWeight=" + compactSchedulingWeight
          + ", clusterSchedulingWeight=" + clusterSchedulingWeight
          + ", deltaSyncSchedulingMinShare=" + deltaSyncSchedulingMinShare
          + ", compactSchedulingMinShare=" + compactSchedulingMinShare
          + ", clusterSchedulingMinShare=" + clusterSchedulingMinShare
          + ", forceDisableCompaction=" + forceDisableCompaction
          + ", checkpoint='" + checkpoint + '\''
          + ", initialCheckpointProvider='" + initialCheckpointProvider + '\''
          + ", ingestionMetricsClass='" + ingestionMetricsClass + '\''
          + ", help=" + help
          + '}';
    }
  }

  public static String toSortedTruncatedString(TypedProperties props) {
    List<String> sensitiveConfigList = props.getStringList(HoodieWriteConfig.SENSITIVE_CONFIG_KEYS_FILTER.key(),
        ",", DEFAULT_SENSITIVE_CONFIG_KEYS);

    List<String> allKeys = new ArrayList<>();
    for (Object k : props.keySet()) {
      allKeys.add(k.toString());
    }
    Collections.sort(allKeys);
    StringBuilder propsLog = new StringBuilder("Creating Hudi Streamer with configs:\n");
    for (String key : allKeys) {
      String value = Option.ofNullable(props.get(key)).orElse("").toString();
      // Truncate too long values.
      if (value.length() > 255 && !LOG.isDebugEnabled()) {
        value = value.substring(0, 128) + "[...]";
      }

      // Mask values for security/ credentials and other sensitive configs
      if (sensitiveConfigList.stream().anyMatch(key::contains)) {
        value = SENSITIVE_VALUES_MASKED;
      }

      propsLog.append(key).append(": ").append(value).append("\n");
    }
    return propsLog.toString();
  }

  public static final Config getConfig(String[] args) {
    Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    return cfg;
  }

  public static void main(String[] args) throws Exception {
    final Config cfg = getConfig(args);
    Map<String, String> additionalSparkConfigs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    JavaSparkContext jssc = null;
    String sparkAppName;
    if (!StringUtils.isNullOrEmpty(cfg.sparkAppName)) {
      sparkAppName = cfg.sparkAppName;
    } else {
      sparkAppName = "streamer-" + cfg.targetTableName;
    }
    if (StringUtils.isNullOrEmpty(cfg.sparkMaster)) {
      jssc = UtilHelpers.buildSparkContext(sparkAppName, additionalSparkConfigs);
    } else {
      jssc = UtilHelpers.buildSparkContext(sparkAppName, cfg.sparkMaster, additionalSparkConfigs);
    }
    if (cfg.enableHiveSync) {
      LOG.warn("--enable-hive-sync will be deprecated in a future release; please use --enable-sync instead for Hive syncing");
    }

    int exitCode = 0;
    try {
      new HoodieStreamer(cfg, jssc).sync();
    } catch (Throwable throwable) {
      exitCode = 1;
      throw new HoodieException("Failed to run HoodieStreamer ", throwable);
    } finally {
      SparkAdapterSupport$.MODULE$.sparkAdapter().stopSparkContext(jssc, exitCode);
    }
  }

  /**
   * Syncs data either in single-run or in continuous mode.
   */
  public static class StreamSyncService extends HoodieIngestionService {

    private static final long serialVersionUID = 1L;
    /**
     * Delta Sync Config.
     */
    private final HoodieStreamer.Config cfg;

    /**
     * Schema provider that supplies the command for reading the input and writing out the target table.
     */
    private transient SchemaProvider schemaProvider;

    /**
     * Spark Session.
     */
    private transient SparkSession sparkSession;

    /**
     * Spark context Wrapper.
     */

    private final transient HoodieSparkEngineContext hoodieSparkContext;

    private transient HoodieStorage storage;

    private transient Configuration hiveConf;

    /**
     * Bag of properties with source, hoodie client, key generator etc.
     */
    TypedProperties props;

    /**
     * Async Compactor Service.
     */
    private Option<AsyncCompactService> asyncCompactService;

    /**
     * Async clustering service.
     */
    private Option<AsyncClusteringService> asyncClusteringService;

    /**
     * Table Type.
     */
    private HoodieTableType tableType;

    /**
     * Delta Sync.
     */
    private transient StreamSync streamSync;

    private final Option<PostWriteTerminationStrategy> postWriteTerminationStrategy;

    private final Option<ConfigurationHotUpdateStrategy> configurationHotUpdateStrategyOpt;

    public StreamSyncService(Config cfg, HoodieSparkEngineContext hoodieSparkContext,
                             FileSystem fs, Configuration conf,
                             Option<TypedProperties> properties, Option<SourceProfileSupplier> sourceProfileSupplier) throws IOException {
      super(HoodieIngestionConfig.newBuilder()
          .isContinuous(cfg.continuousMode)
          .withMinSyncInternalSeconds(cfg.minSyncIntervalSeconds).build());
      this.cfg = cfg;
      this.hoodieSparkContext = hoodieSparkContext;
      this.storage = new HoodieHadoopStorage(fs);
      this.hiveConf = conf;
      this.sparkSession = SparkSession.builder().config(hoodieSparkContext.getConf()).getOrCreate();
      this.asyncCompactService = Option.empty();
      this.asyncClusteringService = Option.empty();
      this.postWriteTerminationStrategy = StringUtils.isNullOrEmpty(cfg.postWriteTerminationStrategyClass) ? Option.empty() :
          TerminationStrategyUtils.createPostWriteTerminationStrategy(properties.get(), cfg.postWriteTerminationStrategyClass);
      this.configurationHotUpdateStrategyOpt = StringUtils.isNullOrEmpty(cfg.configHotUpdateStrategyClass) ? Option.empty() :
          ConfigurationHotUpdateStrategyUtils.createConfigurationHotUpdateStrategy(cfg.configHotUpdateStrategyClass, cfg, properties.get());

      if (this.storage.exists(new StoragePath(cfg.targetBasePath))) {
        try {
          HoodieTableMetaClient meta = HoodieTableMetaClient.builder()
              .setConf(this.storage.getConf().newInstance())
              .setBasePath(cfg.targetBasePath).setLoadActiveTimelineOnLoad(false).build();
          tableType = meta.getTableType();
          // This will guarantee there is no surprise with table type
          checkArgument(tableType.equals(HoodieTableType.valueOf(cfg.tableType)), "Hoodie table is of type " + tableType + " but passed in CLI argument is " + cfg.tableType);

          if (cfg.baseFileFormat == null) {
            cfg.baseFileFormat = "PARQUET"; // default for backward compatibility
          }
          Map<String, String> propsToValidate = new HashMap<>();
          properties.get().forEach((k, v) -> propsToValidate.put(k.toString(), v.toString()));
          HoodieWriterUtils.validateTableConfig(this.sparkSession, org.apache.hudi.HoodieConversionUtils.mapAsScalaImmutableMap(propsToValidate), meta.getTableConfig());
        } catch (HoodieIOException e) {
          LOG.warn("Full exception msg " + e.getLocalizedMessage() + ",  msg " + e.getMessage());
          if (e.getMessage().contains("Could not load Hoodie properties") && e.getMessage().contains(HoodieTableConfig.HOODIE_PROPERTIES_FILE)) {
            initializeTableTypeAndBaseFileFormat();
          } else {
            throw e;
          }
        }
      } else {
        initializeTableTypeAndBaseFileFormat();
      }

      checkArgument(!cfg.filterDupes || cfg.operation != WriteOperationType.UPSERT,
          "'--filter-dupes' needs to be disabled when '--op' is 'UPSERT' to ensure updates are not missed.");

      this.props = properties.get();
      LOG.info(toSortedTruncatedString(props));

      this.schemaProvider = UtilHelpers.wrapSchemaProviderWithPostProcessor(

          UtilHelpers.createSchemaProvider(cfg.schemaProviderClassName, props, hoodieSparkContext.jsc()),
          props, hoodieSparkContext.jsc(), cfg.transformerClassNames);

      streamSync = new StreamSync(cfg, sparkSession, props, hoodieSparkContext,
          fs, conf, this::onInitializingWriteClient, new DefaultStreamContext(schemaProvider, sourceProfileSupplier));
    }

    public StreamSyncService(HoodieStreamer.Config cfg,
                             HoodieSparkEngineContext hoodieSparkContext, FileSystem fs,
                             Configuration conf)
        throws IOException {
      this(cfg, hoodieSparkContext, fs, conf, Option.empty(), Option.empty());
    }

    public StreamSyncService(HoodieStreamer.Config cfg, HoodieSparkEngineContext hoodieSparkContext, FileSystem fs, Configuration conf, Option<TypedProperties> properties)
            throws IOException {
      this(cfg, hoodieSparkContext, fs, conf, properties, Option.empty());
    }

    private void initializeTableTypeAndBaseFileFormat() {
      tableType = HoodieTableType.valueOf(cfg.tableType);
      if (cfg.baseFileFormat == null) {
        cfg.baseFileFormat = "PARQUET"; // default for backward compatibility
      }
    }

    private void reInitDeltaSync() throws IOException {
      if (streamSync != null) {
        streamSync.close();
      }
      streamSync = new StreamSync(cfg, sparkSession, props, hoodieSparkContext,
          (FileSystem) storage.getFileSystem(), hiveConf, this::onInitializingWriteClient,
          new DefaultStreamContext(schemaProvider, Option.empty()));
    }

    @Override
    protected Pair<CompletableFuture, ExecutorService> startService() {
      ExecutorService executor = Executors.newFixedThreadPool(1);
      return Pair.of(CompletableFuture.supplyAsync(() -> {
        boolean error = false;
        if (cfg.isAsyncCompactionEnabled()) {
          // set Scheduler Pool.
          LOG.info("Setting Spark Pool name for delta-sync to " + STREAMSYNC_POOL_NAME);
          hoodieSparkContext.setProperty(EngineProperty.DELTASYNC_POOL_NAME, STREAMSYNC_POOL_NAME);
        }

        HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.from(props);
        try {
          while (!isShutdownRequested()) {
            try {
              long start = System.currentTimeMillis();
              // Send a heartbeat metrics event to track the active ingestion job for this table.
              streamSync.getMetrics().updateStreamerHeartbeatTimestamp(start);
              // check if deltastreamer need to update the configuration before the sync
              if (configurationHotUpdateStrategyOpt.isPresent()) {
                Option<TypedProperties> newProps = configurationHotUpdateStrategyOpt.get().updateProperties(props);
                if (newProps.isPresent()) {
                  this.props = newProps.get();
                  // reinit the DeltaSync only when the props updated
                  LOG.info("Re-init delta sync with new config properties:");
                  LOG.info(toSortedTruncatedString(props));
                  reInitDeltaSync();
                }
              }
              Option<Pair<Option<String>, JavaRDD<WriteStatus>>> scheduledCompactionInstantAndRDD = Option.ofNullable(streamSync.syncOnce());
              if (scheduledCompactionInstantAndRDD.isPresent() && scheduledCompactionInstantAndRDD.get().getLeft().isPresent()) {
                LOG.info("Enqueuing new pending compaction instant (" + scheduledCompactionInstantAndRDD.get().getLeft() + ")");
                asyncCompactService.get().enqueuePendingAsyncServiceInstant(scheduledCompactionInstantAndRDD.get().getLeft().get());
                asyncCompactService.get().waitTillPendingAsyncServiceInstantsReducesTo(cfg.maxPendingCompactions);
                if (asyncCompactService.get().hasError()) {
                  error = true;
                  throw new HoodieException("Async compaction failed.  Shutting down Delta Sync...");
                }
              }
              if (clusteringConfig.isAsyncClusteringEnabled()) {
                Option<String> clusteringInstant = streamSync.getClusteringInstantOpt();
                if (clusteringInstant.isPresent()) {
                  LOG.info("Scheduled async clustering for instant: " + clusteringInstant.get());
                  asyncClusteringService.get().enqueuePendingAsyncServiceInstant(clusteringInstant.get());
                  asyncClusteringService.get().waitTillPendingAsyncServiceInstantsReducesTo(cfg.maxPendingClustering);
                  if (asyncClusteringService.get().hasError()) {
                    error = true;
                    throw new HoodieException("Async clustering failed.  Shutting down Delta Sync...");
                  }
                }
              }
              // check if deltastreamer need to be shutdown
              Option<HoodieData<WriteStatus>> lastWriteStatuses = Option.ofNullable(
                  scheduledCompactionInstantAndRDD.isPresent() ? HoodieJavaRDD.of(scheduledCompactionInstantAndRDD.get().getRight()) : null);
              if (requestShutdownIfNeeded(lastWriteStatuses)) {
                LOG.warn("Closing and shutting down ingestion service");
                error = true;
                onIngestionCompletes(false);
                shutdown(true);
              } else {
                sleepBeforeNextIngestion(start);
              }
            } catch (HoodieUpsertException ue) {
              handleUpsertException(ue);
            } catch (Exception e) {
              LOG.error("Shutting down delta-sync due to exception", e);
              error = true;
              throw new HoodieException(e.getMessage(), e);
            }
          }
        } finally {
          shutdownAsyncServices(error);
          executor.shutdownNow();
        }
        return true;
      }, executor), executor);
    }

    private void handleUpsertException(HoodieUpsertException ue) {
      if (ue.getCause() instanceof HoodieClusteringUpdateException) {
        LOG.warn("Write rejected due to conflicts with pending clustering operation. Going to retry after 1 min with the hope "
            + "that clustering will complete by then.", ue);
        try {
          Thread.sleep(60000); // Intentionally not using cfg.minSyncIntervalSeconds, since it could be too high or it could be 0.
          // Once the Hudi Streamer gets past this clustering update exception, regular syncs will honor cfg.minSyncIntervalSeconds.
        } catch (InterruptedException e) {
          throw new HoodieException("Deltastreamer interrupted while waiting for next round ", e);
        }
      } else {
        throw ue;
      }
    }

    /**
     * Shutdown async services like compaction/clustering as DeltaSync is shutdown.
     */
    private void shutdownAsyncServices(boolean error) {
      LOG.info("Delta Sync shutdown. Error ?" + error);
      if (asyncCompactService.isPresent()) {
        LOG.warn("Gracefully shutting down compactor");
        asyncCompactService.get().shutdown(false);
      }
      if (asyncClusteringService.isPresent()) {
        LOG.warn("Gracefully shutting down clustering service");
        asyncClusteringService.get().shutdown(false);
      }
    }

    @Override
    public void ingestOnce() {
      try {
        streamSync.syncOnce();
      } catch (IOException e) {
        throw new HoodieIngestionException(String.format("Ingestion via %s failed with exception.", this.getClass()), e);
      } finally {
        close();
      }
    }

    @Override
    protected boolean requestShutdownIfNeeded(Option<HoodieData<WriteStatus>> lastWriteStatuses) {
      if (postWriteTerminationStrategy.isPresent()) {
        Option<JavaRDD<WriteStatus>> lastWriteStatusRDD = lastWriteStatuses.map(HoodieJavaRDD::getJavaRDD);
        return postWriteTerminationStrategy.get().shouldShutdown(lastWriteStatusRDD);
      }
      return false;
    }

    /**
     * Callback to initialize write client and start compaction service if required.
     *
     * @param writeClient HoodieWriteClient
     * @return
     */
    protected Boolean onInitializingWriteClient(SparkRDDWriteClient writeClient) {
      if (cfg.isAsyncCompactionEnabled()) {
        if (asyncCompactService.isPresent()) {
          // Update the write client used by Async Compactor.
          asyncCompactService.get().updateWriteClient(writeClient);
        } else {
          asyncCompactService = Option.ofNullable(new SparkAsyncCompactService(hoodieSparkContext, writeClient));
          // Enqueue existing pending compactions first
          HoodieTableMetaClient meta = HoodieTableMetaClient.builder()
              .setConf(HadoopFSUtils.getStorageConfWithCopy(hoodieSparkContext.hadoopConfiguration()))
              .setBasePath(cfg.targetBasePath).setLoadActiveTimelineOnLoad(true).build();
          List<HoodieInstant> pending = CompactionUtils.getPendingCompactionInstantTimes(meta);
          pending.forEach(hoodieInstant -> asyncCompactService.get().enqueuePendingAsyncServiceInstant(hoodieInstant.requestedTime()));
          asyncCompactService.get().start(error -> true);
          try {
            asyncCompactService.get().waitTillPendingAsyncServiceInstantsReducesTo(cfg.maxPendingCompactions);
            if (asyncCompactService.get().hasError()) {
              throw new HoodieException("Async compaction failed during write client initialization.");
            }
          } catch (InterruptedException ie) {
            throw new HoodieException(ie);
          }
        }
      }
      // start async clustering if required
      if (HoodieClusteringConfig.from(props).isAsyncClusteringEnabled()) {
        if (asyncClusteringService.isPresent()) {
          asyncClusteringService.get().updateWriteClient(writeClient);
        } else {
          asyncClusteringService = Option.ofNullable(new SparkAsyncClusteringService(hoodieSparkContext, writeClient));
          HoodieTableMetaClient meta = HoodieTableMetaClient.builder()
              .setConf(HadoopFSUtils.getStorageConfWithCopy(hoodieSparkContext.hadoopConfiguration()))
              .setBasePath(cfg.targetBasePath)
              .setLoadActiveTimelineOnLoad(true).build();
          List<HoodieInstant> pending = ClusteringUtils.getPendingClusteringInstantTimes(meta);
          LOG.info(format("Found %d pending clustering instants ", pending.size()));
          pending.forEach(hoodieInstant -> asyncClusteringService.get().enqueuePendingAsyncServiceInstant(hoodieInstant.requestedTime()));
          asyncClusteringService.get().start(error -> true);
          try {
            asyncClusteringService.get().waitTillPendingAsyncServiceInstantsReducesTo(cfg.maxPendingClustering);
            if (asyncClusteringService.get().hasError()) {
              throw new HoodieException("Async clustering failed during write client initialization.");
            }
          } catch (InterruptedException e) {
            throw new HoodieException(e);
          }
        }
      }
      return true;
    }

    @Override
    protected boolean onIngestionCompletes(boolean hasError) {
      LOG.info("Ingestion completed. Has error: " + hasError);
      close();
      return true;
    }

    @Override
    public Option<HoodieIngestionMetrics> getMetrics() {
      return Option.ofNullable(streamSync.getMetrics());
    }

    @Override
    public void close() {
      if (streamSync != null) {
        streamSync.close();
      }
    }

    public SchemaProvider getSchemaProvider() {
      return schemaProvider;
    }

    public SparkSession getSparkSession() {
      return sparkSession;
    }

    public TypedProperties getProps() {
      return props;
    }

    @VisibleForTesting
    public HoodieSparkEngineContext getHoodieSparkContext() {
      return hoodieSparkContext;
    }

    @VisibleForTesting
    public StreamSync getStreamSync() {
      return streamSync;
    }
  }

  @VisibleForTesting
  public HoodieIngestionService getIngestionService() {
    return ingestionService.get();
  }
}
