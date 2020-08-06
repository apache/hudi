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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.async.AbstractAsyncService;
import org.apache.hudi.async.AsyncCompactService;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.utilities.IdentitySplitter;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.utilities.HiveIncrementalPuller;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.checkpointing.InitialCheckPointProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.JsonDFSSource;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * An Utility which can incrementally take the output from {@link HiveIncrementalPuller} and apply it to the target
 * table. Does not maintain any state, queries at runtime to see how far behind the target table is from the source
 * table. This can be overriden to force sync from a timestamp.
 * <p>
 * In continuous mode, DeltaStreamer runs in loop-mode going through the below operations (a) pull-from-source (b)
 * write-to-sink (c) Schedule Compactions if needed (d) Conditionally Sync to Hive each cycle. For MOR table with
 * continuous mode enabled, a separate compactor thread is allocated to execute compactions
 */
public class HoodieDeltaStreamer implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(HoodieDeltaStreamer.class);

  public static final String CHECKPOINT_KEY = "deltastreamer.checkpoint.key";
  public static final String CHECKPOINT_RESET_KEY = "deltastreamer.checkpoint.reset_key";

  protected final transient Config cfg;

  private final TypedProperties properties;

  protected transient Option<DeltaSyncService> deltaSyncService;

  private final Option<BootstrapExecutor> bootstrapExecutor;

  public static final String DELTASYNC_POOL_NAME = "hoodiedeltasync";

  public HoodieDeltaStreamer(Config cfg, JavaSparkContext jssc) throws IOException {
    this(cfg, jssc, FSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration()),
        jssc.hadoopConfiguration(), null);
  }

  public HoodieDeltaStreamer(Config cfg, JavaSparkContext jssc, TypedProperties props) throws IOException {
    this(cfg, jssc, FSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration()),
        jssc.hadoopConfiguration(), props);
  }

  public HoodieDeltaStreamer(Config cfg, JavaSparkContext jssc, FileSystem fs, Configuration conf) throws IOException {
    this(cfg, jssc, fs, conf, null);
  }

  public HoodieDeltaStreamer(Config cfg, JavaSparkContext jssc, FileSystem fs, Configuration conf,
                             TypedProperties props) throws IOException {
    // Resolving the properties first in a consistent way
    this.properties = props != null ? props : UtilHelpers.readConfig(
        FSUtils.getFs(cfg.propsFilePath, jssc.hadoopConfiguration()),
        new Path(cfg.propsFilePath), cfg.configs).getConfig();

    if (cfg.initialCheckpointProvider != null && cfg.checkpoint == null) {
      InitialCheckPointProvider checkPointProvider =
          UtilHelpers.createInitialCheckpointProvider(cfg.initialCheckpointProvider, this.properties);
      checkPointProvider.init(conf);
      cfg.checkpoint = checkPointProvider.getCheckpoint();
    }
    this.cfg = cfg;
    this.bootstrapExecutor = Option.ofNullable(
        cfg.runBootstrap ? new BootstrapExecutor(cfg, jssc, fs, conf, this.properties) : null);
    this.deltaSyncService = Option.ofNullable(
        cfg.runBootstrap ? null : new DeltaSyncService(cfg, jssc, fs, conf, this.properties));
  }

  public void shutdownGracefully() {
    deltaSyncService.ifPresent(ds -> ds.shutdown(false));
  }

  /**
   * Main method to start syncing.
   *
   * @throws Exception
   */
  public void sync() throws Exception {
    if (bootstrapExecutor.isPresent()) {
      LOG.info("Performing bootstrap. Source=" + bootstrapExecutor.get().getBootstrapConfig().getBootstrapSourceBasePath());
      bootstrapExecutor.get().execute();
    } else {
      if (cfg.continuousMode) {
        deltaSyncService.ifPresent(ds -> {
          ds.start(this::onDeltaSyncShutdown);
          try {
            ds.waitForShutdown();
          } catch (Exception e) {
            throw new HoodieException(e.getMessage(), e);
          }
        });
        LOG.info("Delta Sync shutting down");
      } else {
        LOG.info("Delta Streamer running only single round");
        try {
          deltaSyncService.ifPresent(ds -> {
            try {
              ds.getDeltaSync().syncOnce();
            } catch (IOException e) {
              throw new HoodieIOException(e.getMessage(), e);
            }
          });
        } catch (Exception ex) {
          LOG.error("Got error running delta sync once. Shutting down", ex);
          throw ex;
        } finally {
          deltaSyncService.ifPresent(DeltaSyncService::close);
          LOG.info("Shut down delta streamer");
        }
      }
    }
  }

  public Config getConfig() {
    return cfg;
  }

  private boolean onDeltaSyncShutdown(boolean error) {
    LOG.info("DeltaSync shutdown. Closing write client. Error?" + error);
    deltaSyncService.ifPresent(DeltaSyncService::close);
    return true;
  }

  public enum Operation {
    UPSERT, INSERT, BULK_INSERT
  }

  protected static class OperationConverter implements IStringConverter<Operation> {

    @Override
    public Operation convert(String value) throws ParameterException {
      return Operation.valueOf(value);
    }
  }

  public static class Config implements Serializable {

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
    public String propsFilePath =
        "file://" + System.getProperty("user.dir") + "/src/test/resources/delta-streamer-config/dfs-source.properties";

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
            splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--source-class"},
        description = "Subclass of org.apache.hudi.utilities.sources to read data. "
            + "Built-in options: org.apache.hudi.utilities.sources.{JsonDFSSource (default), AvroDFSSource, "
            + "JsonKafkaSource, AvroKafkaSource, HiveIncrPullSource}")
    public String sourceClassName = JsonDFSSource.class.getName();

    @Parameter(names = {"--source-ordering-field"}, description = "Field within source record to decide how"
        + " to break ties between records with same key in input data. Default: 'ts' holding unix timestamp of record")
    public String sourceOrderingField = "ts";

    @Parameter(names = {"--payload-class"}, description = "subclass of HoodieRecordPayload, that works off "
        + "a GenericRecord. Implement your own, if you want to do something other than overwriting existing value")
    public String payloadClassName = OverwriteWithLatestAvroPayload.class.getName();

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
            + "Pass a comma-separated list of subclass names to chain the transformations.")
    public List<String> transformerClassNames = null;

    @Parameter(names = {"--source-limit"}, description = "Maximum amount of data to read from source. "
        + "Default: No limit, e.g: DFS-Source => max bytes to read, Kafka-Source => max events to read")
    public long sourceLimit = Long.MAX_VALUE;

    @Parameter(names = {"--op"}, description = "Takes one of these values : UPSERT (default), INSERT (use when input "
        + "is purely new data/inserts to gain speed)", converter = OperationConverter.class)
    public Operation operation = Operation.UPSERT;

    @Parameter(names = {"--filter-dupes"},
        description = "Should duplicate records from source be dropped/filtered out before insert/bulk-insert")
    public Boolean filterDupes = false;

    //will abandon in the future version, recommended use --enable-sync
    @Parameter(names = {"--enable-hive-sync"}, description = "Enable syncing to hive")
    public Boolean enableHiveSync = false;

    @Parameter(names = {"--enable-sync"}, description = "Enable syncing meta")
    public Boolean enableMetaSync = false;

    @Parameter(names = {"--sync-tool-classes"}, description = "Meta sync client tool, using comma to separate multi tools")
    public String syncClientToolClass = HiveSyncTool.class.getName();

    @Parameter(names = {"--max-pending-compactions"},
        description = "Maximum number of outstanding inflight/requested compactions. Delta Sync will not happen unless"
            + "outstanding compactions is less than this number")
    public Integer maxPendingCompactions = 5;

    @Parameter(names = {"--continuous"}, description = "Delta Streamer runs in continuous mode running"
        + " source-fetch -> Transform -> Hudi Write in loop")
    public Boolean continuousMode = false;

    @Parameter(names = {"--min-sync-interval-seconds"},
        description = "the min sync interval of each sync in continuous mode")
    public Integer minSyncIntervalSeconds = 0;

    @Parameter(names = {"--spark-master"}, description = "spark master to use.")
    public String sparkMaster = "local[2]";

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
     * Resume Delta Streamer from this checkpoint.
     */
    @Parameter(names = {"--checkpoint"}, description = "Resume Delta Streamer from this checkpoint.")
    public String checkpoint = null;

    @Parameter(names = {"--initial-checkpoint-provider"}, description = "subclass of "
        + "org.apache.hudi.utilities.checkpointing.InitialCheckpointProvider. Generate check point for delta streamer "
        + "for the first run. This field will override the checkpoint of last commit using the checkpoint field. "
        + "Use this field only when switching source, for example, from DFS source to Kafka Source.")
    public String initialCheckpointProvider = null;

    @Parameter(names = {"--run-bootstrap"}, description = "Run bootstrap if bootstrap index is not found")
    public Boolean runBootstrap = false;

    @Parameter(names = {"--bootstrap-index-class"}, description = "subclass of BootstrapIndex")
    public String bootstrapIndexClass = HFileBootstrapIndex.class.getName();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    public boolean isAsyncCompactionEnabled() {
      return continuousMode && !forceDisableCompaction
          && HoodieTableType.MERGE_ON_READ.equals(HoodieTableType.valueOf(tableType));
    }

    public boolean isInlineCompactionEnabled() {
      return !continuousMode && !forceDisableCompaction
          && HoodieTableType.MERGE_ON_READ.equals(HoodieTableType.valueOf(tableType));
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
              && Objects.equals(sourceOrderingField, config.sourceOrderingField)
              && Objects.equals(payloadClassName, config.payloadClassName)
              && Objects.equals(schemaProviderClassName, config.schemaProviderClassName)
              && Objects.equals(transformerClassNames, config.transformerClassNames)
              && operation == config.operation
              && Objects.equals(filterDupes, config.filterDupes)
              && Objects.equals(enableHiveSync, config.enableHiveSync)
              && Objects.equals(maxPendingCompactions, config.maxPendingCompactions)
              && Objects.equals(continuousMode, config.continuousMode)
              && Objects.equals(minSyncIntervalSeconds, config.minSyncIntervalSeconds)
              && Objects.equals(sparkMaster, config.sparkMaster)
              && Objects.equals(commitOnErrors, config.commitOnErrors)
              && Objects.equals(deltaSyncSchedulingWeight, config.deltaSyncSchedulingWeight)
              && Objects.equals(compactSchedulingWeight, config.compactSchedulingWeight)
              && Objects.equals(deltaSyncSchedulingMinShare, config.deltaSyncSchedulingMinShare)
              && Objects.equals(compactSchedulingMinShare, config.compactSchedulingMinShare)
              && Objects.equals(forceDisableCompaction, config.forceDisableCompaction)
              && Objects.equals(checkpoint, config.checkpoint)
              && Objects.equals(initialCheckpointProvider, config.initialCheckpointProvider)
              && Objects.equals(help, config.help);
    }

    @Override
    public int hashCode() {
      return Objects.hash(targetBasePath, targetTableName, tableType,
              baseFileFormat, propsFilePath, configs, sourceClassName,
              sourceOrderingField, payloadClassName, schemaProviderClassName,
              transformerClassNames, sourceLimit, operation, filterDupes,
              enableHiveSync, maxPendingCompactions, continuousMode,
              minSyncIntervalSeconds, sparkMaster, commitOnErrors,
              deltaSyncSchedulingWeight, compactSchedulingWeight, deltaSyncSchedulingMinShare,
              compactSchedulingMinShare, forceDisableCompaction, checkpoint,
              initialCheckpointProvider, help);
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
              + ", sourceOrderingField='" + sourceOrderingField + '\''
              + ", payloadClassName='" + payloadClassName + '\''
              + ", schemaProviderClassName='" + schemaProviderClassName + '\''
              + ", transformerClassNames=" + transformerClassNames
              + ", sourceLimit=" + sourceLimit
              + ", operation=" + operation
              + ", filterDupes=" + filterDupes
              + ", enableHiveSync=" + enableHiveSync
              + ", maxPendingCompactions=" + maxPendingCompactions
              + ", continuousMode=" + continuousMode
              + ", minSyncIntervalSeconds=" + minSyncIntervalSeconds
              + ", sparkMaster='" + sparkMaster + '\''
              + ", commitOnErrors=" + commitOnErrors
              + ", deltaSyncSchedulingWeight=" + deltaSyncSchedulingWeight
              + ", compactSchedulingWeight=" + compactSchedulingWeight
              + ", deltaSyncSchedulingMinShare=" + deltaSyncSchedulingMinShare
              + ", compactSchedulingMinShare=" + compactSchedulingMinShare
              + ", forceDisableCompaction=" + forceDisableCompaction
              + ", checkpoint='" + checkpoint + '\''
              + ", initialCheckpointProvider='" + initialCheckpointProvider + '\''
              + ", help=" + help
              + '}';
    }
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
    JavaSparkContext jssc =
        UtilHelpers.buildSparkContext("delta-streamer-" + cfg.targetTableName, cfg.sparkMaster, additionalSparkConfigs);

    if (cfg.enableHiveSync) {
      LOG.warn("--enable-hive-sync will be deprecated in a future release; please use --enable-sync instead for Hive syncing");
    }

    try {
      new HoodieDeltaStreamer(cfg, jssc).sync();
    } finally {
      jssc.stop();
    }
  }

  /**
   * Syncs data either in single-run or in continuous mode.
   */
  public static class DeltaSyncService extends AbstractAsyncService {

    private static final long serialVersionUID = 1L;
    /**
     * Delta Sync Config.
     */
    private final HoodieDeltaStreamer.Config cfg;

    /**
     * Schema provider that supplies the command for reading the input and writing out the target table.
     */
    private transient SchemaProvider schemaProvider;

    /**
     * Spark Session.
     */
    private transient SparkSession sparkSession;

    /**
     * Spark context.
     */
    private transient JavaSparkContext jssc;

    /**
     * Bag of properties with source, hoodie client, key generator etc.
     */
    TypedProperties props;

    /**
     * Async Compactor Service.
     */
    private AsyncCompactService asyncCompactService;

    /**
     * Table Type.
     */
    private final HoodieTableType tableType;

    /**
     * Delta Sync.
     */
    private transient DeltaSync deltaSync;

    public DeltaSyncService(Config cfg, JavaSparkContext jssc, FileSystem fs, Configuration conf,
                            TypedProperties properties) throws IOException {
      this.cfg = cfg;
      this.jssc = jssc;
      this.sparkSession = SparkSession.builder().config(jssc.getConf()).getOrCreate();

      if (fs.exists(new Path(cfg.targetBasePath))) {
        HoodieTableMetaClient meta =
            new HoodieTableMetaClient(new Configuration(fs.getConf()), cfg.targetBasePath, false);
        tableType = meta.getTableType();
        // This will guarantee there is no surprise with table type
        ValidationUtils.checkArgument(tableType.equals(HoodieTableType.valueOf(cfg.tableType)),
            "Hoodie table is of type " + tableType + " but passed in CLI argument is " + cfg.tableType);

        // Load base file format
        // This will guarantee there is no surprise with base file type
        String baseFileFormat = meta.getTableConfig().getBaseFileFormat().toString();
        ValidationUtils.checkArgument(baseFileFormat.equals(cfg.baseFileFormat) || cfg.baseFileFormat == null,
            "Hoodie table's base file format is of type " + baseFileFormat + " but passed in CLI argument is "
                + cfg.baseFileFormat);
        cfg.baseFileFormat = meta.getTableConfig().getBaseFileFormat().toString();
        this.cfg.baseFileFormat = cfg.baseFileFormat;
      } else {
        tableType = HoodieTableType.valueOf(cfg.tableType);
        if (cfg.baseFileFormat == null) {
          cfg.baseFileFormat = "PARQUET"; // default for backward compatibility
        }
      }

      ValidationUtils.checkArgument(!cfg.filterDupes || cfg.operation != Operation.UPSERT,
          "'--filter-dupes' needs to be disabled when '--op' is 'UPSERT' to ensure updates are not missed.");

      this.props = properties;
      LOG.info("Creating delta streamer with configs : " + props.toString());
      this.schemaProvider = UtilHelpers.createSchemaProvider(cfg.schemaProviderClassName, props, jssc);

      deltaSync = new DeltaSync(cfg, sparkSession, schemaProvider, props, jssc, fs, conf,
          this::onInitializingWriteClient);
    }

    public DeltaSyncService(HoodieDeltaStreamer.Config cfg, JavaSparkContext jssc, FileSystem fs, Configuration conf)
        throws IOException {
      this(cfg, jssc, fs, conf, null);
    }

    public DeltaSync getDeltaSync() {
      return deltaSync;
    }

    @Override
    protected Pair<CompletableFuture, ExecutorService> startService() {
      ExecutorService executor = Executors.newFixedThreadPool(1);
      return Pair.of(CompletableFuture.supplyAsync(() -> {
        boolean error = false;
        if (cfg.isAsyncCompactionEnabled()) {
          // set Scheduler Pool.
          LOG.info("Setting Spark Pool name for delta-sync to " + DELTASYNC_POOL_NAME);
          jssc.setLocalProperty("spark.scheduler.pool", DELTASYNC_POOL_NAME);
        }
        try {
          while (!isShutdownRequested()) {
            try {
              long start = System.currentTimeMillis();
              Pair<Option<String>, JavaRDD<WriteStatus>> scheduledCompactionInstantAndRDD = deltaSync.syncOnce();
              if (null != scheduledCompactionInstantAndRDD && scheduledCompactionInstantAndRDD.getLeft().isPresent()) {
                LOG.info("Enqueuing new pending compaction instant (" + scheduledCompactionInstantAndRDD.getLeft() + ")");
                asyncCompactService.enqueuePendingCompaction(new HoodieInstant(State.REQUESTED,
                    HoodieTimeline.COMPACTION_ACTION, scheduledCompactionInstantAndRDD.getLeft().get()));
                asyncCompactService.waitTillPendingCompactionsReducesTo(cfg.maxPendingCompactions);
              }
              long toSleepMs = cfg.minSyncIntervalSeconds * 1000 - (System.currentTimeMillis() - start);
              if (toSleepMs > 0) {
                LOG.info("Last sync ran less than min sync interval: " + cfg.minSyncIntervalSeconds + " s, sleep: "
                    + toSleepMs + " ms.");
                Thread.sleep(toSleepMs);
              }
            } catch (Exception e) {
              LOG.error("Shutting down delta-sync due to exception", e);
              error = true;
              throw new HoodieException(e.getMessage(), e);
            }
          }
        } finally {
          shutdownCompactor(error);
        }
        return true;
      }, executor), executor);
    }

    /**
     * Shutdown compactor as DeltaSync is shutdown.
     */
    private void shutdownCompactor(boolean error) {
      LOG.info("Delta Sync shutdown. Error ?" + error);
      if (asyncCompactService != null) {
        LOG.warn("Gracefully shutting down compactor");
        asyncCompactService.shutdown(false);
      }
    }

    /**
     * Callback to initialize write client and start compaction service if required.
     *
     * @param writeClient HoodieWriteClient
     * @return
     */
    protected Boolean onInitializingWriteClient(HoodieWriteClient writeClient) {
      if (cfg.isAsyncCompactionEnabled()) {
        asyncCompactService = new AsyncCompactService(jssc, writeClient);
        // Enqueue existing pending compactions first
        HoodieTableMetaClient meta =
            new HoodieTableMetaClient(new Configuration(jssc.hadoopConfiguration()), cfg.targetBasePath, true);
        List<HoodieInstant> pending = CompactionUtils.getPendingCompactionInstantTimes(meta);
        pending.forEach(hoodieInstant -> asyncCompactService.enqueuePendingCompaction(hoodieInstant));
        asyncCompactService.start((error) -> {
          // Shutdown DeltaSync
          shutdown(false);
          return true;
        });
        try {
          asyncCompactService.waitTillPendingCompactionsReducesTo(cfg.maxPendingCompactions);
        } catch (InterruptedException ie) {
          throw new HoodieException(ie);
        }
      }
      return true;
    }

    /**
     * Close all resources.
     */
    public void close() {
      if (null != deltaSync) {
        deltaSync.close();
      }
    }

    public SchemaProvider getSchemaProvider() {
      return schemaProvider;
    }

    public SparkSession getSparkSession() {
      return sparkSession;
    }

    public JavaSparkContext getJavaSparkContext() {
      return jssc;
    }

    public AsyncCompactService getAsyncCompactService() {
      return asyncCompactService;
    }

    public TypedProperties getProps() {
      return props;
    }
  }

  public DeltaSyncService getDeltaSyncService() {
    return deltaSyncService.get();
  }
}
