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

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hudi.HoodieWriteClient;
import org.apache.hudi.OverwriteWithLatestAvroPayload;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.HiveIncrementalPuller;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.JsonDFSSource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * An Utility which can incrementally take the output from {@link HiveIncrementalPuller} and apply it to the target
 * dataset. Does not maintain any state, queries at runtime to see how far behind the target dataset is from the source
 * dataset. This can be overriden to force sync from a timestamp.
 *
 * In continuous mode, DeltaStreamer runs in loop-mode going through the below operations (a) pull-from-source (b)
 * write-to-sink (c) Schedule Compactions if needed (d) Conditionally Sync to Hive each cycle. For MOR table with
 * continuous mode enabled, a seperate compactor thread is allocated to execute compactions
 */
public class HoodieDeltaStreamer implements Serializable {

  private static volatile Logger log = LogManager.getLogger(HoodieDeltaStreamer.class);

  public static String CHECKPOINT_KEY = "deltastreamer.checkpoint.key";

  private final transient Config cfg;

  protected transient DeltaSyncService deltaSyncService;

  public HoodieDeltaStreamer(Config cfg, JavaSparkContext jssc) throws IOException {
    this(cfg, jssc, FSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration()),
        getDefaultHiveConf(jssc.hadoopConfiguration()));
  }

  public HoodieDeltaStreamer(Config cfg, JavaSparkContext jssc, FileSystem fs, HiveConf hiveConf) throws IOException {
    this.cfg = cfg;
    this.deltaSyncService = new DeltaSyncService(cfg, jssc, fs, hiveConf);
  }

  public void shutdownGracefully() {
    deltaSyncService.shutdown(false);
  }

  private static HiveConf getDefaultHiveConf(Configuration cfg) {
    HiveConf hiveConf = new HiveConf();
    hiveConf.addResource(cfg);
    return hiveConf;
  }

  /**
   * Main method to start syncing
   * 
   * @throws Exception
   */
  public void sync() throws Exception {
    if (cfg.continuousMode) {
      deltaSyncService.start(this::onDeltaSyncShutdown);
      deltaSyncService.waitForShutdown();
      log.info("Delta Sync shutting down");
    } else {
      log.info("Delta Streamer running only single round");
      try {
        deltaSyncService.getDeltaSync().syncOnce();
      } catch (Exception ex) {
        log.error("Got error running delta sync once. Shutting down", ex);
        throw ex;
      } finally {
        deltaSyncService.close();
        log.info("Shut down deltastreamer");
      }
    }
  }

  private boolean onDeltaSyncShutdown(boolean error) {
    log.info("DeltaSync shutdown. Closing write client. Error?" + error);
    deltaSyncService.close();
    return true;
  }

  public enum Operation {
    UPSERT, INSERT, BULK_INSERT
  }

  private static class OperationConvertor implements IStringConverter<Operation> {

    @Override
    public Operation convert(String value) throws ParameterException {
      return Operation.valueOf(value);
    }
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--target-base-path"},
        description = "base path for the target hoodie dataset. "
            + "(Will be created if did not exist first time around. If exists, expected to be a hoodie dataset)",
        required = true)
    public String targetBasePath;

    // TODO: How to obtain hive configs to register?
    @Parameter(names = {"--target-table"}, description = "name of the target table in Hive", required = true)
    public String targetTableName;

    @Parameter(names = {"--storage-type"}, description = "Type of Storage. " + "COPY_ON_WRITE (or) MERGE_ON_READ",
        required = true)
    public String storageType;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client, schema provider, key generator and data source. For hoodie client props, sane defaults are "
        + "used, but recommend use to provide basic things like metrics endpoints, hive configs etc. For sources, refer"
        + "to individual classes, for supported properties.")
    public String propsFilePath =
        "file://" + System.getProperty("user.dir") + "/src/test/resources/delta-streamer-config/dfs-source.properties";

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--propsFilePath\") can also be passed command line using this parameter")
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
        description = "subclass of org.apache.hudi.utilities.transform.Transformer"
            + ". Allows transforming raw source dataset to a target dataset (conforming to target schema) before "
            + "writing. Default : Not set. E:g - org.apache.hudi.utilities.transform.SqlQueryBasedTransformer (which "
            + "allows a SQL query templated to be passed as a transformation function)")
    public String transformerClassName = null;

    @Parameter(names = {"--source-limit"}, description = "Maximum amount of data to read from source. "
        + "Default: No limit For e.g: DFS-Source => max bytes to read, Kafka-Source => max events to read")
    public long sourceLimit = Long.MAX_VALUE;

    @Parameter(names = {"--op"}, description = "Takes one of these values : UPSERT (default), INSERT (use when input "
        + "is purely new data/inserts to gain speed)", converter = OperationConvertor.class)
    public Operation operation = Operation.UPSERT;

    @Parameter(names = {"--filter-dupes"},
        description = "Should duplicate records from source be dropped/filtered out" + "before insert/bulk-insert")
    public Boolean filterDupes = false;

    @Parameter(names = {"--enable-hive-sync"}, description = "Enable syncing to hive")
    public Boolean enableHiveSync = false;

    @Parameter(names = {"--max-pending-compactions"},
        description = "Maximum number of outstanding inflight/requested compactions. Delta Sync will not happen unless"
            + "outstanding compactions is less than this number")
    public Integer maxPendingCompactions = 5;

    @Parameter(names = {"--continuous"}, description = "Delta Streamer runs in continuous mode running"
        + " source-fetch -> Transform -> Hudi Write in loop")
    public Boolean continuousMode = false;

    @Parameter(names = {"--min-sync-interval-seconds"},
        description = "the min sync interval of each sync in " + "continuous mode")
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
        description = "Compaction is enabled for MoR table by default." + "This flag disables it ")
    public Boolean forceDisableCompaction = false;

    /**
     * Resume Delta Streamer from this checkpoint.
     */
    @Parameter(names = {"--checkpoint"}, description = "Resume Delta Streamer from this checkpoint.")
    public String checkpoint = null;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    public boolean isAsyncCompactionEnabled() {
      return continuousMode && !forceDisableCompaction
          && HoodieTableType.MERGE_ON_READ.equals(HoodieTableType.valueOf(storageType));
    }

    public boolean isInlineCompactionEnabled() {
      return !continuousMode && !forceDisableCompaction
          && HoodieTableType.MERGE_ON_READ.equals(HoodieTableType.valueOf(storageType));
    }
  }

  public static void main(String[] args) throws Exception {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    Map<String, String> additionalSparkConfigs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    JavaSparkContext jssc =
        UtilHelpers.buildSparkContext("delta-streamer-" + cfg.targetTableName, cfg.sparkMaster, additionalSparkConfigs);
    try {
      new HoodieDeltaStreamer(cfg, jssc).sync();
    } finally {
      jssc.stop();
    }
  }

  /**
   * Syncs data either in single-run or in continuous mode.
   */
  public static class DeltaSyncService extends AbstractDeltaStreamerService {

    /**
     * Delta Sync Config
     */
    private final HoodieDeltaStreamer.Config cfg;

    /**
     * Schema provider that supplies the command for reading the input and writing out the target table.
     */
    private transient SchemaProvider schemaProvider;

    /**
     * Spark Session
     */
    private transient SparkSession sparkSession;

    /**
     * Spark context
     */
    private transient JavaSparkContext jssc;

    /**
     * Bag of properties with source, hoodie client, key generator etc.
     */
    TypedProperties props;

    /**
     * Async Compactor Service
     */
    private AsyncCompactService asyncCompactService;

    /**
     * Table Type
     */
    private final HoodieTableType tableType;

    /**
     * Delta Sync
     */
    private transient DeltaSync deltaSync;

    public DeltaSyncService(HoodieDeltaStreamer.Config cfg, JavaSparkContext jssc, FileSystem fs, HiveConf hiveConf)
        throws IOException {
      this.cfg = cfg;
      this.jssc = jssc;
      this.sparkSession = SparkSession.builder().config(jssc.getConf()).getOrCreate();

      if (fs.exists(new Path(cfg.targetBasePath))) {
        HoodieTableMetaClient meta =
            new HoodieTableMetaClient(new Configuration(fs.getConf()), cfg.targetBasePath, false);
        tableType = meta.getTableType();
        // This will guarantee there is no surprise with table type
        Preconditions.checkArgument(tableType.equals(HoodieTableType.valueOf(cfg.storageType)),
            "Hoodie table is of type " + tableType + " but passed in CLI argument is " + cfg.storageType);
      } else {
        tableType = HoodieTableType.valueOf(cfg.storageType);
      }

      this.props = UtilHelpers.readConfig(fs, new Path(cfg.propsFilePath), cfg.configs).getConfig();
      log.info("Creating delta streamer with configs : " + props.toString());
      this.schemaProvider = UtilHelpers.createSchemaProvider(cfg.schemaProviderClassName, props, jssc);

      if (cfg.filterDupes) {
        cfg.operation = cfg.operation == Operation.UPSERT ? Operation.INSERT : cfg.operation;
      }

      deltaSync = new DeltaSync(cfg, sparkSession, schemaProvider, tableType, props, jssc, fs, hiveConf,
          this::onInitializingWriteClient);
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
          log.info("Setting Spark Pool name for delta-sync to " + SchedulerConfGenerator.DELTASYNC_POOL_NAME);
          jssc.setLocalProperty("spark.scheduler.pool", SchedulerConfGenerator.DELTASYNC_POOL_NAME);
        }
        try {
          while (!isShutdownRequested()) {
            try {
              long start = System.currentTimeMillis();
              Pair<Option<String>, JavaRDD<WriteStatus>> scheduledCompactionInstantAndRDD = deltaSync.syncOnce();
              if (scheduledCompactionInstantAndRDD.getLeft().isPresent()) {
                log.info("Enqueuing new pending compaction instant (" + scheduledCompactionInstantAndRDD.getLeft() + ")");
                asyncCompactService.enqueuePendingCompaction(new HoodieInstant(State.REQUESTED,
                    HoodieTimeline.COMPACTION_ACTION, scheduledCompactionInstantAndRDD.getLeft().get()));
                asyncCompactService.waitTillPendingCompactionsReducesTo(cfg.maxPendingCompactions);
              }
              long toSleepMs = cfg.minSyncIntervalSeconds * 1000 - (System.currentTimeMillis() - start);
              if (toSleepMs > 0) {
                log.info("Last sync ran less than min sync interval: " + cfg.minSyncIntervalSeconds + " s, sleep: "
                    + toSleepMs + " ms.");
                Thread.sleep(toSleepMs);
              }
            } catch (Exception e) {
              log.error("Shutting down delta-sync due to exception", e);
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
     * Shutdown compactor as DeltaSync is shutdown
     */
    private void shutdownCompactor(boolean error) {
      log.info("Delta Sync shutdown. Error ?" + error);
      if (asyncCompactService != null) {
        log.warn("Gracefully shutting down compactor");
        asyncCompactService.shutdown(false);
      }
    }

    /**
     * Callback to initialize write client and start compaction service if required
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
        pending.stream().forEach(hoodieInstant -> asyncCompactService.enqueuePendingCompaction(hoodieInstant));
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
     * Close all resources
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

  /**
   * Async Compactor Service tha runs in separate thread. Currently, only one compactor is allowed to run at any time.
   */
  public static class AsyncCompactService extends AbstractDeltaStreamerService {

    private final int maxConcurrentCompaction;
    private transient Compactor compactor;
    private transient JavaSparkContext jssc;
    private transient BlockingQueue<HoodieInstant> pendingCompactions = new LinkedBlockingQueue<>();
    private transient ReentrantLock queueLock = new ReentrantLock();
    private transient Condition consumed = queueLock.newCondition();

    public AsyncCompactService(JavaSparkContext jssc, HoodieWriteClient client) {
      this.jssc = jssc;
      this.compactor = new Compactor(client, jssc);
      // TODO: HUDI-157 : Only allow 1 compactor to run in parallel till Incremental View on MOR is fully implemented.
      this.maxConcurrentCompaction = 1;
    }

    /**
     * Enqueues new Pending compaction
     */
    public void enqueuePendingCompaction(HoodieInstant instant) {
      pendingCompactions.add(instant);
    }

    /**
     * Wait till outstanding pending compactions reduces to the passed in value
     * 
     * @param numPendingCompactions Maximum pending compactions allowed
     * @throws InterruptedException
     */
    public void waitTillPendingCompactionsReducesTo(int numPendingCompactions) throws InterruptedException {
      try {
        queueLock.lock();
        while (!isShutdown() && (pendingCompactions.size() > numPendingCompactions)) {
          consumed.await();
        }
      } finally {
        queueLock.unlock();
      }
    }

    /**
     * Fetch Next pending compaction if available
     * 
     * @return
     * @throws InterruptedException
     */
    private HoodieInstant fetchNextCompactionInstant() throws InterruptedException {
      log.info("Compactor waiting for next instant for compaction upto 60 seconds");
      HoodieInstant instant = pendingCompactions.poll(60, TimeUnit.SECONDS);
      if (instant != null) {
        try {
          queueLock.lock();
          // Signal waiting thread
          consumed.signal();
        } finally {
          queueLock.unlock();
        }
      }
      return instant;
    }

    /**
     * Start Compaction Service
     */
    protected Pair<CompletableFuture, ExecutorService> startService() {
      ExecutorService executor = Executors.newFixedThreadPool(maxConcurrentCompaction);
      List<CompletableFuture<Boolean>> compactionFutures =
          IntStream.range(0, maxConcurrentCompaction).mapToObj(i -> CompletableFuture.supplyAsync(() -> {
            try {
              // Set Compactor Pool Name for allowing users to prioritize compaction
              log.info("Setting Spark Pool name for compaction to " + SchedulerConfGenerator.COMPACT_POOL_NAME);
              jssc.setLocalProperty("spark.scheduler.pool", SchedulerConfGenerator.COMPACT_POOL_NAME);

              while (!isShutdownRequested()) {
                final HoodieInstant instant = fetchNextCompactionInstant();
                if (null != instant) {
                  compactor.compact(instant);
                }
              }
              log.info("Compactor shutting down properly!!");
            } catch (InterruptedException ie) {
              log.warn("Compactor executor thread got interrupted exception. Stopping", ie);
            } catch (IOException e) {
              log.error("Compactor executor failed", e);
              throw new HoodieIOException(e.getMessage(), e);
            }
            return true;
          }, executor)).collect(Collectors.toList());
      return Pair.of(CompletableFuture.allOf(compactionFutures.stream().toArray(CompletableFuture[]::new)), executor);
    }
  }

  public DeltaSyncService getDeltaSyncService() {
    return deltaSyncService;
  }
}
