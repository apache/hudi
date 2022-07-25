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

package org.apache.hudi.timeline.service;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.javalin.Javalin;
import io.javalin.core.util.JettyServerUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import java.io.IOException;
import java.io.Serializable;

/**
 * A standalone timeline service exposing File-System View interfaces to clients.
 */
public class TimelineService {

  private static final Logger LOG = LogManager.getLogger(TimelineService.class);
  private static final int START_SERVICE_MAX_RETRIES = 16;
  private static final int DEFAULT_NUM_THREADS = -1;

  private int serverPort;
  private Config timelineServerConf;
  private Configuration conf;
  private transient HoodieEngineContext context;
  private transient FileSystem fs;
  private transient Javalin app = null;
  private transient FileSystemViewManager fsViewsManager;
  private transient RequestHandler requestHandler;

  public int getServerPort() {
    return serverPort;
  }

  public TimelineService(HoodieEngineContext context, Configuration hadoopConf, Config timelineServerConf,
                         FileSystem fileSystem, FileSystemViewManager globalFileSystemViewManager) throws IOException {
    this.conf = FSUtils.prepareHadoopConf(hadoopConf);
    this.timelineServerConf = timelineServerConf;
    this.serverPort = timelineServerConf.serverPort;
    this.context = context;
    this.fs = fileSystem;
    this.fsViewsManager = globalFileSystemViewManager;
  }

  /**
   * Config for {@code TimelineService} class.
   */
  public static class Config implements Serializable {

    @Parameter(names = {"--server-port", "-p"}, description = " Server Port")
    public Integer serverPort = 26754;

    @Parameter(names = {"--view-storage", "-st"}, description = "View Storage Type. Default - SPILLABLE_DISK")
    public FileSystemViewStorageType viewStorageType = FileSystemViewStorageType.SPILLABLE_DISK;

    @Parameter(names = {"--max-view-mem-per-table", "-mv"},
        description = "Maximum view memory per table in MB to be used for storing file-groups."
            + " Overflow file-groups will be spilled to disk. Used for SPILLABLE_DISK storage type")
    public Integer maxViewMemPerTableInMB = 2048;

    @Parameter(names = {"--mem-overhead-fraction-pending-compaction", "-cf"},
        description = "Memory Fraction of --max-view-mem-per-table to be allocated for managing pending compaction"
            + " storage. Overflow entries will be spilled to disk. Used for SPILLABLE_DISK storage type")
    public Double memFractionForCompactionPerTable = 0.001;

    @Parameter(names = {"--base-store-path", "-sp"},
        description = "Directory where spilled view entries will be stored. Used for SPILLABLE_DISK storage type")
    public String baseStorePathForFileGroups = FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue();

    @Parameter(names = {"--rocksdb-path", "-rp"}, description = "Root directory for RocksDB")
    public String rocksDBPath = FileSystemViewStorageConfig.ROCKSDB_BASE_PATH.defaultValue();

    @Parameter(names = {"--threads", "-t"}, description = "Number of threads to use for serving requests")
    public int numThreads = DEFAULT_NUM_THREADS;

    @Parameter(names = {"--async"}, description = "Use asyncronous request processing")
    public boolean async = false;

    @Parameter(names = {"--compress"}, description = "Compress output using gzip")
    public boolean compress = true;

    @Parameter(names = {"--enable-marker-requests", "-em"}, description = "Enable handling of marker-related requests")
    public boolean enableMarkerRequests = false;

    @Parameter(names = {"--marker-batch-threads", "-mbt"}, description = "Number of threads to use for batch processing marker creation requests")
    public int markerBatchNumThreads = 20;

    @Parameter(names = {"--marker-batch-interval-ms", "-mbi"}, description = "The interval in milliseconds between two batch processing of marker creation requests")
    public long markerBatchIntervalMs = 50;

    @Parameter(names = {"--marker-parallelism", "-mdp"}, description = "Parallelism to use for reading and deleting marker files")
    public int markerParallelism = 100;

    @Parameter(names = {"--help", "-h"})
    public Boolean help = false;

    public static Builder builder() {
      return new Builder();
    }

    /**
     * Builder of Config class.
     */
    public static class Builder {
      private Integer serverPort = 26754;
      private FileSystemViewStorageType viewStorageType = FileSystemViewStorageType.SPILLABLE_DISK;
      private Integer maxViewMemPerTableInMB = 2048;
      private Double memFractionForCompactionPerTable = 0.001;
      private String baseStorePathForFileGroups = FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue();
      private String rocksDBPath = FileSystemViewStorageConfig.ROCKSDB_BASE_PATH.defaultValue();
      private int numThreads = DEFAULT_NUM_THREADS;
      private boolean async = false;
      private boolean compress = true;
      private boolean enableMarkerRequests = false;
      private int markerBatchNumThreads = 20;
      private long markerBatchIntervalMs = 50L;
      private int markerParallelism = 100;

      public Builder() {
      }

      public Builder serverPort(int serverPort) {
        this.serverPort = serverPort;
        return this;
      }

      public Builder viewStorageType(FileSystemViewStorageType viewStorageType) {
        this.viewStorageType = viewStorageType;
        return this;
      }

      public Builder maxViewMemPerTableInMB(int maxViewMemPerTableInMB) {
        this.maxViewMemPerTableInMB = maxViewMemPerTableInMB;
        return this;
      }

      public Builder memFractionForCompactionPerTable(double memFractionForCompactionPerTable) {
        this.memFractionForCompactionPerTable = memFractionForCompactionPerTable;
        return this;
      }

      public Builder baseStorePathForFileGroups(String baseStorePathForFileGroups) {
        this.baseStorePathForFileGroups = baseStorePathForFileGroups;
        return this;
      }

      public Builder rocksDBPath(String rocksDBPath) {
        this.rocksDBPath = rocksDBPath;
        return this;
      }

      public Builder numThreads(int numThreads) {
        this.numThreads = numThreads;
        return this;
      }

      public Builder async(boolean async) {
        this.async = async;
        return this;
      }

      public Builder compress(boolean compress) {
        this.compress = compress;
        return this;
      }

      public Builder enableMarkerRequests(boolean enableMarkerRequests) {
        this.enableMarkerRequests = enableMarkerRequests;
        return this;
      }

      public Builder markerBatchNumThreads(int markerBatchNumThreads) {
        this.markerBatchNumThreads = markerBatchNumThreads;
        return this;
      }

      public Builder markerBatchIntervalMs(long markerBatchIntervalMs) {
        this.markerBatchIntervalMs = markerBatchIntervalMs;
        return this;
      }

      public Builder markerParallelism(int markerParallelism) {
        this.markerParallelism = markerParallelism;
        return this;
      }

      public Config build() {
        Config config = new Config();
        config.serverPort = this.serverPort;
        config.viewStorageType = this.viewStorageType;
        config.maxViewMemPerTableInMB = this.maxViewMemPerTableInMB;
        config.memFractionForCompactionPerTable = this.memFractionForCompactionPerTable;
        config.baseStorePathForFileGroups = this.baseStorePathForFileGroups;
        config.rocksDBPath = this.rocksDBPath;
        config.numThreads = this.numThreads;
        config.async = this.async;
        config.compress = this.compress;
        config.enableMarkerRequests = this.enableMarkerRequests;
        config.markerBatchNumThreads = this.markerBatchNumThreads;
        config.markerBatchIntervalMs = this.markerBatchIntervalMs;
        config.markerParallelism = this.markerParallelism;
        return config;
      }
    }
  }

  private int startServiceOnPort(int port) throws IOException {
    if (!(port == 0 || (1024 <= port && port < 65536))) {
      throw new IllegalArgumentException(String.format("startPort should be between 1024 and 65535 (inclusive), "
          + "or 0 for a random free port. but now is %s.", port));
    }
    for (int attempt = 0; attempt < START_SERVICE_MAX_RETRIES; attempt++) {
      // Returns port to try when trying to bind a service. Handles wrapping and skipping privileged ports.
      int tryPort = port == 0 ? port : (port + attempt - 1024) % (65536 - 1024) + 1024;
      try {
        app.start(tryPort);
        return app.port();
      } catch (Exception e) {
        if (e.getMessage() != null && e.getMessage().contains("Failed to bind to")) {
          if (tryPort == 0) {
            LOG.warn("Timeline server could not bind on a random free port.");
          } else {
            LOG.warn(String.format("Timeline server could not bind on port %d. "
                + "Attempting port %d + 1.",tryPort, tryPort));
          }
        } else {
          LOG.warn(String.format("Timeline server start failed on port %d. Attempting port %d + 1.",tryPort, tryPort), e);
        }
      }
    }
    throw new IOException(String.format("Timeline server start failed on port %d, after retry %d times", port, START_SERVICE_MAX_RETRIES));
  }

  public int startService() throws IOException {
    final Server server = timelineServerConf.numThreads == DEFAULT_NUM_THREADS ? JettyServerUtil.defaultServer()
            : new Server(new QueuedThreadPool(timelineServerConf.numThreads));

    app = Javalin.create().server(() -> server);
    if (!timelineServerConf.compress) {
      app.disableDynamicGzip();
    }

    requestHandler = new RequestHandler(
        app, conf, timelineServerConf, context, fs, fsViewsManager);
    app.get("/", ctx -> ctx.result("Hello Hudi"));
    requestHandler.register();
    int realServerPort = startServiceOnPort(serverPort);
    LOG.info("Starting Timeline server on port :" + realServerPort);
    this.serverPort = realServerPort;
    return realServerPort;
  }

  public void run() throws IOException {
    startService();
  }

  public static FileSystemViewManager buildFileSystemViewManager(Config config, SerializableConfiguration conf) {
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(conf.get());
    // Just use defaults for now
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();
    HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().build();

    switch (config.viewStorageType) {
      case MEMORY:
        FileSystemViewStorageConfig.Builder inMemConfBuilder = FileSystemViewStorageConfig.newBuilder();
        inMemConfBuilder.withStorageType(FileSystemViewStorageType.MEMORY);
        return FileSystemViewManager.createViewManager(localEngineContext, metadataConfig, inMemConfBuilder.build(), commonConfig);
      case SPILLABLE_DISK: {
        FileSystemViewStorageConfig.Builder spillableConfBuilder = FileSystemViewStorageConfig.newBuilder();
        spillableConfBuilder.withStorageType(FileSystemViewStorageType.SPILLABLE_DISK)
            .withBaseStoreDir(config.baseStorePathForFileGroups)
            .withMaxMemoryForView(config.maxViewMemPerTableInMB * 1024 * 1024L)
            .withMemFractionForPendingCompaction(config.memFractionForCompactionPerTable);
        return FileSystemViewManager.createViewManager(localEngineContext, metadataConfig, spillableConfBuilder.build(), commonConfig);
      }
      case EMBEDDED_KV_STORE: {
        FileSystemViewStorageConfig.Builder rocksDBConfBuilder = FileSystemViewStorageConfig.newBuilder();
        rocksDBConfBuilder.withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE)
            .withRocksDBPath(config.rocksDBPath);
        return FileSystemViewManager.createViewManager(localEngineContext, metadataConfig, rocksDBConfBuilder.build(), commonConfig);
      }
      default:
        throw new IllegalArgumentException("Invalid view manager storage type :" + config.viewStorageType);
    }
  }

  public void close() {
    LOG.info("Closing Timeline Service");
    if (requestHandler != null) {
      this.requestHandler.stop();
    }
    if (this.app != null) {
      this.app.stop();
      this.app = null;
    }
    this.fsViewsManager.close();
    LOG.info("Closed Timeline Service");
  }

  public Configuration getConf() {
    return conf;
  }

  public FileSystem getFs() {
    return fs;
  }

  public static void main(String[] args) throws Exception {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help) {
      cmd.usage();
      System.exit(1);
    }

    Configuration conf = FSUtils.prepareHadoopConf(new Configuration());
    FileSystemViewManager viewManager = buildFileSystemViewManager(cfg, new SerializableConfiguration(conf));
    TimelineService service = new TimelineService(
        new HoodieLocalEngineContext(FSUtils.prepareHadoopConf(new Configuration())),
        new Configuration(), cfg, FileSystem.get(new Configuration()), viewManager);
    service.run();
  }
}
