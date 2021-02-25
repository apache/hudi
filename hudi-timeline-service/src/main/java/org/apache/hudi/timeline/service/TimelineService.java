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

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.javalin.Javalin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;

/**
 * A standalone timeline service exposing File-System View interfaces to clients.
 */
public class TimelineService {

  private static final Logger LOG = LogManager.getLogger(TimelineService.class);
  private static final int START_SERVICE_MAX_RETRIES = 16;

  private int serverPort;
  private Configuration conf;
  private transient FileSystem fs;
  private transient Javalin app = null;
  private transient FileSystemViewManager fsViewsManager;

  public int getServerPort() {
    return serverPort;
  }

  public TimelineService(int serverPort, FileSystemViewManager globalFileSystemViewManager, Configuration conf)
      throws IOException {
    this.conf = FSUtils.prepareHadoopConf(conf);
    this.fs = FileSystem.get(conf);
    this.serverPort = serverPort;
    this.fsViewsManager = globalFileSystemViewManager;
  }

  public TimelineService(int serverPort, FileSystemViewManager globalFileSystemViewManager) throws IOException {
    this(serverPort, globalFileSystemViewManager, new Configuration());
  }

  public TimelineService(Config config) throws IOException {
    this(config.serverPort, buildFileSystemViewManager(config,
        new SerializableConfiguration(FSUtils.prepareHadoopConf(new Configuration()))));
  }

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
    public String baseStorePathForFileGroups = FileSystemViewStorageConfig.DEFAULT_VIEW_SPILLABLE_DIR;

    @Parameter(names = {"--rocksdb-path", "-rp"}, description = "Root directory for RocksDB")
    public String rocksDBPath = FileSystemViewStorageConfig.DEFAULT_ROCKSDB_BASE_PATH;

    @Parameter(names = {"--help", "-h"})
    public Boolean help = false;
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
    app = Javalin.create();
    RequestHandler requestHandler = new RequestHandler(app, conf, fsViewsManager);
    app.get("/", ctx -> ctx.result("Hello World"));
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

    switch (config.viewStorageType) {
      case MEMORY:
        FileSystemViewStorageConfig.Builder inMemConfBuilder = FileSystemViewStorageConfig.newBuilder();
        inMemConfBuilder.withStorageType(FileSystemViewStorageType.MEMORY);
        return FileSystemViewManager.createViewManager(localEngineContext, metadataConfig, inMemConfBuilder.build());
      case SPILLABLE_DISK: {
        FileSystemViewStorageConfig.Builder spillableConfBuilder = FileSystemViewStorageConfig.newBuilder();
        spillableConfBuilder.withStorageType(FileSystemViewStorageType.SPILLABLE_DISK)
            .withBaseStoreDir(config.baseStorePathForFileGroups)
            .withMaxMemoryForView(config.maxViewMemPerTableInMB * 1024 * 1024L)
            .withMemFractionForPendingCompaction(config.memFractionForCompactionPerTable);
        return FileSystemViewManager.createViewManager(localEngineContext, metadataConfig, spillableConfBuilder.build());
      }
      case EMBEDDED_KV_STORE: {
        FileSystemViewStorageConfig.Builder rocksDBConfBuilder = FileSystemViewStorageConfig.newBuilder();
        rocksDBConfBuilder.withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE)
            .withRocksDBPath(config.rocksDBPath);
        return FileSystemViewManager.createViewManager(localEngineContext, metadataConfig, rocksDBConfBuilder.build());
      }
      default:
        throw new IllegalArgumentException("Invalid view manager storage type :" + config.viewStorageType);
    }
  }

  public void close() {
    LOG.info("Closing Timeline Service");
    this.app.stop();
    this.app = null;
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
    TimelineService service = new TimelineService(cfg.serverPort, viewManager);
    service.run();
  }
}
