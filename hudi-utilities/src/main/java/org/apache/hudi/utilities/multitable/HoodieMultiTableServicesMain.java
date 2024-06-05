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

package org.apache.hudi.utilities.multitable;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy;
import org.apache.hudi.utilities.HoodieCompactor;
import org.apache.hudi.utilities.IdentitySplitter;
import org.apache.hudi.utilities.UtilHelpers;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Main function for executing multi-table services.
 */
public class HoodieMultiTableServicesMain {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieMultiTableServicesMain.class);
  final Config cfg;
  final TypedProperties props;

  private final JavaSparkContext jsc;

  private ScheduledExecutorService executorService;

  private void batchRunTableServices(List<String> tablePaths) throws InterruptedException, ExecutionException {
    ExecutorService executorService = Executors.newFixedThreadPool(cfg.poolSize);
    List<CompletableFuture<Void>> futures = tablePaths.stream()
        .map(basePath -> CompletableFuture.runAsync(
            () -> MultiTableServiceUtils.buildTableServicePipeline(jsc, basePath, cfg, props).execute(),
            executorService))
        .collect(Collectors.toList());
    CompletableFuture<?> allComplete =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    CompletableFuture<?> anyException = new CompletableFuture<>();
    for (CompletableFuture<?> future : futures) {
      future.exceptionally((e) -> {
        anyException.completeExceptionally(e);
        return null;
      });
    }
    try {
      CompletableFuture.anyOf(allComplete, anyException).get();
    } catch (ExecutionException ee) {
      throw new ExecutionException("some table service failed", ee);
    } finally {
      executorService.shutdownNow();
    }
  }

  private void streamRunTableServices(List<String> tablePaths) throws InterruptedException {
    executorService = Executors.newScheduledThreadPool(cfg.poolSize);
    for (String tablePath : tablePaths) {
      TableServicePipeline pipeline = MultiTableServiceUtils.buildTableServicePipeline(jsc, tablePath, cfg, props);
      executorService.scheduleAtFixedRate(pipeline::execute, 0, cfg.scheduleDelay, TimeUnit.MILLISECONDS);
    }
    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
  }

  public void cancel() {
    if (executorService != null) {
      executorService.shutdown();
    }
  }

  public HoodieMultiTableServicesMain(JavaSparkContext jsc, Config cfg) {
    this.cfg = cfg;
    this.jsc = jsc;
    this.props = cfg.propsFilePath == null ? UtilHelpers.buildProperties(cfg.configs) : readConfigFromFileSystem(jsc, cfg);
  }

  public void startServices() throws ExecutionException, InterruptedException {
    LOG.info("StartServices Config: " + cfg);
    List<String> tablePaths;
    if (cfg.autoDiscovery) {
      // We support defining multi base paths
      tablePaths = cfg.basePath.stream()
          .filter(this::pathExists)
          .flatMap(p -> MultiTableServiceUtils.findHoodieTablesUnderPath(jsc, p).stream())
          .collect(Collectors.toList());
    } else {
      tablePaths = MultiTableServiceUtils.getTablesToBeServedFromProps(jsc, props);
    }
    LOG.info("All table paths: " + String.join(",", tablePaths));
    if (cfg.batch) {
      batchRunTableServices(tablePaths);
    } else {
      streamRunTableServices(tablePaths);
    }
  }

  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, Config cfg) {
    return UtilHelpers.readConfig(jsc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.configs).getProps(true);
  }

  private boolean pathExists(String path) {
    try {
      Path p = new Path(path);
      FileSystem fs = p.getFileSystem(jsc.hadoopConfiguration());
      return fs.exists(p);
    } catch (IOException e) {
      throw new HoodieIOException("Error checking path existing:", e);
    }
  }

  /**
   * Command line configs to run table services
   */
  public static class Config implements Serializable {
    @Parameter(names = {"--base-path"}, description = "Base path for all the tables, this can be repeated",
        required = true, splitter = IdentitySplitter.class)
    public List<String> basePath = Collections.emptyList();

    @Parameter(names = {"--auto-discovery", "-a"}, description = "Whether to discover hudi tables in the base path")
    public boolean autoDiscovery = false;

    @Parameter(names = {"--parallelism"}, description = "Parallelism for hoodie table service")
    public int parallelism = 200;

    @Parameter(names = {"--batch", "-b"}, description = "Run services in batch or streaming mode")
    public boolean batch = false;

    @Parameter(names = {"--schedule-delay", "-d"}, description = "Table services schedule delay")
    public int scheduleDelay = 2000;

    @Parameter(names = {"--retry", "-r"}, description = "Table service retry count")
    public int retry = 1;

    @Parameter(names = {"--poolSize", "-p"}, description = "thread pool size")
    public int poolSize = Runtime.getRuntime().availableProcessors();

    @Parameter(names = {"--name", "-n"}, description = "Spark APP name")
    public String appName = "Hudi Table Service";

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Parameter(names = {"--enable-compaction"}, help = true)
    public Boolean enableCompaction = false;

    @Parameter(names = {"--enable-clustering"}, help = true)
    public Boolean enableClustering = false;

    @Parameter(names = {"--enable-clean"}, help = true)
    public Boolean enableClean = false;

    @Parameter(names = {"--enable-archive"}, help = true)
    public Boolean enableArchive = false;

    @Parameter(names = {"--compaction-mode"}, description = "Set job mode: Set \"schedule\" means make a compact plan; "
        + "Set \"execute\" means execute a compact plan at given instant which means --instant-time is needed here; "
        + "Set \"scheduleAndExecute\" means make a compact plan first and execute that plan immediately")
    public String compactionRunningMode = HoodieCompactor.EXECUTE;

    @Parameter(names = {"--strategy", "-st"}, description = "Strategy Class")
    public String compactionStrategyClassName = LogFileSizeBasedCompactionStrategy.class.getName();

    @Parameter(names = {"--clustering-mode"}, description = "Set job mode: Set \"schedule\" means make a clustering plan; "
        + "Set \"execute\" means execute a clustering plan at given instant which means --instant-time is needed here; "
        + "Set \"scheduleAndExecute\" means make a clustering plan first and execute that plan immediately")
    public String clusteringRunningMode = HoodieCompactor.SCHEDULE_AND_EXECUTE;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master")
    public String sparkMaster;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use")
    public String sparkMemory = null;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for table service")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Override
    public String toString() {
      return new StringJoiner(", ", Config.class.getSimpleName() + "[", "]")
          .add("basePath=" + basePath)
          .add("autoDiscovery=" + autoDiscovery)
          .add("parallelism=" + parallelism)
          .add("batch=" + batch)
          .add("scheduleDelay=" + scheduleDelay)
          .add("retry=" + retry)
          .add("poolSize=" + poolSize)
          .add("appName='" + appName + "'")
          .add("help=" + help)
          .add("enableCompaction=" + enableCompaction)
          .add("enableClustering=" + enableClustering)
          .add("enableClean=" + enableClean)
          .add("enableArchive=" + enableArchive)
          .add("compactionRunningMode='" + compactionRunningMode + "'")
          .add("compactionStrategyClassName='" + compactionStrategyClassName + "'")
          .add("clusteringRunningMode='" + clusteringRunningMode + "'")
          .add("sparkMaster='" + sparkMaster + "'")
          .add("sparkMemory='" + sparkMemory + "'")
          .add("propsFilePath='" + propsFilePath + "'")
          .add("configs=" + configs)
          .toString();
    }
  }

  public static void main(String[] args) {
    final HoodieMultiTableServicesMain.Config cfg = new HoodieMultiTableServicesMain.Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    JavaSparkContext jsc = UtilHelpers.buildSparkContext(cfg.appName, cfg.sparkMaster, cfg.sparkMemory);
    try {
      new HoodieMultiTableServicesMain(jsc, cfg).startServices();
    } catch (Throwable throwable) {
      LOG.error("Fail to run table services, ", throwable);
    } finally {
      jsc.stop();
    }
  }

}
