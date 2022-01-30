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

package org.apache.hudi.utilities;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.async.HoodieAsyncService;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class HoodieMetadataTableValidator {

  private static final Logger LOG = LogManager.getLogger(HoodieMetadataTableValidator.class);

  // Spark context
  private  transient JavaSparkContext jsc;
  // config
  private  Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;

  private HoodieTableMetaClient metaClient;

  protected transient Option<AsyncMetadataTableValidateService> asyncMetadataTableValidateService;

  public HoodieMetadataTableValidator(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
  }

  public HoodieMetadataTableValidator(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.cfg = cfg;

    this.props = cfg.propsFilePath == null
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);

    this.metaClient = HoodieTableMetaClient.builder()
        .setConf(jsc.hadoopConfiguration()).setBasePath(cfg.basePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();

    this.asyncMetadataTableValidateService = cfg.runningMode.equalsIgnoreCase(Mode.CONTINUOUS.name())
        ? Option.of(new AsyncMetadataTableValidateService()) : Option.empty();
  }

  /**
   * Reads config from the file system.
   *
   * @param jsc {@link JavaSparkContext} instance.
   * @param cfg {@link Config} instance.
   * @return the {@link TypedProperties} instance.
   */
  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, Config cfg) {
    return UtilHelpers.readConfig(jsc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.configs)
        .getProps(true);
  }

  public enum Mode {
    // Running MetadataTableValidator in continuous
    CONTINUOUS,
    // Running MetadataTableValidator once
    ONCE
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = true)
    public String basePath = null;

    @Parameter(names = {"--mode", "-m"}, description = "Set job mode: "
        + "Set \"CONTINUOUS\" Running MetadataTableValidator in continuous"
        + "Set \"ONCE\" Running MetadataTableValidator once", required = true)
    public String runningMode = null;

    @Parameter(names = {"--table-name", "-tn"}, description = "Table name", required = true)
    public String tableName = null;

    @Parameter(names = {"--min-validate-interval-seconds"},
        description = "the min validate interval of each validate in continuous mode")
    public Integer minValidateIntervalSeconds = 10;

    @Parameter(names = {"--ignore-failed", "-ig"}, description = "Ignore metadata validate failure and continue.", required = false)
    public boolean ignoreFailed = false;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for deleting table partitions")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "MetadataTableValidatorConfig {\n"
          + "   --base-path " + basePath + ", \n"
          + "   --mode " + runningMode + ", \n"
          + "   --min-validate-interval-seconds " + minValidateIntervalSeconds + ", \n"
          + "   --table-name " + tableName + ", \n"
          + "   --spark-master " + sparkMaster + ", \n"
          + "   --spark-memory " + sparkMemory + ", \n"
          + "   --props " + propsFilePath + ", \n"
          + "   --hoodie-conf " + configs
          + "\n}";
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
      return basePath.equals(config.basePath)
          && Objects.equals(runningMode, config.runningMode)
          && Objects.equals(minValidateIntervalSeconds, config.minValidateIntervalSeconds)
          && Objects.equals(tableName, config.tableName)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, runningMode, minValidateIntervalSeconds, tableName, sparkMaster, sparkMemory, propsFilePath, configs, help);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    SparkConf sparkConf = UtilHelpers.buildSparkConf("Hoodie-Metadata-Table-Validator", cfg.sparkMaster);
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, cfg);

    try {
      validator.run();
    } catch (Throwable throwable) {
      LOG.error("Fail to run deleting table partitions for " + validator.cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      LOG.info(cfg);
      Mode mode = Mode.valueOf(cfg.runningMode.toUpperCase());
      switch (mode) {
        case CONTINUOUS:
          LOG.info(" ****** do hoodie metadata table validation in CONTINUOUS mode ******");
          doHoodieMetadataTableValidationContinuous();
          break;
        case ONCE:
          LOG.info(" ****** do hoodie metadata table validation once ******");
          doHoodieMetadataTableValidationOnce();
          break;
        default:
          LOG.info("Unsupported running mode [" + cfg.runningMode + "], quit the job directly");
      }
    } catch (Exception e) {
      throw new HoodieException("Unable to do hoodie metadata table validation in " + cfg.basePath, e);
    }
  }

  private void doHoodieMetadataTableValidationOnce() {
    doMetaTableValidation();
  }

  private void doHoodieMetadataTableValidationContinuous() {
    asyncMetadataTableValidateService.ifPresent(service -> {
      service.start(null);
      try {
        service.waitForShutdown();
      } catch (Exception e) {
        throw new HoodieException(e.getMessage(), e);
      }
    });
  }

  public void doMetaTableValidation() {
    String basePath = metaClient.getBasePath();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // compare partitions
    List<String> allPartitionPathsFromFS = FSUtils.getAllPartitionPaths(engineContext, basePath, false, false);
    List<String> allPartitionPathsMeta = FSUtils.getAllPartitionPaths(engineContext, basePath, true, false);

    if (!compareCollectionResult(allPartitionPathsFromFS, allPartitionPathsMeta)) {
      String message = "Compare Partitions Failedddd! " + "AllPartitionPathsFromFS : " + allPartitionPathsFromFS + " and allPartitionPathsMeta : " + allPartitionPathsMeta;
      LOG.error(message);
      throw new RuntimeException(message);
    }

    for (String partitionPath : allPartitionPathsMeta) {
      validateFilesInPartition(engineContext, new Path(partitionPath));
    }

    LOG.info("MetaTable Validation Success.");
  }

  private void validateFilesInPartition(HoodieSparkEngineContext engineContext, Path partitionPath) {
    String partitionName = FSUtils.getRelativePartitionPath(new Path(metaClient.getBasePath()), partitionPath);

    HoodieTableFileSystemView metaFsView = createHoodieTableFileSystemView(engineContext, true);
    HoodieTableFileSystemView fsView = createHoodieTableFileSystemView(engineContext, false);

    List<FileSlice> fileSlicesFromMetadataTable = metaFsView.getLatestFileSlices(partitionPath.toString()).sorted().collect(Collectors.toList());
    List<FileSlice> fileSlicesFromFS = fsView.getLatestFileSlices(partitionPath.toString()).sorted().collect(Collectors.toList());

    if (!fileSlicesFromMetadataTable.equals(fileSlicesFromFS)) {
      String message = "Validation of metadata file listing for partition " + partitionName + " failed.";
      LOG.error(message);
      LOG.error("File list from metadata: " + fileSlicesFromMetadataTable);
      LOG.error("File list from direct listing: " + fileSlicesFromFS);
      throw new RuntimeException(message);
    }

    List<HoodieBaseFile> latestFilesFromMetadata = metaFsView.getLatestBaseFiles(partitionPath.toString()).collect(Collectors.toList());
    List<HoodieBaseFile> latestFilesFromFS = fsView.getLatestBaseFiles(partitionPath.toString()).collect(Collectors.toList());

    if (!latestFilesFromMetadata.equals(latestFilesFromFS)) {
      String message = "Validation of metadata get latest base file for partition " + partitionName + " failed.";
      LOG.error(message);
      LOG.error("Latest base file from metadata: " + latestFilesFromMetadata);
      LOG.error("Latest base file from direct listing: " + latestFilesFromFS);
      throw new RuntimeException(message);
    }
  }

  private HoodieTableFileSystemView createHoodieTableFileSystemView(HoodieSparkEngineContext engineContext, boolean enableMetadataTable) {

    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(enableMetadataTable)
        .withAssumeDatePartitioning(false)
        .build();

    return FileSystemViewManager.createInMemoryFileSystemView(engineContext,
        metaClient, metadataConfig);
  }

  protected boolean compareCollectionResult(Collection<String> c1, Collection<String> c2) {
    return c1.size() == c2.size() && c1.containsAll(c2) && c1.containsAll(c2);
  }

  public class AsyncMetadataTableValidateService extends HoodieAsyncService {
    private final transient ExecutorService executor = Executors.newSingleThreadExecutor();

    @Override
    protected Pair<CompletableFuture, ExecutorService> startService() {
      return Pair.of(CompletableFuture.supplyAsync(() -> {
        while (true) {
          try {
            long start = System.currentTimeMillis();
            doMetaTableValidation();
            long toSleepMs = cfg.minValidateIntervalSeconds * 1000 - (System.currentTimeMillis() - start);

            if (toSleepMs > 0) {
              LOG.info("Last validate ran less than min validate interval: " + cfg.minValidateIntervalSeconds + " s, sleep: "
                  + toSleepMs + " ms.");
              Thread.sleep(toSleepMs);
            }
          } catch (Exception e) {
            LOG.error("Shutting down AsyncMetadataTableValidateService due to exception", e);
            if (!cfg.ignoreFailed) {
              throw new HoodieException(e.getMessage(), e);
            }
          }
        }
      }, executor), executor);
    }
  }
}