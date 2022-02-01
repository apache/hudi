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
import org.apache.hudi.exception.HoodieValidationException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * A validator with spark-submit to compare list partitions and list files between metadata table and filesystem
 * <p>
 * You can specify the running mode of the validator through `--mode`.
 * There are 2 modes of the {@link HoodieMetadataTableValidator}:
 * - CONTINUOUS : This validator will compare the result of listing partitions/listing files between metadata table and filesystem every 10 minutes(default).
 * <p>
 * Example command:
 * ```
 * spark-submit \
 *  --class org.apache.hudi.utilities.HoodieMetadataTableValidator \
 *  --packages org.apache.spark:spark-avro_2.11:2.4.4 \
 *  --master spark://xxxx:7077 \
 *  --driver-memory 1g \
 *  --executor-memory 1g \
 *  $HUDI_DIR/hudi/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.11.0-SNAPSHOT.jar \
 *  --base-path basePath \
 *  --min-validate-interval-seconds 60 \
 *  --mode CONTINUOUS
 * ```
 *
 * <p>
 * You can specify the running mode of the validator through `--mode`.
 * There are 2 modes of the {@link HoodieMetadataTableValidator}:
 * - ONCE : This validator will compare the result of listing partitions/listing files between metadata table and filesystem only once.
 * <p>
 * Example command:
 * ```
 * spark-submit \
 *  --class org.apache.hudi.utilities.HoodieMetadataTableValidator \
 *  --packages org.apache.spark:spark-avro_2.11:2.4.4 \
 *  --master spark://xxxx:7077 \
 *  --driver-memory 1g \
 *  --executor-memory 1g \
 *  $HUDI_DIR/hudi/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.11.0-SNAPSHOT.jar \
 *  --base-path basePath \
 *  --min-validate-interval-seconds 60 \
 *  --mode ONCE
 * ```
 *
 */
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
        + "Set \"ONCE\" Running MetadataTableValidator once", required = false)
    public String runningMode = "once";

    @Parameter(names = {"--min-validate-interval-seconds"},
        description = "the min validate interval of each validate in continuous mode, default is 10 minutes.")
    public Integer minValidateIntervalSeconds = 10 * 60;

    @Parameter(names = {"--ignore-failed", "-ig"}, description = "Ignore metadata validate failure and continue.", required = false)
    public boolean ignoreFailed = false;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";

    @Parameter(names = {"--assume-date-partitioning"}, description = "Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path."
        + "This is a stop-gap to support tables created by versions < 0.3.1. Will be removed eventually", required = false)
    public Boolean assumeDatePartitioning = false;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client")
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
          + "   --spark-master " + sparkMaster + ", \n"
          + "   --spark-memory " + sparkMemory + ", \n"
          + "   --assumeDatePartitioning-memory " + assumeDatePartitioning + ", \n"
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
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(assumeDatePartitioning, config.assumeDatePartitioning)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, runningMode, minValidateIntervalSeconds, sparkMaster,
          sparkMemory, assumeDatePartitioning, propsFilePath, configs, help);
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
      LOG.error("Fail to do hoodie metadata table validation for " + validator.cfg, throwable);
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
    } finally {

      if (asyncMetadataTableValidateService.isPresent()) {
        asyncMetadataTableValidateService.get().shutdown(true);
      }
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
    metaClient.reloadActiveTimeline();
    String basePath = metaClient.getBasePath();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    List<String> allPartitions = validatePartitions(engineContext, basePath);

    for (String partitionPath : allPartitions) {
      validateFilesInPartition(engineContext, partitionPath);
    }

    LOG.info("MetaTable Validation Success.");
  }

  /**
   * Compare the listing partitions result between metadata table and fileSystem.
   */
  private List<String> validatePartitions(HoodieSparkEngineContext engineContext, String basePath) {
    // compare partitions
    List<String> allPartitionPathsFromFS = FSUtils.getAllPartitionPaths(engineContext, basePath, false, cfg.assumeDatePartitioning);
    List<String> allPartitionPathsMeta = FSUtils.getAllPartitionPaths(engineContext, basePath, true, cfg.assumeDatePartitioning);

    if (!compareCollectionResult(allPartitionPathsFromFS, allPartitionPathsMeta)) {
      String message = "Compare Partitions Failed! " + "AllPartitionPathsFromFS : " + allPartitionPathsFromFS + " and allPartitionPathsMeta : " + allPartitionPathsMeta;
      LOG.error(message);
      throw new HoodieValidationException(message);
    }

    return allPartitionPathsMeta;
  }

  /**
   * Compare the listing files result between metadata table and fileSystem.
   * For now, validate two kinds of apis:
   * 1. getLatestFileSlices
   * 2. getLatestBaseFiles
   * @param engineContext
   * @param partitionPath
   */
  private void validateFilesInPartition(HoodieSparkEngineContext engineContext, String partitionPath) {
    HoodieTableFileSystemView metaFsView = null;
    HoodieTableFileSystemView fsView = null;

    try {
      metaFsView = createHoodieTableFileSystemView(engineContext, true);
      fsView = createHoodieTableFileSystemView(engineContext, false);

      validateLatestFileSlices(metaFsView, fsView, partitionPath);
      validateLatestBaseFiles(metaFsView, fsView, partitionPath);

    } finally {
      if (metaFsView != null) {
        metaFsView.close();
      }

      if (fsView != null) {
        fsView.close();
      }
    }
  }

  /**
   * Compare getLatestBaseFiles between metadata table and fileSystem.
   */
  private void validateLatestBaseFiles(HoodieTableFileSystemView metaFsView, HoodieTableFileSystemView fsView, String partitionPath) {

    List<HoodieBaseFile> latestFilesFromMetadata = metaFsView.getLatestBaseFiles(partitionPath).sorted(new HoodieBaseFileCompactor()).collect(Collectors.toList());
    List<HoodieBaseFile> latestFilesFromFS = fsView.getLatestBaseFiles(partitionPath).sorted(new HoodieBaseFileCompactor()).collect(Collectors.toList());

    LOG.info("Latest base file from metadata: " + latestFilesFromMetadata);
    LOG.info("Latest base file from direct listing: " + latestFilesFromFS);
    if (!latestFilesFromMetadata.equals(latestFilesFromFS)) {
      String message = "Validation of metadata get latest base file for partition " + partitionPath + " failed. "
          + "Latest base file from metadata: " + latestFilesFromMetadata
          + "Latest base file from direct listing: " + latestFilesFromFS;
      LOG.error(message);
      throw new HoodieValidationException(message);
    } else {
      LOG.info("Validation of getLatestBaseFiles success.");
    }
  }

  /**
   * Compare getLatestFileSlices between metadata table and fileSystem.
   */
  private void validateLatestFileSlices(HoodieTableFileSystemView metaFsView, HoodieTableFileSystemView fsView, String partitionPath) {

    List<FileSlice> latestFileSlicesFromMetadataTable = metaFsView.getLatestFileSlices(partitionPath).sorted(new FileSliceCompactor()).collect(Collectors.toList());
    List<FileSlice> latestFileSlicesFromFS = fsView.getLatestFileSlices(partitionPath).sorted(new FileSliceCompactor()).collect(Collectors.toList());

    LOG.info("Latest file list from metadata: " + latestFileSlicesFromMetadataTable);
    LOG.info("Latest file list from direct listing: " + latestFileSlicesFromFS);
    if (!latestFileSlicesFromMetadataTable.equals(latestFileSlicesFromFS)) {
      String message = "Validation of metadata get latest file slices for partition " + partitionPath + " failed. "
          + "Latest file list from metadata: " + latestFileSlicesFromMetadataTable
          + "Latest file list from direct listing: " + latestFileSlicesFromFS;
      LOG.error(message);
      throw new HoodieValidationException(message);
    } else {
      LOG.info("Validation of getLatestFileSlices success.");
    }
  }

  private HoodieTableFileSystemView createHoodieTableFileSystemView(HoodieSparkEngineContext engineContext, boolean enableMetadataTable) {

    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(enableMetadataTable)
        .withAssumeDatePartitioning(cfg.assumeDatePartitioning)
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
          } catch (HoodieValidationException e) {
            LOG.error("Shutting down AsyncMetadataTableValidateService due to HoodieValidationException", e);
            if (!cfg.ignoreFailed) {
              throw e;
            }
          } catch (InterruptedException e) {
            // ignore InterruptedException here.
          }
        }
      }, executor), executor);
    }
  }

  public static class FileSliceCompactor implements Comparator<FileSlice>, Serializable {

    @Override
    public int compare(FileSlice o1, FileSlice o2) {
      return (o1.getFileGroupId() + o1.getBaseInstantTime()).compareTo(o2.getFileGroupId() + o1.getBaseInstantTime());
    }
  }

  public static class HoodieBaseFileCompactor implements Comparator<HoodieBaseFile>, Serializable {

    @Override
    public int compare(HoodieBaseFile o1, HoodieBaseFile o2) {
      return o1.getPath().compareTo(o2.getPath());
    }
  }
}