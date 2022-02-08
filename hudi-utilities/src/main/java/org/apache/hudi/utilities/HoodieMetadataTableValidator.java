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

import org.apache.hudi.async.HoodieAsyncService;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieValidationException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * A validator with spark-submit to compare list partitions and list files between metadata table and filesystem.
 *
 * <p>
 * - Default : This validator will compare the result of listing partitions/listing files between metadata table and filesystem only once.
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
 *  --validate-latest-file-slices \
 *  --validate-latest-base-files \
 *  --validate-all-file-groups
 * ```
 *
 * <p>
 * Also You can set `--continuous` for long running this validator.
 * And use `--min-validate-interval-seconds` to control the validation frequency, default is 10 minutes.
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
 *  --validate-latest-file-slices \
 *  --validate-latest-base-files \
 *  --validate-all-file-groups \
 *  --continuous \
 *  --min-validate-interval-seconds 60
 * ```
 *
 */
public class HoodieMetadataTableValidator implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieMetadataTableValidator.class);

  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private Config cfg;
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

    this.asyncMetadataTableValidateService = cfg.continuous ? Option.of(new AsyncMetadataTableValidateService()) : Option.empty();
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

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = true)
    public String basePath = null;

    @Parameter(names = {"--continuous"}, description = "Running MetadataTableValidator in continuous. "
        + "Can use --min-validate-interval-seconds to control validation frequency", required = false)
    public boolean continuous = false;

    @Parameter(names = {"--validate-latest-file-slices"}, description = "Validate latest file slices for all partitions.", required = false)
    public boolean validateLatestFileSlices = false;

    @Parameter(names = {"--validate-latest-base-files"}, description = "Validate latest base files for all partitions.", required = false)
    public boolean validateLatestBaseFiles = false;

    @Parameter(names = {"--validate-all-file-groups"}, description = "Validate all file groups, and all file slices within file groups.", required = false)
    public boolean validateAllFileGroups = false;

    @Parameter(names = {"--min-validate-interval-seconds"},
        description = "the min validate interval of each validate when set --continuous, default is 10 minutes.")
    public Integer minValidateIntervalSeconds = 10 * 60;

    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for valuation", required = false)
    public int parallelism = 200;

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
          + "   --validate-latest-file-slices " + validateLatestFileSlices + ", \n"
          + "   --validate-latest-base-files " + validateLatestBaseFiles + ", \n"
          + "   --validate-all-file-groups " + validateAllFileGroups + ", \n"
          + "   --continuous " + continuous + ", \n"
          + "   --ignore-failed " + ignoreFailed + ", \n"
          + "   --min-validate-interval-seconds " + minValidateIntervalSeconds + ", \n"
          + "   --parallelism " + parallelism + ", \n"
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
          && Objects.equals(continuous, config.continuous)
          && Objects.equals(validateLatestFileSlices, config.validateLatestFileSlices)
          && Objects.equals(validateLatestBaseFiles, config.validateLatestBaseFiles)
          && Objects.equals(validateAllFileGroups, config.validateAllFileGroups)
          && Objects.equals(minValidateIntervalSeconds, config.minValidateIntervalSeconds)
          && Objects.equals(parallelism, config.parallelism)
          && Objects.equals(ignoreFailed, config.ignoreFailed)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(assumeDatePartitioning, config.assumeDatePartitioning)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, continuous, validateLatestFileSlices, validateLatestBaseFiles, validateAllFileGroups,
          minValidateIntervalSeconds, parallelism, ignoreFailed, sparkMaster, sparkMemory, assumeDatePartitioning, propsFilePath, configs, help);
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
      if (cfg.continuous) {
        LOG.info(" ****** do hoodie metadata table validation in CONTINUOUS mode ******");
        doHoodieMetadataTableValidationContinuous();
      } else {
        LOG.info(" ****** do hoodie metadata table validation once ******");
        doHoodieMetadataTableValidationOnce();
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
    try {
      doMetadataTableValidation();
    } catch (HoodieValidationException e) {
      LOG.error("Metadata table validation failed to HoodieValidationException", e);
      if (!cfg.ignoreFailed) {
        throw e;
      }
    }
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

  public void doMetadataTableValidation() {
    boolean finalResult = true;
    metaClient.reloadActiveTimeline();
    String basePath = metaClient.getBasePath();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    List<String> allPartitions = validatePartitions(engineContext, basePath);
    HoodieTableFileSystemView metaFsView = createHoodieTableFileSystemView(engineContext, true);
    HoodieTableFileSystemView fsView = createHoodieTableFileSystemView(engineContext, false);

    List<Boolean> result = engineContext.parallelize(allPartitions, allPartitions.size()).map(partitionPath -> {
      try {
        validateFilesInPartition(metaFsView, fsView, partitionPath);
        LOG.info("Metadata table validation succeeded for " + partitionPath);
        return true;
      } catch (HoodieValidationException e) {
        LOG.error("Metadata table validation failed for " + partitionPath + " due to HoodieValidationException", e);
        if (!cfg.ignoreFailed) {
          throw e;
        }
        return false;
      }
    }).collectAsList();

    for (Boolean res : result) {
      finalResult &= res;
    }

    if (finalResult) {
      LOG.info("Metadata table validation succeeded.");
    } else {
      LOG.warn("Metadata table validation failed.");
    }
  }

  /**
   * Compare the listing partitions result between metadata table and fileSystem.
   */
  private List<String> validatePartitions(HoodieSparkEngineContext engineContext, String basePath) {
    // compare partitions
    List<String> allPartitionPathsFromFS = FSUtils.getAllPartitionPaths(engineContext, basePath, false, cfg.assumeDatePartitioning);
    List<String> allPartitionPathsMeta = FSUtils.getAllPartitionPaths(engineContext, basePath, true, cfg.assumeDatePartitioning);

    Collections.sort(allPartitionPathsFromFS);
    Collections.sort(allPartitionPathsMeta);

    if (allPartitionPathsFromFS.size() != allPartitionPathsMeta.size()
        || !allPartitionPathsFromFS.equals(allPartitionPathsMeta)) {
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
   * 3. getAllFileGroups and getAllFileSlices
   * @param metaFsView
   * @param fsView
   * @param partitionPath
   */
  private void validateFilesInPartition(HoodieTableFileSystemView metaFsView, HoodieTableFileSystemView fsView, String partitionPath) {
    if (cfg.validateLatestFileSlices) {
      validateLatestFileSlices(metaFsView, fsView, partitionPath);
    }

    if (cfg.validateLatestBaseFiles) {
      validateLatestBaseFiles(metaFsView, fsView, partitionPath);
    }

    if (cfg.validateAllFileGroups) {
      validateAllFileGroups(metaFsView, fsView, partitionPath);
    }
  }

  private void validateAllFileGroups(HoodieTableFileSystemView metaFsView, HoodieTableFileSystemView fsView, String partitionPath) {
    List<HoodieFileGroup> fileGroupsFromMetadata = metaFsView.getAllFileGroups(partitionPath).sorted(new HoodieFileGroupCompactor()).collect(Collectors.toList());
    List<HoodieFileGroup> fileGroupsFromFS = fsView.getAllFileGroups(partitionPath).sorted(new HoodieFileGroupCompactor()).collect(Collectors.toList());

    List<FileSlice> allFileSlicesFromMeta = fileGroupsFromMetadata.stream().flatMap(HoodieFileGroup::getAllFileSlices).sorted(new FileSliceCompactor()).collect(Collectors.toList());
    List<FileSlice> allFileSlicesFromFS = fileGroupsFromFS.stream().flatMap(HoodieFileGroup::getAllFileSlices).sorted(new FileSliceCompactor()).collect(Collectors.toList());

    LOG.info("All file slices from metadata: " + allFileSlicesFromMeta + ". For partitions " + partitionPath);
    LOG.info("All file slices from direct listing: " + allFileSlicesFromFS + ". For partitions " + partitionPath);
    validateFileSlice(allFileSlicesFromMeta, allFileSlicesFromFS, partitionPath);

    LOG.info("Validation of all file groups succeeded for partition " + partitionPath);
  }

  private void validateFileSlice(List<FileSlice> fileSlicesFromMeta, List<FileSlice> fileSlicesFromFS, String partitionPath) {
    if (fileSlicesFromMeta.size() != fileSlicesFromFS.size() || !fileSlicesFromMeta.equals(fileSlicesFromFS)) {
      String message = "Validation of metadata file slices for partition " + partitionPath + " failed. "
          + "File slices from metadata: " + fileSlicesFromMeta
          + "File slices from direct listing: " + fileSlicesFromFS;
      LOG.error(message);
      throw new HoodieValidationException(message);
    } else {
      LOG.info("Validation of file slices succeeded for partition " + partitionPath);
    }
  }

  /**
   * Compare getLatestBaseFiles between metadata table and fileSystem.
   */
  private void validateLatestBaseFiles(HoodieTableFileSystemView metaFsView, HoodieTableFileSystemView fsView, String partitionPath) {

    List<HoodieBaseFile> latestFilesFromMetadata = metaFsView.getLatestBaseFiles(partitionPath).sorted(new HoodieBaseFileCompactor()).collect(Collectors.toList());
    List<HoodieBaseFile> latestFilesFromFS = fsView.getLatestBaseFiles(partitionPath).sorted(new HoodieBaseFileCompactor()).collect(Collectors.toList());

    LOG.info("Latest base file from metadata: " + latestFilesFromMetadata + ". For partitions " + partitionPath);
    LOG.info("Latest base file from direct listing: " + latestFilesFromFS + ". For partitions " + partitionPath);
    if (latestFilesFromMetadata.size() != latestFilesFromFS.size()
        || !latestFilesFromMetadata.equals(latestFilesFromFS)) {
      String message = "Validation of metadata get latest base file for partition " + partitionPath + " failed. "
          + "Latest base file from metadata: " + latestFilesFromMetadata
          + "Latest base file from direct listing: " + latestFilesFromFS;
      LOG.error(message);
      throw new HoodieValidationException(message);
    } else {
      LOG.info("Validation of getLatestBaseFiles succeeded for partition " + partitionPath);
    }
  }

  /**
   * Compare getLatestFileSlices between metadata table and fileSystem.
   */
  private void validateLatestFileSlices(HoodieTableFileSystemView metaFsView, HoodieTableFileSystemView fsView, String partitionPath) {

    List<FileSlice> latestFileSlicesFromMetadataTable = metaFsView.getLatestFileSlices(partitionPath).sorted(new FileSliceCompactor()).collect(Collectors.toList());
    List<FileSlice> latestFileSlicesFromFS = fsView.getLatestFileSlices(partitionPath).sorted(new FileSliceCompactor()).collect(Collectors.toList());

    LOG.info("Latest file list from metadata: " + latestFileSlicesFromMetadataTable + ". For partition " + partitionPath);
    LOG.info("Latest file list from direct listing: " + latestFileSlicesFromFS + ". For partition " + partitionPath);

    validateFileSlice(latestFileSlicesFromMetadataTable, latestFileSlicesFromFS, partitionPath);
    LOG.info("Validation of getLatestFileSlices succeeded for partition " + partitionPath);
  }

  private HoodieTableFileSystemView createHoodieTableFileSystemView(HoodieSparkEngineContext engineContext, boolean enableMetadataTable) {

    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(enableMetadataTable)
        .withAssumeDatePartitioning(cfg.assumeDatePartitioning)
        .build();

    return FileSystemViewManager.createInMemoryFileSystemView(engineContext,
        metaClient, metadataConfig);
  }

  public class AsyncMetadataTableValidateService extends HoodieAsyncService {
    private final transient ExecutorService executor = Executors.newSingleThreadExecutor();

    @Override
    protected Pair<CompletableFuture, ExecutorService> startService() {
      return Pair.of(CompletableFuture.supplyAsync(() -> {
        while (true) {
          try {
            long start = System.currentTimeMillis();
            doMetadataTableValidation();
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
      return (o1.getPartitionPath() + o1.getFileId() + o1.getBaseInstantTime())
          .compareTo(o2.getPartitionPath() + o2.getFileId() + o2.getBaseInstantTime());
    }
  }

  public static class HoodieBaseFileCompactor implements Comparator<HoodieBaseFile>, Serializable {

    @Override
    public int compare(HoodieBaseFile o1, HoodieBaseFile o2) {
      return o1.getPath().compareTo(o2.getPath());
    }
  }

  public static class HoodieFileGroupCompactor implements Comparator<HoodieFileGroup>, Serializable {

    @Override
    public int compare(HoodieFileGroup o1, HoodieFileGroup o2) {
      return o1.getFileGroupId().compareTo(o2.getFileGroupId());
    }
  }
}