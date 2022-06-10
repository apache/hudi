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
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.utilities.util.BloomFilterData;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import jline.internal.Log;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.INSTANT_TIME;

/**
 * A validator with spark-submit to compare information, such as partitions, file listing, index, etc.,
 * between metadata table and filesystem.
 * <p>
 * There are five validation tasks, that can be enabled independently through the following CLI options:
 * - `--validate-latest-file-slices`: validate latest file slices for all partitions.
 * - `--validate-latest-base-files`: validate latest base files for all partitions.
 * - `--validate-all-file-groups`: validate all file groups, and all file slices within file groups.
 * - `--validate-all-column-stats`: validate column stats for all columns in the schema
 * - `--validate-bloom-filters`: validate bloom filters of base files
 *
 * If the Hudi table is on the local file system, the base path passed to `--base-path` must have
 * "file:" prefix to avoid validation failure.
 * <p>
 * - Default : This validator will compare the results between metadata table and filesystem only once.
 * <p>
 * Example command:
 * ```
 * spark-submit \
 *  --class org.apache.hudi.utilities.HoodieMetadataTableValidator \
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

  private final String taskLabels;

  public HoodieMetadataTableValidator(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
    this.taskLabels = StringUtils.EMPTY_STRING;
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
    this.taskLabels = generateValidationTaskLabels();
  }

  private String generateValidationTaskLabels() {
    List<String> labelList = new ArrayList<>();
    if (cfg.validateLatestBaseFiles) {
      labelList.add("validate-latest-base-files");
    }
    if (cfg.validateLatestFileSlices) {
      labelList.add("validate-latest-file-slices");
    }
    if (cfg.validateAllFileGroups) {
      labelList.add("validate-all-file-groups");
    }
    if (cfg.validateAllColumnStats) {
      labelList.add("validate-all-column-stats");
    }
    if (cfg.validateBloomFilters) {
      labelList.add("validate-bloom-filters");
    }
    return String.join(",", labelList);
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

    @Parameter(names = {"--skip-data-files-for-cleaning"}, description = "Skip to compare the data files which are under deletion by cleaner", required = false)
    public boolean skipDataFilesForCleaning = false;

    @Parameter(names = {"--validate-latest-file-slices"}, description = "Validate latest file slices for all partitions.", required = false)
    public boolean validateLatestFileSlices = false;

    @Parameter(names = {"--validate-latest-base-files"}, description = "Validate latest base files for all partitions.", required = false)
    public boolean validateLatestBaseFiles = false;

    @Parameter(names = {"--validate-all-file-groups"}, description = "Validate all file groups, and all file slices within file groups.", required = false)
    public boolean validateAllFileGroups = false;

    @Parameter(names = {"--validate-all-column-stats"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean validateAllColumnStats = false;

    @Parameter(names = {"--validate-bloom-filters"}, description = "Validate bloom filters of base files", required = false)
    public boolean validateBloomFilters = false;

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
          + "   --validate-all-column-stats " + validateAllColumnStats + ", \n"
          + "   --validate-bloom-filters " + validateBloomFilters + ", \n"
          + "   --continuous " + continuous + ", \n"
          + "   --skip-data-files-for-cleaning " + skipDataFilesForCleaning + ", \n"
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
          && Objects.equals(skipDataFilesForCleaning, config.skipDataFilesForCleaning)
          && Objects.equals(validateLatestFileSlices, config.validateLatestFileSlices)
          && Objects.equals(validateLatestBaseFiles, config.validateLatestBaseFiles)
          && Objects.equals(validateAllFileGroups, config.validateAllFileGroups)
          && Objects.equals(validateAllColumnStats, config.validateAllColumnStats)
          && Objects.equals(validateBloomFilters, config.validateBloomFilters)
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
      return Objects.hash(basePath, continuous, skipDataFilesForCleaning, validateLatestFileSlices, validateLatestBaseFiles,
          validateAllFileGroups, validateAllColumnStats, validateBloomFilters, minValidateIntervalSeconds,
          parallelism, ignoreFailed, sparkMaster, sparkMemory, assumeDatePartitioning, propsFilePath, configs, help);
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
    Set<String> baseFilesForCleaning = Collections.emptySet();

    // check metadata table is available to read.
    checkMetadataTableIsAvailable();

    if (cfg.skipDataFilesForCleaning) {
      HoodieTimeline inflightCleaningTimeline = metaClient.getActiveTimeline().getCleanerTimeline().filterInflights();

      baseFilesForCleaning = inflightCleaningTimeline.getInstants().flatMap(instant -> {
        try {
          // convert inflight instant to requested and get clean plan
          instant = new HoodieInstant(HoodieInstant.State.REQUESTED, instant.getAction(), instant.getTimestamp());
          HoodieCleanerPlan cleanerPlan = CleanerUtils.getCleanerPlan(metaClient, instant);

          return cleanerPlan.getFilePathsToBeDeletedPerPartition().values().stream().flatMap(cleanerFileInfoList -> {
            return cleanerFileInfoList.stream().map(fileInfo -> {
              return new Path(fileInfo.getFilePath()).getName();
            });
          });

        } catch (IOException e) {
          throw new HoodieIOException("Error reading cleaner metadata for " + instant);
        }
        // only take care of base files here.
      }).filter(path -> {
        String fileExtension = FSUtils.getFileExtension(path);
        return HoodieFileFormat.BASE_FILE_EXTENSIONS.contains(fileExtension);
      }).collect(Collectors.toSet());
    }

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    List<String> allPartitions = validatePartitions(engineContext, basePath);
    HoodieMetadataValidationContext metadataTableBasedContext =
        new HoodieMetadataValidationContext(engineContext, cfg, metaClient, true);
    HoodieMetadataValidationContext fsBasedContext =
        new HoodieMetadataValidationContext(engineContext, cfg, metaClient, false);

    Set<String> finalBaseFilesForCleaning = baseFilesForCleaning;
    List<Boolean> result = engineContext.parallelize(allPartitions, allPartitions.size()).map(partitionPath -> {
      try {
        validateFilesInPartition(metadataTableBasedContext, fsBasedContext, partitionPath, finalBaseFilesForCleaning);
        LOG.info(String.format("Metadata table validation succeeded for partition %s (partition %s)", partitionPath, taskLabels));
        return true;
      } catch (HoodieValidationException e) {
        LOG.error(
            String.format("Metadata table validation failed for partition %s due to HoodieValidationException (partition %s)",
                partitionPath, taskLabels), e);
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
      LOG.info(String.format("Metadata table validation succeeded (%s).", taskLabels));
    } else {
      LOG.warn(String.format("Metadata table validation failed (%s).", taskLabels));
    }
  }

  /**
   * Check metadata is initialized and available to ready.
   * If not we will log.warn and skip current validation.
   */
  private void checkMetadataTableIsAvailable() {
    try {
      HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder()
          .setConf(jsc.hadoopConfiguration()).setBasePath(new Path(cfg.basePath, HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH).toString())
          .setLoadActiveTimelineOnLoad(true)
          .build();
      int finishedInstants = mdtMetaClient.getActiveTimeline().filterCompletedInstants().countInstants();
      if (finishedInstants == 0) {
        throw new HoodieValidationException("There is no completed instant for metadata table.");
      }
    } catch (Exception ex) {
      LOG.warn("Metadata table is not available to ready for now, ", ex);
    }
  }

  /**
   * Compare the listing partitions result between metadata table and fileSystem.
   */
  private List<String> validatePartitions(HoodieSparkEngineContext engineContext, String basePath) {
    // compare partitions
    List<String> allPartitionPathsFromFS = FSUtils.getAllPartitionPaths(engineContext, basePath, false, cfg.assumeDatePartitioning);
    HoodieTimeline completedTimeline = metaClient.getActiveTimeline().filterCompletedInstants();

    // ignore partitions created by uncommitted ingestion.
    allPartitionPathsFromFS = allPartitionPathsFromFS.stream().parallel().filter(part -> {
      HoodiePartitionMetadata hoodiePartitionMetadata =
          new HoodiePartitionMetadata(metaClient.getFs(), FSUtils.getPartitionPath(basePath, part));

      Option<String> instantOption = hoodiePartitionMetadata.readPartitionCreatedCommitTime();
      if (instantOption.isPresent()) {
        String instantTime = instantOption.get();
        return completedTimeline.containsOrBeforeTimelineStarts(instantTime);
      } else {
        return false;
      }
    }).collect(Collectors.toList());

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
   * Compare the file listing and index data between metadata table and fileSystem.
   * For now, validate five kinds of apis:
   * 1. HoodieMetadataFileSystemView::getLatestFileSlices
   * 2. HoodieMetadataFileSystemView::getLatestBaseFiles
   * 3. HoodieMetadataFileSystemView::getAllFileGroups and HoodieMetadataFileSystemView::getAllFileSlices
   * 4. HoodieBackedTableMetadata::getColumnStats
   * 5. HoodieBackedTableMetadata::getBloomFilters
   *
   * @param metadataTableBasedContext Validation context containing information based on metadata table
   * @param fsBasedContext            Validation context containing information based on the file system
   * @param partitionPath             Partition path String
   * @param baseDataFilesForCleaning    Base files for un-complete cleaner action
   */
  private void validateFilesInPartition(
      HoodieMetadataValidationContext metadataTableBasedContext,
      HoodieMetadataValidationContext fsBasedContext, String partitionPath,
      Set<String> baseDataFilesForCleaning) {
    if (cfg.validateLatestFileSlices) {
      validateLatestFileSlices(metadataTableBasedContext, fsBasedContext, partitionPath, baseDataFilesForCleaning);
    }

    if (cfg.validateLatestBaseFiles) {
      validateLatestBaseFiles(metadataTableBasedContext, fsBasedContext, partitionPath, baseDataFilesForCleaning);
    }

    if (cfg.validateAllFileGroups) {
      validateAllFileGroups(metadataTableBasedContext, fsBasedContext, partitionPath, baseDataFilesForCleaning);
    }

    if (cfg.validateAllColumnStats) {
      validateAllColumnStats(metadataTableBasedContext, fsBasedContext, partitionPath, baseDataFilesForCleaning);
    }

    if (cfg.validateBloomFilters) {
      validateBloomFilters(metadataTableBasedContext, fsBasedContext, partitionPath, baseDataFilesForCleaning);
    }
  }

  private void validateAllFileGroups(
      HoodieMetadataValidationContext metadataTableBasedContext,
      HoodieMetadataValidationContext fsBasedContext,
      String partitionPath,
      Set<String> baseDataFilesForCleaning) {

    List<FileSlice> allFileSlicesFromMeta;
    List<FileSlice> allFileSlicesFromFS;

    if (!baseDataFilesForCleaning.isEmpty()) {
      List<FileSlice> fileSlicesFromMeta = metadataTableBasedContext
          .getSortedAllFileGroupList(partitionPath).stream()
          .flatMap(HoodieFileGroup::getAllFileSlices).sorted(new FileSliceComparator())
          .collect(Collectors.toList());
      List<FileSlice> fileSlicesFromFS = fsBasedContext
          .getSortedAllFileGroupList(partitionPath).stream()
          .flatMap(HoodieFileGroup::getAllFileSlices).sorted(new FileSliceComparator())
          .collect(Collectors.toList());

      allFileSlicesFromMeta = filterFileSliceBasedOnInflightCleaning(fileSlicesFromMeta, baseDataFilesForCleaning);
      allFileSlicesFromFS = filterFileSliceBasedOnInflightCleaning(fileSlicesFromFS, baseDataFilesForCleaning);
    } else {
      allFileSlicesFromMeta = metadataTableBasedContext
          .getSortedAllFileGroupList(partitionPath).stream()
          .flatMap(HoodieFileGroup::getAllFileSlices).sorted(new FileSliceComparator())
          .collect(Collectors.toList());
      allFileSlicesFromFS = fsBasedContext
          .getSortedAllFileGroupList(partitionPath).stream()
          .flatMap(HoodieFileGroup::getAllFileSlices).sorted(new FileSliceComparator())
          .collect(Collectors.toList());
    }

    LOG.debug("All file slices from metadata: " + allFileSlicesFromMeta + ". For partitions " + partitionPath);
    LOG.debug("All file slices from direct listing: " + allFileSlicesFromFS + ". For partitions " + partitionPath);
    validateFileSlices(
        allFileSlicesFromMeta, allFileSlicesFromFS, partitionPath,
        fsBasedContext.getMetaClient(), "all file groups");
  }

  /**
   * Compare getLatestBaseFiles between metadata table and fileSystem.
   */
  private void validateLatestBaseFiles(
      HoodieMetadataValidationContext metadataTableBasedContext,
      HoodieMetadataValidationContext fsBasedContext,
      String partitionPath,
      Set<String> baseDataFilesForCleaning) {

    List<HoodieBaseFile> latestFilesFromMetadata;
    List<HoodieBaseFile> latestFilesFromFS;

    if (!baseDataFilesForCleaning.isEmpty()) {
      latestFilesFromMetadata = filterBaseFileBasedOnInflightCleaning(metadataTableBasedContext.getSortedLatestBaseFileList(partitionPath), baseDataFilesForCleaning);
      latestFilesFromFS = filterBaseFileBasedOnInflightCleaning(fsBasedContext.getSortedLatestBaseFileList(partitionPath), baseDataFilesForCleaning);
    } else {
      latestFilesFromMetadata = metadataTableBasedContext.getSortedLatestBaseFileList(partitionPath);
      latestFilesFromFS = fsBasedContext.getSortedLatestBaseFileList(partitionPath);
    }

    LOG.debug("Latest base file from metadata: " + latestFilesFromMetadata + ". For partitions " + partitionPath);
    LOG.debug("Latest base file from direct listing: " + latestFilesFromFS + ". For partitions " + partitionPath);

    validate(latestFilesFromMetadata, latestFilesFromFS, partitionPath, "latest base files");
  }

  /**
   * Compare getLatestFileSlices between metadata table and fileSystem.
   */
  private void validateLatestFileSlices(
      HoodieMetadataValidationContext metadataTableBasedContext,
      HoodieMetadataValidationContext fsBasedContext,
      String partitionPath,
      Set<String> baseDataFilesForCleaning) {
    List<FileSlice> latestFileSlicesFromMetadataTable;
    List<FileSlice> latestFileSlicesFromFS;

    if (!baseDataFilesForCleaning.isEmpty()) {
      latestFileSlicesFromMetadataTable = filterFileSliceBasedOnInflightCleaning(metadataTableBasedContext.getSortedLatestFileSliceList(partitionPath), baseDataFilesForCleaning);
      latestFileSlicesFromFS = filterFileSliceBasedOnInflightCleaning(fsBasedContext.getSortedLatestFileSliceList(partitionPath), baseDataFilesForCleaning);
    } else {
      latestFileSlicesFromMetadataTable = metadataTableBasedContext.getSortedLatestFileSliceList(partitionPath);
      latestFileSlicesFromFS = fsBasedContext.getSortedLatestFileSliceList(partitionPath);
    }

    LOG.debug("Latest file list from metadata: " + latestFileSlicesFromMetadataTable + ". For partition " + partitionPath);
    LOG.debug("Latest file list from direct listing: " + latestFileSlicesFromFS + ". For partition " + partitionPath);

    validateFileSlices(
        latestFileSlicesFromMetadataTable, latestFileSlicesFromFS, partitionPath,
        fsBasedContext.getMetaClient(), "latest file slices");
  }

  private List<FileSlice> filterFileSliceBasedOnInflightCleaning(List<FileSlice> sortedLatestFileSliceList, Set<String> baseDataFilesForCleaning) {
    return sortedLatestFileSliceList.stream()
        .filter(fileSlice -> {
          if (!fileSlice.getBaseFile().isPresent()) {
            return true;
          } else {
            return !baseDataFilesForCleaning.contains(fileSlice.getBaseFile().get().getFileName());
          }
        }).collect(Collectors.toList());
  }

  private List<HoodieBaseFile> filterBaseFileBasedOnInflightCleaning(List<HoodieBaseFile> sortedBaseFileList, Set<String> baseDataFilesForCleaning) {
    return sortedBaseFileList.stream()
        .filter(baseFile -> {
          return !baseDataFilesForCleaning.contains(baseFile.getFileName());
        }).collect(Collectors.toList());
  }

  @SuppressWarnings("rawtypes")
  private void validateAllColumnStats(
      HoodieMetadataValidationContext metadataTableBasedContext,
      HoodieMetadataValidationContext fsBasedContext,
      String partitionPath,
      Set<String> baseDataFilesForCleaning) {

    List<String> latestBaseFilenameList = getLatestBaseFileNames(fsBasedContext, partitionPath, baseDataFilesForCleaning);
    List<HoodieColumnRangeMetadata<Comparable>> metadataBasedColStats = metadataTableBasedContext
        .getSortedColumnStatsList(partitionPath, latestBaseFilenameList);
    List<HoodieColumnRangeMetadata<Comparable>> fsBasedColStats = fsBasedContext
        .getSortedColumnStatsList(partitionPath, latestBaseFilenameList);

    validate(metadataBasedColStats, fsBasedColStats, partitionPath, "column stats");
  }

  private void validateBloomFilters(
      HoodieMetadataValidationContext metadataTableBasedContext,
      HoodieMetadataValidationContext fsBasedContext,
      String partitionPath,
      Set<String> baseDataFilesForCleaning) {

    List<String> latestBaseFilenameList = getLatestBaseFileNames(fsBasedContext, partitionPath, baseDataFilesForCleaning);
    List<BloomFilterData> metadataBasedBloomFilters = metadataTableBasedContext
        .getSortedBloomFilterList(partitionPath, latestBaseFilenameList);
    List<BloomFilterData> fsBasedBloomFilters = fsBasedContext
        .getSortedBloomFilterList(partitionPath, latestBaseFilenameList);

    validate(metadataBasedBloomFilters, fsBasedBloomFilters, partitionPath, "bloom filters");
  }

  private List<String> getLatestBaseFileNames(HoodieMetadataValidationContext fsBasedContext, String partitionPath, Set<String> baseDataFilesForCleaning) {
    List<String> latestBaseFilenameList;
    if (!baseDataFilesForCleaning.isEmpty()) {
      List<HoodieBaseFile> sortedLatestBaseFileList = fsBasedContext.getSortedLatestBaseFileList(partitionPath);
      latestBaseFilenameList = filterBaseFileBasedOnInflightCleaning(sortedLatestBaseFileList, baseDataFilesForCleaning)
          .stream().map(BaseFile::getFileName).collect(Collectors.toList());
    } else {
      latestBaseFilenameList = fsBasedContext.getSortedLatestBaseFileList(partitionPath)
          .stream().map(BaseFile::getFileName).collect(Collectors.toList());
    }
    return latestBaseFilenameList;
  }

  private <T> void validate(
      List<T> infoListFromMetadataTable, List<T> infoListFromFS, String partitionPath, String label) {
    if (infoListFromMetadataTable.size() != infoListFromFS.size()
        || !infoListFromMetadataTable.equals(infoListFromFS)) {
      String message = String.format("Validation of %s for partition %s failed."
              + "\n%s from metadata: %s\n%s from file system and base files: %s",
          label, partitionPath, label, infoListFromMetadataTable, label, infoListFromFS);
      LOG.error(message);
      throw new HoodieValidationException(message);
    } else {
      LOG.info(String.format("Validation of %s succeeded for partition %s", label, partitionPath));
    }
  }

  private void validateFileSlices(
      List<FileSlice> fileSliceListFromMetadataTable, List<FileSlice> fileSliceListFromFS,
      String partitionPath, HoodieTableMetaClient metaClient, String label) {
    boolean mismatch = false;
    if (fileSliceListFromMetadataTable.size() != fileSliceListFromFS.size()) {
      mismatch = true;
    } else if (!fileSliceListFromMetadataTable.equals(fileSliceListFromFS)) {
      for (int i = 0; i < fileSliceListFromMetadataTable.size(); i++) {
        FileSlice fileSlice1 = fileSliceListFromMetadataTable.get(i);
        FileSlice fileSlice2 = fileSliceListFromFS.get(i);
        if (!Objects.equals(fileSlice1.getFileGroupId(), fileSlice2.getFileGroupId())
            || !Objects.equals(fileSlice1.getBaseInstantTime(), fileSlice2.getBaseInstantTime())
            || !Objects.equals(fileSlice1.getBaseFile(), fileSlice2.getBaseFile())) {
          mismatch = true;
          break;
        }
        if (!areFileSliceCommittedLogFilesMatching(fileSlice1, fileSlice2, metaClient)) {
          mismatch = true;
          break;
        } else {
          LOG.warn(String.format("There are uncommitted log files in the latest file slices "
              + "but the committed log files match: %s %s", fileSlice1, fileSlice2));
        }
      }
    }

    if (mismatch) {
      String message = String.format("Validation of %s for partition %s failed."
              + "\n%s from metadata: %s\n%s from file system and base files: %s",
          label, partitionPath, label, fileSliceListFromMetadataTable, label, fileSliceListFromFS);
      LOG.error(message);
      throw new HoodieValidationException(message);
    } else {
      LOG.info(String.format("Validation of %s succeeded for partition %s", label, partitionPath));
    }
  }

  /**
   * Compares committed log files from two file slices.
   *
   * @param fs1        File slice 1
   * @param fs2        File slice 2
   * @param metaClient {@link HoodieTableMetaClient} instance
   * @return {@code true} if matching; {@code false} otherwise.
   */
  private boolean areFileSliceCommittedLogFilesMatching(
      FileSlice fs1, FileSlice fs2, HoodieTableMetaClient metaClient) {
    Set<String> fs1LogPathSet =
        fs1.getLogFiles().map(f -> f.getPath().toString()).collect(Collectors.toSet());
    Set<String> fs2LogPathSet =
        fs2.getLogFiles().map(f -> f.getPath().toString()).collect(Collectors.toSet());
    Set<String> commonLogPathSet = new HashSet<>(fs1LogPathSet);
    commonLogPathSet.retainAll(fs2LogPathSet);
    // Only keep log file paths that differ
    fs1LogPathSet.removeAll(commonLogPathSet);
    fs2LogPathSet.removeAll(commonLogPathSet);
    // Check if the remaining log files are uncommitted.  If there is any log file
    // that is committed, the committed log files of two file slices are different
    FileSystem fileSystem = metaClient.getFs();
    HoodieTimeline commitsTimeline = metaClient.getCommitsTimeline();
    if (hasCommittedLogFiles(fileSystem, fs1LogPathSet, commitsTimeline)) {
      LOG.error("The first file slice has committed log files that cause mismatching: "
          + fs1);
      return false;
    }
    if (hasCommittedLogFiles(fileSystem, fs2LogPathSet, commitsTimeline)) {
      LOG.error("The second file slice has committed log files that cause mismatching: "
          + fs2);
      return false;
    }
    return true;
  }

  private boolean hasCommittedLogFiles(
      FileSystem fs, Set<String> logFilePathSet, HoodieTimeline commitsTimeline) {
    if (logFilePathSet.isEmpty()) {
      return false;
    }

    AvroSchemaConverter converter = new AvroSchemaConverter();
    HoodieTimeline completedInstantsTimeline = commitsTimeline.filterCompletedInstants();
    HoodieTimeline inflightInstantsTimeline = commitsTimeline.filterInflights();

    for (String logFilePathStr : logFilePathSet) {
      HoodieLogFormat.Reader reader = null;
      try {
        MessageType messageType =
            TableSchemaResolver.readSchemaFromLogFile(fs, new Path(logFilePathStr));
        if (messageType == null) {
          LOG.warn(String.format("Cannot read schema from log file %s. "
              + "Skip the check as it's likely being written by an inflight instant.", logFilePathStr));
          continue;
        }
        Schema readerSchema = converter.convert(messageType);
        reader =
            HoodieLogFormat.newReader(fs, new HoodieLogFile(new Path(logFilePathStr)), readerSchema);
        // read the avro blocks
        if (reader.hasNext()) {
          HoodieLogBlock block = reader.next();
          final String instantTime = block.getLogBlockHeader().get(INSTANT_TIME);
          if (!completedInstantsTimeline.containsOrBeforeTimelineStarts(instantTime)
              || inflightInstantsTimeline.containsInstant(instantTime)) {
            // hit an uncommitted block possibly from a failed write
            LOG.warn("Log file is uncommitted: " + logFilePathStr);
          } else {
            LOG.warn("Log file is committed: " + logFilePathStr);
            return true;
          }
        } else {
          LOG.warn("There is no log block in " + logFilePathStr);
        }
      } catch (IOException e) {
        LOG.warn(String.format("Cannot read log file %s: %s. "
                + "Skip the check as it's likely being written by an inflight instant.",
            logFilePathStr, e.getMessage()), e);
      } finally {
        FileIOUtils.closeQuietly(reader);
      }
    }
    return false;
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

  public static class FileSliceComparator implements Comparator<FileSlice>, Serializable {

    @Override
    public int compare(FileSlice o1, FileSlice o2) {
      return (o1.getPartitionPath() + o1.getFileId() + o1.getBaseInstantTime())
          .compareTo(o2.getPartitionPath() + o2.getFileId() + o2.getBaseInstantTime());
    }
  }

  public static class HoodieBaseFileComparator implements Comparator<HoodieBaseFile>, Serializable {

    @Override
    public int compare(HoodieBaseFile o1, HoodieBaseFile o2) {
      return o1.getPath().compareTo(o2.getPath());
    }
  }

  public static class HoodieFileGroupComparator implements Comparator<HoodieFileGroup>, Serializable {

    @Override
    public int compare(HoodieFileGroup o1, HoodieFileGroup o2) {
      return o1.getFileGroupId().compareTo(o2.getFileGroupId());
    }
  }

  public static class HoodieColumnRangeMetadataComparator
      implements Comparator<HoodieColumnRangeMetadata<Comparable>>, Serializable {

    @Override
    public int compare(HoodieColumnRangeMetadata<Comparable> o1, HoodieColumnRangeMetadata<Comparable> o2) {
      return o1.toString().compareTo(o2.toString());
    }
  }

  /**
   * Class for storing relevant information for metadata table validation.
   * <p>
   * If metadata table is disabled, the APIs provide the information, e.g., file listing,
   * index, from the file system and base files.  If metadata table is enabled, the APIs
   * provide the information from the metadata table.  The same API is expected to return
   * the same information regardless of whether metadata table is enabled, which is
   * verified in the {@link HoodieMetadataTableValidator}.
   */
  private static class HoodieMetadataValidationContext implements Serializable {
    private HoodieTableMetaClient metaClient;
    private HoodieTableFileSystemView fileSystemView;
    private HoodieTableMetadata tableMetadata;
    private boolean enableMetadataTable;
    private List<String> allColumnNameList;

    public HoodieMetadataValidationContext(
        HoodieEngineContext engineContext, Config cfg, HoodieTableMetaClient metaClient,
        boolean enableMetadataTable) {
      this.metaClient = metaClient;
      this.enableMetadataTable = enableMetadataTable;
      HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
          .enable(enableMetadataTable)
          .withMetadataIndexBloomFilter(enableMetadataTable)
          .withMetadataIndexColumnStats(enableMetadataTable)
          .withAssumeDatePartitioning(cfg.assumeDatePartitioning)
          .build();
      this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(engineContext,
          metaClient, metadataConfig);
      this.tableMetadata = HoodieTableMetadata.create(engineContext, metadataConfig, metaClient.getBasePath(),
          FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue());
      if (metaClient.getCommitsTimeline().filterCompletedInstants().countInstants() > 0) {
        this.allColumnNameList = getAllColumnNames();
      }
    }

    public HoodieTableMetaClient getMetaClient() {
      return metaClient;
    }

    public List<HoodieBaseFile> getSortedLatestBaseFileList(String partitionPath) {
      return fileSystemView.getLatestBaseFiles(partitionPath)
          .sorted(new HoodieBaseFileComparator()).collect(Collectors.toList());
    }

    public List<FileSlice> getSortedLatestFileSliceList(String partitionPath) {
      return fileSystemView.getLatestFileSlices(partitionPath)
          .sorted(new FileSliceComparator()).collect(Collectors.toList());
    }

    public List<HoodieFileGroup> getSortedAllFileGroupList(String partitionPath) {
      return fileSystemView.getAllFileGroups(partitionPath)
          .sorted(new HoodieFileGroupComparator()).collect(Collectors.toList());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public List<HoodieColumnRangeMetadata<Comparable>> getSortedColumnStatsList(
        String partitionPath, List<String> baseFileNameList) {
      LOG.info("All column names for getting column stats: " + allColumnNameList);
      if (enableMetadataTable) {
        List<Pair<String, String>> partitionFileNameList = baseFileNameList.stream()
            .map(filename -> Pair.of(partitionPath, filename)).collect(Collectors.toList());
        return allColumnNameList.stream()
            .flatMap(columnName ->
                tableMetadata.getColumnStats(partitionFileNameList, columnName).values().stream()
                    .map(HoodieTableMetadataUtil::convertColumnStatsRecordToColumnRangeMetadata)
                    .collect(Collectors.toList())
                    .stream())
            .sorted(new HoodieColumnRangeMetadataComparator())
            .collect(Collectors.toList());
      } else {
        return baseFileNameList.stream().flatMap(filename ->
                new ParquetUtils().readRangeFromParquetMetadata(
                    metaClient.getHadoopConf(),
                    new Path(FSUtils.getPartitionPath(metaClient.getBasePath(), partitionPath), filename),
                    allColumnNameList).stream())
            .sorted(new HoodieColumnRangeMetadataComparator())
            .collect(Collectors.toList());
      }
    }

    public List<BloomFilterData> getSortedBloomFilterList(
        String partitionPath, List<String> baseFileNameList) {
      if (enableMetadataTable) {
        List<Pair<String, String>> partitionFileNameList = baseFileNameList.stream()
            .map(filename -> Pair.of(partitionPath, filename)).collect(Collectors.toList());
        return tableMetadata.getBloomFilters(partitionFileNameList).entrySet().stream()
            .map(entry -> BloomFilterData.builder()
                .setPartitionPath(entry.getKey().getKey())
                .setFilename(entry.getKey().getValue())
                .setBloomFilter(ByteBuffer.wrap(entry.getValue().serializeToString().getBytes()))
                .build())
            .sorted()
            .collect(Collectors.toList());
      } else {
        return baseFileNameList.stream()
            .map(filename -> readBloomFilterFromFile(partitionPath, filename))
            .filter(Option::isPresent)
            .map(Option::get)
            .sorted()
            .collect(Collectors.toList());
      }
    }

    private List<String> getAllColumnNames() {
      TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
      try {
        return schemaResolver.getTableAvroSchema().getFields().stream()
            .map(entry -> entry.name()).collect(Collectors.toList());
      } catch (Exception e) {
        throw new HoodieException("Failed to get all column names for " + metaClient.getBasePath());
      }
    }

    private Option<BloomFilterData> readBloomFilterFromFile(String partitionPath, String filename) {
      Path path = new Path(FSUtils.getPartitionPath(metaClient.getBasePath(), partitionPath), filename);
      HoodieFileReader fileReader;
      try {
        fileReader = HoodieFileReaderFactory.getFileReader(metaClient.getHadoopConf(), path);
      } catch (IOException e) {
        Log.error("Failed to get file reader for " + path + " " + e.getMessage());
        return Option.empty();
      }
      final BloomFilter fileBloomFilter = fileReader.readBloomFilter();
      if (fileBloomFilter == null) {
        Log.error("Failed to read bloom filter for " + path);
        return Option.empty();
      }
      return Option.of(BloomFilterData.builder()
          .setPartitionPath(partitionPath)
          .setFilename(filename)
          .setBloomFilter(ByteBuffer.wrap(fileBloomFilter.serializeToString().getBytes()))
          .build());
    }
  }
}
