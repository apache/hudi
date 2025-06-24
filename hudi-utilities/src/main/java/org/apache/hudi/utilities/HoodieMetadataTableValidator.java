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

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.PartitionStatsIndexSupport;
import org.apache.hudi.async.HoodieAsyncService;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.data.HoodieSparkRDDUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.util.BloomFilterData;
import org.apache.hudi.common.util.HoodieDataUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.JavaConverters;

import static org.apache.hudi.common.model.HoodieRecord.FILENAME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.toStringWithThreshold;
import static org.apache.hudi.io.storage.HoodieSparkIOFactory.getHoodieSparkIOFactory;
import static org.apache.hudi.metadata.HoodieTableMetadata.getMetadataTableBasePath;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.META_COL_SET_TO_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getLocationFromRecordIndexInfo;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getLogFileColumnRangeMetadata;

/**
 * TODO: [HUDI-8294]
 * A validator with spark-submit to compare information, such as partitions, file listing, index, etc.,
 * between metadata table and filesystem.
 * <p>
 * There are seven validation tasks, that can be enabled independently through the following CLI options:
 * - `--validate-latest-file-slices`: validate the latest file slices for all partitions.
 * - `--validate-latest-base-files`: validate the latest base files for all partitions.
 * - `--validate-all-file-groups`: validate all file groups, and all file slices within file groups.
 * - `--validate-all-column-stats`: validate column stats for all columns in the schema
 * - `--validate-bloom-filters`: validate bloom filters of base files
 * - `--validate-record-index-count`: validate the number of entries in the record index, which
 * should be equal to the number of record keys in the latest snapshot of the table.
 * - `--validate-record-index-content`: validate the content of the record index so that each
 * record key should have the correct location, and there is no additional or missing entry.
 * <p>
 * If the Hudi table is on the local file system, the base path passed to `--base-path` must have
 * "file:" prefix to avoid validation failure.
 * <p>
 * - Default : This validator will compare the results between metadata table and filesystem only once.
 * <p>
 * Example command:
 * ```
 * spark-submit \
 * --class org.apache.hudi.utilities.HoodieMetadataTableValidator \
 * --master spark://xxxx:7077 \
 * --driver-memory 1g \
 * --executor-memory 1g \
 * $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.13.0-SNAPSHOT.jar \
 * --base-path basePath \
 * --validate-latest-file-slices \
 * --validate-latest-base-files \
 * --validate-all-file-groups
 * ```
 *
 * <p>
 * Also, You can set `--continuous` for long running this validator.
 * And use `--min-validate-interval-seconds` to control the validation frequency, default is 10 minutes.
 * <p>
 * Example command:
 * ```
 * spark-submit \
 * --class org.apache.hudi.utilities.HoodieMetadataTableValidator \
 * --master spark://xxxx:7077 \
 * --driver-memory 1g \
 * --executor-memory 1g \
 * $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.13.0-SNAPSHOT.jar \
 * --base-path basePath \
 * --validate-latest-file-slices \
 * --validate-latest-base-files \
 * --validate-all-file-groups \
 * --continuous \
 * --min-validate-interval-seconds 60
 * ```
 */
public class HoodieMetadataTableValidator implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(HoodieMetadataTableValidator.class);

  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private final Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private final TypedProperties props;

  private Option<HoodieTableMetaClient> metaClientOpt = Option.empty();

  protected transient Option<AsyncMetadataTableValidateService> asyncMetadataTableValidateService;

  private final String taskLabels;

  private final List<Throwable> throwables = new ArrayList<>();

  public HoodieMetadataTableValidator(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.cfg = cfg;

    this.props = cfg.propsFilePath == null
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);

    try {
      this.metaClientOpt = Option.of(HoodieTableMetaClient.builder()
          .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
          .setBasePath(cfg.basePath)
          .setLoadActiveTimelineOnLoad(true)
          .build());
    } catch (TableNotFoundException tbe) {
      // Suppress the TableNotFound exception, table not yet created for a new stream
      LOG.warn("Data table is not found. Skip current validation for: " + cfg.basePath);
    }

    this.asyncMetadataTableValidateService = cfg.continuous ? Option.of(new AsyncMetadataTableValidateService()) : Option.empty();
    this.taskLabels = generateValidationTaskLabels();
  }

  /**
   * Returns list of Throwable which were encountered during validation. This method is useful
   * when ignoreFailed parameter is set to true.
   */
  public List<Throwable> getThrowables() {
    return throwables;
  }

  /**
   * Returns true if there is a validation failure encountered during validation.
   * This method is useful when ignoreFailed parameter is set to true.
   */
  public boolean hasValidationFailure() {
    for (Throwable throwable : throwables) {
      if (throwable instanceof HoodieValidationException) {
        return true;
      }
    }
    return false;
  }

  private String generateValidationTaskLabels() {
    List<String> labelList = new ArrayList<>();
    labelList.add(cfg.basePath);
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
    if (cfg.validatePartitionStats) {
      labelList.add("validate-partition-stats");
    }
    if (cfg.validateBloomFilters) {
      labelList.add("validate-bloom-filters");
    }
    if (cfg.validateRecordIndexCount) {
      labelList.add("validate-record-index-count");
    }
    if (cfg.validateRecordIndexContent) {
      labelList.add("validate-record-index-content");
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

    @Parameter(names = {"--validate-last-n-file-slices"}, description = "Validate just the last N file slices for all file groups. Specify N.", required = false)
    public Integer validateLastNFileSlices = null;

    @Parameter(names = {"--validate-all-column-stats"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean validateAllColumnStats = false;

    @Parameter(names = {"--validate-partition-stats"}, description = "Validate partition stats for all columns in the schema", required = false)
    public boolean validatePartitionStats = false;

    @Parameter(names = {"--validate-bloom-filters"}, description = "Validate bloom filters of base files", required = false)
    public boolean validateBloomFilters = false;

    @Parameter(names = {"--validate-record-index-count"},
        description = "Validate the number of entries in the record index, which should be equal "
            + "to the number of record keys in the latest snapshot of the table",
        required = false)
    public boolean validateRecordIndexCount = true;

    @Parameter(names = {"--validate-record-index-content"},
        description = "Validate the content of the record index so that each record key should "
            + "have the correct location, and there is no additional or missing entry",
        required = false)
    public boolean validateRecordIndexContent = true;

    @Parameter(names = {"--validate-secondary-index"},
        description = "Validate the entries in secondary index match the primary key for the records",
        required = false)
    public boolean validateSecondaryIndex = false;

    @Parameter(names = {"--num-record-index-error-samples"},
        description = "Number of error samples to show for record index validation",
        required = false)
    public int numRecordIndexErrorSamples = 100;

    @Parameter(names = {"--view-storage-type-fs-listing"},
        description = "View storage type to use for File System based listing. "
            + "Supported values are MEMORY (by default) and SPILLABLE_DISK.",
        required = false)
    public String viewStorageTypeForFSListing = FileSystemViewStorageType.MEMORY.name();

    @Parameter(names = {"--view-storage-type-mdt"},
        description = "View storage type to use for metadata table based listing. "
            + "Supported values are MEMORY (by default) and SPILLABLE_DISK.",
        required = false)
    public String viewStorageTypeForMetadata = FileSystemViewStorageType.MEMORY.name();

    @Parameter(names = {"--min-validate-interval-seconds"},
        description = "the min validate interval of each validate when set --continuous, default is 10 minutes.")
    public Integer minValidateIntervalSeconds = 10 * 60;

    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for valuation", required = false)
    public int parallelism = 200;

    @Parameter(names = {"--record-index-parallelism", "-rpl"}, description = "Parallelism for validating record index", required = false)
    public int recordIndexParallelism = 100;

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

    @Parameter(names = {"--log-detail-max-length"}, description = "Maximum length for log details in validation messages. To avoid truncation, input a non-positive value.")
    public int logDetailMaxLength = 100_000;

    @Override
    public String toString() {
      return "MetadataTableValidatorConfig {\n"
          + "   --base-path " + basePath + ", \n"
          + "   --validate-latest-file-slices " + validateLatestFileSlices + ", \n"
          + "   --validate-latest-base-files " + validateLatestBaseFiles + ", \n"
          + "   --validate-all-file-groups " + validateAllFileGroups + ", \n"
          + "   --validate-last-n-file-slices " + validateLastNFileSlices + ", \n"
          + "   --validate-all-column-stats " + validateAllColumnStats + ", \n"
          + "   --validate-partition-stats " + validatePartitionStats + ", \n"
          + "   --validate-bloom-filters " + validateBloomFilters + ", \n"
          + "   --validate-secondary-index " + validateSecondaryIndex + ", \n"
          + "   --validate-record-index-count " + validateRecordIndexCount + ", \n"
          + "   --validate-record-index-content " + validateRecordIndexContent + ", \n"
          + "   --num-record-index-error-samples " + numRecordIndexErrorSamples + ", \n"
          + "   --view-storage-type-fs-listing " + viewStorageTypeForFSListing + ", \n"
          + "   --view-storage-type-mdt " + viewStorageTypeForMetadata + ", \n"
          + "   --continuous " + continuous + ", \n"
          + "   --skip-data-files-for-cleaning " + skipDataFilesForCleaning + ", \n"
          + "   --ignore-failed " + ignoreFailed + ", \n"
          + "   --min-validate-interval-seconds " + minValidateIntervalSeconds + ", \n"
          + "   --parallelism " + parallelism + ", \n"
          + "   --record-index-parallelism " + recordIndexParallelism + ", \n"
          + "   --spark-master " + sparkMaster + ", \n"
          + "   --spark-memory " + sparkMemory + ", \n"
          + "   --assumeDatePartitioning-memory " + assumeDatePartitioning + ", \n"
          + "   --log-detail-max-length " + logDetailMaxLength + ", \n"
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
          && Objects.equals(validatePartitionStats, config.validatePartitionStats)
          && Objects.equals(validateBloomFilters, config.validateBloomFilters)
          && Objects.equals(validateSecondaryIndex, config.validateSecondaryIndex)
          && Objects.equals(validateRecordIndexCount, config.validateRecordIndexCount)
          && Objects.equals(validateRecordIndexContent, config.validateRecordIndexContent)
          && Objects.equals(viewStorageTypeForFSListing, config.viewStorageTypeForFSListing)
          && Objects.equals(viewStorageTypeForMetadata, config.viewStorageTypeForMetadata)
          && Objects.equals(numRecordIndexErrorSamples, config.numRecordIndexErrorSamples)
          && Objects.equals(minValidateIntervalSeconds, config.minValidateIntervalSeconds)
          && Objects.equals(parallelism, config.parallelism)
          && Objects.equals(recordIndexParallelism, config.recordIndexParallelism)
          && Objects.equals(ignoreFailed, config.ignoreFailed)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(assumeDatePartitioning, config.assumeDatePartitioning)
          && Objects.equals(logDetailMaxLength, config.logDetailMaxLength)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, continuous, skipDataFilesForCleaning, validateLatestFileSlices,
          validateLatestBaseFiles, validateAllFileGroups, validateAllColumnStats, validatePartitionStats, validateBloomFilters,
          validateSecondaryIndex, validateRecordIndexCount, validateRecordIndexContent, numRecordIndexErrorSamples,
          viewStorageTypeForFSListing, viewStorageTypeForMetadata,
          minValidateIntervalSeconds, parallelism, recordIndexParallelism, ignoreFailed,
          sparkMaster, sparkMemory, assumeDatePartitioning, logDetailMaxLength, propsFilePath, configs, help);
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

    try {
      HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, cfg);
      validator.run();
    } catch (Throwable throwable) {
      LOG.error("Fail to do hoodie metadata table validation for {}", cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public boolean run() {
    if (!metaClientOpt.isPresent()) {
      LOG.warn("Data table is not available to read for now, skip current validation for: " + cfg.basePath);
      return true;
    }
    boolean result = false;
    try {
      LOG.info(cfg.toString());
      if (cfg.continuous) {
        LOG.info(" ****** do hoodie metadata table validation in CONTINUOUS mode - {} ******", taskLabels);
        doHoodieMetadataTableValidationContinuous();
      } else {
        LOG.info(" ****** do hoodie metadata table validation once - {} ******", taskLabels);
        result = doHoodieMetadataTableValidationOnce();
      }
      return result;
    } catch (HoodieValidationException ve) {
      if (!cfg.ignoreFailed) {
        throw ve;
      }
    } catch (Exception e) {
      throw new HoodieException("Unable to do hoodie metadata table validation in " + cfg.basePath, e);
    } finally {
      if (asyncMetadataTableValidateService.isPresent()) {
        asyncMetadataTableValidateService.get().shutdown(true);
      }
    }
    return result;
  }

  private boolean doHoodieMetadataTableValidationOnce() {
    try {
      return doMetadataTableValidation();
    } catch (Throwable e) {
      LOG.error("Metadata table validation failed to HoodieValidationException {}", taskLabels, e);
      if (!cfg.ignoreFailed) {
        throw e;
      }
      throwables.add(e);
      return false;
    }
  }

  private void doHoodieMetadataTableValidationContinuous() {
    asyncMetadataTableValidateService.ifPresent(service -> {
      service.start(null);
      try {
        service.waitForShutdown();
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    });
  }

  public boolean doMetadataTableValidation() {
    boolean finalResult = true;
    if (!metaClientOpt.isPresent()) {
      LOG.warn("Data table is not available to read for now, skip current validation.");
      return true;
    }
    HoodieTableMetaClient metaClient = this.metaClientOpt.get();
    metaClient.reloadActiveTimeline();
    StoragePath basePath = metaClient.getBasePath();
    Set<String> baseFilesForCleaning = Collections.emptySet();

    // check metadata table is available to read.
    if (!checkMetadataTableIsAvailable()) {
      return true;
    }

    if (cfg.skipDataFilesForCleaning) {
      HoodieTimeline inflightCleaningTimeline = metaClient.getActiveTimeline().getCleanerTimeline().filterInflights();

      baseFilesForCleaning = inflightCleaningTimeline.getInstantsAsStream().flatMap(instant -> {
        try {
          // convert inflight instant to requested and get clean plan
          instant = metaClient.createNewInstant(HoodieInstant.State.REQUESTED, instant.getAction(),
              instant.requestedTime());
          HoodieCleanerPlan cleanerPlan = CleanerUtils.getCleanerPlan(metaClient, instant);

          return cleanerPlan.getFilePathsToBeDeletedPerPartition().values().stream().flatMap(cleanerFileInfoList ->
            cleanerFileInfoList.stream().map(fileInfo -> new Path(fileInfo.getFilePath()).getName())
          );

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
    // compare partitions

    List<String> allPartitions = validatePartitions(engineContext, basePath, metaClient);
    if (allPartitions.isEmpty()) {
      LOG.warn("The result of getting all partitions is null or empty, skip current validation. {}", taskLabels);
      return true;
    }

    try (HoodieMetadataValidationContext metadataTableBasedContext =
             new HoodieMetadataValidationContext(engineContext, props, metaClient, true, cfg.viewStorageTypeForMetadata);
         HoodieMetadataValidationContext fsBasedContext =
             new HoodieMetadataValidationContext(engineContext, props, metaClient, false, cfg.viewStorageTypeForFSListing)) {
      Set<String> finalBaseFilesForCleaning = baseFilesForCleaning;
      List<Pair<Boolean, HoodieValidationException>> result = new ArrayList<>(
          engineContext.parallelize(allPartitions, allPartitions.size()).map(partitionPath -> {
            try {
              validateFilesInPartition(metadataTableBasedContext, fsBasedContext, partitionPath, finalBaseFilesForCleaning);
              LOG.info("Metadata table validation succeeded for partition {} (partition {})", partitionPath, taskLabels);
              return Pair.<Boolean, HoodieValidationException>of(true, null);
            } catch (HoodieValidationException e) {
              LOG.error("Metadata table validation failed for partition {} due to HoodieValidationException (partition {})",
                  partitionPath, taskLabels, e);
              if (!cfg.ignoreFailed) {
                throw e;
              }
              return Pair.of(false, new HoodieValidationException("Validation failed for partition: " + partitionPath, e));
            }
          }).collectAsList());

      try {
        validateRecordIndex(engineContext, metaClient);
        result.add(Pair.of(true, null));
      } catch (HoodieValidationException e) {
        handleValidationException(e, result, "Metadata table validation failed due to HoodieValidationException in record index validation");
      }

      try {
        if (cfg.validateSecondaryIndex) {
          validateSecondaryIndex(engineContext, metadataTableBasedContext, metaClient);
          result.add(Pair.of(true, null));
        }
      } catch (HoodieValidationException e) {
        handleValidationException(e, result, "Metadata table validation failed due to HoodieValidationException in secondary index validation");
      }

      try {
        if (cfg.validatePartitionStats) {
          validatePartitionStats(metadataTableBasedContext, finalBaseFilesForCleaning, allPartitions);
          result.add(Pair.of(true, null));
        }
      } catch (HoodieValidationException e) {
        handleValidationException(e, result, "Metadata table validation failed due to HoodieValidationException in partition stats validation");
      }

      for (Pair<Boolean, HoodieValidationException> res : result) {
        finalResult &= res.getKey();
        if (res.getKey().equals(false)) {
          LOG.error("Metadata Validation failed for table: {}", cfg.basePath, res.getValue());
          if (res.getRight() != null) {
            throwables.add(res.getRight());
          }
        }
      }

      if (finalResult) {
        LOG.info("Metadata table validation succeeded ({}).", taskLabels);
        return true;
      } else {
        LOG.warn("Metadata table validation failed ({}).", taskLabels);
        return false;
      }
    } catch (HoodieValidationException validationException) {
      throw validationException;
    } catch (SparkException sparkException) {
      if (sparkException.getCause() instanceof HoodieValidationException) {
        throw (HoodieValidationException) sparkException.getCause();
      } else {
        throw new HoodieValidationException("Unexpected spark failure", sparkException);
      }
    } catch (Exception e) {
      LOG.warn("Error closing HoodieMetadataValidationContext, "
          + "ignoring the error as the validation is successful.", e);
      return true;
    }
  }

  private void handleValidationException(HoodieValidationException e, List<Pair<Boolean, HoodieValidationException>> result, String errorMsg) {
    LOG.error("{} for table: {} ", errorMsg, cfg.basePath, e);
    if (!cfg.ignoreFailed) {
      throw e;
    }
    result.add(Pair.of(false, e));
  }

  /**
   * Check metadata is initialized and available to ready.
   * If not we will log.warn and skip current validation.
   */
  private boolean checkMetadataTableIsAvailable() {
    try {
      HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder()
          .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
          .setBasePath(new Path(cfg.basePath, HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH).toString())
          .setLoadActiveTimelineOnLoad(true)
          .build();
      int finishedInstants = mdtMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants();
      if (finishedInstants == 0) {
        if (metaClientOpt.get().getCommitsTimeline().filterCompletedInstants().countInstants() == 0) {
          LOG.info("There is no completed commit in both metadata table and corresponding data table: {}", taskLabels);
          return false;
        } else {
          throw new HoodieValidationException("There is no completed instant for metadata table: " + cfg.basePath);
        }
      }
      return true;
    } catch (TableNotFoundException tbe) {
      // Suppress the TableNotFound exception if Metadata table is not available to read for now
      LOG.warn("Metadata table is not found for table: {}. Skip current validation.", cfg.basePath);
      return false;
    } catch (Exception ex) {
      LOG.warn("Metadata table is not available to read for now for table: {}, ", cfg.basePath, ex);
      return false;
    }
  }

  /**
   * Compare the listing partitions result between metadata table and fileSystem.
   */
  @VisibleForTesting
  List<String> validatePartitions(HoodieSparkEngineContext engineContext, StoragePath basePath, HoodieTableMetaClient metaClient) {
    // compare partitions
    HoodieTimeline completedTimeline = metaClient.getCommitsTimeline().filterCompletedInstants();
    List<String> allPartitionPathsFromFS = getPartitionsFromFileSystem(engineContext, metaClient, completedTimeline);

    List<String> allPartitionPathsMeta = getPartitionsFromMDT(engineContext, metaClient);

    Collections.sort(allPartitionPathsFromFS);
    Collections.sort(allPartitionPathsMeta);

    if (allPartitionPathsFromFS.size() != allPartitionPathsMeta.size()
        || !allPartitionPathsFromFS.equals(allPartitionPathsMeta)) {
      List<String> additionalFromFS = new ArrayList<>(allPartitionPathsFromFS);
      additionalFromFS.removeAll(allPartitionPathsMeta);
      List<String> additionalFromMDT = new ArrayList<>(allPartitionPathsMeta);
      additionalFromMDT.removeAll(allPartitionPathsFromFS);
      AtomicBoolean misMatch = new AtomicBoolean(true);
      List<String> actualAdditionalPartitionsInMDT = new ArrayList<>(additionalFromMDT);
      if (additionalFromFS.isEmpty() && !additionalFromMDT.isEmpty()) {
        // there is a chance that when we polled MDT there could have been a new completed commit which was not complete when we polled FS based
        // listing. let's rule that out.
        additionalFromMDT.forEach(partitionFromMDT -> {
          try {
            if (metaClient.getStorage().exists(new StoragePath(basePath + "/" + partitionFromMDT))) {
              Option<String> partitionCreationTimeOpt = getPartitionCreationInstant(metaClient.getStorage(), basePath, partitionFromMDT);
              // if creation time is greater than last completed instant in active timeline, we can ignore the additional partition from MDT.
              if (partitionCreationTimeOpt.isPresent() && !completedTimeline.containsInstant(partitionCreationTimeOpt.get())) {
                Option<HoodieInstant> lastInstant = completedTimeline.lastInstant();
                if (lastInstant.isPresent()
                    && InstantComparison.compareTimestamps(partitionCreationTimeOpt.get(), GREATER_THAN, lastInstant.get().requestedTime())) {
                  LOG.warn("Ignoring additional partition {}, as it was deduced to be part of a "
                      + "latest completed commit which was inflight when FS based listing was polled.", partitionFromMDT);
                  actualAdditionalPartitionsInMDT.remove(partitionFromMDT);
                }
              }
            }
          } catch (IOException e) {
            throw new HoodieValidationException("IOException thrown while trying to validate partition match b/w FS based listing and MDT based listing", e);
          }
        });
        // if there is no additional partitions from FS listing and only additional partitions from MDT based listing is due to a new commit, we are good
        if (actualAdditionalPartitionsInMDT.isEmpty()) {
          misMatch.set(false);
        }
      }
      if (!additionalFromFS.isEmpty()) {
        // check for all additional partitions from FS is they are empty ones.
        List<String> emptyPartitions = additionalFromFS.stream().filter(partition -> {
          try {
            Path partitionPath = new Path(basePath, partition);
            return Arrays.stream(metaClient.getFs().listStatus(partitionPath))
                .noneMatch(fileStatus -> fileStatus.isFile() && !fileStatus.getPath().getName().startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX));
          } catch (IOException ex) {
            throw new HoodieIOException("Error listing partition " + partition, ex);
          }
        }).collect(Collectors.toList());
        additionalFromFS.removeAll(emptyPartitions);
        if (additionalFromFS.isEmpty()) {
          LOG.warn("All out of sync partitions turned out to be empty {}", emptyPartitions);
          misMatch.set(false);
        } else {
          misMatch.set(true);
        }
      }
      if (misMatch.get()) {
        String message = "Compare Partitions Failed! " + " Additional "
            + additionalFromFS.size() + " partitions from FS, but missing from MDT : \""
            + toStringWithThreshold(additionalFromFS, cfg.logDetailMaxLength)
            + "\" and additional " + actualAdditionalPartitionsInMDT.size()
            + " partitions from MDT, but missing from FS listing : \""
            + toStringWithThreshold(actualAdditionalPartitionsInMDT, cfg.logDetailMaxLength)
            + "\".\n All " + allPartitionPathsFromFS.size() + " partitions from FS listing "
            + toStringWithThreshold(allPartitionPathsFromFS, cfg.logDetailMaxLength);
        LOG.error(message);
        throw new HoodieValidationException(message);
      }
    }
    return allPartitionPathsMeta;
  }

  @VisibleForTesting
  Option<String> getPartitionCreationInstant(HoodieStorage storage, StoragePath basePath, String partition) {
    HoodiePartitionMetadata hoodiePartitionMetadata =
        new HoodiePartitionMetadata(storage, FSUtils.constructAbsolutePath(basePath, partition));
    return hoodiePartitionMetadata.readPartitionCreatedCommitTime();
  }

  @VisibleForTesting
  List<String> getPartitionsFromMDT(HoodieEngineContext engineContext, HoodieTableMetaClient metaClient) {
    return FSUtils.getAllPartitionPaths(engineContext, metaClient, true);
  }

  @VisibleForTesting
  List<String> getPartitionsFromFileSystem(HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, HoodieTimeline completedTimeline) {
    List<String> allPartitionPathsFromFS = FSUtils.getAllPartitionPaths(engineContext, metaClient, false);

    // ignore partitions created by uncommitted ingestion.
    return allPartitionPathsFromFS.stream().parallel().filter(part -> {
      HoodiePartitionMetadata hoodiePartitionMetadata =
          new HoodiePartitionMetadata(metaClient.getStorage(), FSUtils.constructAbsolutePath(metaClient.getBasePath().toString(), part));
      Option<String> instantOption = hoodiePartitionMetadata.readPartitionCreatedCommitTime();
      if (instantOption.isPresent()) {
        String instantTime = instantOption.get();
        // There are two cases where the created commit time is written to the partition metadata:
        // (1) Commit C1 creates the partition and C1 succeeds, the partition metadata has C1 as
        // the created commit time.
        // (2) Commit C1 creates the partition, the partition metadata is written, and C1 fails
        // during writing data files.  Next time, C2 adds new data to the same partition after C1
        // is rolled back. In this case, the partition metadata still has C1 as the created commit
        // time, since Hudi does not rewrite the partition metadata in C2.
        if (!completedTimeline.containsOrBeforeTimelineStarts(instantTime)) {
          Option<HoodieInstant> lastInstant = completedTimeline.lastInstant();
          return lastInstant.isPresent()
              && compareTimestamps(
              instantTime, LESSER_THAN_OR_EQUALS, lastInstant.get().requestedTime());
        }
        return true;
      } else {
        return false;
      }
    }).collect(Collectors.toList());
  }

  /**
   * Compare the file listing and index data between metadata table and fileSystem.
   * For now, validate five kinds of apis:
   * 1. HoodieTableFileSystemView::getLatestFileSlices
   * 2. HoodieTableFileSystemView::getLatestBaseFiles
   * 3. HoodieTableFileSystemView::getAllFileGroups and HoodieTableFileSystemView::getAllFileSlices
   * 4. HoodieTableFileSystemView::getColumnStats
   * 5. HoodieTableFileSystemView::getBloomFilters
   *
   * @param metadataTableBasedContext Validation context containing information based on metadata table
   * @param fsBasedContext            Validation context containing information based on the file system
   * @param partitionPath             Partition path String
   * @param baseDataFilesForCleaning  Base files for un-complete cleaner action
   */
  private void validateFilesInPartition(
      HoodieMetadataValidationContext metadataTableBasedContext,
      HoodieMetadataValidationContext fsBasedContext, String partitionPath,
      Set<String> baseDataFilesForCleaning) throws Exception {
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
    if (cfg.validateLastNFileSlices != null) {
      int lastNFileSlices = cfg.validateLastNFileSlices;
      int allFileSlicesFromFSCount = allFileSlicesFromFS.size();
      if (allFileSlicesFromFSCount > lastNFileSlices) {
        allFileSlicesFromFS = allFileSlicesFromFS.subList(allFileSlicesFromFSCount - lastNFileSlices, allFileSlicesFromFSCount);
      }
      int allFileSlicesFromMetaCount = allFileSlicesFromMeta.size();
      if (allFileSlicesFromMetaCount > lastNFileSlices) {
        allFileSlicesFromMeta = allFileSlicesFromMeta.subList(allFileSlicesFromMetaCount - lastNFileSlices, allFileSlicesFromMetaCount);
      }
    }

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

    validateFileSlices(
        latestFileSlicesFromMetadataTable, latestFileSlicesFromFS, partitionPath,
        fsBasedContext.getMetaClient(), "latest file slices");
  }

  private static List<FileSlice> filterFileSliceBasedOnInflightCleaning(List<FileSlice> sortedLatestFileSliceList, Set<String> baseDataFilesForCleaning) {
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
  private void validateAllColumnStats(HoodieMetadataValidationContext metadataTableBasedContext, HoodieMetadataValidationContext fsBasedContext,
                                      String partitionPath, Set<String> baseDataFilesForCleaning) throws Exception {

    List<String> latestFileNames = getLatestFileNames(fsBasedContext, partitionPath, baseDataFilesForCleaning);
    List<HoodieColumnRangeMetadata<Comparable>> metadataBasedColStats = metadataTableBasedContext.getSortedColumnStatsList(partitionPath, latestFileNames, metadataTableBasedContext.getSchema());
    List<HoodieColumnRangeMetadata<Comparable>> fsBasedColStats = fsBasedContext.getSortedColumnStatsList(partitionPath, latestFileNames, fsBasedContext.getSchema());

    validate(metadataBasedColStats, fsBasedColStats, partitionPath, "column stats");
  }

  private void validatePartitionStats(HoodieMetadataValidationContext metadataTableBasedContext, Set<String> baseDataFilesForCleaning, List<String> allPartitions) throws Exception {

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieData<HoodieMetadataColumnStats> partitionStatsUsingColStats = getPartitionStatsUsingColStats(metadataTableBasedContext,
        baseDataFilesForCleaning, allPartitions, engineContext);

    PartitionStatsIndexSupport partitionStatsIndexSupport = new PartitionStatsIndexSupport(engineContext.getSqlContext().sparkSession(),
        AvroConversionUtils.convertAvroSchemaToStructType(metadataTableBasedContext.getSchema()), metadataTableBasedContext.getMetadataConfig(),
        metaClientOpt.get(), false);
    HoodieData<HoodieMetadataColumnStats> partitionStats =
        partitionStatsIndexSupport.loadColumnStatsIndexRecords(JavaConverters.asScalaBufferConverter(
            metadataTableBasedContext.allColumnNameList).asScala().toSeq(), scala.Option.empty(), false)
            // set isTightBound to false since partition stats generated using column stats does not contain the field
            .map(colStat -> HoodieMetadataColumnStats.newBuilder(colStat).setIsTightBound(false).build());
    JavaRDD<HoodieMetadataColumnStats> diffRDD = HoodieJavaRDD.getJavaRDD(partitionStats).subtract(HoodieJavaRDD.getJavaRDD(partitionStatsUsingColStats));
    if (!diffRDD.isEmpty()) {
      List<HoodieMetadataColumnStats> diff = diffRDD.collect();
      Set<String> partitionPaths = diff.stream().map(HoodieMetadataColumnStats::getFileName).collect(Collectors.toSet());
      StringBuilder statDiffMsg = new StringBuilder();
      for (String partitionPath : partitionPaths) {
        List<HoodieMetadataColumnStats> diffPartitionStatsColStats = partitionStatsUsingColStats.filter(stat -> stat.getFileName().equals(partitionPath)).collectAsList();
        List<HoodieMetadataColumnStats> diffPartitionStats = partitionStats.filter(stat -> stat.getFileName().equals(partitionPath)).collectAsList();
        statDiffMsg.append(String.format("Partition stats from MDT: %s from colstats: %s", Arrays.toString(diffPartitionStats.toArray()), Arrays.toString(diffPartitionStatsColStats.toArray())));
      }
      throw new HoodieValidationException(String.format("Partition stats validation failed diff: %s", statDiffMsg));
    }
  }

  private HoodieData<HoodieMetadataColumnStats> getPartitionStatsUsingColStats(HoodieMetadataValidationContext metadataTableBasedContext, Set<String> baseDataFilesForCleaning,
                                                                               List<String> allPartitions, HoodieSparkEngineContext engineContext) {
    return engineContext.parallelize(allPartitions).flatMap(partitionPath -> {
      List<FileSlice> latestFileSlicesFromMetadataTable = filterFileSliceBasedOnInflightCleaning(metadataTableBasedContext.getSortedLatestFileSliceList(partitionPath),
          baseDataFilesForCleaning);
      List<String> latestFileNames = new ArrayList<>();
      latestFileSlicesFromMetadataTable.stream().filter(fs -> fs.getBaseFile().isPresent()).forEach(fs -> getLatestFiles(fs, latestFileNames));
      List<HoodieColumnRangeMetadata<Comparable>> colStats = metadataTableBasedContext.getSortedColumnStatsList(partitionPath, latestFileNames, metadataTableBasedContext.getSchema());

      TreeSet<HoodieColumnRangeMetadata<Comparable>> aggregatedColumnStats = aggregateColumnStats(partitionPath, colStats);
      // TODO: fix `isTightBound` flag when stats based on log files are available
      List<HoodieRecord> partitionStatRecords = HoodieMetadataPayload.createPartitionStatsRecords(partitionPath, new ArrayList<>(aggregatedColumnStats), false, false, Option.empty())
          .collect(Collectors.toList());
      return partitionStatRecords.stream()
          .map(record -> {
            try {
              return ((HoodieMetadataPayload) record.getData()).getInsertValue(null, null)
                  .map(metadataRecord -> ((HoodieMetadataRecord) metadataRecord).getColumnStatsMetadata());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          })
          .filter(Option::isPresent)
          .map(Option::get)
          .collect(Collectors.toList())
          .iterator();
    });
  }

  private static void getLatestFiles(FileSlice fs, List<String> latestFileNames) {
    latestFileNames.add(fs.getBaseFile().get().getFileName());
    latestFileNames.addAll(fs.getLogFiles().map(HoodieLogFile::getFileName).collect(Collectors.toList()));
  }

  /**
   * Generates aggregated column stats which also signify as partition stat for the particular partition
   * path.
   *
   * @param partitionPath Provided partition path
   * @param colStats Column stat records for the partition
   */
  static TreeSet<HoodieColumnRangeMetadata<Comparable>> aggregateColumnStats(String partitionPath, List<HoodieColumnRangeMetadata<Comparable>> colStats) {
    TreeSet<HoodieColumnRangeMetadata<Comparable>> aggregatedColumnStats = new TreeSet<>(Comparator.comparing(HoodieColumnRangeMetadata::getColumnName));
    for (HoodieColumnRangeMetadata<Comparable> colStat : colStats) {
      HoodieColumnRangeMetadata<Comparable> partitionStat = HoodieColumnRangeMetadata.create(partitionPath, colStat.getColumnName(),
          colStat.getMinValue(), colStat.getMaxValue(), colStat.getNullCount(), colStat.getValueCount(), colStat.getTotalSize(), colStat.getTotalUncompressedSize());
      HoodieColumnRangeMetadata<Comparable> storedPartitionStat = aggregatedColumnStats.floor(partitionStat);
      if (storedPartitionStat == null || !storedPartitionStat.getColumnName().equals(partitionStat.getColumnName())) {
        aggregatedColumnStats.add(partitionStat);
        continue;
      }
      aggregatedColumnStats.remove(partitionStat);
      aggregatedColumnStats.add(HoodieColumnRangeMetadata.merge(storedPartitionStat, partitionStat));
    }
    return aggregatedColumnStats;
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

  @VisibleForTesting
  void validateRecordIndex(HoodieSparkEngineContext sparkEngineContext,
                                   HoodieTableMetaClient metaClient) {
    if (!metaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)) {
      return;
    }
    if (cfg.validateRecordIndexContent) {
      validateRecordIndexContent(sparkEngineContext, metaClient);
    } else if (cfg.validateRecordIndexCount) {
      validateRecordIndexCount(sparkEngineContext, metaClient);
    }
  }

  private void validateSecondaryIndex(HoodieSparkEngineContext engineContext, HoodieMetadataValidationContext metadataContext,
                                      HoodieTableMetaClient metaClient) {
    Collection<HoodieIndexDefinition> indexDefinitions = metaClient.getIndexMetadata().get().getIndexDefinitions().values();
    for (HoodieIndexDefinition indexDefinition : indexDefinitions) {
      if (indexDefinition.getIndexType().equals(PARTITION_NAME_SECONDARY_INDEX)) {
        validateSecondaryIndex(engineContext, metadataContext, metaClient, indexDefinition);
      }
    }
  }

  void validateSecondaryIndex(HoodieSparkEngineContext engineContext, HoodieMetadataValidationContext metadataContext,
                                      HoodieTableMetaClient metaClient, HoodieIndexDefinition indexDefinition) {
    String basePath = metaClient.getBasePath().toString();
    String latestCompletedCommit = metaClient.getActiveTimeline().getCommitsAndCompactionTimeline()
        .filterCompletedInstants().lastInstant().get().requestedTime();

    JavaRDD<String> secondaryKeys = readSecondaryKeys(engineContext, indexDefinition.getSourceFields(), basePath, latestCompletedCommit);
    secondaryKeys.persist(StorageLevel.MEMORY_AND_DISK());
    long numSecondaryKeys = secondaryKeys.count();
    int numPartitions = (int) Math.max(1, numSecondaryKeys / 100);
    secondaryKeys = secondaryKeys.sortBy(x -> x, true, numPartitions);
    try {
      for (int i = 0; i < numPartitions; i++) {
        List<String> secKeys = secondaryKeys.collectPartitions(new int[] {i})[0];
        HoodiePairData<String, String> secondaryIndexData = ((HoodieBackedTableMetadata) metadataContext.tableMetadata).readSecondaryIndexDataTableRecordKeysWithKeys(
            HoodieListData.lazy(secKeys), indexDefinition.getIndexName());
        try {
          Map<String, Set<String>> mdtSecondaryKeyToRecordKeys = HoodieDataUtils.collectPairDataAsMap(secondaryIndexData);
          Map<String, Set<String>> fsSecondaryKeyToRecordKeys = getFSSecondaryKeyToRecordKeys(engineContext, basePath, latestCompletedCommit, indexDefinition.getSourceFields().get(0), secKeys);
          if (!fsSecondaryKeyToRecordKeys.equals(mdtSecondaryKeyToRecordKeys)) {
            throw new HoodieValidationException(String.format("Secondary Index does not match : \nMDT secondary index: %s \nFS secondary index: %s",
                StringUtils.join(mdtSecondaryKeyToRecordKeys), StringUtils.join(fsSecondaryKeyToRecordKeys)));
          }
        } finally {
          // Clean up the RDD to avoid memory leaks
          secondaryIndexData.unpersistWithDependencies();
        }
      }
    } finally {
      HoodieSparkRDDUtils.unpersistRDDWithDependencies(secondaryKeys.rdd());
    }
  }

  /**
   * Queries data in the table and generates a mapping from secondary key to list of record keys with value
   * as secondary key. Here secondary key is the value of secondary key column or the secondaryField. Also the
   * function returns the secondary key mapping only for the input secondary keys.
   *
   * @param sparkEngineContext    Spark Engine context
   * @param basePath              Table base path
   * @param latestCompletedCommit Latest completed commit in the table
   * @param secondaryField        The secondary key column used to determine the secondary keys
   * @param secKeys               Input secondary keys which will be filtered
   * @return Mapping of secondary keys to list of record keys with value as secondary key
   */
  Map<String, Set<String>> getFSSecondaryKeyToRecordKeys(HoodieSparkEngineContext sparkEngineContext, String basePath, String latestCompletedCommit,
                                                         String secondaryField, List<String> secKeys) {
    List<Tuple2<String, String>> recordAndSecondaryKeys = sparkEngineContext.getSqlContext().read().format("hudi")
        .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT().key(), latestCompletedCommit)
        .load(basePath)
        .filter(functions.col(secondaryField).isin(secKeys.toArray()))
        .select(RECORD_KEY_METADATA_FIELD, secondaryField)
        .javaRDD()
        .map(row -> new Tuple2<>(row.getAs(RECORD_KEY_METADATA_FIELD).toString(), row.getAs(secondaryField).toString()))
        .collect();
    Map<String, Set<String>> secondaryKeyToRecordKeys = new HashMap<>();
    for (Tuple2<String, String> recordAndSecondaryKey : recordAndSecondaryKeys) {
      secondaryKeyToRecordKeys.compute(recordAndSecondaryKey._2, (k, v) -> {
        Set<String> recKeys = v != null ? v : new HashSet<>();
        recKeys.add(recordAndSecondaryKey._1);
        return recKeys;
      });
    }
    return secondaryKeyToRecordKeys;
  }

  private JavaRDD<String> readSecondaryKeys(HoodieSparkEngineContext engineContext, List<String> sourceFields, String basePath, String latestCompletedCommit) {
    return engineContext.getSqlContext().read().format("hudi")
        .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT().key(), latestCompletedCommit)
        .load(basePath)
        .select(sourceFields.get(0))
        .toJavaRDD()
        .map(row -> row.getAs(sourceFields.get(0)).toString());
  }

  private void validateRecordIndexCount(HoodieSparkEngineContext sparkEngineContext,
                                        HoodieTableMetaClient metaClient) {
    String basePath = metaClient.getBasePath().toString();
    String latestCompletedCommit = metaClient.getActiveTimeline().getCommitsAndCompactionTimeline()
        .filterCompletedInstants().lastInstant().get().requestedTime();
    long countKeyFromTable = sparkEngineContext.getSqlContext().read().format("hudi")
        .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT().key(),latestCompletedCommit)
        .load(basePath)
        .select(RECORD_KEY_METADATA_FIELD)
        .count();
    long countKeyFromRecordIndex = sparkEngineContext.getSqlContext().read().format("hudi")
        .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT().key(),latestCompletedCommit)
        .load(getMetadataTableBasePath(basePath))
        .select("key")
        .filter("type = 5")
        .count();

    if (countKeyFromTable != countKeyFromRecordIndex) {
      String message = String.format("Validation of record index count failed: %s entries from record index metadata, %s keys from the data table: %s",
          countKeyFromRecordIndex, countKeyFromTable, cfg.basePath);
      LOG.error(message);
      throw new HoodieValidationException(message);
    } else {
      LOG.info("Validation of record index count succeeded: {} entries. Table: {}", countKeyFromRecordIndex, cfg.basePath);
    }
  }

  private void validateRecordIndexContent(HoodieSparkEngineContext sparkEngineContext,
                                          HoodieTableMetaClient metaClient) {
    String basePath = metaClient.getBasePath().toString();
    String latestCompletedCommit = metaClient.getActiveTimeline().getCommitsAndCompactionTimeline()
        .filterCompletedInstants().lastInstant().get().requestedTime();
    JavaPairRDD<String, Pair<String, String>> keyToLocationOnFsRdd =
        getRecordLocationsFromFSBasedListing(sparkEngineContext, basePath, latestCompletedCommit);

    JavaPairRDD<String, Pair<String, String>> keyToLocationFromRecordIndexRdd =
        getRecordLocationsFromRLI(sparkEngineContext, basePath, latestCompletedCommit);

    int numErrorSamples = cfg.numRecordIndexErrorSamples;
    Pair<Long, List<String>> result = keyToLocationOnFsRdd.fullOuterJoin(keyToLocationFromRecordIndexRdd, cfg.recordIndexParallelism)
        .map(e -> {
          String recordKey = e._1;
          Optional<Pair<String, String>> locationOnFs = e._2._1;
          Optional<Pair<String, String>> locationFromRecordIndex = e._2._2;
          List<String> errorSampleList = new ArrayList<>();
          if (locationOnFs.isPresent() && locationFromRecordIndex.isPresent()) {
            if (locationOnFs.get().getLeft().equals(locationFromRecordIndex.get().getLeft())
                && locationOnFs.get().getRight().equals(locationFromRecordIndex.get().getRight())) {
              return Pair.of(0L, errorSampleList);
            }
            errorSampleList.add(constructLocationInfoString(recordKey, locationOnFs, locationFromRecordIndex));
            return Pair.of(1L, errorSampleList);
          }
          if (!locationOnFs.isPresent() && !locationFromRecordIndex.isPresent()) {
            return Pair.of(0L, errorSampleList);
          }
          errorSampleList.add(constructLocationInfoString(recordKey, locationOnFs, locationFromRecordIndex));
          return Pair.of(1L, errorSampleList);
        })
        .reduce((pair1, pair2) -> {
          long errorCount = pair1.getLeft() + pair2.getLeft();
          List<String> list1 = pair1.getRight();
          List<String> list2 = pair2.getRight();
          if (!list1.isEmpty() && !list2.isEmpty()) {
            if (list1.size() >= numErrorSamples) {
              return Pair.of(errorCount, list1);
            }
            if (list2.size() >= numErrorSamples) {
              return Pair.of(errorCount, list2);
            }

            List<String> resultList = new ArrayList<>();
            if (list1.size() > list2.size()) {
              resultList.addAll(list1);
              for (String item : list2) {
                resultList.add(item);
                if (resultList.size() >= numErrorSamples) {
                  break;
                }
              }
            } else {
              resultList.addAll(list2);
              for (String item : list1) {
                resultList.add(item);
                if (resultList.size() >= numErrorSamples) {
                  break;
                }
              }
            }
            return Pair.of(errorCount, resultList);
          } else if (!list1.isEmpty()) {
            return Pair.of(errorCount, list1);
          } else {
            return Pair.of(errorCount, list2);
          }
        });
    HoodieSparkRDDUtils.unpersistRDDWithDependencies(keyToLocationFromRecordIndexRdd.rdd());
    long countKey = keyToLocationOnFsRdd.count();
    HoodieSparkRDDUtils.unpersistRDDWithDependencies(keyToLocationOnFsRdd.rdd());

    long diffCount = result.getLeft();
    if (diffCount > 0) {
      String message = String.format("Validation of record index content failed: "
              + "%s keys (total %s) from the data table have wrong location in record index "
              + "metadata. Table: %s   Sample mismatches: %s",
          diffCount, countKey, cfg.basePath, String.join(";", result.getRight()));
      LOG.error(message);
      throw new HoodieValidationException(message);
    } else {
      LOG.info("Validation of record index content succeeded: {} entries. Table: {}", countKey, cfg.basePath);
    }
  }

  @VisibleForTesting
  JavaPairRDD<String, Pair<String, String>> getRecordLocationsFromFSBasedListing(HoodieSparkEngineContext sparkEngineContext,
                                                                                                      String basePath,
                                                                                                      String latestCompletedCommit) {
    return sparkEngineContext.getSqlContext().read().format("hudi")
        .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT().key(), latestCompletedCommit)
        .load(basePath)
        .select(RECORD_KEY_METADATA_FIELD, PARTITION_PATH_METADATA_FIELD, FILENAME_METADATA_FIELD)
        .toJavaRDD()
        .mapToPair(row -> new Tuple2<>(row.getString(row.fieldIndex(RECORD_KEY_METADATA_FIELD)),
            Pair.of(row.getString(row.fieldIndex(PARTITION_PATH_METADATA_FIELD)),
                FSUtils.getFileId(row.getString(row.fieldIndex(FILENAME_METADATA_FIELD))))))
        .cache();
  }

  @VisibleForTesting
  JavaPairRDD<String, Pair<String, String>> getRecordLocationsFromRLI(HoodieSparkEngineContext sparkEngineContext,
                                                                      String basePath,
                                                                      String latestCompletedCommit) {
    return sparkEngineContext.getSqlContext().read().format("hudi")
        .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT().key(), latestCompletedCommit)
        .load(getMetadataTableBasePath(basePath))
        .filter("type = 5")
        .select(functions.col("key"),
            functions.col("recordIndexMetadata.partitionName").as("partitionName"),
            functions.col("recordIndexMetadata.fileIdHighBits").as("fileIdHighBits"),
            functions.col("recordIndexMetadata.fileIdLowBits").as("fileIdLowBits"),
            functions.col("recordIndexMetadata.fileIndex").as("fileIndex"),
            functions.col("recordIndexMetadata.fileId").as("fileId"),
            functions.col("recordIndexMetadata.instantTime").as("instantTime"),
            functions.col("recordIndexMetadata.fileIdEncoding").as("fileIdEncoding"))
        .toJavaRDD()
        .map(row -> {
          HoodieRecordGlobalLocation location = getLocationFromRecordIndexInfo(
              row.getString(row.fieldIndex("partitionName")),
              row.getInt(row.fieldIndex("fileIdEncoding")),
              row.getLong(row.fieldIndex("fileIdHighBits")),
              row.getLong(row.fieldIndex("fileIdLowBits")),
              row.getInt(row.fieldIndex("fileIndex")),
              row.getString(row.fieldIndex("fileId")),
              row.getLong(row.fieldIndex("instantTime")));
          return new Tuple2<>(row, Option.of(location));
        }).filter(tuple2 -> tuple2._2.isPresent()) // filter the false positives
        .mapToPair(rowAndLocation -> new Tuple2<>(rowAndLocation._1.getString(rowAndLocation._1.fieldIndex("key")),
            Pair.of(rowAndLocation._2.get().getPartitionPath(), rowAndLocation._2.get().getFileId())))
        .cache();
  }

  private String constructLocationInfoString(String recordKey, Optional<Pair<String, String>> locationOnFs,
                                             Optional<Pair<String, String>> locationFromRecordIndex) {
    StringBuilder sb = new StringBuilder();
    sb.append("Record key " + recordKey + " -> ");
    sb.append("FS: ");
    if (locationOnFs.isPresent()) {
      sb.append(locationOnFs.get());
    } else {
      sb.append("<empty>");
    }
    sb.append(", Record Index: ");
    if (locationFromRecordIndex.isPresent()) {
      sb.append(locationFromRecordIndex.get());
    } else {
      sb.append("<empty>");
    }
    return sb.toString();
  }

  private static List<String> getLatestFileNames(HoodieMetadataValidationContext fsBasedContext, String partitionPath, Set<String> baseDataFilesForCleaning) {
    List<String> latestFileNames = new ArrayList<>();
    List<FileSlice> latestFileSlices;
    if (!baseDataFilesForCleaning.isEmpty()) {
      latestFileSlices = filterFileSliceBasedOnInflightCleaning(fsBasedContext.getSortedLatestFileSliceList(partitionPath), baseDataFilesForCleaning);
    } else {
      latestFileSlices = fsBasedContext.getSortedLatestFileSliceList(partitionPath);
    }
    latestFileSlices.forEach(fileSlice -> getLatestFiles(fileSlice, latestFileNames));
    return latestFileNames;
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

  <T> void validate(
      List<T> infoListFromMetadataTable, List<T> infoListFromFS, String partitionPath, String label) {
    boolean mismatch = false;
    String errorDetails = "";
    if (infoListFromMetadataTable.size() != infoListFromFS.size()) {
      errorDetails = String.format(
          "Number of %s based on the file system does not match that based on the "
              + "metadata table. File system-based listing (%s): %s; "
              + "MDT-based listing (%s): %s.",
          label,
          infoListFromFS.size(),
          toStringWithThreshold(infoListFromFS, cfg.logDetailMaxLength),
          infoListFromMetadataTable.size(),
          toStringWithThreshold(infoListFromMetadataTable, cfg.logDetailMaxLength));
      mismatch = true;
    } else {
      for (int i = 0; i < infoListFromMetadataTable.size(); i++) {
        T itemFromMdt = infoListFromMetadataTable.get(i);
        T itemFromFs = infoListFromFS.get(i);
        if (!Objects.equals(itemFromFs, itemFromMdt)) {
          errorDetails = String.format("%s mismatch. "
                  + "File slice from file system-based listing: %s; "
                  + "File slice from MDT-based listing: %s.",
              label, itemFromFs, itemFromMdt);
          mismatch = true;
          break;
        }
      }
    }
    if (mismatch) {
      String message = String.format("Validation of %s for partition %s failed for table: %s. %s",
          label, partitionPath, cfg.basePath, errorDetails);
      LOG.error(message);
      throw new HoodieValidationException(message);
    } else {
      LOG.info("Validation of {} succeeded for partition {} for table: {}", label, partitionPath, cfg.basePath);
    }
  }

  void validateFileSlices(
      List<FileSlice> fileSliceListFromMetadataTable, List<FileSlice> fileSliceListFromFS,
      String partitionPath, HoodieTableMetaClient metaClient, String label) {
    boolean mismatch = false;
    String errorDetails = "";
    if (fileSliceListFromMetadataTable.size() != fileSliceListFromFS.size()) {
      errorDetails = String.format(
          "Number of file slices based on the file system does not match that based on the "
              + "metadata table. File system-based listing (%s file slices): %s; "
              + "MDT-based listing (%s file slices): %s.",
          fileSliceListFromFS.size(),
          toStringWithThreshold(fileSliceListFromFS, cfg.logDetailMaxLength),
          fileSliceListFromMetadataTable.size(),
          toStringWithThreshold(fileSliceListFromMetadataTable, cfg.logDetailMaxLength));
      mismatch = true;
    } else if (!fileSliceListFromMetadataTable.equals(fileSliceListFromFS)) {
      // In-memory cache for the set of committed files of commits of interest
      Map<String, Set<String>> committedFilesMap = new HashMap<>();
      for (int i = 0; i < fileSliceListFromMetadataTable.size(); i++) {
        FileSlice fileSlice1 = fileSliceListFromMetadataTable.get(i);
        FileSlice fileSlice2 = fileSliceListFromFS.get(i);
        if (!Objects.equals(fileSlice1.getFileGroupId(), fileSlice2.getFileGroupId())
            || !Objects.equals(fileSlice1.getBaseInstantTime(), fileSlice2.getBaseInstantTime())) {
          errorDetails = String.format("File group ID (missing a file group in MDT) "
                  + "or base instant time mismatches. File slice from file system-based listing: %s; "
                  + "File slice from MDT-based listing: %s.",
              fileSlice2, fileSlice1);
          mismatch = true;
          break;
        }
        if (!assertBaseFilesEquality(fileSlice1, fileSlice2)) {
          errorDetails = String.format("Base files mismatch. "
                  + "File slice from file system-based listing: %s; "
                  + "File slice from MDT-based listing: %s.",
              fileSlice2, fileSlice1);
          mismatch = true;
          break;
        }
        // test for log files equality
        Pair<Boolean, String> matchingResult = areFileSliceCommittedLogFilesMatching(
            fileSlice1, fileSlice2, metaClient, committedFilesMap);
        if (Boolean.FALSE.equals(matchingResult.getLeft())) {
          errorDetails = "Log files mismatch (first file slice based on MDT, "
              + "second file slice based on the file system). "
              + matchingResult.getRight();
          mismatch = true;
          break;
        } else {
          LOG.warn("There are uncommitted log files in the latest file slices but the committed log files match: {} {}", fileSlice1, fileSlice2);
        }
      }
    }

    if (mismatch) {
      String message = String.format("Validation of %s for partition %s failed for table: %s. %s",
          label, partitionPath, cfg.basePath, errorDetails);
      LOG.error(message);
      throw new HoodieValidationException(message);
    } else {
      LOG.info("Validation of {} succeeded for partition {} for table: {}", label, partitionPath, cfg.basePath);
    }
  }

  private boolean assertBaseFilesEquality(FileSlice fileSlice1, FileSlice fileSlice2) {
    if (fileSlice1.getBaseFile().isPresent() && fileSlice2.getBaseFile().isPresent()) {
      HoodieBaseFile baseFile1 = fileSlice1.getBaseFile().get();
      HoodieBaseFile baseFile2 = fileSlice2.getBaseFile().get();
      return baseFile1.getFileName().equals(baseFile2.getFileName()) && baseFile1.getFileId().equals(baseFile2.getFileId())
          && baseFile1.getFileSize() == baseFile2.getFileSize();
    } else {
      return fileSlice1.getBaseFile().isPresent() == fileSlice2.getBaseFile().isPresent();
    }
  }

  /**
   * Compares committed log files from two file slices.
   *
   * @param fs1               File slice 1
   * @param fs2               File slice 2
   * @param metaClient        {@link HoodieTableMetaClient} instance
   * @param committedFilesMap In-memory map for caching committed files of commits
   * @return {@code true} if matching; {@code false} and the error message otherwise.
   */
  private Pair<Boolean, String> areFileSliceCommittedLogFilesMatching(
      FileSlice fs1,
      FileSlice fs2,
      HoodieTableMetaClient metaClient,
      Map<String, Set<String>> committedFilesMap) {
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
    HoodieStorage storage = metaClient.getStorage();

    Pair<Boolean, String> checkResult =
        hasCommittedLogFiles(storage, fs1LogPathSet, metaClient, committedFilesMap);
    if (Boolean.TRUE.equals(checkResult.getLeft())) {
      return Pair.of(false, String.format(
          "The first file slice has committed log files that cause mismatching: %s"
              + "; Different log files are: %s. Details: %s.",
          fs1, fs1LogPathSet, checkResult.getRight()));
    }
    checkResult =
        hasCommittedLogFiles(storage, fs2LogPathSet, metaClient, committedFilesMap);
    if (Boolean.TRUE.equals(checkResult.getLeft())) {
      return Pair.of(false, String.format(
          "The second file slice has committed log files that cause mismatching: %s"
              + "; Different log files are: %s.  Details: %s.",
          fs2, fs2LogPathSet, checkResult.getRight()));
    }
    return Pair.of(true, "");
  }

  Pair<Boolean, String> hasCommittedLogFiles(
      HoodieStorage storage,
      Set<String> logFilePathSet,
      HoodieTableMetaClient metaClient,
      Map<String, Set<String>> committedFilesMap) {
    if (logFilePathSet.isEmpty()) {
      return Pair.of(false, "");
    }

    String basePath = metaClient.getBasePath().toString();
    HoodieTimeline commitsTimeline = metaClient.getCommitsTimeline();
    HoodieTimeline completedInstantsTimeline = commitsTimeline.filterCompletedInstants();
    HoodieTimeline inflightInstantsTimeline = commitsTimeline.filterInflights();

    for (String logFilePathStr : logFilePathSet) {
      HoodieLogFormat.Reader reader = null;
      try {
        Schema readerSchema =
            TableSchemaResolver.readSchemaFromLogFile(storage, new StoragePath(logFilePathStr));
        if (readerSchema == null) {
          LOG.warn("Cannot read schema from log file {}. Skip the check as it's likely being written by an inflight instant.", logFilePathStr);
          continue;
        }
        reader =
            HoodieLogFormat.newReader(storage, new HoodieLogFile(logFilePathStr), readerSchema, false);
        // read the avro blocks
        if (reader.hasNext()) {
          HoodieLogBlock block = reader.next();
          final String instantTime = block.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME);
          if (completedInstantsTimeline.containsInstant(instantTime)) {
            // The instant is completed, in active timeline
            // Checking commit metadata only as log files can only be written by COMMIT or DELTA_COMMIT
            if (!committedFilesMap.containsKey(instantTime)) {
              HoodieInstant instant =  completedInstantsTimeline.filter(i -> i.requestedTime().equals(instantTime))
                  .firstInstant().get();
              HoodieCommitMetadata commitMetadata =
                  completedInstantsTimeline.readCommitMetadata(instant);
              committedFilesMap.put(
                  instantTime,
                  commitMetadata.getWriteStats().stream()
                      .map(HoodieWriteStat::getPath).collect(Collectors.toSet())
              );
            }

            // Here we check if the commit metadata contains the log file.
            // Note that, a log file may be written by a successful write transaction
            // leading to a delta commit, but such a log file can be uncommitted and
            // not be part of any snapshot, due to Spark task retries for example.
            // In such a case, the log file can stay in the file system, but the metadata
            // table does not contain the log file for file listing, which is an expected
            // behavior.
            String relativeLogFilePathStr = getRelativePath(basePath, logFilePathStr);
            if (committedFilesMap.get(instantTime).contains(relativeLogFilePathStr)) {
              return Pair.of(true, String.format(
                  "Log file is committed in an instant in active timeline: instantTime=%s %s",
                  instantTime, logFilePathStr));
            } else {
              LOG.warn("Log file is uncommitted in a completed instant, likely due to retry: instantTime={} {}", instantTime, logFilePathStr);
            }
          } else if (completedInstantsTimeline.isBeforeTimelineStarts(instantTime)) {
            // The instant is in archived timeline
            return Pair.of(true, String.format(
                "Log file is committed in an instant in archived timeline: instantTime=%s %s",
                instantTime, logFilePathStr));
          } else if (inflightInstantsTimeline.containsInstant(instantTime)) {
            // The instant is inflight in active timeline
            // hit an uncommitted block possibly from a failed write
            LOG.warn("Log file is uncommitted because of an inflight instant: instantTime={} {}", instantTime, logFilePathStr);
          } else {
            // The instant is after the start of the active timeline,
            // but it cannot be found in the active timeline
            LOG.warn("Log file is uncommitted because the instant is after the start of the active timeline but absent or in requested in the active timeline: instantTime={} {}",
                instantTime, logFilePathStr);
          }
        } else {
          LOG.warn("There is no log block in {}", logFilePathStr);
        }
      } catch (IOException e) {
        LOG.warn("Cannot read log file {}. Skip the check as it's likely being written by an inflight instant.",
            logFilePathStr, e);
      } finally {
        FileIOUtils.closeQuietly(reader);
      }
    }
    return Pair.of(false, "");
  }

  private String getRelativePath(String basePath, String absoluteFilePath) {
    String basePathStr = new StoragePath(basePath).getPathWithoutSchemeAndAuthority().toString();
    String absoluteFilePathStr = new StoragePath(absoluteFilePath).getPathWithoutSchemeAndAuthority().toString();

    if (!absoluteFilePathStr.startsWith(basePathStr)) {
      throw new IllegalArgumentException("File path does not belong to the base path! basePath="
          + basePathStr + " absoluteFilePathStr=" + absoluteFilePathStr);
    }

    String relativePathStr = absoluteFilePathStr.substring(basePathStr.length());
    return relativePathStr.startsWith("/") ? relativePathStr.substring(1) : relativePathStr;
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
              LOG.info("Last validate ran less than min validate interval: {} s, sleep: {} ms.", cfg.minValidateIntervalSeconds, toSleepMs);
              Thread.sleep(toSleepMs);
            }
          } catch (HoodieValidationException e) {
            LOG.error("Shutting down AsyncMetadataTableValidateService due to HoodieValidationException", e);
            if (!cfg.ignoreFailed) {
              throw e;
            }
            throwables.add(e);
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
  private static class HoodieMetadataValidationContext implements AutoCloseable, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(HoodieMetadataValidationContext.class);

    private final Properties props;
    private final HoodieTableMetaClient metaClient;
    private final HoodieMetadataConfig metadataConfig;
    private final Schema schema;
    private final HoodieTableFileSystemView fileSystemView;
    private final HoodieTableMetadata tableMetadata;
    private final boolean enableMetadataTable;
    private List<String> allColumnNameList;

    public HoodieMetadataValidationContext(
        HoodieEngineContext engineContext, Properties props, HoodieTableMetaClient metaClient,
        boolean enableMetadataTable, String viewStorageType) {
      try {
        this.props = new Properties();
        this.props.putAll(props);
        this.metaClient = metaClient;
        this.schema = new TableSchemaResolver(metaClient).getTableAvroSchema();
        this.enableMetadataTable = enableMetadataTable;
        this.metadataConfig = HoodieMetadataConfig.newBuilder()
            .enable(enableMetadataTable)
            .withMetadataIndexBloomFilter(enableMetadataTable)
            .withMetadataIndexColumnStats(enableMetadataTable)
            .withEnableRecordIndex(enableMetadataTable)
            .build();
        props.put(FileSystemViewStorageConfig.VIEW_TYPE.key(), viewStorageType);
        FileSystemViewStorageConfig viewConf = FileSystemViewStorageConfig.newBuilder().fromProperties(props).build();
        ValidationUtils.checkArgument(viewConf.getStorageType().name().equals(viewStorageType), "View storage type not reflected");
        HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().fromProperties(props).build();
        this.tableMetadata = metaClient.getTableFormat().getMetadataFactory().create(
            engineContext, metaClient.getStorage(), metadataConfig, metaClient.getBasePath().toString());
        this.fileSystemView = getFileSystemView(engineContext,
            metaClient, metadataConfig, viewConf, commonConfig);
        if (metaClient.getCommitsTimeline().filterCompletedInstants().countInstants() > 0) {
          this.allColumnNameList = getAllColumnNames();
        }
      } catch (Exception e) {
        throw new HoodieException("Failed to initialize metadata validation context for " + metaClient.getBasePath());
      }
    }

    private HoodieTableFileSystemView getFileSystemView(HoodieEngineContext context,
                                                        HoodieTableMetaClient metaClient, HoodieMetadataConfig metadataConfig,
                                                        FileSystemViewStorageConfig viewConf, HoodieCommonConfig commonConfig) {
      switch (viewConf.getStorageType()) {
        case SPILLABLE_DISK:
          LOG.debug("Creating Spillable Disk based Table View");
          break;
        case MEMORY:
          LOG.debug("Creating in-memory based Table View");
          break;
        default:
          throw new HoodieException("Unsupported storage type " + viewConf.getStorageType() + ", used with HoodieMetadataTableValidator");
      }
      return (HoodieTableFileSystemView) FileSystemViewManager.createViewManager(context, metadataConfig, viewConf, commonConfig, unused -> tableMetadata).getFileSystemView(metaClient);
    }

    public HoodieTableMetaClient getMetaClient() {
      return metaClient;
    }

    public HoodieMetadataConfig getMetadataConfig() {
      return metadataConfig;
    }

    public Schema getSchema() {
      return schema;
    }

    public HoodieTableMetadata getTableMetadata() {
      return tableMetadata;
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
    public List<HoodieColumnRangeMetadata<Comparable>> getSortedColumnStatsList(String partitionPath, List<String> fileNames, Schema readerSchema) throws Exception {
      LOG.info("All column names for getting column stats: {}", allColumnNameList);
      if (enableMetadataTable) {
        List<Pair<String, String>> partitionFileNameList = fileNames.stream()
            .map(filename -> Pair.of(partitionPath, filename)).collect(Collectors.toList());
        return allColumnNameList.stream()
            .flatMap(columnName ->
                tableMetadata.getColumnStats(partitionFileNameList, columnName).values().stream()
                    .map(HoodieColumnRangeMetadata::fromColumnStats)
                    .collect(Collectors.toList())
                    .stream())
            .sorted(new HoodieColumnRangeMetadataComparator())
            .collect(Collectors.toList());
      } else {
        FileFormatUtils formatUtils = HoodieIOFactory.getIOFactory(metaClient.getStorage()).getFileFormatUtils(HoodieFileFormat.PARQUET);
        return (List<HoodieColumnRangeMetadata<Comparable>>) fileNames.stream().flatMap(filename -> {
          if (filename.endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
            return formatUtils.readColumnStatsFromMetadata(
                metaClient.getStorage(),
                new StoragePath(FSUtils.constructAbsolutePath(metaClient.getBasePath(), partitionPath), filename),
                allColumnNameList
            ).stream();
          } else {
            StoragePath storagePartitionPath = new StoragePath(metaClient.getBasePath(), partitionPath);
            String filePath = new StoragePath(storagePartitionPath, filename).toString();
            try {
              return getLogFileColumnRangeMetadata(filePath, partitionPath, metaClient, allColumnNameList, Option.of(readerSchema),
                  metadataConfig.getMaxReaderBufferSize())
                  .stream()
                  // We need to convert file path and use only the file name instead of the complete file path
                  .map(m -> (HoodieColumnRangeMetadata<Comparable>) HoodieColumnRangeMetadata.create(filename, m.getColumnName(), m.getMinValue(), m.getMaxValue(),
                      m.getNullCount(), m.getValueCount(), m.getTotalSize(), m.getTotalUncompressedSize()));
            } catch (IOException e) {
              throw new HoodieIOException(String.format("Failed to get column stats for file: %s", filePath), e);
            }
          }
        })
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
                .setBloomFilter(ByteBuffer.wrap(getUTF8Bytes(entry.getValue().serializeToString())))
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
      try {
        return schema.getFields().stream().filter(field -> META_COL_SET_TO_INDEX.contains(field.name()))
            .map(Schema.Field::name).collect(Collectors.toList());
      } catch (Exception e) {
        throw new HoodieException("Failed to get all column names for " + metaClient.getBasePath());
      }
    }

    private Option<BloomFilterData> readBloomFilterFromFile(String partitionPath, String filename) {
      StoragePath path = new StoragePath(
          FSUtils.constructAbsolutePath(metaClient.getBasePath(), partitionPath).toString(), filename);
      BloomFilter bloomFilter;
      HoodieConfig hoodieConfig = new HoodieConfig();
      hoodieConfig.setValue(HoodieReaderConfig.USE_NATIVE_HFILE_READER,
          Boolean.toString(ConfigUtils.getBooleanWithAltKeys(props, HoodieReaderConfig.USE_NATIVE_HFILE_READER)));
      try (HoodieFileReader fileReader = getHoodieSparkIOFactory(metaClient.getStorage())
          .getReaderFactory(HoodieRecordType.AVRO)
          .getFileReader(hoodieConfig, path)) {
        bloomFilter = fileReader.readBloomFilter();
        if (bloomFilter == null) {
          LOG.error("Failed to read bloom filter for {}", path);
          return Option.empty();
        }
      } catch (IOException e) {
        LOG.error("Failed to get file reader for {} {}", path, e);
        return Option.empty();
      }
      return Option.of(BloomFilterData.builder()
          .setPartitionPath(partitionPath)
          .setFilename(filename)
          .setBloomFilter(ByteBuffer.wrap(getUTF8Bytes(bloomFilter.serializeToString())))
          .build());
    }

    @Override
    public void close() throws Exception {
      tableMetadata.close();
      fileSystemView.close();
    }
  }
}
