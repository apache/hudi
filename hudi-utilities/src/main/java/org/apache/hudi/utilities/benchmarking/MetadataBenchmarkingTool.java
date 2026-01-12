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

package org.apache.hudi.utilities.benchmarking;

import org.apache.hudi.BaseHoodieTableFileIndex;
import org.apache.hudi.HoodieFileIndex;
import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataWriterTestUtils;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.stats.ValueMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.IdentitySplitter;
import org.apache.hudi.utilities.UtilHelpers;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.HoodieCatalystExpressionUtils$;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.execution.datasources.NoopCache$;
import org.apache.spark.sql.types.StructType;
import org.bouncycastle.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;

public class MetadataBenchmarkingTool implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataBenchmarkingTool.class);

  // Table and column constants
  private static final String TABLE_NAME = "test_mdt_stats_tbl";
  private static final String COL_TENANT_ID = "tenantID";
  private static final String COL_AGE = "age";

  // Partition generation constants
  private static final LocalDate PARTITION_START_DATE = LocalDate.of(2025, 1, 1);
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

  // TenantID column stats range: 30000-60000
  private static final long TENANT_ID_MIN_BASE = 30000L;
  private static final int TENANT_ID_RANGE = 30000;
  private static final long TENANT_ID_MAX = 60000L;

  // Age column stats range: 20-99
  private static final int AGE_MIN_BASE = 20;
  private static final int AGE_MIN_RANGE = 30;
  private static final int AGE_MAX_RANGE = 50;

  // Column stats metadata defaults
  private static final int COL_STATS_NULL_COUNT = 0;
  private static final int COL_STATS_VALUE_COUNT = 1000;
  private static final long COL_STATS_TOTAL_SIZE = 123456L;
  private static final long COL_STATS_TOTAL_UNCOMPRESSED_SIZE = 123456L;

  private final Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;

  private final SparkSession spark;

  private final JavaSparkContext jsc;

  private final HoodieEngineContext engineContext;

  /**
   * Returns the AVRO schema string for the table.
   * Schema includes: id, name, city, age, tenantID, dt
   */
  private static String getAvroSchema() {
    return "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Employee\",\n"
        + "  \"namespace\": \"com.example.avro\",\n"
        + "  \"fields\": [\n"
        + "    { \"name\": \"id\", \"type\": \"string\" },\n"
        + "    { \"name\": \"name\", \"type\": \"string\" },\n"
        + "    { \"name\": \"city\", \"type\": \"string\" },\n"
        + "    { \"name\": \"age\", \"type\": \"int\" },\n"
        + "    { \"name\": \"tenantID\", \"type\": \"long\" },\n"
        + "    { \"name\": \"dt\", \"type\": \"string\" }\n"
        + "  ]\n"
        + "}\n";
  }

  private static final String RECORD_ID = "id";

  private static final String PARTITION_FIELDS = "dt";
  /**
   * Returns the Spark StructType schema for data skipping queries.
   * Reused across multiple places to avoid duplication.
   */
  private static StructType getDataSchema() {
    return new StructType()
        .add("id", "string")
        .add("name", "string")
        .add("city", "string")
        .add("age", "int")
        .add("tenantID", "long")
        .add("dt", "string");
  }

  /**
   * Returns the list of columns to index based on numColumnsToIndex config.
   * @param numColumnsToIndex 1 for tenantID only, 2 for tenantID & age
   * @return List of column names to index
   */
  private List<String> getColumnsToIndex(int numColumnsToIndex) {
    return numColumnsToIndex == 2 ? Arrays.asList(COL_TENANT_ID, COL_AGE)
        : Collections.singletonList(COL_TENANT_ID);
  }

  private String getColumnsToIndexString(int numColumnsToIndex) {
    return String.join(",", getColumnsToIndex(numColumnsToIndex));
  }

  public MetadataBenchmarkingTool(SparkSession spark, Config cfg) {
    this.spark = spark;
    this.jsc = new JavaSparkContext(spark.sparkContext());
    this.engineContext = new HoodieSparkEngineContext(jsc);
    this.cfg = cfg;
    this.props = StringUtils.isNullOrEmpty(cfg.propsFilePath)
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
  }

  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, Config cfg) {
    return UtilHelpers.readConfig(jsc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.configs)
        .getProps(true);
  }

  public static class Config implements Serializable {

    /**
     * Benchmark mode options.
     */
    public enum BenchmarkMode {
      BOOTSTRAP,           // Only bootstrap metadata table
      QUERY,               // Only run data skipping benchmark
      BOOTSTRAP_AND_QUERY  // Run both (default)
    }

    @Parameter(names = {"--mode", "-m"}, description = "Benchmark mode: BOOTSTRAP (write only), QUERY (read only), BOOTSTRAP_AND_QUERY (default)")
    public BenchmarkMode mode = BenchmarkMode.BOOTSTRAP_AND_QUERY;

    @Parameter(names = {"--table-base-path", "-tbp"}, description = "Base path for the Hudi table", required = true)
    public String tableBasePath = null;

    @Parameter(names = {"--num-cols-to-index", "-num-cols"}, description = "Number of columns to index (1 for tenantID, 2 for tenantID & age)", required = true)
    public Integer numColumnsToIndex = 1;

    @Parameter(names = {"--col-stats-file-group-count", "-col-fg-count"}, description = "Number of file groups for column stats partition in metadata table", required = true)
    public Integer colStatsFileGroupCount = 10;

    @Parameter(names = {"--num-partitions", "-np"}, description = "Number of partitions to create in the table", required = true)
    public Integer numPartitions = 1;

    @Parameter(names = {"--num-files-to-bootstrap", "-nfb"}, description = "Number of files to create during bootstrap", required = true)
    public Integer numFilesToBootstrap = 1000;

    @Parameter(names = {"--num-files-for-incremental", "-nfi"}, description = "Total number of files to create across incremental commits")
    public Integer numFilesForIncrementalIngestion = 0;

    @Parameter(names = {"--num-commits-for-incremental", "-nci"}, description = "Number of incremental commits to distribute files across")
    public Integer numOfcommitForIncrementalIngestion = 0;

    @Parameter(names = {"--partition-filter", "-pf"}, description = "Partition filter predicate for querying (e.g., \"dt > '2025-01-01'\")")
    public String partitionFilter = "dt = '2025-01-01'";

    @Parameter(names = {"--data-filter", "-df"}, description = "data filter predicate for querying (e.g., \"age > 70\")")
    public String dataFilters = "";

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--props"}, description = "Path to properties file on localfs or dfs, with configurations for "
        + "Hoodie client")
    public String propsFilePath = null;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "MetadataBenchmarkingTool {\n"
          + "   --mode " + mode + ",\n"
          + "   --table-base-path " + tableBasePath + ",\n"
          + "   --num-partitions " + numPartitions + ",\n"
          + "   --num-cols-to-index " + numColumnsToIndex + ",\n"
          + "   --col-stats-file-group-count " + colStatsFileGroupCount + ",\n"
          + "   --num-files-to-bootstrap " + numFilesToBootstrap + ",\n"
          + "   --num-files-for-incremental " + numFilesForIncrementalIngestion + ",\n"
          + "   --num-commits-for-incremental " + numOfcommitForIncrementalIngestion + ",\n"
          + "   --partition-filter " + partitionFilter + "\n"
          + "}";
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    final LocalDateTime now = LocalDateTime.now();
    final String currentHour = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"));
    String jobName = "metadata-table-stats-analyzer";
    String sparkAppName = jobName + "-" + currentHour;
    SparkSession spark = SparkSession.builder()
        .appName(sparkAppName)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate();


    try (MetadataBenchmarkingTool metadataBenchmarkingTool = new MetadataBenchmarkingTool(spark, cfg)) {
      metadataBenchmarkingTool.run();
    } catch (Throwable throwable) {
      LOG.error("Failed to get table size stats for " + cfg, throwable);
    } finally {
      spark.stop();
    }
  }

  public void run() throws Exception {
    int numPartitions = cfg.numPartitions;
    List<String> colsToIndex = getColumnsToIndex(cfg.numColumnsToIndex);
    LOG.info("Data table base path: {}", cfg.tableBasePath);
    LOG.info("Benchmark mode: {}", cfg.mode);

    HoodieWriteConfig dataWriteConfig = getWriteConfig(getAvroSchema(), cfg.tableBasePath, HoodieFailedWritesCleaningPolicy.EAGER);

    int totalFilesCreated = 0;
    if (cfg.mode == Config.BenchmarkMode.BOOTSTRAP || cfg.mode == Config.BenchmarkMode.BOOTSTRAP_AND_QUERY) {
      HoodieTableMetaClient dataMetaClient = initializeDataTableMetaClient(TABLE_NAME, dataWriteConfig);
      
      // Bootstrap phase
      Pair<Integer, List<String>> bootstrapResult = bootstrapMetadataTable(
          cfg.numFilesToBootstrap, numPartitions, colsToIndex, dataWriteConfig, dataMetaClient);
      totalFilesCreated = bootstrapResult.getLeft();
      List<String> partitions = bootstrapResult.getRight();
      LOG.info("Completed bootstrapping Metadata table with {} files", totalFilesCreated);
      
      // Incremental ingestion phase
      if (cfg.numFilesForIncrementalIngestion > 0 && cfg.numOfcommitForIncrementalIngestion > 0) {
        int incrementalFiles = runIncrementalIngestion(
            cfg.numFilesForIncrementalIngestion,
            cfg.numOfcommitForIncrementalIngestion,
            partitions,
            colsToIndex,
            dataWriteConfig,
            dataMetaClient);
        totalFilesCreated += incrementalFiles;
        LOG.info("Completed incremental ingestion with {} additional files across {} commits", 
            incrementalFiles, cfg.numOfcommitForIncrementalIngestion);
      }
    }

    if (cfg.mode == Config.BenchmarkMode.QUERY || cfg.mode == Config.BenchmarkMode.BOOTSTRAP_AND_QUERY) {
      HoodieTableMetaClient dataMetaClient = loadExistingMetaClient(dataWriteConfig);
      if (totalFilesCreated == 0) {
        totalFilesCreated = cfg.numFilesToBootstrap + cfg.numFilesForIncrementalIngestion;
        if (totalFilesCreated == 0) {
          LOG.warn("Total files count is 0. Data skipping ratio calculation may be inaccurate.");
        }
      }
      benchmarkDataSkipping(dataWriteConfig, dataMetaClient, totalFilesCreated);
      LOG.info("Completed query benchmarking");
    }
  }

  private Pair<Integer, List<String>> bootstrapMetadataTable(
      int numFiles, int numPartitions, List<String> colsToIndex,
      HoodieWriteConfig dataWriteConfig, HoodieTableMetaClient dataTableMetaClient) throws Exception {

    LOG.info("Bootstrapping metadata table: {} files, {} partitions, columns [{}], {} col stats file groups",
        numFiles, numPartitions, String.join(",", colsToIndex), cfg.colStatsFileGroupCount);

    List<String> partitions = generatePartitions(numPartitions);
    int filesPerPartition = numFiles / numPartitions;

    String dataCommitTime = InProcessTimeGenerator.createNewInstantTime();

    // Create partition directories on the filesystem
    createPartitionPaths(dataTableMetaClient, partitions, dataCommitTime);

    HoodieCommitMetadata commitMetadata = createCommitMetadataAndAddToTimeline(partitions, filesPerPartition, dataCommitTime, dataTableMetaClient);

    HoodieTimer timer = HoodieTimer.start();
    try (SparkHoodieBackedTableMetadataBenchmarkWriter metadataWriter =
             new SparkHoodieBackedTableMetadataBenchmarkWriter(
                 engineContext.getStorageConf(), dataWriteConfig,
                 HoodieFailedWritesCleaningPolicy.EAGER, engineContext, Option.empty(), false)) {

      metadataWriter.initMetadataMetaClient();
      bootstrapFilesPartition(metadataWriter, commitMetadata, dataCommitTime);
      bootstrapColumnStatsPartition(metadataWriter, commitMetadata, colsToIndex);
    }
    LOG.info("Time taken to perform bootstrapping metadata table is {}", timer.endTimer());

    return Pair.of(filesPerPartition * numPartitions, partitions);
  }

  /**
   * Creates commit metadata for the test table and adds it to the timeline.
   */
  private HoodieCommitMetadata createCommitMetadataAndAddToTimeline(List<String> partitions,
      int filesPerPartition, String dataCommitTime, HoodieTableMetaClient dataTableMetaClient) throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(dataTableMetaClient);
    HoodieSchema hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(getDataSchema(), "mdt_benchmarking_struct","mdt_benchmarking_namespace");
        HoodieTestTable.TEST_TABLE_SCHEMA = hoodieSchema.toString();

    HoodieCommitMetadata commitMetadata = testTable.createCommitMetadata(
        dataCommitTime, WriteOperationType.INSERT, partitions, filesPerPartition, false);

            HoodieInstant requestedInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, dataCommitTime,
                InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    dataTableMetaClient.getActiveTimeline().createNewInstant(requestedInstant);
    dataTableMetaClient.getActiveTimeline().transitionRequestedToInflight(requestedInstant, Option.empty());

            Map<String, String> extraMetadata = new HashMap<>();
        extraMetadata.put(HoodieCommitMetadata.SCHEMA_KEY, commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY));

    dataTableMetaClient.getActiveTimeline().saveAsComplete(false,
        dataTableMetaClient.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, dataCommitTime), Option.of(commitMetadata));
    LOG.info("Created commit metadata at instant {} with {} files per partition", dataCommitTime, filesPerPartition);
    return commitMetadata;
  }

  /**
   * Bootstraps the FILES partition in the metadata table.
   */
  @SuppressWarnings("unchecked")
  private void bootstrapFilesPartition(
      SparkHoodieBackedTableMetadataBenchmarkWriter metadataWriter,
      HoodieCommitMetadata commitMetadata,
      String dataCommitTime) throws IOException {
    HoodieTimer timer = HoodieTimer.start();
    List<HoodieRecord> filesRecords = HoodieTableMetadataUtil
        .convertMetadataToFilesPartitionRecords(commitMetadata, dataCommitTime);
    LOG.info("Bootstrapping FILES partition with {} records", filesRecords.size());

    String instantTime = generateUniqueInstantTime(0);
    Pair<Integer, HoodieData<HoodieRecord>> fileGroupCountAndRecords =
        Pair.of(1, engineContext.parallelize(filesRecords, 10));
    metadataWriter.initializeFilegroupsAndCommit(FILES, FILES.getPartitionPath(), fileGroupCountAndRecords, instantTime);
    LOG.info("Time taken for bootstrapping files partition is {}", timer.endTimer());
  }

  /**
   * Bootstraps the COLUMN_STATS partition in the metadata table.
   */
  @SuppressWarnings("rawtypes")
  private void bootstrapColumnStatsPartition(
      SparkHoodieBackedTableMetadataBenchmarkWriter metadataWriter,
      HoodieCommitMetadata commitMetadata,
      List<String> colsToIndex) throws IOException {

    HoodieTimer timer = HoodieTimer.start();
    HoodieData<HoodieRecord> columnStatsRecords = generateColumnStatsRecordsForCommitMetadata(commitMetadata);
    LOG.info("Bootstrapping COLUMN_STATS partition with {} file groups", cfg.colStatsFileGroupCount);

    String instantTime = generateUniqueInstantTime(1);
    Pair<Integer, HoodieData<HoodieRecord>> fileGroupCountAndRecords =
        Pair.of(cfg.colStatsFileGroupCount, columnStatsRecords);
    metadataWriter.initializeFilegroupsAndCommit(
        COLUMN_STATS, COLUMN_STATS.getPartitionPath(), fileGroupCountAndRecords, instantTime, colsToIndex);
    LOG.info("Time taken to bootstrap column stats is {}", timer.endTimer());
  }

  /**
   * Runs incremental ingestion rounds after bootstrap.
   * Distributes files across multiple commits and uses upsertPreppedRecords for each commit.
   * Reuses the partitions created during bootstrap.
   */
  private int runIncrementalIngestion(
      int totalFiles,
      int numCommits,
      List<String> partitions,
      List<String> colsToIndex,
      HoodieWriteConfig dataWriteConfig,
      HoodieTableMetaClient dataMetaClient) throws Exception {

    LOG.info("Starting incremental ingestion: {} files across {} commits using {} existing partitions", 
        totalFiles, numCommits, partitions.size());
    
    int filesPerCommit = totalFiles / numCommits;
    int remainingFiles = totalFiles % numCommits;
    
    HoodieWriteConfig mdtWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        dataWriteConfig,
        HoodieFailedWritesCleaningPolicy.EAGER,
        HoodieTableVersion.NINE);

    int totalFilesCreated = 0;

    try (HoodieBackedTableMetadataWriter<?, ?> metadataWriter = 
        (HoodieBackedTableMetadataWriter) org.apache.hudi.metadata.SparkMetadataWriterFactory.create(
            engineContext.getStorageConf(),
            dataWriteConfig,
            engineContext,
            Option.empty(),
            dataMetaClient.getTableConfig())) {

      // HoodieBackedTableMetadataWriter initializes metadata reader in constructor, no need to call initMetadataMetaClient()

      for (int commitIdx = 0; commitIdx < numCommits; commitIdx++) {
        int filesThisCommit = filesPerCommit + (commitIdx < remainingFiles ? 1 : 0);
        if (filesThisCommit == 0) {
          continue;
        }

        String dataCommitTime = InProcessTimeGenerator.createNewInstantTime();
        HoodieCommitMetadata commitMetadata = createCommitMetadataAndAddToTimeline(
            partitions, filesThisCommit, dataCommitTime, dataMetaClient);

        // Partitions already exist from bootstrap, no need to create them again
        // createFilesForCommit(dataMetaClient, commitMetadata);

        // Generate records for both partitions
        @SuppressWarnings("unchecked")
        List<HoodieRecord<HoodieMetadataPayload>> filesRecords = 
            (List<HoodieRecord<HoodieMetadataPayload>>) (List<?>)
            HoodieTableMetadataUtil.convertMetadataToFilesPartitionRecords(commitMetadata, dataCommitTime);

        HoodieData<HoodieRecord> columnStatsRecords = generateColumnStatsRecordsForCommitMetadata(commitMetadata);

        // Use upsertPreppedRecords for incremental commits
        performIncrementalCommit(
            metadataWriter,
            mdtWriteConfig,
            dataCommitTime,
            filesRecords,
            columnStatsRecords,
            colsToIndex);

        totalFilesCreated += filesThisCommit;
        LOG.info("Completed incremental commit {}: {} files (total: {})", 
            commitIdx + 1, filesThisCommit, totalFilesCreated);
      }
    }

    return totalFilesCreated;
  }

  /**
   * Performs an incremental commit using upsertPreppedRecords.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void performIncrementalCommit(
      HoodieBackedTableMetadataWriter metadataWriter,
      HoodieWriteConfig mdtWriteConfig,
      String dataCommitTime,
      List<HoodieRecord<HoodieMetadataPayload>> filesRecords,
      HoodieData<HoodieRecord> columnStatsRecords,
      List<String> colsToIndex) throws Exception {

    String mdtCommitTime = dataCommitTime;
    
    try (SparkRDDWriteClient<HoodieMetadataPayload> mdtWriteClient = 
        new SparkRDDWriteClient<>(engineContext, mdtWriteConfig)) {

      WriteClientTestUtils.startCommitWithTime(mdtWriteClient, mdtCommitTime);
      
      JavaRDD<HoodieRecord<HoodieMetadataPayload>> filesRDD = jsc.parallelize(filesRecords, 1);

      org.apache.hudi.metadata.HoodieBackedTableMetadataWriter<JavaRDD<HoodieRecord>, JavaRDD<WriteStatus>>
          sparkMetadataWriter = (org.apache.hudi.metadata.HoodieBackedTableMetadataWriter) metadataWriter;

      @SuppressWarnings({"rawtypes", "unchecked"})
      Map<String, HoodieData<HoodieRecord>> partitionRecordsMap = new HashMap<>();
      partitionRecordsMap.put(
          HoodieTableMetadataUtil.PARTITION_NAME_FILES,
          (HoodieData<HoodieRecord>) (HoodieData) HoodieJavaRDD.of(filesRDD));
      partitionRecordsMap.put(
          HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS,
          columnStatsRecords);

      // Tag records with location
      Pair<HoodieData<HoodieRecord>, List<HoodieFileGroupId>> taggedResult =
          MetadataWriterTestUtils.tagRecordsWithLocation(
              sparkMetadataWriter,
              partitionRecordsMap,
              false // isInitializing = false for incremental commits
          );

      // Convert back to JavaRDD
      JavaRDD<HoodieRecord<HoodieMetadataPayload>> allTaggedRecords =
          (JavaRDD<HoodieRecord<HoodieMetadataPayload>>) (JavaRDD) HoodieJavaRDD
              .getJavaRDD(taggedResult.getKey());

      // Use upsertPreppedRecords for incremental commits
      JavaRDD<WriteStatus> writeStatuses = mdtWriteClient.upsertPreppedRecords(allTaggedRecords, mdtCommitTime);
      mdtWriteClient.commit(mdtCommitTime, writeStatuses, Option.empty(), HoodieTimeline.DELTA_COMMIT_ACTION, Collections.emptyMap());
    }
  }

  String generateUniqueInstantTime(int offset) {
    return HoodieInstantTimeGenerator.instantTimePlusMillis(SOLO_COMMIT_TIMESTAMP, offset);
  }

  private HoodieTableMetaClient initializeDataTableMetaClient(String tableName, HoodieWriteConfig dataConfig) throws IOException {
    return HoodieTableMetaClient.newTableBuilder()
        .setTableVersion(HoodieTableVersion.NINE)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(tableName)
        .setPartitionFields(PARTITION_FIELDS)
        .setRecordKeyFields(RECORD_ID)
        .initTable(engineContext.getStorageConf(), dataConfig.getBasePath());
  }

  /**
   * Loads an existing HoodieTableMetaClient. Throws exception if table doesn't exist.
   * Used in QUERY mode where the table must already exist.
   */
  private HoodieTableMetaClient loadExistingMetaClient(HoodieWriteConfig dataConfig) {
    try {
      return HoodieTableMetaClient.builder()
          .setConf(engineContext.getStorageConf())
          .setBasePath(dataConfig.getBasePath())
          .build();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Table does not exist at " + dataConfig.getBasePath() + ". "
              + "QUERY mode requires an existing table. Use BOOTSTRAP or BOOTSTRAP_AND_QUERY mode first.", e);
    }
  }

  /**
   * Creates partition directories on the filesystem.
   */
  private void createPartitionPaths(HoodieTableMetaClient metaClient, List<String> partitions, String instantTime) throws IOException {
    StoragePath basePath = metaClient.getBasePath();
    for (String partition : partitions) {
      StoragePath fullPartitionPath = new StoragePath(basePath, partition);
      metaClient.getStorage().createDirectory(fullPartitionPath);

      new HoodiePartitionMetadata(metaClient.getStorage(), instantTime,
                    new StoragePath(metaClient.getBasePath().toString()), fullPartitionPath,
                    metaClient.getTableConfig().getPartitionMetafileFormat()).trySave();
    }
    LOG.info("Created {} partition directories under {}", partitions.size(), basePath);
  }

  /**
   * Generates a list of date-based partition paths incrementing by day.
   * Starting from 2020-01-01, creates partitions for consecutive days based on numPartitions.
   * <p>
   * Example:
   * numPartitions = 1  -> ["2020-01-01"]
   * numPartitions = 3  -> ["2020-01-01", "2020-01-02", "2020-01-03"]
   * numPartitions = 10 -> ["2020-01-01", "2020-01-02", ..., "2020-01-10"]
   *
   * @param numPartitions Number of partitions to generate
   * @return List of partition paths in yyyy-MM-dd format
   */
  private List<String> generatePartitions(int numPartitions) {
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("numPartitions must be greater than 0, got: " + numPartitions);
    }

    List<String> partitions = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      LocalDate partitionDate = PARTITION_START_DATE.plusDays(i);
      partitions.add(partitionDate.format(DATE_FORMATTER));
    }

    LOG.warn("Generated {} partitions: {} to {}. Ensure --partition-filter matches these partitions.",
        numPartitions,
        partitions.get(0),
        partitions.get(partitions.size() - 1));

    return partitions;
  }

  /**
   * Benchmarks data skipping using column stats index via HoodieFileIndex.filterFileSlices.
   *
   * @param dataConfig     The write config for the data table
   * @param dataMetaClient The meta client for the data table
   * @param numFiles       The total number of files in the commit
   */
  private void benchmarkDataSkipping(
      HoodieWriteConfig dataConfig,
      HoodieTableMetaClient dataMetaClient,
      int numFiles) {

    LOG.info("Running data skipping benchmark");
    dataMetaClient = HoodieTableMetaClient.reload(dataMetaClient);

    HoodieFileIndex fileIndex = createHoodieFileIndex(dataConfig, dataMetaClient);
    StructType dataSchema = getDataSchema();

    Seq<Expression> dataFiltersSeq = JavaConverters
        .asScalaBuffer(buildDataFilters(dataSchema)).toList();
    Seq<Expression> partitionFiltersSeq = JavaConverters
        .asScalaBuffer(Collections.singletonList(buildPartitionFilter(dataSchema))).toList();

    long startTime = System.currentTimeMillis();
    Seq<Tuple2<scala.Option<BaseHoodieTableFileIndex.PartitionPath>, Seq<FileSlice>>> filteredSlices =
        fileIndex.filterFileSlices(dataFiltersSeq, partitionFiltersSeq, false);
    long filterTimeMs = System.currentTimeMillis() - startTime;

    int totalFileSlices = countFileSlices(filteredSlices);

    LOG.info("filterFileSlices took {} ms", filterTimeMs);
    LOG.info("File slices returned: {} / {}", totalFileSlices, numFiles);
    if (numFiles > 0) {
      double skippingRatio = ((double) (numFiles - totalFileSlices) / numFiles) * 100.0;
      LOG.info(String.format("Data skipping ratio: %.2f%%", skippingRatio));
    }
  }

  /**
   * Creates a HoodieFileIndex configured for data skipping with column stats.
   */
  @SuppressWarnings("deprecation")
  private HoodieFileIndex createHoodieFileIndex(HoodieWriteConfig dataConfig, HoodieTableMetaClient metaClient) {
    Map<String, String> options = new HashMap<>();
    options.put("path", dataConfig.getBasePath());
    options.put("hoodie.datasource.read.data.skipping.enable", "true");
    options.put("hoodie.metadata.enable", "true");
    options.put("hoodie.metadata.index.column.stats.enable", "true");
    options.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key(), "false");
    options.put("hoodie.metadata.index.column.stats.column.list", getColumnsToIndexString(cfg.numColumnsToIndex));
    spark.sqlContext().conf().setConfString("hoodie.fileIndex.dataSkippingFailureMode", "strict");

    scala.collection.immutable.Map<String, String> scalaOptions = JavaConverters.mapAsScalaMap(options)
        .toMap(scala.Predef$.MODULE$.conforms());

    return new HoodieFileIndex(
        spark,
        metaClient,
        scala.Option.apply(getDataSchema()),
        scalaOptions,
        NoopCache$.MODULE$,
        false,
        false);
  }

  /**
   * Builds a data filter expression based on the indexed columns.
   */
  private List<Expression> buildDataFilters(StructType dataSchema) {
    final List<String> filterStrings;

    if (StringUtils.nonEmpty(cfg.dataFilters)) {
      filterStrings = Arrays.stream(Strings.split(cfg.dataFilters, ','))
          .map(String::trim)
          .filter(StringUtils::nonEmpty)
          .collect(Collectors.toList());
    } else {
      filterStrings = getDefaultDataFilters();
    }

    LOG.info("Using data filters: {}", filterStrings);

    return filterStrings.stream()
        .map(filter ->
            HoodieCatalystExpressionUtils$.MODULE$.resolveExpr(spark, filter, dataSchema))
        .collect(Collectors.toList());
  }

  private List<String> getDefaultDataFilters() {
    if (cfg.numColumnsToIndex == 1) {
      return Collections.singletonList(COL_AGE + " > 70");
    }
    return Arrays.asList(
        COL_AGE + " > 70",
        COL_TENANT_ID + " > 50000"
    );
  }

  /**
   * Builds a partition filter expression based on the configured filter percentage.
   */
  private Expression buildPartitionFilter(StructType dataSchema) {
    String partitionFilter = getPartitionFilter();
    LOG.info("Using partition filter: {}", partitionFilter);
    return HoodieCatalystExpressionUtils$.MODULE$.resolveExpr(spark, partitionFilter, dataSchema);
  }

  /**
   * Counts total file slices across all partitions.
   */
  private int countFileSlices(Seq<Tuple2<scala.Option<BaseHoodieTableFileIndex.PartitionPath>, Seq<FileSlice>>> filteredSlices) {
    int total = 0;
    for (int j = 0; j < filteredSlices.size(); j++) {
      total += filteredSlices.apply(j)._2().size();
    }
    return total;
  }

  /**
   * Generates column stats records based on commit metadata file structure in a distributed manner.
   * This method distributes work by table partition - each Spark partition processes
   * all files within a single table partition to avoid memory issues.
   *
   * @param commitMetadata The commit metadata containing partition and file information
   * @return HoodieData of column stats records, distributed across Spark partitions
   */
  @SuppressWarnings("rawtypes")
  private HoodieData<HoodieRecord> generateColumnStatsRecordsForCommitMetadata(
      HoodieCommitMetadata commitMetadata) {

    Map<String, List<HoodieWriteStat>> partitionToWriteStats = commitMetadata.getPartitionToWriteStats();
    List<Map.Entry<String, List<HoodieWriteStat>>> partitionEntries = new ArrayList<>(partitionToWriteStats.entrySet());

    LOG.info("Processing {} partitions with {} total files for column stats generation",
        partitionEntries.size(),
        partitionToWriteStats.values().stream().mapToInt(List::size).sum());

    final int numColumnsToIndex = cfg.numColumnsToIndex;

    JavaRDD<HoodieRecord> recordsRDD = jsc
        .parallelize(partitionEntries, partitionEntries.size())
        .flatMap(entry -> processPartitionWriteStats(entry.getKey(), entry.getValue(), numColumnsToIndex).iterator());

    return HoodieJavaRDD.of(recordsRDD);
  }

  /**
   * Processes all write stats for a partition and generates column stats records.
   */
  @SuppressWarnings("unchecked")
  private static List<HoodieRecord> processPartitionWriteStats(
      String partitionPath, List<HoodieWriteStat> writeStats, int numColumnsToIndex) {

    List<HoodieRecord> partitionRecords = new ArrayList<>();

    for (HoodieWriteStat writeStat : writeStats) {
      String fileName = new StoragePath(writeStat.getPath()).getName();
      List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadata =
          generateColumnRangeMetadataForFile(fileName, numColumnsToIndex);

      List<HoodieRecord<HoodieMetadataPayload>> fileRecords = HoodieMetadataPayload
          .createColumnStatsRecords(partitionPath, columnRangeMetadata, false)
          .map(record -> (HoodieRecord<HoodieMetadataPayload>) record)
          .collect(Collectors.toList());

      partitionRecords.addAll(fileRecords);
    }

    return partitionRecords;
  }

  /**
   * Generates column range metadata for a single file.
   */
  private static List<HoodieColumnRangeMetadata<Comparable>> generateColumnRangeMetadataForFile(
      String fileName, int numColumnsToIndex) {

    Random fileRandom = new Random(fileName.hashCode());
    List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadata = new ArrayList<>();

    columnRangeMetadata.add(createTenantIDStats(fileName, fileRandom));

    if (numColumnsToIndex == 2) {
      columnRangeMetadata.add(createAgeStats(fileName, fileRandom));
    }

    return columnRangeMetadata;
  }

  /**
   * Creates column stats for tenantID column with random values in range 30000-60000.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static HoodieColumnRangeMetadata<Comparable> createTenantIDStats(String fileName, Random random) {
    long minTenantID = TENANT_ID_MIN_BASE + random.nextInt(TENANT_ID_RANGE);
    long maxTenantID = minTenantID + random.nextInt((int) (TENANT_ID_MAX - minTenantID + 1));

    return (HoodieColumnRangeMetadata<Comparable>) (HoodieColumnRangeMetadata<?>)
        HoodieColumnRangeMetadata.create(
            fileName, COL_TENANT_ID, minTenantID, maxTenantID,
            COL_STATS_NULL_COUNT, COL_STATS_VALUE_COUNT,
            COL_STATS_TOTAL_SIZE, COL_STATS_TOTAL_UNCOMPRESSED_SIZE,
            ValueMetadata.V1EmptyMetadata.get());
  }

  /**
   * Creates column stats for age column with random values in range 20-99.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static HoodieColumnRangeMetadata<Comparable> createAgeStats(String fileName, Random random) {
    int minAge = AGE_MIN_BASE + random.nextInt(AGE_MIN_RANGE);
    int maxAge = minAge + random.nextInt(AGE_MAX_RANGE);

    return (HoodieColumnRangeMetadata<Comparable>) (HoodieColumnRangeMetadata<?>)
        HoodieColumnRangeMetadata.create(
            fileName, COL_AGE, minAge, maxAge,
            COL_STATS_NULL_COUNT, COL_STATS_VALUE_COUNT,
            COL_STATS_TOTAL_SIZE, COL_STATS_TOTAL_UNCOMPRESSED_SIZE,
            ValueMetadata.V1EmptyMetadata.get());
  }

  private HoodieWriteConfig getWriteConfig(String schemaStr, String basePath, HoodieFailedWritesCleaningPolicy cleaningPolicy) {
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withProperties(props)
        .forTable(TABLE_NAME)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMetadataIndexColumnStats(true)
            .withMetadataIndexColumnStatsFileGroupCount(cfg.colStatsFileGroupCount)
            .withMetadataIndexPartitionStats(false)
            .build());
    if (StringUtils.nonEmpty(schemaStr)) {
      builder.withSchema(schemaStr);
    }
    builder.withEngineType(EngineType.SPARK);
    return builder.build();
  }

  /**
   * Returns the partition filter predicate from configuration.
   *
   * @return Partition filter expression string
   */
  private String getPartitionFilter() {
    return cfg.partitionFilter;
  }

  public void close() {
    engineContext.cancelAllJobs();
  }
}
