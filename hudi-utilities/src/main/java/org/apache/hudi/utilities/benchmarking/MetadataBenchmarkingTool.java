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
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.stats.ValueMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;

public class MetadataBenchmarkingTool implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataBenchmarkingTool.class);

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
    List<String> columns = new ArrayList<>();
    columns.add("tenantID");
    if (numColumnsToIndex == 2) {
      columns.add("age");
    }
    return columns;
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
    @Parameter(names = {"--table-base-path", "-tbp"}, description = "Base path for the Hudi table", required = true)
    public String tableBasePath = null;

    @Parameter(names = {"--num-cols-to-index", "-num-cols"}, description = "Number of columns to index (1 for tenantID, 2 for tenantID & age)", required = true)
    public Integer numColumnsToIndex = 1;

    @Parameter(names = {"--col-stats-file-group-count", "-col-fg-count"}, description = "Number of file groups for column stats partition in metadata table", required = true)
    public Integer colStatsFileGroupCount = 10;

    @Parameter(names = {"--num-files", "-nf"}, description = "Number of files to create in the table", required = true)
    public Integer numFiles = 1000;

    @Parameter(names = {"--num-partitions", "-np"}, description = "Number of partitions to create in the table", required = true)
    public Integer numPartitions = 1;

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
          + "   --num-cols-to-index " + numColumnsToIndex + ", \n"
          + "   --col-stats-file-group-count " + colStatsFileGroupCount + ", \n"
          + "\n}";
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
    int numFiles = cfg.numFiles;
    int numPartitions = cfg.numPartitions;
    List<String> colsToIndex = getColumnsToIndex(cfg.numColumnsToIndex);
    LOG.info("Data table base path: {}", cfg.tableBasePath);

    String tableName = "test_mdt_stats_tbl";
    HoodieWriteConfig dataWriteConfig = getWriteConfig(tableName, getAvroSchema(), cfg.tableBasePath, HoodieFailedWritesCleaningPolicy.EAGER);
    HoodieTableMetaClient dataMetaClient = initializeDataTableMetaClient(tableName, dataWriteConfig);

    int totalFilesCreated = writeToMetadataTable(numFiles, numPartitions, colsToIndex, dataWriteConfig, dataMetaClient);
    System.out.println("Completed bootstrapping Metadata table");

    queryAndVerifyColumnStats(dataWriteConfig, dataMetaClient, Collections.emptyMap(), totalFilesCreated);

    System.out.println("Completed query benchmarking");
  }

  private int writeToMetadataTable(int numFiles, int numPartitions, List<String> colsToIndex, HoodieWriteConfig dataWriteConfig, HoodieTableMetaClient dataTableMetaClient) throws Exception {
    LOG.info("Starting Metadata table benchmarking with {} files, {} partitions, {} columns ({}), {} file groups",
        numFiles, numPartitions, cfg.numColumnsToIndex, String.join(",", colsToIndex), cfg.colStatsFileGroupCount);
    List<String> partitions = generatePartitions(numPartitions);
    HoodieTestTable testTable = HoodieTestTable.of(dataTableMetaClient);
    try (SparkHoodieBackedTableMetadataBenchmarkWriter metadataWriter = new SparkHoodieBackedTableMetadataBenchmarkWriter(
        engineContext.getStorageConf(), dataWriteConfig, HoodieFailedWritesCleaningPolicy.EAGER, engineContext, Option.empty(), false)) {

      int filesPerPartition = numFiles / numPartitions;
      String dataCommitTime = InProcessTimeGenerator.createNewInstantTime();
      // Create commit metadata
      HoodieCommitMetadata commitMetadata = testTable.createCommitMetadata(dataCommitTime, WriteOperationType.INSERT, partitions,
          filesPerPartition, false);
      // Add commit to timeline
      testTable.addCommit(dataCommitTime, Option.of(commitMetadata));
      LOG.info("Created commit metadata at instant {}", dataCommitTime);

      metadataWriter.initMetadataMetaClient();

      // Convert commit metadata to files partition records and commit
      @SuppressWarnings("unchecked")
      List<HoodieRecord> filesRecords = HoodieTableMetadataUtil.convertMetadataToFilesPartitionRecords(commitMetadata, dataCommitTime);
      System.out.println("Count of files created in the metadata table " + filesRecords.size());

      String instantTimeForFilesPartition = generateUniqueInstantTime(0);
      Pair<Integer, HoodieData<HoodieRecord>> fileGroupCountAndRecordsPair = Pair.of(1, engineContext.parallelize(filesRecords, 10));
      metadataWriter.initializeFilegroupsAndCommit(FILES, FILES.getPartitionPath(), fileGroupCountAndRecordsPair, instantTimeForFilesPartition);

      // Generate column stats records
      @SuppressWarnings("rawtypes")
      HoodieData<HoodieRecord> columnStatsRecords = generateColumnStatsRecordsForCommitMetadata(
          commitMetadata);
      String instantTimeForColStatsPartition = generateUniqueInstantTime(1);
      Pair<Integer, HoodieData<HoodieRecord>> colStatsFileGroupCountAndRecordsPair = Pair.of(cfg.colStatsFileGroupCount, columnStatsRecords);
      metadataWriter.initializeFilegroupsAndCommit(COLUMN_STATS, COLUMN_STATS.getPartitionPath(), colStatsFileGroupCountAndRecordsPair, instantTimeForColStatsPartition, colsToIndex);
      return filesRecords.size();
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
    LocalDate startDate = LocalDate.of(2020, 1, 1);
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    for (int i = 0; i < numPartitions; i++) {
      LocalDate partitionDate = startDate.plusDays(i);
      String partitionPath = partitionDate.format(formatter);
      partitions.add(partitionPath);
    }

    LOG.info("Generated {} partitions from {} to {}",
        numPartitions,
        partitions.get(0),
        partitions.get(partitions.size() - 1));

    return partitions;
  }

  /**
   * Query the column stats index using HoodieFileIndex.filterFileSlices and verify results.
   *
   * @param dataConfig     The write config for the data table
   * @param dataMetaClient The meta client for the data table
   * @param expectedStats  The expected column stats for verification
   * @param numFiles       The total number of files in the commit
   */
  @SuppressWarnings("rawtypes")
  private void queryAndVerifyColumnStats(
      HoodieWriteConfig dataConfig,
      HoodieTableMetaClient dataMetaClient,
      Map<String, Map<String, HoodieColumnRangeMetadata<Comparable>>> expectedStats,
      int numFiles) {

    LOG.info("=== STEP 5: Querying column stats index using HoodieFileIndex ===");
    dataMetaClient = HoodieTableMetaClient.reload(dataMetaClient);

    // Create HoodieFileIndex
    Map<String, String> options = new HashMap<>();
    options.put("path", dataConfig.getBasePath());
    options.put("hoodie.datasource.read.data.skipping.enable", "true");
    options.put("hoodie.metadata.enable", "true");
    options.put("hoodie.metadata.index.column.stats.enable", "true");
    options.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key(), "false");
    // Also ensure the columns are specified for column stats
    String columnsToIndexStr = getColumnsToIndexString(cfg.numColumnsToIndex);
    options.put("hoodie.metadata.index.column.stats.column.list", columnsToIndexStr);
    spark.sqlContext().conf().setConfString("hoodie.fileIndex.dataSkippingFailureMode", "strict");

    // Use consolidated schema helper method
    StructType dataSchema = getDataSchema();
    scala.Option<StructType> schemaOption = scala.Option.apply(dataSchema);

    @SuppressWarnings("deprecation")
    scala.collection.immutable.Map<String, String> scalaOptions = JavaConverters.mapAsScalaMap(options)
        .toMap(scala.Predef$.MODULE$.<scala.Tuple2<String, String>>conforms());

    HoodieFileIndex fileIndex = new HoodieFileIndex(
        spark,
        dataMetaClient,
        schemaOption,
        scalaOptions,
        NoopCache$.MODULE$,
        false,
        false);

    // Create data filters based on columns to index
    // Unresolved expressions cause translateIntoColumnStatsIndexFilterExpr to return TrueLiteral (no filtering).
    List<Expression> dataFilters = new ArrayList<>();
    String filterString;
    if (cfg.numColumnsToIndex == 2) {
      filterString = "age > 70";
    } else {
      filterString = "tenantID > 50000"; // More selective filter for 30k-60k range
    }
    Expression filter1 = HoodieCatalystExpressionUtils$.MODULE$
        .resolveExpr(spark, filterString, dataSchema);
    LOG.info("Using filter: {}", filterString);
    LOG.info("DEBUG: Resolved filter expression: {}", filter1);
    LOG.info("DEBUG: Resolved filter resolved: {}", filter1.resolved());
    LOG.info("DEBUG: Resolved filter tree:\n{}", filter1.treeString());

    dataFilters.add(filter1);

    String partitionFilter = getPartitionFilter();
    Expression partitionFilter1 = HoodieCatalystExpressionUtils$.MODULE$
        .resolveExpr(spark, partitionFilter, dataSchema);

    List<Expression> partitionFilters = Collections.singletonList(partitionFilter1);

    // Convert to Scala Seq
    scala.collection.immutable.List<Expression> dataFiltersList = JavaConverters.asScalaBuffer(dataFilters)
        .toList();
    scala.collection.Seq<Expression> dataFiltersSeq = dataFiltersList;
    scala.collection.immutable.List<Expression> partitionFiltersList = JavaConverters
        .asScalaBuffer(partitionFilters).toList();
    scala.collection.Seq<Expression> partitionFiltersSeq = partitionFiltersList;

    // Call filterFileSlices with timing
    long startTime = System.currentTimeMillis();
    scala.collection.Seq<scala.Tuple2<scala.Option<BaseHoodieTableFileIndex.PartitionPath>,
        scala.collection.Seq<FileSlice>>> filteredSlices = fileIndex
        .filterFileSlices(
            dataFiltersSeq,
            partitionFiltersSeq,
            false);
    long endTime = System.currentTimeMillis();
    long filterTimeMs = endTime - startTime;
    LOG.info("=== filterFileSlices benchmark: {} ms ===", filterTimeMs);

    LOG.info("Filtered File Slices Min/Max Values:");
    String headerFormat;
    String header;
    if (cfg.numColumnsToIndex == 2) {
      headerFormat = "%-30s %-20s %-20s %-20s %-20s";
      header = String.format(headerFormat, "FileName", "tenantID_min", "tenantID_max", "age_min", "age_max");
    } else {
      headerFormat = "%-30s %-20s %-20s";
      header = String.format(headerFormat, "FileName", "tenantID_min", "tenantID_max");
    }
    LOG.info(header);
    LOG.info(String.join("", Collections.nCopies(100, "-")));

    int totalFileSlices = 0;
    for (int j = 0; j < filteredSlices.size(); j++) {
      scala.Tuple2<scala.Option<BaseHoodieTableFileIndex.PartitionPath>,
          scala.collection.Seq<FileSlice>> tuple = filteredSlices.apply(j);
      scala.collection.Seq<FileSlice> fileSliceSeq = tuple._2();
      totalFileSlices += fileSliceSeq.size();

      for (int k = 0; k < fileSliceSeq.size(); k++) {
        FileSlice fileSlice = fileSliceSeq.apply(k);
        String fileName = fileSlice.getBaseFile().get().getFileName();

        Map<String, HoodieColumnRangeMetadata<Comparable>> fileExpectedStats = expectedStats.get(fileName);
        if (fileExpectedStats != null) {
          HoodieColumnRangeMetadata<Comparable> tenantIDStats = fileExpectedStats.get("tenantID");
          Object tenantIDMin = (tenantIDStats != null) ? tenantIDStats.getMinValue() : "null";
          Object tenantIDMax = (tenantIDStats != null) ? tenantIDStats.getMaxValue() : "null";
          
          if (cfg.numColumnsToIndex == 2) {
            HoodieColumnRangeMetadata<Comparable> ageStats = fileExpectedStats.get("age");
            Object ageMin = (ageStats != null) ? ageStats.getMinValue() : "null";
            Object ageMax = (ageStats != null) ? ageStats.getMaxValue() : "null";
            LOG.info(String.format("%-30s %-20s %-20s %-20s %-20s",
                fileName, tenantIDMin.toString(), tenantIDMax.toString(), ageMin.toString(),
                ageMax.toString()));
          } else {
            LOG.info(String.format("%-30s %-20s %-20s",
                fileName, tenantIDMin.toString(), tenantIDMax.toString()));
          }
        }
      }
    }

    LOG.info(String.join("", Collections.nCopies(100, "-")));
    LOG.info("Total file slices returned: {}", totalFileSlices);
    LOG.info("Total files in commit: {}", numFiles);

    if (numFiles > 0) {
      double skippingRatio = ((double) (numFiles - totalFileSlices) / numFiles) * 100.0;
      LOG.info(String.format("Data skipping ratio: %.2f%%", skippingRatio));
    }
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

    // Extract partition to writeStats mapping
    Map<String, List<HoodieWriteStat>> partitionToWriteStats = commitMetadata.getPartitionToWriteStats();

    // Create a list of partition entries to distribute
    // This only holds metadata (partition paths and file references), not actual records
    List<Map.Entry<String, List<HoodieWriteStat>>> partitionEntries =
        new ArrayList<>(partitionToWriteStats.entrySet());

    LOG.info("Processing {} table partitions for column stats generation with total {} files",
        partitionEntries.size(),
        partitionToWriteStats.values().stream().mapToInt(List::size).sum());

    // Capture config for serialization into the closure
    final int numColumnsToIndex = cfg.numColumnsToIndex;

    // Parallelize by table partition - each Spark partition processes one table partition
    // Use number of table partitions as the parallelism level
    JavaRDD<Map.Entry<String, List<HoodieWriteStat>>> partitionsRDD =
        jsc.parallelize(partitionEntries, partitionEntries.size());

    // Process each table partition separately on executors
    // Each Spark task generates column stats records for ALL files in its assigned table partition
    JavaRDD<HoodieRecord> recordsRDD = partitionsRDD.flatMap(
        partitionEntry -> {
          String partitionPath = partitionEntry.getKey();
          List<HoodieWriteStat> writeStats = partitionEntry.getValue();

          // Process all files in this table partition
          // Records are accumulated locally on the executor, not in driver memory
          List<HoodieRecord> partitionRecords = new ArrayList<>();

          for (HoodieWriteStat writeStat : writeStats) {
            String filePath = writeStat.getPath();
            String fileName = new StoragePath(filePath).getName();

            // Use fileName hash as seed for deterministic but unique random values per file
            Random fileRandom = new Random(fileName.hashCode());

            List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadata = new ArrayList<>();

            // Generate stats for tenantID (always first)
            // tenantID range: 30000-60000 (30k-60k)
            long minTenantID = 30000L + fileRandom.nextInt(30000); // Range: 30000-59999
            long maxTenantID = minTenantID + fileRandom.nextInt((int)(60000L - minTenantID + 1)); // Range: minTenantID to 60000
            @SuppressWarnings({"rawtypes", "unchecked"})
            HoodieColumnRangeMetadata<Comparable> tenantIDStats = (HoodieColumnRangeMetadata<Comparable>) 
                (HoodieColumnRangeMetadata<?>) HoodieColumnRangeMetadata.create(
                    fileName,
                    "tenantID",
                    minTenantID,
                    maxTenantID,
                    0,
                    1000,
                    123456,
                    123456,
                    ValueMetadata.V1EmptyMetadata.get());
            columnRangeMetadata.add(tenantIDStats);

            // Generate stats for age if numColumnsToIndex == 2
            if (numColumnsToIndex == 2) {
              int minAge = 20 + fileRandom.nextInt(30);
              int maxAge = minAge + fileRandom.nextInt(50); // Range: minAge to (minAge + 49)
              @SuppressWarnings({"rawtypes", "unchecked"})
              HoodieColumnRangeMetadata<Comparable> ageStats = (HoodieColumnRangeMetadata<Comparable>) 
                  (HoodieColumnRangeMetadata<?>) HoodieColumnRangeMetadata.create(
                      fileName,
                      "age",
                      minAge,
                      maxAge,
                      0,
                      1000,
                      123456,
                      123456,
                      ValueMetadata.V1EmptyMetadata.get());
              columnRangeMetadata.add(ageStats);
            }

            // Generate column stats records for this file
            // These records are added to the partition's local list on the executor
            @SuppressWarnings("unchecked")
            List<HoodieRecord<HoodieMetadataPayload>> fileRecords = HoodieMetadataPayload
                .createColumnStatsRecords(partitionPath, columnRangeMetadata, false)
                .map(record -> (HoodieRecord<HoodieMetadataPayload>) record)
                .collect(Collectors.toList());

            partitionRecords.addAll(fileRecords);
          }

          // Return all records for this table partition as an iterator
          return partitionRecords.iterator();
        }
    );

    // Return as HoodieData - records stay distributed across Spark partitions
    return HoodieJavaRDD.of(recordsRDD);
  }

  private HoodieWriteConfig getWriteConfig(String tableName, String schemaStr, String basePath, HoodieFailedWritesCleaningPolicy cleaningPolicy) {
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withProperties(props)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(cleaningPolicy).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).orcMaxFileSize(1024 * 1024).build())
        .forTable(tableName)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withEnableBackupForRemoteFileSystemView(false) // Fail test if problem connecting to timeline-server
            .withRemoteServerPort(FileSystemViewStorageConfig.REMOTE_PORT_NUM.defaultValue()).build())
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
   * Generates a partition filter expression to filter out approximately 25% of the data.
   * Uses the partition field 'dt' with a greater-than comparison.
   *
   * Since partitions are generated as consecutive dates starting from 2020-01-01,
   * this method calculates the date threshold that filters out the first ~25% of partitions.
   *
   * Example:
   * numPartitions = 4   -> "dt > '2020-01-01'" (filters out 1 partition, keeps 3 = 75%)
   * numPartitions = 10  -> "dt > '2020-01-03'" (filters out 3 partitions, keeps 7 = 70%)
   * numPartitions = 100 -> "dt > '2020-01-25'" (filters out 25 partitions, keeps 75 = 75%)
   * numPartitions = 1   -> "" (no filtering possible for single partition)
   *
   * @param numPartitions Number of partitions in the table
   * @return Filter expression string or empty string if filtering is not applicable
   */
  private String getPartitionFilter(int numPartitions) {
    // Can't filter meaningfully with only 1 partition
    if (numPartitions <= 1) {
      return "";
    }

    // Calculate number of partitions to exclude (approximately 25%)
    int numPartitionsToExclude = (int) Math.ceil(numPartitions * 0.25);

    // Ensure we exclude at least 1 partition
    numPartitionsToExclude = Math.max(1, numPartitionsToExclude);

    // The cutoff date is the last date to exclude (0-indexed: numPartitionsToExclude - 1)
    LocalDate startDate = LocalDate.of(2020, 1, 1);
    LocalDate cutoffDate = startDate.plusDays(numPartitionsToExclude - 1);

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    String cutoffDateStr = cutoffDate.format(formatter);

    return "dt > '" + cutoffDateStr + "'";
  }

  /**
   * Generates a partition filter using the configured number of partitions.
   * @return Filter expression string or empty string if filtering is not applicable
   */
  private String getPartitionFilter() {
    return getPartitionFilter(cfg.numPartitions);
  }

  public void close() {
    engineContext.cancelAllJobs();
  }
}

