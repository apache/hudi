package org.apache.hudi.utilities;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
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
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
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
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.MetadataWriterTestUtils;
import org.apache.hudi.metadata.SparkMetadataWriterFactory;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.stats.ValueMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.execution.datasources.NoopCache$;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
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
import java.util.UUID;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

public class HoodieMDTStats  implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMDTStats.class);

  private static final int INITIAL_ROW_COUNT = 50; // Rows to insert in STEP 1

  private final Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;

  private final SparkSession spark;

  private final JavaSparkContext jsc;

  private final HoodieEngineContext engineContext;

  private static final String avroSchema = "{\n" +
      "  \"type\": \"record\",\n" +
      "  \"name\": \"Employee\",\n" +
      "  \"namespace\": \"com.example.avro\",\n" +
      "  \"fields\": [\n" +
      "    { \"name\": \"id\", \"type\": \"string\" },\n" +
      "    { \"name\": \"name\", \"type\": \"string\" },\n" +
      "    { \"name\": \"city\", \"type\": \"string\" },\n" +
      "    { \"name\": \"age\", \"type\": \"int\" },\n" +
      "    { \"name\": \"salary\", \"type\": \"double\" },\n" +
      "    { \"name\": \"dt\", \"type\": \"string\" }\n" +
      "  ]\n" +
      "}\n";

  public HoodieMDTStats(SparkSession spark, Config cfg) {
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
    @Parameter(names = {"--table-base-path", "-tbp"}, description = "Number of columns to index", required = true)
    public String tableBasePath = null;

    @Parameter(names = {"--cols-to-index", "-num-cols"}, description = "Number of columns to index", required = true)
    public String colsToIndex = "age,salary";

    @Parameter(names = {"--col-stats-file-group-count", "-col-fg-count"}, description = "Target Base path for the table", required = true)
    public Integer colStatsFileGroupCount = 10;

    @Parameter(names = {"--num-files", "-nf"}, description = "Target Base path for the table", required = true)
    public Integer numFiles = 1000;

    @Parameter(names = {"--num-partitions", "-np"}, description = "Target Base path for the table", required = true)
    public Integer numPartitions = 1;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for clustering")
    public String propsFilePath = null;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "TableSizeStats {\n"
          + "   --col-to-index " + colsToIndex + ", \n"
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


    try (HoodieMDTStats hoodieMDTStats = new HoodieMDTStats(spark, cfg)) {
      hoodieMDTStats.run();
    }
    catch (Throwable throwable) {
      LOG.error("Failed to get table size stats for " + cfg, throwable);
    } finally {
      spark.stop();
    }
  }

  public void run() throws Exception {
    int numFiles = cfg.numFiles;
    int numPartitions = cfg.numPartitions;

    LOG.info("Starting MDT stats test with {} files, {} partitions, {} columns, {} file groups",
        numFiles, numPartitions, cfg.colsToIndex, cfg.colStatsFileGroupCount);
    LOG.info("Data table base path: {}", cfg.tableBasePath);
    String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(cfg.tableBasePath);
    LOG.info("Metadata table base path: {}", metadataTableBasePath);

    String tableName = "test_mdt_stats_tbl";
    initializeTableWithSampleData(tableName);
    // Create data table config with metadata enabled
    HoodieWriteConfig dataWriteConfig = getWriteConfig(tableName, avroSchema, cfg.tableBasePath, HoodieFailedWritesCleaningPolicy.EAGER);
    HoodieMetadataConfig metadataConfig = dataWriteConfig.getMetadataConfig();
    HoodieWriteConfig dataConfig = HoodieWriteConfig.newBuilder()
        .withProperties(dataWriteConfig.getProps())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .fromProperties(metadataConfig.getProps())
            .enable(true)
            .withMetadataIndexColumnStats(true)
            .withMetadataIndexColumnStatsFileGroupCount(cfg.colStatsFileGroupCount)
            .build())
        .build();

    HoodieTableMetaClient dataMetaClient = HoodieTableMetaClient.builder()
        .setBasePath(dataConfig.getBasePath())
        .setConf(engineContext.getStorageConf().newInstance())
        .build();

    // STEP 1: Insert 50 rows with age and salary columns to initialize table schema
    // and metadata table
    dataMetaClient = HoodieTableMetaClient.reload(dataMetaClient);

    // STEP 2: Create commit metadata using HoodieTestTable without writing data
    // files
    String dataCommitTime = InProcessTimeGenerator.createNewInstantTime();
    List<String> partitions = generatePartitions(cfg.numPartitions);
    int filesPerPartition = numFiles / numPartitions; // Evenly distribute files
    HoodieTestTable testTable = HoodieTestTable.of(dataMetaClient);
    HoodieCommitMetadata commitMetadata = testTable.createCommitMetadata(
        dataCommitTime,
        WriteOperationType.INSERT,
        partitions,
        filesPerPartition,
        false); // bootstrap

    // Add commit to timeline using HoodieTestTable
    testTable.addCommit(dataCommitTime, Option.of(commitMetadata));
    LOG.info("Created commit metadata with {} files per partition at instant {}", filesPerPartition, dataCommitTime);
    // Create actual empty parquet files on disk so filesystem listing finds them
    createEmptyParquetFiles(dataMetaClient, commitMetadata);
    LOG.info("Created {} empty parquet files on disk", numFiles);
    dataMetaClient = HoodieTableMetaClient.reload(dataMetaClient);

    HoodieWriteConfig mdtConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        dataConfig,
        HoodieFailedWritesCleaningPolicy.EAGER,
        HoodieTableVersion.NINE);

    // STEP 3: Write both /files and /column_stats partitions to metadata table in a single commit
    @SuppressWarnings("rawtypes")
    Map<String, Map<String, HoodieColumnRangeMetadata<Comparable>>> expectedStats =
        writeFilesAndColumnStatsToMetadataTable(dataConfig, dataMetaClient, commitMetadata, dataCommitTime, mdtConfig);

    // STEP 4: Print column stats for verification (up to 10 files per partition)
    printColumnStatsForVerification(commitMetadata, expectedStats);

    // STEP 5: Use HoodieFileIndex.filterFileSlices to query and verify
    queryAndVerifyColumnStats(dataConfig, dataMetaClient, expectedStats, numFiles);
  }

  /**
   * Generates a list of date-based partition paths incrementing by day.
   * Starting from 2020-01-01, creates partitions for consecutive days based on numPartitions.
   *
   * Example:
   *   numPartitions = 1  -> ["2020-01-01"]
   *   numPartitions = 3  -> ["2020-01-01", "2020-01-02", "2020-01-03"]
   *   numPartitions = 10 -> ["2020-01-01", "2020-01-02", ..., "2020-01-10"]
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
   * Print column stats for verification - shows min/max values for up to 10 files per partition.
   * This helps verify that column stats were constructed properly before querying.
   *
   * @param commitMetadata The commit metadata containing partition and file information
   * @param expectedStats The expected column stats map (file name -> column name -> stats)
   */
  @SuppressWarnings("rawtypes")
  private void printColumnStatsForVerification(
      HoodieCommitMetadata commitMetadata,
      Map<String, Map<String, HoodieColumnRangeMetadata<Comparable>>> expectedStats) {

    LOG.info("=== STEP 4: Verifying column stats construction (max 10 files per partition) ===");

    Map<String, List<HoodieWriteStat>> partitionToWriteStats = commitMetadata.getPartitionToWriteStats();

    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      String partitionPath = entry.getKey();
      List<HoodieWriteStat> writeStats = entry.getValue();

      LOG.info("Partition: {} ({} files total)", partitionPath, writeStats.size());
      LOG.info(String.format("%-50s %-15s %-15s %-15s %-15s",
          "FileName", "age_min", "age_max", "salary_min", "salary_max"));
      LOG.info(String.join("", Collections.nCopies(110, "-")));

      int filesDisplayed = 0;
      for (HoodieWriteStat writeStat : writeStats) {
        if (filesDisplayed >= 10) {
          LOG.info("... and {} more files", writeStats.size() - 10);
          break;
        }

        String filePath = writeStat.getPath();
        String fileName = new StoragePath(filePath).getName();

        Map<String, HoodieColumnRangeMetadata<Comparable>> fileStats = expectedStats.get(fileName);
        if (fileStats != null) {
          HoodieColumnRangeMetadata<Comparable> ageStats = fileStats.get("age");
          HoodieColumnRangeMetadata<Comparable> salaryStats = fileStats.get("salary");

          String ageMin = (ageStats != null) ? String.valueOf(ageStats.getMinValue()) : "N/A";
          String ageMax = (ageStats != null) ? String.valueOf(ageStats.getMaxValue()) : "N/A";
          String salaryMin = (salaryStats != null) ? String.valueOf(salaryStats.getMinValue()) : "N/A";
          String salaryMax = (salaryStats != null) ? String.valueOf(salaryStats.getMaxValue()) : "N/A";

          LOG.info(String.format("%-50s %-15s %-15s %-15s %-15s",
              fileName.length() > 48 ? fileName.substring(0, 48) + ".." : fileName,
              ageMin, ageMax, salaryMin, salaryMax));
        } else {
          LOG.info(String.format("%-50s %-15s", fileName, "NO STATS FOUND"));
        }

        filesDisplayed++;
      }
    }

    LOG.info("");
    LOG.info("Total files with stats: {}", expectedStats.size());
  }

  /**
   * Query the column stats index using HoodieFileIndex.filterFileSlices and verify results.
   *
   * @param dataConfig The write config for the data table
   * @param dataMetaClient The meta client for the data table
   * @param expectedStats The expected column stats for verification
   * @param numFiles The total number of files in the commit
   */
  @SuppressWarnings("rawtypes")
  private void queryAndVerifyColumnStats(
      HoodieWriteConfig dataConfig,
      HoodieTableMetaClient dataMetaClient,
      Map<String, Map<String, HoodieColumnRangeMetadata<Comparable>>> expectedStats,
      int numFiles) throws Exception {

    LOG.info("=== STEP 5: Querying column stats index using HoodieFileIndex ===");
    dataMetaClient = HoodieTableMetaClient.reload(dataMetaClient);

    // Create HoodieFileIndex
    Map<String, String> options = new HashMap<>();
    options.put("path", dataConfig.getBasePath());
    options.put("hoodie.datasource.read.data.skipping.enable", "true");
    options.put("hoodie.metadata.enable", "true");
    options.put("hoodie.metadata.index.column.stats.enable", "true");
    // Also ensure the columns are specified for column stats
    options.put("hoodie.metadata.index.column.stats.column.list", "age,salary");
    spark.sqlContext().conf().setConfString("hoodie.fileIndex.dataSkippingFailureMode", "strict");

    // Create schema with the columns used for data skipping
    StructType dataSchema = new StructType()
        .add("id", "string")
        .add("name", "string")
        .add("city", "string")
        .add("age", "int")
        .add("salary", "long");
    scala.Option<StructType> schemaOption = scala.Option.apply(dataSchema);

    @SuppressWarnings("deprecation")
    scala.collection.immutable.Map<String, String> scalaOptions = JavaConverters.mapAsScalaMap(options)
        .toMap(scala.Predef$.MODULE$.<scala.Tuple2<String, String>>conforms());

    org.apache.hudi.HoodieFileIndex fileIndex = new org.apache.hudi.HoodieFileIndex(
        spark,
        dataMetaClient,
        schemaOption,
        scalaOptions,
        NoopCache$.MODULE$,
        false,
        false);

    // Create data filters for age and salary columns
    // Unresolved expressions cause translateIntoColumnStatsIndexFilterExpr to return TrueLiteral (no filtering).
    List<Expression> dataFilters = new ArrayList<>();
    String filterString = "age > 90";
    Expression filter1 = org.apache.spark.sql.HoodieCatalystExpressionUtils$.MODULE$
        .resolveExpr(spark, filterString, dataSchema);
    LOG.info("DEBUG: Resolved filter expression: {}", filter1);
    LOG.info("DEBUG: Resolved filter resolved: {}", filter1.resolved());
    LOG.info("DEBUG: Resolved filter tree:\n{}", filter1.treeString());

    dataFilters.add(filter1);
    // Expression filter2 = org.apache.spark.sql.HoodieCatalystExpressionUtils.resolveExpr(
    //     sparkSession, "salary > 100000", dataSchema);
    // dataFilters.add(filter2);

    List<Expression> partitionFilters = new ArrayList<>(); // Empty partition filters

    // Convert to Scala Seq
    scala.collection.immutable.List<Expression> dataFiltersList = JavaConverters.asScalaBuffer(dataFilters)
        .toList();
    scala.collection.Seq<Expression> dataFiltersSeq = dataFiltersList;
    scala.collection.immutable.List<Expression> partitionFiltersList = JavaConverters
        .asScalaBuffer(partitionFilters).toList();
    scala.collection.Seq<Expression> partitionFiltersSeq = partitionFiltersList;

    // Call filterFileSlices
    scala.collection.Seq<scala.Tuple2<scala.Option<org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath>,
        scala.collection.Seq<FileSlice>>> filteredSlices = fileIndex
        .filterFileSlices(
            dataFiltersSeq,
            partitionFiltersSeq,
            false);

    // Print results
    LOG.info("");
    LOG.info("Filtered File Slices Min/Max Values:");
    LOG.info(String.format("%-30s %-20s %-20s %-20s %-20s",
        "FileName", "age_min", "age_max", "salary_min", "salary_max"));
    LOG.info(String.join("", Collections.nCopies(100, "-")));

    int totalFileSlices = 0;
    for (int j = 0; j < filteredSlices.size(); j++) {
      scala.Tuple2<scala.Option<org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath>,
          scala.collection.Seq<FileSlice>> tuple = filteredSlices.apply(j);
      scala.collection.Seq<FileSlice> fileSliceSeq = tuple._2();
      totalFileSlices += fileSliceSeq.size();

      for (int k = 0; k < fileSliceSeq.size(); k++) {
        FileSlice fileSlice = fileSliceSeq.apply(k);
        String fileName = fileSlice.getBaseFile().get().getFileName();

        Map<String, HoodieColumnRangeMetadata<Comparable>> fileExpectedStats = expectedStats.get(fileName);
        if (fileExpectedStats != null) {
          HoodieColumnRangeMetadata<Comparable> ageStats = fileExpectedStats.get("age");
          HoodieColumnRangeMetadata<Comparable> salaryStats = fileExpectedStats.get("salary");

          Object ageMin = (ageStats != null) ? ageStats.getMinValue() : "null";
          Object ageMax = (ageStats != null) ? ageStats.getMaxValue() : "null";
          Object salaryMin = (salaryStats != null) ? salaryStats.getMinValue() : "null";
          Object salaryMax = (salaryStats != null) ? salaryStats.getMaxValue() : "null";

          LOG.info(String.format("%-30s %-20s %-20s %-20s %-20s",
              fileName, ageMin.toString(), ageMax.toString(), salaryMin.toString(),
              salaryMax.toString()));
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
   * Write both /files and /column_stats partitions to metadata table in a single commit.
   * This method handles initialization of partitions if needed, tags records with location,
   * and writes them together to simulate how actual code writes metadata.
   *
   * @param dataConfig The write config for the data table
   * @param dataMetaClient The meta client for the data table
   * @param commitMetadata The commit metadata containing file information
   * @param dataCommitTime The commit time for the data table commit
   * @param mdtWriteConfig The write config for the metadata table
   * @return Map of file names to their column stats metadata for verification
   * @throws Exception if there's an error writing to the metadata table
   */
  @SuppressWarnings("rawtypes")
  private Map<String, Map<String, HoodieColumnRangeMetadata<Comparable>>> writeFilesAndColumnStatsToMetadataTable(
      HoodieWriteConfig dataConfig,
      HoodieTableMetaClient dataMetaClient,
      HoodieCommitMetadata commitMetadata,
      String dataCommitTime,
      HoodieWriteConfig mdtWriteConfig) throws Exception {

    try (HoodieTableMetadataWriter<?, ?> metadataWriter = SparkMetadataWriterFactory.create(
        engineContext.getStorageConf(),
        dataConfig,
        engineContext,
        Option.empty(),
        dataMetaClient.getTableConfig())) {

      // STEP 3a: Check if /files partition exists and initialize if needed
      String metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(cfg.tableBasePath);
      HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
          .setBasePath(metadataBasePath)
          .setConf(engineContext.getStorageConf().newInstance())
          .build();

      boolean filesPartitionExists = dataMetaClient.getTableConfig()
          .isMetadataPartitionAvailable(MetadataPartitionType.FILES);

      LOG.info("BEFORE initialization - Metadata table exists: {}, partitions: {}",
          filesPartitionExists,
          metadataMetaClient.getTableConfig().getMetadataPartitions());

      if (!filesPartitionExists) {
        // Mark partition as inflight in table config - this is required for tagRecordsWithLocation
        // to work with isInitializing=true
        dataMetaClient.getTableConfig().setMetadataPartitionsInflight(
            dataMetaClient, MetadataPartitionType.FILES);
        LOG.info("Marked /files partition as inflight for initialization");
      }

      // Also mark column_stats partition as inflight for initialization
      boolean colStatsPartitionExists = dataMetaClient.getTableConfig()
          .isMetadataPartitionAvailable(MetadataPartitionType.COLUMN_STATS);
      if (!colStatsPartitionExists) {
        dataMetaClient.getTableConfig().setMetadataPartitionsInflight(
            dataMetaClient, MetadataPartitionType.COLUMN_STATS);
        LOG.info("Marked /column_stats partition as inflight for initialization");

        // Create index definition for column stats - this tells data skipping which columns are indexed
        org.apache.hudi.common.model.HoodieIndexDefinition colStatsIndexDef =
            new org.apache.hudi.common.model.HoodieIndexDefinition.Builder()
                .withIndexName(MetadataPartitionType.COLUMN_STATS.getPartitionPath())
                .withIndexType("column_stats")
                .withSourceFields(java.util.Arrays.asList("age", "salary"))
                .build();
        dataMetaClient.buildIndexDefinition(colStatsIndexDef);
        LOG.info("Created column stats index definition for columns: age, salary");
      }

      // Convert commit metadata to files partition records
      @SuppressWarnings("unchecked")
      List<HoodieRecord<HoodieMetadataPayload>> filesRecords = (List<HoodieRecord<HoodieMetadataPayload>>) (List<?>)
          HoodieTableMetadataUtil.convertMetadataToFilesPartitionRecords(commitMetadata, dataCommitTime);

      // Generate column stats records
      @SuppressWarnings("rawtypes")
      Map<String, Map<String, HoodieColumnRangeMetadata<Comparable>>> expectedStats = new HashMap<>();
      List<HoodieRecord<HoodieMetadataPayload>> columnStatsRecords = generateColumnStatsRecordsForCommitMetadata(
          commitMetadata,
          expectedStats,
          dataCommitTime);

      LOG.info("Generated {} files records and {} column stats records", filesRecords.size(), columnStatsRecords.size());

      // STEP 3b: Tag records with location for both partitions and write them together
      // IMPORTANT: Use dataCommitTime for the metadata table commit to ensure timeline sync.
      // HoodieFileIndex expects metadata table commits to match data table commit times.
      String mdtCommitTime = dataCommitTime;
      try (SparkRDDWriteClient<HoodieMetadataPayload> mdtWriteClient = new SparkRDDWriteClient<>(engineContext, mdtWriteConfig)) {

        WriteClientTestUtils.startCommitWithTime(mdtWriteClient, mdtCommitTime);
        JavaRDD<HoodieRecord<HoodieMetadataPayload>> filesRDD = jsc.parallelize(filesRecords, 1);
        JavaRDD<HoodieRecord<HoodieMetadataPayload>> colStatsRDD = jsc.parallelize(columnStatsRecords, 1);

        @SuppressWarnings({ "unchecked", "rawtypes" })
        org.apache.hudi.metadata.HoodieBackedTableMetadataWriter<JavaRDD<HoodieRecord>, JavaRDD<WriteStatus>>
            sparkMetadataWriter
            = (org.apache.hudi.metadata.HoodieBackedTableMetadataWriter) metadataWriter;

        @SuppressWarnings({ "rawtypes", "unchecked" })
        Map<String, org.apache.hudi.common.data.HoodieData<HoodieRecord>> partitionRecordsMap = new HashMap<>();
        partitionRecordsMap.put(
            HoodieTableMetadataUtil.PARTITION_NAME_FILES,
            (org.apache.hudi.common.data.HoodieData<HoodieRecord>) (org.apache.hudi.common.data.HoodieData) HoodieJavaRDD
                .of(filesRDD));
        partitionRecordsMap.put(
            HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS,
            (org.apache.hudi.common.data.HoodieData<HoodieRecord>) (org.apache.hudi.common.data.HoodieData) HoodieJavaRDD
                .of(colStatsRDD));

        // Tag records for all partitions together - use isInitializing=true if /files partition was just marked as inflight
        @SuppressWarnings("rawtypes")
        Pair<HoodieData<HoodieRecord>, List<HoodieFileGroupId>> taggedResult =
            MetadataWriterTestUtils.tagRecordsWithLocation(
                sparkMetadataWriter,
                partitionRecordsMap,
                !filesPartitionExists // isInitializing = true if we just marked /files as inflight
            );

        // Check metadata table state after tagging
        metadataMetaClient = HoodieTableMetaClient.reload(metadataMetaClient);
        LOG.info("AFTER tagging - Metadata table exists: {}, partitions: {}",
            metadataMetaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.FILES),
            metadataMetaClient.getTableConfig().getMetadataPartitions());

        // Convert back to JavaRDD - taggedResult contains all records from all partitions
        @SuppressWarnings("unchecked")
        JavaRDD<HoodieRecord<HoodieMetadataPayload>> allTaggedRecords =
            (JavaRDD<HoodieRecord<HoodieMetadataPayload>>) (JavaRDD) HoodieJavaRDD
                .getJavaRDD(taggedResult.getKey());

        allTaggedRecords.take(3).forEach(r ->
            LOG.info("DEBUG: Record key={}, location={}", r.getRecordKey(), r.getCurrentLocation()));

        JavaRDD<WriteStatus> writeStatuses = mdtWriteClient.upsertPreppedRecords(allTaggedRecords, mdtCommitTime);
        List<WriteStatus> statusList = writeStatuses.collect();
        mdtWriteClient.commit(mdtCommitTime, jsc.parallelize(statusList, 1), Option.empty(), HoodieTimeline.DELTA_COMMIT_ACTION, Collections.emptyMap());

        // Mark partition as completed if we initialized it
        if (!filesPartitionExists) {
          dataMetaClient.getTableConfig().setMetadataPartitionState(
              dataMetaClient, MetadataPartitionType.FILES.getPartitionPath(), true);
          LOG.info("Marked /files partition as completed");
        }

        if (!colStatsPartitionExists) {
          dataMetaClient.getTableConfig().setMetadataPartitionState(
              dataMetaClient, MetadataPartitionType.COLUMN_STATS.getPartitionPath(), true);
          LOG.info("Marked /column_stats partition as completed");
        }

        // Verify final state
        dataMetaClient = HoodieTableMetaClient.reload(dataMetaClient);
        LOG.info("AFTER commit - Metadata table exists: {}, partitions: {}",
            dataMetaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.FILES),
            dataMetaClient.getTableConfig().getMetadataPartitions());

        LOG.info("Wrote {} files partition records and {} column stats records to metadata table in a single commit",
            filesRecords.size(), columnStatsRecords.size());
      }
      return expectedStats;
    }
  }

  /**
   * Initialize Hudi table with sample data to set up the table schema and
   * metadata table.
   *
   * @return the name of the created table
   */
  private String initializeTableWithSampleData(String tableName) {
    // Define a schema with 'id', 'age', 'salary' columns
    StructType schema = new StructType()
        .add("id", "string")
        .add("name", "string")
        .add("city", "string")
        .add("age", "int")
        .add("salary", "double")
        .add("dt", "string");

    // Generate 50 rows of sample data
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < INITIAL_ROW_COUNT; i++) {
      rows.add(org.apache.spark.sql.RowFactory.create(
          UUID.randomUUID().toString(),
          "user_" + i,
          "frisco",
          20 + (i % 30), // age: 20..49
          50000.0 + (i * 1000), //
          "2020-01-01"// salary varies
      ));
    }
    Dataset<Row> df = spark.createDataFrame(rows, schema);

    // Write the data to the Hudi table using spark sql
    df.write()
        .format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.partitionpath.field", "dt")
        .option("hoodie.datasource.write.table.name", tableName)
        .option("hoodie.datasource.write.operation", "insert")
        .option("hoodie.datasource.write.precombine.field", "id")
        .option("hoodie.metadata.enabled", "true")
        .option("path", cfg.tableBasePath)
        .mode("overwrite")
        .save();

    // Refresh table metadata in Spark
    spark.catalog().clearCache();
    spark.read().format("hudi").load(cfg.tableBasePath).createOrReplaceTempView(tableName);

    // print total rows in table
    long totalRows = spark.read().format("hudi").load(cfg.tableBasePath).count();
    LOG.info("Total rows in table: {}", totalRows);
    // print the table first few rows
    Dataset<Row> tableDF = spark.read().format("hudi").load(cfg.tableBasePath);
    tableDF.show(10, false);

    return tableName;
  }

  /**
   * Creates empty parquet files on disk for all files in the commit metadata.
   * This ensures that filesystem listing will find these files even if metadata table
   * lookup falls back to filesystem.
   *
   * @param metaClient The meta client for the data table
   * @param commitMetadata The commit metadata containing file information
   */
  private void createEmptyParquetFiles(HoodieTableMetaClient metaClient,
                                       HoodieCommitMetadata commitMetadata) throws Exception {
    org.apache.hudi.storage.HoodieStorage storage = metaClient.getStorage();
    StoragePath basePath = metaClient.getBasePath();

    for (Map.Entry<String, List<HoodieWriteStat>> entry :
        commitMetadata.getPartitionToWriteStats().entrySet()) {
      String partitionPath = entry.getKey();
      StoragePath partitionDir = new StoragePath(basePath, partitionPath);
      if (!storage.exists(partitionDir)) {
        storage.createDirectory(partitionDir);
      }
      for (HoodieWriteStat stat : entry.getValue()) {
        String relativePath = stat.getPath();
        StoragePath filePath = new StoragePath(basePath, relativePath);
        if (!storage.exists(filePath)) {
          storage.create(filePath).close();
        }
      }
    }
  }

  /**
   * Generates column stats records based on commit metadata file structure.
   * Ensures file names match those in the commit metadata.
   */
  @SuppressWarnings("rawtypes")
  private List<HoodieRecord<HoodieMetadataPayload>> generateColumnStatsRecordsForCommitMetadata(
      HoodieCommitMetadata commitMetadata,
      Map<String, Map<String, HoodieColumnRangeMetadata<Comparable>>> expectedStatsMap,
      String commitTime) {

    Random random = new Random(42);
    List<HoodieRecord<HoodieMetadataPayload>> allRecords = new ArrayList<>();

    // Extract file information from commit metadata
    Map<String, List<HoodieWriteStat>> partitionToWriteStats = commitMetadata.getPartitionToWriteStats();

    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      String partitionPath = entry.getKey();
      List<HoodieWriteStat> writeStats = entry.getValue();

      LOG.info("Processing partition {} with {} write stats", partitionPath, writeStats.size());

      for (HoodieWriteStat writeStat : writeStats) {
        String fileId = writeStat.getFileId();
        String filePath = writeStat.getPath();
        String fileName = new StoragePath(filePath).getName();

        if (allRecords.size() < 5) {
          LOG.debug("Processing file: {} (fileId: {}, path: {})", fileName, fileId, filePath);
        }

        List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadata = new ArrayList<>();

        // Generate stats for age (int) and salary (long) columns
        String[] columnNames = getColumnsToIndex();
        int numColumns = columnNames.length;
        for (int colIdx = 0; colIdx < numColumns; colIdx++) {
          String colName = columnNames[colIdx];

          Comparable minValue;
          Comparable maxValue;

          if (colIdx == 0) {
            // age column: values between 20-100
            int minAge = 20 + random.nextInt(30);
            int maxAge = minAge + random.nextInt(50);
            minValue = minAge;
            maxValue = maxAge;
          } else {
            // salary column: values between 50000-250000
            long minSalary = 50000L + random.nextInt(100000);
            long maxSalary = minSalary + random.nextInt(100000);
            minValue = minSalary;
            maxValue = maxSalary;
          }

          @SuppressWarnings({ "rawtypes", "unchecked" })
          int compareResult = minValue.compareTo(maxValue);
          if (compareResult > 0) {
            Comparable temp = minValue;
            minValue = maxValue;
            maxValue = temp;
          }

          @SuppressWarnings({ "rawtypes", "unchecked" })
          HoodieColumnRangeMetadata<Comparable> colStats = HoodieColumnRangeMetadata.create(
              fileName,
              colName,
              minValue,
              maxValue,
              0,
              1000,
              123456,
              123456,
              ValueMetadata.V1EmptyMetadata.get());

          columnRangeMetadata.add(colStats);
          expectedStatsMap.computeIfAbsent(fileName, k -> new HashMap<>()).put(colName, colStats);
        }

        @SuppressWarnings("unchecked")
        List<HoodieRecord<HoodieMetadataPayload>> fileRecords = HoodieMetadataPayload
            .createColumnStatsRecords(partitionPath, columnRangeMetadata, false)
            .map(record -> (HoodieRecord<HoodieMetadataPayload>) record)
            .collect(Collectors.toList());

        allRecords.addAll(fileRecords);
      }
    }

    LOG.info("Generated {} column stats records total for {} unique files",
        allRecords.size(), expectedStatsMap.size());
    if (expectedStatsMap.size() <= 10) {
      LOG.info("File names in expectedStatsMap: {}", expectedStatsMap.keySet());
    } else {
      LOG.info("First 10 file names: {}",
          expectedStatsMap.keySet().stream().limit(10).collect(Collectors.toList()));
    }

    // Log sample record keys and file names to verify they're unique
    if (allRecords.size() > 0) {
      LOG.info("Sample record keys (first 5):");
      for (int i = 0; i < Math.min(5, allRecords.size()); i++) {
        HoodieRecord<HoodieMetadataPayload> record = allRecords.get(i);
        HoodieMetadataPayload payload = record.getData();
        if (payload.getColumnStatMetadata().isPresent()) {
          String fileName = payload.getColumnStatMetadata().get().getFileName();
          String columnName = payload.getColumnStatMetadata().get().getColumnName();
          LOG.info("  Record {}: key={}, fileName={}, columnName={}",
              i, record.getRecordKey(), fileName, columnName);
        }
      }
    }

    return allRecords;
  }

  private String[] getColumnsToIndex() {
    return cfg.colsToIndex.split(",");
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
          .withMarkersTimelineServerBasedBatchIntervalMs(10)
          .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
              .withEnableBackupForRemoteFileSystemView(false) // Fail test if problem connecting to timeline-server
              .withRemoteServerPort(FileSystemViewStorageConfig.REMOTE_PORT_NUM.defaultValue()).build());
      if (StringUtils.nonEmpty(schemaStr)) {
        builder.withSchema(schemaStr);
      }
      builder.withEngineType(EngineType.SPARK);
      return builder.build();
  }

  public void close() {
    engineContext.cancelAllJobs();
  }
}

