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

package org.apache.hudi.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.TableWriteStats;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkMetadataWriterFactory;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.stats.ValueMetadata;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.common.model.FileSlice;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.execution.datasources.NoopCache$;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import scala.collection.JavaConverters;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Benchmark test for Hudi Metadata Table (MDT) performance.
 * 
 * This test benchmarks the performance of writing and reading column stats
 * to/from the metadata table without creating actual data files.
 * 
 * Key features:
 * - Writes column stats for 2 columns only
 * - Uses 10 file groups for column stats
 * - Writes directly to metadata table without creating data files
 * - Verifies column stats by reading back from metadata table
 * 
 * Note: This code is Java 8 compatible. All APIs used (Stream API, Optional,
 * lambda expressions, method references) are available in Java 8.
 */
public class TestMDTStats extends HoodieSparkClientTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestMDTStats.class);

  // Configuration constants
  private static final int NUM_COLUMNS = 2;
  private static final int FILE_GROUP_COUNT = 10;
  private static final String DEFAULT_PARTITION_PATH = "p1";
  private static final int DEFAULT_NUM_FILES = 1000; // Configurable via system property

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestMDTStats");
    initPath();
    initHoodieStorage();
    initTestDataGenerator();
    initMetaClient();
    initTimelineService();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  /**
   * Main benchmark test that generates column stats for files and writes to metadata table.
   */
  @Test
  public void testMDTStatsBenchmark() throws Exception {
    // Get number of files from system property or use default
    int numFiles = Integer.getInteger("hudi.mdt.stats.num.files", DEFAULT_NUM_FILES);

    LOG.info("Starting MDT stats benchmark with {} files, {} columns, {} file groups", 
        numFiles, NUM_COLUMNS, FILE_GROUP_COUNT);
    
    // Log table locations for debugging/inspection
    // Note: basePath is initialized by HoodieCommonTestHarness.initPath() which uses @TempDir
    // The table will be automatically cleaned up after the test completes
    LOG.info("Data table base path: {}", basePath);
    String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
    LOG.info("Metadata table base path: {}", metadataTableBasePath);

    // Create data table config with file group count override in metadata config
    HoodieWriteConfig dataWriteConfig = getConfig();
    
    // Override file group count for column stats in the data table's metadata config
    // Ensure metadata table and column stats index are enabled
    HoodieMetadataConfig metadataConfig = dataWriteConfig.getMetadataConfig();
    HoodieWriteConfig dataConfigWithFileGroupCount = HoodieWriteConfig.newBuilder()
        .withProperties(dataWriteConfig.getProps())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .fromProperties(metadataConfig.getProps())
            .enable(true)  // Ensure metadata table is enabled
            .withMetadataIndexColumnStats(true)  // Enable column stats index
            .withMetadataIndexColumnStatsFileGroupCount(FILE_GROUP_COUNT)
            .build())
        .build();
    
    // Initialize metadata table first
    HoodieTableMetaClient dataMetaClient = HoodieTableMetaClient.builder()
        .setBasePath(dataConfigWithFileGroupCount.getBasePath())
        .setConf(context.getStorageConf().newInstance())
        .build();
    
    try (HoodieTableMetadataWriter<?, ?> metadataWriter = SparkMetadataWriterFactory.create(
        context.getStorageConf(), 
        dataConfigWithFileGroupCount, 
        context, 
        Option.empty(), 
        dataMetaClient.getTableConfig())) {
      LOG.info("Metadata table initialized");
    }
    
    // Create metadata write config for writing to metadata table
    HoodieWriteConfig finalMdtConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        dataConfigWithFileGroupCount, 
        HoodieFailedWritesCleaningPolicy.EAGER, 
        HoodieTableVersion.EIGHT);
    
    // Build the metadata table meta client
    HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder()
        .setBasePath(finalMdtConfig.getBasePath())
        .setConf(context.getStorageConf().newInstance())
        .build();

    // Generate column stats records for 2 columns
    // Note: We need to use the same commitTime for both column stats and file creation
    // so that file names match between metadata table and actual files
    String dataCommitTime = InProcessTimeGenerator.createNewInstantTime();
    
    @SuppressWarnings("rawtypes")
    Map<String, Map<String, HoodieColumnRangeMetadata<Comparable>>> expectedStats = 
        new HashMap<>();
    List<HoodieRecord<HoodieMetadataPayload>> allColumnStatsRecords = 
        generateColumnStatsRecords(numFiles, NUM_COLUMNS, DEFAULT_PARTITION_PATH, expectedStats, dataCommitTime);

    LOG.info("Generated {} column stats records", allColumnStatsRecords.size());

    // Write to metadata table using SparkRDDWriteClient
    String commitTime = InProcessTimeGenerator.createNewInstantTime();
    long writeStartTime = System.currentTimeMillis();
    
    try (SparkRDDWriteClient<HoodieMetadataPayload> mdtWriteClient = 
        new SparkRDDWriteClient<>(context, finalMdtConfig)) {
      
      // Start commit to create requested instant
      WriteClientTestUtils.startCommitWithTime(mdtWriteClient, commitTime);
      
      JavaRDD<HoodieRecord<HoodieMetadataPayload>> recordsRDD = 
          jsc.parallelize(allColumnStatsRecords, Math.min(10, allColumnStatsRecords.size() / 100 + 1));
      
      LOG.info("Writing {} records to metadata table at instant {}", 
          allColumnStatsRecords.size(), commitTime);
      
      JavaRDD<WriteStatus> writeStatuses = mdtWriteClient.upsertPreppedRecords(recordsRDD, commitTime);
      List<WriteStatus> writeStatusList = writeStatuses.collect(); // Trigger execution
      
      // Extract write stats - for direct metadata table writes, put them in data table position
      // because finalizeWrite() uses dataTableWriteStats to finalize the files
      List<HoodieWriteStat> writeStats = writeStatusList.stream()
          .map(WriteStatus::getStat)
          .collect(Collectors.toList());
      
      LOG.info("Collected {} write stats for metadata table", writeStats.size());
      if (writeStats.isEmpty()) {
        LOG.warn("No write stats collected! This indicates writes may have failed.");
      } else {
        LOG.info("Sample write stat: fileId={}, path={}, numWrites={}", 
            writeStats.get(0).getFileId(), writeStats.get(0).getPath(), writeStats.get(0).getNumWrites());
      }
      
      // Commit the writes using commitStats
      // For direct metadata table writes, put stats in data table position so finalizeWrite() processes them
      // Set skipStreamingWritesToMetadataTable=true to avoid trying to write metadata table's metadata
      TableWriteStats tableWriteStats = new TableWriteStats(writeStats, Collections.emptyList());
      boolean committed = mdtWriteClient.commitStats(commitTime, tableWriteStats, Option.empty(), 
          HoodieActiveTimeline.DELTA_COMMIT_ACTION, Collections.emptyMap(), Option.empty(), 
          true, // skipStreamingWritesToMetadataTable - we're already writing to metadata table directly
          Option.empty());
      assertTrue(committed, "Metadata table commit should succeed");
      
      LOG.info("Committed metadata table writes at instant {}", commitTime);
      LOG.info("Write stats summary: {} files written", writeStats.size());
    }
    
    long writeEndTime = System.currentTimeMillis();
    long writeDuration = writeEndTime - writeStartTime;
    LOG.info("Write completed in {} ms ({} seconds)", writeDuration, writeDuration / 1000.0);

    // Reload meta client to ensure we see the latest committed data
    mdtMetaClient = HoodieTableMetaClient.reload(mdtMetaClient);
    Option<String> lastInstant = mdtMetaClient.getActiveTimeline().lastInstant().map(instant -> instant.requestedTime());
    LOG.info("Reloaded metadata table meta client. Latest instant: {}", 
        lastInstant.isPresent() ? lastInstant.get() : "None");
     
    // Fetch column stats from metadata table using Spark SQL
    // Query for column stats metadata for col0 (int) and col1 (long)
    // Note: Use basePath as the table identifier for hudi_metadata function
    // For Long values, they are stored in member2, not member1
    String metadataSql = String.format(
        "SELECT "
            + "  ColumnStatsMetadata.fileName, "
            + "  ColumnStatsMetadata.columnName, "
            + "  CAST(COALESCE("
            + "    CAST(ColumnStatsMetadata.minValue.member1.value AS STRING), "  // IntWrapper (for col_0)
            + "    CAST(ColumnStatsMetadata.minValue.member2.value AS STRING), "  // LongWrapper (for col_1)
            + "    CAST(ColumnStatsMetadata.minValue.member3.value AS STRING)"    // Other types
            + "  ) AS STRING) as minValue, "
            + "  CAST(COALESCE("
            + "    CAST(ColumnStatsMetadata.maxValue.member1.value AS STRING), "  // IntWrapper (for col_0)
            + "    CAST(ColumnStatsMetadata.maxValue.member2.value AS STRING), "  // LongWrapper (for col_1)
            + "    CAST(ColumnStatsMetadata.maxValue.member3.value AS STRING)"    // Other types
            + "  ) AS STRING) as maxValue "
            + "FROM hudi_metadata('%s') "
            + "WHERE type = 3 "
            + "  AND (ColumnStatsMetadata.columnName = 'col_0' OR ColumnStatsMetadata.columnName = 'col_1') "
            + "ORDER BY ColumnStatsMetadata.fileName, ColumnStatsMetadata.columnName",
        dataConfigWithFileGroupCount.getBasePath()
    );

    LOG.info("Executing SQL query to fetch column stats:");
    LOG.info("{}", metadataSql);

    Dataset<Row> metadataDF = sparkSession.sql(metadataSql);

    long recordCount = metadataDF.count();
    LOG.info("Total column stats records found: {}", recordCount);

    // Show first 50 rows
    LOG.info("First 50 column stats records:");
    metadataDF.show(50, false);
    
    // Create empty base files and register them in a commit so HoodieFileIndex can discover file slices
    // This is necessary because filterFileSlices() loads file slices from the file system view,
    // which requires actual files to exist (even if empty)
    // IMPORTANT: Use the same commitTime (dataCommitTime) so file names match between
    // column stats in metadata table and actual file slices
    LOG.info("Creating empty base files for file slices...");
    createEmptyBaseFilesAndCommit(dataConfigWithFileGroupCount, dataMetaClient, expectedStats, dataCommitTime);
    
    // Benchmark index lookup overhead
    benchmarkIndexLookup(dataConfigWithFileGroupCount, dataMetaClient, numFiles, expectedStats);
  }
  
  /**
   * Creates empty base files and registers them in a commit so HoodieFileIndex can discover file slices.
   * This is necessary because filterFileSlices() loads file slices from the file system view,
   * which requires actual files to exist (even if empty).
   * 
   * @param dataConfig Write config
   * @param dataMetaClient Meta client for the data table
   * @param expectedStats Map of file names to their column stats
   * @param commitTime Commit time to use for the files
   */
  @SuppressWarnings("rawtypes")
  private void createEmptyBaseFilesAndCommit(
      HoodieWriteConfig dataConfig,
      HoodieTableMetaClient dataMetaClient,
      Map<String, Map<String, HoodieColumnRangeMetadata<Comparable>>> expectedStats,
      String commitTime) throws Exception {
    
    LOG.info("Creating empty base files for {} files...", expectedStats.size());
    
    HoodieStorage storage = dataMetaClient.getStorage();
    String basePath = dataConfig.getBasePath();
    String partitionPath = DEFAULT_PARTITION_PATH;
    
    // Create partition directory if it doesn't exist
    StoragePath partitionStoragePath = FSUtils.constructAbsolutePath(basePath, partitionPath);
    String partitionAbsolutePath = partitionStoragePath.toString();
    if (!storage.exists(partitionStoragePath)) {
      storage.createDirectory(partitionStoragePath);
    }
    
    // Create empty base files for each file that has column stats
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    String writeToken = "0-0-0";
    String fileExtension = dataMetaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    
    for (String fileName : expectedStats.keySet()) {
      // Extract fileId from fileName (e.g., "file_00000000.parquet" -> "file_00000000")
      String fileId = fileName.substring(0, fileName.lastIndexOf('.'));
      
      // Create base file name using Hudi naming convention
      String baseFileName = FSUtils.makeBaseFileName(commitTime, writeToken, fileId, fileExtension);
      StoragePath filePath = new StoragePath(partitionAbsolutePath, baseFileName);
      
      // Create empty file
      try (java.io.OutputStream os = storage.create(filePath, true)) {
        // File is empty, just close the stream
      }
      
      // Add to commit metadata
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPartitionPath(partitionPath);
      StoragePath fileAbsolutePath = FSUtils.constructAbsolutePath(partitionAbsolutePath, baseFileName);
      writeStat.setPath(fileAbsolutePath.toString());
      writeStat.setFileId(fileId);
      writeStat.setTotalWriteBytes(0L); // Empty file
      writeStat.setPrevCommit("00000000000000");
      writeStat.setNumWrites(0);
      writeStat.setNumUpdateWrites(0);
      writeStat.setTotalLogBlocks(0);
      writeStat.setTotalLogRecords(0);
      commitMetadata.addWriteStat(partitionPath, writeStat);
    }
    
    // Create commit in timeline using the metaClient's instant generator
    HoodieActiveTimeline timeline = dataMetaClient.getActiveTimeline();
    HoodieInstant inflightInstant = dataMetaClient.getTimelineLayout().getInstantGenerator()
        .createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, commitTime);
    
    // Create the inflight instant
    timeline.createNewInstant(inflightInstant);
    
    // Save as complete with commit metadata
    timeline.saveAsComplete(inflightInstant, Option.of(commitMetadata));
    
    LOG.info("Created {} empty base files and registered commit {}", expectedStats.size(), commitTime);
  }

  /**
   * Benchmarks HoodieFileIndex.filterFileSlices() method end-to-end.
   * Creates data filters and partition filters based on TestDataSkippingUtils examples,
   * then measures the time to filter file slices using column stats index.
   * 
   * @param dataConfig Write config with metadata settings
   * @param dataMetaClient Meta client for the data table
   * @param numFiles Number of files that have column stats
   * @param expectedStats Map of expected column stats for each file (for printing min/max)
   */
  @SuppressWarnings("rawtypes")
  private void benchmarkIndexLookup(
      HoodieWriteConfig dataConfig,
      HoodieTableMetaClient dataMetaClient,
      int numFiles,
      Map<String, Map<String, HoodieColumnRangeMetadata<Comparable>>> expectedStats) throws Exception {
    
    String separator = String.join("", Collections.nCopies(80, "="));
    LOG.info(separator);
    LOG.info("Benchmarking HoodieFileIndex.filterFileSlices() with Column Stats Index");
    LOG.info(separator);
    
    // Reload meta client to ensure we have latest metadata
    dataMetaClient = HoodieTableMetaClient.reload(dataMetaClient);
    
    // Create HoodieFileIndex instance
    Map<String, String> options = new HashMap<>();
    options.put("path", dataConfig.getBasePath());
    options.put("hoodie.datasource.read.data.skipping.enable", "true");
    
    scala.Option<StructType> schemaOption = scala.Option.empty();
    // Convert Java Map to Scala immutable Map
    // In Scala: Map("path" -> basePath) creates an immutable Map
    // In Java, we use JavaConverters to convert
    @SuppressWarnings("deprecation")
    scala.collection.immutable.Map<String, String> scalaOptions = 
        JavaConverters.mapAsScalaMap(options).toMap(scala.Predef$.MODULE$.<scala.Tuple2<String, String>>conforms());
    
    // Create HoodieFileIndex - it's a Scala case class, accessible from Java
    // Note: HoodieFileIndex is compiled Scala code, accessible from Java
    // We use fully qualified name to ensure proper resolution
    // IMPORTANT: This will only resolve after compiling the Scala code in hudi-spark-common module first
    // Build order: mvn clean compile -pl hudi-spark-datasource/hudi-spark-common -am
    org.apache.hudi.HoodieFileIndex fileIndex = new org.apache.hudi.HoodieFileIndex(
        sparkSession,
        dataMetaClient,
        schemaOption,
        scalaOptions,
        NoopCache$.MODULE$,
        false, // includeLogFiles
        false  // shouldEmbedFileSlices
    );
    
    // Create table schema for the columns we have stats for
    StructType tableSchema = new StructType(new StructField[] {
        new StructField("col_0", IntegerType$.MODULE$, true, Metadata.empty()),
        new StructField("col_1", LongType$.MODULE$, true, Metadata.empty())
    });
    
    // Create data filters using Spark SQL parser (similar to TestDataSkippingUtils examples)
    // Example filters: "col_0 > 500000", "col_0 = 0", "col_1 > 500000000"
    LOG.info("Creating data filters...");
    List<Expression> dataFilters = new ArrayList<>();
    
    // Filter 1: col_0 > 500000 (GreaterThan filter like in TestDataSkippingUtils)
    Expression filter1 = sparkSession.sessionState().sqlParser().parseExpression("col_0 > 500000");
    dataFilters.add(filter1);
    
    // Filter 2: col_1 > 500000000L (additional filter)
    Expression filter2 = sparkSession.sessionState().sqlParser().parseExpression("col_1 > 500000000");
    dataFilters.add(filter2);
    
    // No partition filters - use empty list to let HoodieFileIndex list all partitions
    List<Expression> partitionFilters = new ArrayList<>();
    
    LOG.info("Data filters: col_0 > 500000, col_1 > 500000000");
    LOG.info("Partition filters: (empty - will list all partitions)");
    
    // Warm up: First call to initialize caches
    LOG.info("Warming up filterFileSlices...");
    long warmupStart = System.currentTimeMillis();
    try {
      // Convert Java List to Scala Seq using JavaConverters
      // Convert via List to avoid toSeq() ambiguity
      scala.collection.immutable.List<Expression> dataFiltersList = JavaConverters.asScalaBuffer(dataFilters).toList();
      scala.collection.Seq<Expression> dataFiltersSeq = dataFiltersList;
      scala.collection.immutable.List<Expression> partitionFiltersList = JavaConverters.asScalaBuffer(partitionFilters).toList();
      scala.collection.Seq<Expression> partitionFiltersSeq = partitionFiltersList;
      
      scala.collection.Seq<scala.Tuple2<scala.Option<org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath>, 
          scala.collection.Seq<FileSlice>>> warmupResult = 
          fileIndex.filterFileSlices(
              dataFiltersSeq,
              partitionFiltersSeq,
              false // isPartitionPruned
          );
      long warmupTime = System.currentTimeMillis() - warmupStart;
      LOG.info("Warmup completed in {} ms, returned {} partition/file slice pairs", 
          warmupTime, warmupResult.size());
    } catch (Exception e) {
      LOG.warn("Warmup failed (this is OK): {}", e.getMessage());
    }
    
    // Run benchmark iterations
    int numIterations = Integer.getInteger("hudi.mdt.stats.benchmark.iterations", 5);
    LOG.info("Running {} benchmark iterations...", numIterations);
    
    List<Long> filterTimes = new ArrayList<>();
    List<Integer> resultSizes = new ArrayList<>();
    
    for (int i = 0; i < numIterations; i++) {
      long filterStart = System.currentTimeMillis();
      
      try {
        // Call filterFileSlices() - this internally uses:
        // 1. prunePartitionsAndGetFileSlices() for partition pruning
        // 2. lookupCandidateFilesInMetadataTable() for data skipping
        // 3. ColumnStatsIndexSupport.computeCandidateFileNames() for column stats lookup
        // Convert Java List to Scala Seq
        // Convert via List to avoid toSeq() ambiguity
        scala.collection.immutable.List<Expression> dataFiltersList = JavaConverters.asScalaBuffer(dataFilters).toList();
        scala.collection.Seq<Expression> dataFiltersSeq = dataFiltersList;
        scala.collection.immutable.List<Expression> partitionFiltersList = JavaConverters.asScalaBuffer(partitionFilters).toList();
        scala.collection.Seq<Expression> partitionFiltersSeq = partitionFiltersList;
        
        scala.collection.Seq<scala.Tuple2<scala.Option<org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath>, 
            scala.collection.Seq<FileSlice>>> filteredSlices = 
            fileIndex.filterFileSlices(
                dataFiltersSeq,
                partitionFiltersSeq,
                false // isPartitionPruned
            );
        
        long filterEnd = System.currentTimeMillis();
        long filterTime = filterEnd - filterStart;
        filterTimes.add(filterTime);
        
        // Count total file slices in result and print min/max for each file
        int totalFileSlices = 0;
        
        // Print min/max for each file (only for first iteration to avoid excessive output)
        if (i == 0) {
          LOG.info("");
          LOG.info("Filtered File Slices Min/Max Values:");
          LOG.info(String.format("%-30s %-20s %-20s %-20s %-20s",
              "FileName", "col_0_min", "col_0_max", "col_1_min", "col_1_max"));
          LOG.info(String.join("", Collections.nCopies(100, "-")));
        }
        
        for (int j = 0; j < filteredSlices.size(); j++) {
          scala.Tuple2<scala.Option<org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath>, 
              scala.collection.Seq<FileSlice>> tuple = filteredSlices.apply(j);
          scala.collection.Seq<FileSlice> fileSliceSeq = tuple._2();
          totalFileSlices += fileSliceSeq.size();
          
          // Print min/max for each file in filtered slices (only for first iteration)
          if (i == 0) {
            for (int k = 0; k < fileSliceSeq.size(); k++) {
              FileSlice fileSlice = fileSliceSeq.apply(k);
              String fileName = fileSlice.getBaseFile().get().getFileName();
              
              // Retrieve expected stats for this file
              @SuppressWarnings("rawtypes")
              Map<String, HoodieColumnRangeMetadata<Comparable>> fileExpectedStats = expectedStats.get(fileName);
              if (fileExpectedStats != null) {
                @SuppressWarnings("rawtypes")
                HoodieColumnRangeMetadata<Comparable> col0Stats = fileExpectedStats.get("col_0");
                @SuppressWarnings("rawtypes")
                HoodieColumnRangeMetadata<Comparable> col1Stats = fileExpectedStats.get("col_1");
                
                Object col0Min = (col0Stats != null) ? col0Stats.getMinValue() : "null";
                Object col0Max = (col0Stats != null) ? col0Stats.getMaxValue() : "null";
                Object col1Min = (col1Stats != null) ? col1Stats.getMinValue() : "null";
                Object col1Max = (col1Stats != null) ? col1Stats.getMaxValue() : "null";
                
                LOG.info(String.format("%-30s %-20s %-20s %-20s %-20s",
                    fileName,
                    col0Min.toString(),
                    col0Max.toString(),
                    col1Min.toString(),
                    col1Max.toString()));
              } else {
                LOG.warn("No expected stats found for file: {}", fileName);
              }
            }
          }
        }
        resultSizes.add(totalFileSlices);
        
        if (i == 0) {
          LOG.info(String.join("", Collections.nCopies(100, "-")));
          LOG.info("");
        }
        
        LOG.info("Iteration {}: filterFileSlices took {} ms, returned {} partitions with {} total file slices", 
            i + 1, filterTime, filteredSlices.size(), totalFileSlices);
      } catch (Exception e) {
        LOG.error("Iteration {} failed: {}", i + 1, e.getMessage(), e);
        filterTimes.add(-1L);
      }
    }
    
    // Calculate and print statistics
    List<Long> successfulTimes = filterTimes.stream()
        .filter(t -> t > 0)
        .collect(Collectors.toList());
    
    if (!successfulTimes.isEmpty()) {
      long minTime = Collections.min(successfulTimes);
      long maxTime = Collections.max(successfulTimes);
      double avgTime = successfulTimes.stream()
          .mapToLong(Long::longValue)
          .average()
          .orElse(0.0);
      
      int avgResultSize = (int) resultSizes.stream()
          .filter(s -> s > 0)
          .mapToInt(Integer::intValue)
          .average()
          .orElse(0.0);
      
      LOG.info(separator);
      LOG.info("HoodieFileIndex.filterFileSlices() Benchmark Results");
      LOG.info(separator);
      LOG.info("Total files with column stats: {}", numFiles);
      LOG.info("Average file slices after filtering: {}", avgResultSize);
      LOG.info("Data skipping ratio: {:.2f}%", 
          numFiles > 0 ? (1.0 - (avgResultSize / (double) numFiles)) * 100.0 : 0.0);
      LOG.info("");
      LOG.info("filterFileSlices() time statistics ({} successful iterations):", successfulTimes.size());
      LOG.info("  Min: {} ms", minTime);
      LOG.info("  Max: {} ms", maxTime);
      LOG.info("  Avg: {:.2f} ms", avgTime);
      LOG.info("");
      LOG.info("This benchmark measures the end-to-end overhead of:");
      LOG.info("  - Partition pruning (prunePartitionsAndGetFileSlices)");
      LOG.info("  - Column stats index lookup (lookupCandidateFilesInMetadataTable)");
      LOG.info("  - File slice filtering (filterFileSlices)");
      LOG.info(separator);
    } else {
      LOG.warn("No successful benchmark iterations completed!");
    }
  }

  /**
   * Generates column stats records for the specified number of files and columns.
   * 
   * @param numFiles Number of files to generate stats for
   * @param numColumns Number of columns per file (should be 2)
   * @param partitionPath Partition path
   * @param expectedStatsMap Map to populate with expected stats for verification
   * @return List of HoodieRecord with column stats
   */
  @SuppressWarnings("rawtypes")
  private List<HoodieRecord<HoodieMetadataPayload>> generateColumnStatsRecords(
      int numFiles, 
      int numColumns, 
      String partitionPath,
      Map<String, Map<String, HoodieColumnRangeMetadata<Comparable>>> expectedStatsMap,
      String commitTime) {
    
    Random random = new Random(42); // Fixed seed for reproducibility
    List<HoodieRecord<HoodieMetadataPayload>> allRecords = new ArrayList<>();
    String writeToken = "0-0-0";
    String fileExtension = ".parquet";
    
    for (int fileIdx = 0; fileIdx < numFiles; fileIdx++) {
      // Extract fileId from simple name
      String fileId = String.format("file_%08d", fileIdx);
      // Create full Hudi file name that matches what will be created in createEmptyBaseFilesAndCommit
      String fileName = FSUtils.makeBaseFileName(commitTime, writeToken, fileId, fileExtension);
      List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadata = new ArrayList<>();
      
      for (int colIdx = 0; colIdx < numColumns; colIdx++) {
        String colName = "col_" + colIdx;
        
        // Generate min/max values for 2 columns
        // Column 0: Integer values
        // Column 1: Long values
        Comparable minValue;
        Comparable maxValue;
        
        if (colIdx == 0) {
          // Integer column
          int minInt = random.nextInt(1000000);
          int maxInt = minInt + random.nextInt(1000000);
          minValue = minInt;
          maxValue = maxInt;
        } else {
          // Long column
          // Use Math.abs to ensure positive values since random.nextLong() can return negative
          long minLong = Math.abs(random.nextLong()) % 1000000000L;
          long maxLong = minLong + (Math.abs(random.nextLong()) % 1000000000L);
          minValue = minLong;
          maxValue = maxLong;
        }
        
        // Ensure min < max
        @SuppressWarnings({"rawtypes", "unchecked"})
        int compareResult = minValue.compareTo(maxValue);
        if (compareResult > 0) {
          Comparable temp = minValue;
          minValue = maxValue;
          maxValue = temp;
        }
        
        @SuppressWarnings({"rawtypes", "unchecked"})
        HoodieColumnRangeMetadata<Comparable> colStats = HoodieColumnRangeMetadata.create(
            fileName,
            colName,
            minValue,
            maxValue,
            0, // nullCount
            1000, // valueCount
            123456, // totalSize
            123456, // totalUncompressedSize
            ValueMetadata.V1EmptyMetadata.get()
        );
        
        columnRangeMetadata.add(colStats);
        
        // Store expected stats for verification
        expectedStatsMap.computeIfAbsent(fileName, k -> new HashMap<>()).put(colName, colStats);
      }
      
      // Create column stats records for this file
      @SuppressWarnings("unchecked")
      List<HoodieRecord<HoodieMetadataPayload>> fileRecords = 
          HoodieMetadataPayload.createColumnStatsRecords(partitionPath, columnRangeMetadata, false)
              .map(record -> (HoodieRecord<HoodieMetadataPayload>) record)
              .collect(Collectors.toList());
      
      allRecords.addAll(fileRecords);
      
      if ((fileIdx + 1) % 1000 == 0) {
        LOG.info("Generated stats for {} files", fileIdx + 1);
      }
    }
    
    return allRecords;
  }

}
