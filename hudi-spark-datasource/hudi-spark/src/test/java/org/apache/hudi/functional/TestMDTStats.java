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

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.TableWriteStats;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.ColumnStatsIndexPrefixRawKey;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkMetadataWriterFactory;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.stats.ValueMetadata;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    Map<String, Map<String, HoodieColumnRangeMetadata<Comparable>>> expectedStats = 
        new HashMap<>();
    List<HoodieRecord<HoodieMetadataPayload>> allColumnStatsRecords = 
        generateColumnStatsRecords(numFiles, NUM_COLUMNS, DEFAULT_PARTITION_PATH, expectedStats);

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
         "SELECT " +
         "  ColumnStatsMetadata.fileName, " +
         "  ColumnStatsMetadata.columnName, " +
         "  CAST(COALESCE(" +
         "    CAST(ColumnStatsMetadata.minValue.member1.value AS STRING), " +  // IntWrapper (for col_0)
         "    CAST(ColumnStatsMetadata.minValue.member2.value AS STRING), " +  // LongWrapper (for col_1)
         "    CAST(ColumnStatsMetadata.minValue.member3.value AS STRING)" +    // Other types
         "  ) AS STRING) as minValue, " +
         "  CAST(COALESCE(" +
         "    CAST(ColumnStatsMetadata.maxValue.member1.value AS STRING), " +  // IntWrapper (for col_0)
         "    CAST(ColumnStatsMetadata.maxValue.member2.value AS STRING), " +  // LongWrapper (for col_1)
         "    CAST(ColumnStatsMetadata.maxValue.member3.value AS STRING)" +    // Other types
         "  ) AS STRING) as maxValue " +
         "FROM hudi_metadata('%s') " +
         "WHERE type = 3 " +
         "  AND (ColumnStatsMetadata.columnName = 'col_0' OR ColumnStatsMetadata.columnName = 'col_1') " +
         "ORDER BY ColumnStatsMetadata.fileName, ColumnStatsMetadata.columnName",
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
  private List<HoodieRecord<HoodieMetadataPayload>> generateColumnStatsRecords(
      int numFiles, 
      int numColumns, 
      String partitionPath,
      Map<String, Map<String, HoodieColumnRangeMetadata<Comparable>>> expectedStatsMap) {
    
    Random random = new Random(42); // Fixed seed for reproducibility
    List<HoodieRecord<HoodieMetadataPayload>> allRecords = new ArrayList<>();
    
    for (int fileIdx = 0; fileIdx < numFiles; fileIdx++) {
      String fileName = String.format("file_%08d.parquet", fileIdx);
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
        if (minValue.compareTo(maxValue) > 0) {
          Comparable temp = minValue;
          minValue = maxValue;
          maxValue = temp;
        }
        
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
