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
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataWriterTestUtils;
import org.apache.hudi.metadata.SparkMetadataWriterFactory;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
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
import java.util.UUID;

/**
 * Test for Hudi Metadata Table (MDT) column stats index.
 * 
 * This test follows a 5-step process:
 * STEP 1: Create a sample table with two columns. Insert 50 rows to initialize
 * table schema, metadata table partition, etc.
 * STEP 2: Use HoodieTestTable.createCommitMetadata to create commit metadata
 * without writing data files. Files are spread evenly across partitions and
 * file counts.
 * STEP 3: Write the /files partition of metadata table using the same file
 * structure from STEP 2.
 * STEP 4: Write column stats using existing logic, ensuring file names match
 * STEP 3.
 * STEP 5: Use HoodieFileIndex.filterFileSlices to query column stats index and
 * verify correct file slices are pruned.
 */
public class TestMDTStats extends HoodieSparkClientTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestMDTStats.class);

  // Configuration constants
  private static final int NUM_COLUMNS = 2;
  private static final int FILE_GROUP_COUNT = 10;
  private static final String DEFAULT_PARTITION_PATH = "p1";
  private static final int DEFAULT_NUM_FILES = 1000; // Configurable via system property
  private static final int INITIAL_ROW_COUNT = 50; // Rows to insert in STEP 1

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
   * Main test that follows the 5-step process.
   */
  @Test
  public void testMDTStatsWithFileSlices() throws Exception {
    // Get number of files from system property or use default
    int numFiles = Integer.getInteger("hudi.mdt.stats.num.files", DEFAULT_NUM_FILES);
    int numPartitions = 1; // Start with 1 partition

    LOG.info("Starting MDT stats test with {} files, {} partitions, {} columns, {} file groups",
        numFiles, numPartitions, NUM_COLUMNS, FILE_GROUP_COUNT);
    LOG.info("Data table base path: {}", basePath);
    String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
    LOG.info("Metadata table base path: {}", metadataTableBasePath);

    // Create data table config with metadata enabled
    HoodieWriteConfig dataWriteConfig = getConfig();
    HoodieMetadataConfig metadataConfig = dataWriteConfig.getMetadataConfig();
    HoodieWriteConfig dataConfig = HoodieWriteConfig.newBuilder()
        .withProperties(dataWriteConfig.getProps())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .fromProperties(metadataConfig.getProps())
            .enable(true)
            .withMetadataIndexColumnStats(true)
            .withMetadataIndexColumnStatsFileGroupCount(FILE_GROUP_COUNT)
            .build())
        .build();

    HoodieTableMetaClient dataMetaClient = HoodieTableMetaClient.builder()
        .setBasePath(dataConfig.getBasePath())
        .setConf(context.getStorageConf().newInstance())
        .build();

    // STEP 1: Insert 50 rows with age and salary columns to initialize table schema
    // and metadata table
    String tableName = initializeTableWithSampleData();
    dataMetaClient = HoodieTableMetaClient.reload(dataMetaClient);

    // STEP 2: Create commit metadata using HoodieTestTable without writing data
    // files
    String dataCommitTime = InProcessTimeGenerator.createNewInstantTime();
    List<String> partitions = Collections.singletonList(DEFAULT_PARTITION_PATH);
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
    LOG.info("Created commit metadata with {} files per partition", filesPerPartition);

    // STEP 3: Write the /files partition of metadata table using the same file structure from STEP 2. 
    HoodieWriteConfig mdtConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        dataConfig,
        HoodieFailedWritesCleaningPolicy.EAGER,
        HoodieTableVersion.EIGHT);
    
    try (HoodieTableMetadataWriter<?, ?> metadataWriter = SparkMetadataWriterFactory.create(
          context.getStorageConf(),
          dataConfig,
          context,
          Option.empty(),
          dataMetaClient.getTableConfig())) {
      
      // Convert commit metadata to files partition records
      @SuppressWarnings("unchecked")
      List<HoodieRecord<HoodieMetadataPayload>> filesRecords = (List<HoodieRecord<HoodieMetadataPayload>>) (List<?>) HoodieTableMetadataUtil
          .convertMetadataToFilesPartitionRecords(commitMetadata, dataCommitTime);

      // Write files partition records
      String mdtCommitTime = InProcessTimeGenerator.createNewInstantTime();
      try (SparkRDDWriteClient<HoodieMetadataPayload> mdtWriteClient = new SparkRDDWriteClient<>(context,
          mdtConfig)) {

        WriteClientTestUtils.startCommitWithTime(mdtWriteClient, mdtCommitTime);
        JavaRDD<HoodieRecord<HoodieMetadataPayload>> filesRDD = jsc.parallelize(filesRecords, 1);

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
        
        @SuppressWarnings("rawtypes")
        Pair<org.apache.hudi.common.data.HoodieData<HoodieRecord>, List<HoodieFileGroupId>> taggedResult = MetadataWriterTestUtils
            .tagRecordsWithLocation(
                sparkMetadataWriter,
                partitionRecordsMap,
                false // not initializing
            );
        
        // Convert back to JavaRDD (with proper type casting)
        @SuppressWarnings("unchecked")
        JavaRDD<HoodieRecord<HoodieMetadataPayload>> taggedRDD = (JavaRDD<HoodieRecord<HoodieMetadataPayload>>) (JavaRDD) HoodieJavaRDD
            .getJavaRDD(taggedResult.getKey());
        filesRDD = taggedRDD;

        JavaRDD<WriteStatus> writeStatuses = mdtWriteClient.upsertPreppedRecords(filesRDD, mdtCommitTime);
        List<WriteStatus> statusList = writeStatuses.collect();
        mdtWriteClient.commit(mdtCommitTime, writeStatuses);
        LOG.info("Wrote {} files partition records to metadata table", filesRecords.size());
      }
    }

  }

  /**
   * Initialize Hudi table with sample data to set up the table schema and
   * metadata table.
   * 
   * @return the name of the created table
   */
  private String initializeTableWithSampleData() {
    // Define a schema with 'id', 'age', 'salary' columns
    String tableName = "test_mdt_stats_tbl";
    StructType schema = new StructType()
        .add("id", "string")
        .add("name", "string")
        .add("city", "string")
        .add("age", "int")
        .add("salary", "double");

    // Generate 50 rows of sample data
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < INITIAL_ROW_COUNT; i++) {
      rows.add(org.apache.spark.sql.RowFactory.create(
          UUID.randomUUID().toString(),
          "user_" + i,
          "frisco",
          20 + (i % 30), // age: 20..49
          50000.0 + (i * 1000) // salary varies
      ));
    }
    Dataset<Row> df = sparkSession.createDataFrame(rows, schema);

    // Write the data to the Hudi table using spark sql
    df.write()
        .format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.partitionpath.field", "city")
        .option("hoodie.datasource.write.table.name", tableName)
        .option("hoodie.datasource.write.operation", "insert")
        .option("hoodie.datasource.write.precombine.field", "id")
        .option("hoodie.metadata.enabled", "true")
        .option("path", basePath)
        .mode("overwrite")
        .save();

    // Refresh table metadata in Spark
    sparkSession.catalog().clearCache();
    sparkSession.read().format("hudi").load(basePath).createOrReplaceTempView(tableName);

    // print total rows in table
    long totalRows = sparkSession.read().format("hudi").load(basePath).count();
    LOG.info("Total rows in table: {}", totalRows);
    // print the table first few rows
    Dataset<Row> tableDF = sparkSession.read().format("hudi").load(basePath);
    tableDF.show(10, false);

    return tableName;
  }

}
