/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.testutils.providers.SparkProvider;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;
import static org.apache.hudi.config.HoodieCleanConfig.CLEANER_COMMITS_RETAINED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMetadataSync extends SparkClientFunctionalTestHarness implements SparkProvider {

  static final String PARTITION_PATH_1 = "2020";
  static final String PARTITION_PATH_2 = "2021";
  static final String PARTITION_PATH_3 = "2023";
  static final String TABLE_NAME = "testing";

  private static final HoodieTestDataGenerator DATA_GENERATOR = new HoodieTestDataGenerator(0L);
  private HoodieTableMetaClient metaClient;
  private HoodieTableMetaClient sourceMetaClient1;
  private HoodieTableMetaClient sourceMetaClient2;

  String sourcePath1;
  String sourcePath2;
  String targetPath;

  @BeforeEach
  public void init() throws IOException {
    this.metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);


    // Initialize test data dirs
    sourcePath1 = Paths.get(basePath(), "source").toString();
    sourcePath2 = Paths.get(basePath(), "source_2").toString();
    targetPath = Paths.get(basePath(), "target").toString();

//    HoodieTableMetaClient.withPropertyBuilder()
//        .setTableType(HoodieTableType.COPY_ON_WRITE)
//        .setTableName(TABLE_NAME)
//        .setPayloadClass(HoodieAvroPayload.class)
//        .initTable(jsc().hadoopConfiguration(), sourcePath1);
//
//    // Prepare data as source Hudi dataset
//    HoodieWriteConfig cfg = getHoodieWriteConfig(sourcePath1);
//    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(cfg)) {
//      String commitTime = writeClient.startCommit();
//      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_1});
//      List<HoodieRecord> records = dataGen.generateInserts(commitTime, NUM_RECORDS);
//      JavaRDD<HoodieRecord> recordsRDD = jsc().parallelize(records, 1);
//      writeClient.bulkInsert(recordsRDD, commitTime);
//    }
//
//    sourceMetaClient1 = HoodieTableMetaClient.builder().setBasePath(sourcePath1).setConf(jsc().hadoopConfiguration()).build();
//
//    HoodieTableMetaClient.withPropertyBuilder()
//        .setTableType(HoodieTableType.COPY_ON_WRITE)
//        .setTableName(TABLE_NAME)
//        .setPayloadClass(HoodieAvroPayload.class)
//        .initTable(jsc().hadoopConfiguration(), sourcePath2);
//
//    // Prepare data as source Hudi dataset
//    cfg = getHoodieWriteConfig(sourcePath2);
//    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(cfg)) {
//      String commitTime = writeClient.startCommit();
//      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_2});
//      List<HoodieRecord> records = dataGen.generateInserts(commitTime, NUM_RECORDS);
//      JavaRDD<HoodieRecord> recordsRDD = jsc().parallelize(records, 1);
//      writeClient.bulkInsert(recordsRDD, commitTime);
//    }
//
//    sourceMetaClient2 = HoodieTableMetaClient.builder().setBasePath(sourcePath2).setConf(jsc().hadoopConfiguration()).build();
  }

  private HoodieWriteConfig getHoodieWriteConfig(String basePath) {
    Properties props = new Properties();
    props.put("hoodie.metadata.compact.max.delta.commits", "3");
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withEmbeddedTimelineServerEnabled(false)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withDeleteParallelism(2)
        .forTable(TABLE_NAME)
        .withProps(props)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .build();
  }

  @AfterAll
  public static void cleanup() {
    DATA_GENERATOR.close();
  }

  @Test
  public void simpleTest() throws Exception {
    init();
    spark().read().format("hudi").load(sourcePath1 + "/.hoodie/metadata/").registerTempTable("srcMetadata");

    Dataset<Row> srcMdtDF = spark().sql("select key, type, filesystemMetadata  from srcMetadata where fileSystemMetadata is not null");

    srcMdtDF.show(false);
    HoodieMetadataSync.Config cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath1;
    cfg.targetBasePath = targetPath;
    String latestCommit = sourceMetaClient1.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    HoodieMetadataSync metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath1).registerTempTable("sTable1");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tTable1");

    Dataset<Row> sDf1 = spark().sql("select * from sTable1").drop("city_to_state");
    sDf1.cache();

    spark().read().format("hudi").load(targetPath + "/.hoodie/metadata/").registerTempTable("tTableMetadata");

    Dataset<Row> mdtDF = spark().sql("select key, type, filesystemMetadata  from tTableMetadata where fileSystemMetadata is not null and type = 1");

    mdtDF.show(false);

    Dataset<Row> tDf = spark().sql("select * from tTable1").drop("city_to_state");

    assertEquals(0, sDf1.except(tDf).count());
    assertEquals(0, tDf.except(sDf1).count());

    // lets sync 2nd table as well.

    cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath2;
    cfg.targetBasePath = targetPath;
    latestCommit = sourceMetaClient2.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath2).registerTempTable("sTable2");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tTable1");

    spark().read().format("hudi").load(targetPath + "/.hoodie/metadata/").registerTempTable("tTableMetadata");
    Dataset<Row> sDf2 = spark().sql("select * from sTable2").drop("city_to_state");

    mdtDF = spark().sql("select key, type, filesystemMetadata  from tTableMetadata where fileSystemMetadata is not null and type = 1");

    mdtDF.show(false);

    Dataset<Row> tDf2 = spark().sql("select * from tTable1").drop("city_to_state");

    assertEquals(0, sDf1.union(sDf2).except(tDf2).count());
    assertEquals(0, tDf.except(sDf1.union(sDf2)).count());
    System.out.println("Asdfasd");
  }

  @Test
  void testHoodieMetadataSync() throws Exception {
    sourcePath1 = Paths.get(basePath(), "source1").toString();
    sourcePath2 = Paths.get(basePath(), "source2").toString();

    Properties props = HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(new Properties())
        .build();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig1)) {
      String instant = writeClient.startCommit();
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_1});
      List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
      JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
      writeClient.insert(dataset, instant);

      String instant2 = writeClient.startCommit();
      List<HoodieRecord> updates = dataGen.generateUpdates(instant2, 5);
      dataset = jsc().parallelize(updates);
      writeClient.upsert(dataset, instant2);
    }

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);

    HoodieWriteConfig writeConfig2 = getHoodieWriteConfig(sourcePath2);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig2)) {
      String instant = writeClient.startCommit();
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_2});
      List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
      JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
      writeClient.insert(dataset, instant);

      String instant2 = writeClient.startCommit();
      List<HoodieRecord> updates = dataGen.generateUpdates(instant2, 5);
      dataset = jsc().parallelize(updates);
      writeClient.upsert(dataset, instant2);
    }


    HoodieMetadataSync.Config cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath1;
    cfg.targetBasePath = targetPath;
    String latestCommit = sourceMetaClient1.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    HoodieMetadataSync metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath1).registerTempTable("srcTable1");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable1");

    Dataset<Row> srcDf1 = spark().sql("select * from srcTable1").drop("city_to_state");
    srcDf1.cache();
    Dataset<Row> tgtDf = spark().sql("select * from tgtTable1").drop("city_to_state");

    assertEquals(srcDf1.schema(), tgtDf.schema());
    assertTrue(srcDf1.except(tgtDf).isEmpty() && tgtDf.except(srcDf1).isEmpty());

    cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath2;
    cfg.targetBasePath = targetPath;
    latestCommit = sourceMetaClient2.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath2).registerTempTable("srcTable2");

    Dataset<Row> srcDf2 = spark().sql("select * from srcTable2").drop("city_to_state");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable2");

    Dataset<Row> tgtDf2 = spark().sql("select * from tgtTable2").drop("city_to_state");

    assertEquals(srcDf2.schema(), tgtDf2.schema());
    assertTrue(srcDf1.union(srcDf2).except(tgtDf2).isEmpty() && tgtDf2.except(srcDf1.union(srcDf2)).isEmpty());
    System.out.println("Done");
  }

  @Test
  void testHoodieMetadataSyncWithBulkInsert() throws Exception {
    sourcePath1 = Paths.get(basePath(), "source1").toString();
    sourcePath2 = Paths.get(basePath(), "source2").toString();

    Properties props = HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(new Properties())
        .build();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig1)) {
      String instant = writeClient.startCommit();
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_1});
      List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
      JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
      writeClient.insert(dataset, instant);

      String instant2 = writeClient.startCommit();
      List<HoodieRecord> updates = dataGen.generateUpdates(instant2, 5);
      dataset = jsc().parallelize(updates);
      writeClient.insert(dataset, instant2);
    }

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);

    HoodieWriteConfig writeConfig2 = getHoodieWriteConfig(sourcePath2);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig2)) {
      String instant = writeClient.startCommit();
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_2});
      List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
      JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
      writeClient.bulkInsert(dataset, instant);

      String instant2 = writeClient.startCommit();
      List<HoodieRecord> updates = dataGen.generateUpdates(instant2, 5);
      dataset = jsc().parallelize(updates);
      writeClient.bulkInsert(dataset, instant2);
    }


    HoodieMetadataSync.Config cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath1;
    cfg.targetBasePath = targetPath;
    String latestCommit = sourceMetaClient1.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    HoodieMetadataSync metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath1).registerTempTable("srcTable1");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable1");

    Dataset<Row> srcDf1 = spark().sql("select * from srcTable1").drop("city_to_state");
    srcDf1.cache();
    Dataset<Row> tgtDf = spark().sql("select * from tgtTable1").drop("city_to_state");

    assertEquals(srcDf1.schema(), tgtDf.schema());
    assertTrue(srcDf1.except(tgtDf).isEmpty() && tgtDf.except(srcDf1).isEmpty());

    cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath2;
    cfg.targetBasePath = targetPath;
    latestCommit = sourceMetaClient2.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath2).registerTempTable("srcTable2");

    Dataset<Row> srcDf2 = spark().sql("select * from srcTable2").drop("city_to_state");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable2");

    Dataset<Row> tgtDf2 = spark().sql("select * from tgtTable2").drop("city_to_state");

    assertEquals(srcDf2.schema(), tgtDf2.schema());
    assertTrue(srcDf1.union(srcDf2).except(tgtDf2).isEmpty() && tgtDf2.except(srcDf1.union(srcDf2)).isEmpty());
    System.out.println("Done");
  }

  @Test
  void testHoodieMetadataSyncWithClustering() throws Exception {
    sourcePath1 = Paths.get(basePath(), "source1").toString();
    sourcePath2 = Paths.get(basePath(), "source2").toString();

    Properties props = HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(new Properties())
        .build();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig1)) {
      String instant = writeClient.startCommit();
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_1});
      List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
      JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
      writeClient.insert(dataset, instant);

      String instant2 = writeClient.startCommit();
      List<HoodieRecord> updates = dataGen.generateUpdates(instant2, 5);
      dataset = jsc().parallelize(updates);
      writeClient.insert(dataset, instant2);
    }

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);

    HoodieWriteConfig writeConfig2 = getHoodieWriteConfig(sourcePath2);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig2)) {
      String instant = writeClient.startCommit();
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_2});
      List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
      JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
      writeClient.bulkInsert(dataset, instant);

      String instant2 = writeClient.startCommit();
      List<HoodieRecord> updates = dataGen.generateUpdates(instant2, 5);
      dataset = jsc().parallelize(updates);
      writeClient.bulkInsert(dataset, instant2);
    }

    //perform clustering
    HoodieClusteringJob.Config clusterConfig = buildHoodieClusteringUtilConfig(sourcePath1, true, "scheduleAndExecute", false);
    HoodieClusteringJob clusteringJob = new HoodieClusteringJob(jsc(), clusterConfig);
    clusteringJob.cluster(0);

    HoodieMetadataSync.Config cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath1;
    cfg.targetBasePath = targetPath;
    String latestCommit = sourceMetaClient1.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    HoodieMetadataSync metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath1).registerTempTable("srcTable1");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable1");

    Dataset<Row> srcDf1 = spark().sql("select * from srcTable1").drop("city_to_state");
    srcDf1.cache();
    Dataset<Row> tgtDf = spark().sql("select * from tgtTable1").drop("city_to_state");

    assertEquals(srcDf1.schema(), tgtDf.schema());
    assertTrue(srcDf1.except(tgtDf).isEmpty() && tgtDf.except(srcDf1).isEmpty());

    cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath2;
    cfg.targetBasePath = targetPath;
    latestCommit = sourceMetaClient2.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath2).registerTempTable("srcTable2");

    Dataset<Row> srcDf2 = spark().sql("select * from srcTable2").drop("city_to_state");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable2");

    Dataset<Row> tgtDf2 = spark().sql("select * from tgtTable2").drop("city_to_state");

    assertEquals(srcDf2.schema(), tgtDf2.schema());
    assertTrue(srcDf1.union(srcDf2).except(tgtDf2).isEmpty() && tgtDf2.except(srcDf1.union(srcDf2)).isEmpty());
    System.out.println("Done");
  }

  private HoodieClusteringJob.Config  buildHoodieClusteringUtilConfig(String basePath, boolean runSchedule, String runningMode, boolean isAutoClean) {
    HoodieClusteringJob.Config config = new HoodieClusteringJob.Config();
    config.basePath = basePath;
    config.runSchedule = runSchedule;
    config.runningMode = runningMode;
    config.configs.add("hoodie.metadata.enable=false");
    config.configs.add(String.format("%s=%s", HoodieCleanConfig.AUTO_CLEAN.key(), isAutoClean));
    config.configs.add(String.format("%s=%s", CLEANER_COMMITS_RETAINED.key(), 1));
    config.configs.add(String.format("%s=%s", HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), 1));
    config.configs.add(String.format("%s=%s", HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), 1));
    return config;
  }

  @Test
  void testHoodieMetadataSyncWithInsertOverwrite() throws Exception {
    sourcePath1 = Paths.get(basePath(), "source1").toString();
    sourcePath2 = Paths.get(basePath(), "source2").toString();

    Properties props = HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(new Properties())
        .build();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig1)) {
      String instant = writeClient.startCommit();
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_1});
      List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
      JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
      writeClient.insert(dataset, instant);

      String instant2 = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, sourceMetaClient1);
      records = dataGen.generateInserts(instant2, 10);
      dataset = jsc().parallelize(records);
      writeClient.insertOverwrite(dataset, instant2);
    }

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);

    HoodieWriteConfig writeConfig2 = getHoodieWriteConfig(sourcePath2);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig2)) {
      String instant = writeClient.startCommit();
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_2});
      List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
      JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
      writeClient.insert(dataset, instant);

      String instant2 = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, sourceMetaClient2);
      records = dataGen.generateInserts(instant2, 10);
      dataset = jsc().parallelize(records);
      writeClient.insertOverwrite(dataset, instant2);
    }


    HoodieMetadataSync.Config cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath1;
    cfg.targetBasePath = targetPath;
    String latestCommit = sourceMetaClient1.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    HoodieMetadataSync metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath1).registerTempTable("srcTable1");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable1");

    Dataset<Row> srcDf1 = spark().sql("select * from srcTable1").drop("city_to_state");
    srcDf1.cache();
    Dataset<Row> tgtDf = spark().sql("select * from tgtTable1").drop("city_to_state");

    assertEquals(srcDf1.schema(), tgtDf.schema());
    assertTrue(srcDf1.except(tgtDf).isEmpty() && tgtDf.except(srcDf1).isEmpty());

    cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath2;
    cfg.targetBasePath = targetPath;
    latestCommit = sourceMetaClient2.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath2).registerTempTable("srcTable2");

    Dataset<Row> srcDf2 = spark().sql("select * from srcTable2").drop("city_to_state");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable2");

    Dataset<Row> tgtDf2 = spark().sql("select * from tgtTable2").drop("city_to_state");

    assertEquals(srcDf2.schema(), tgtDf2.schema());
    assertTrue(srcDf1.union(srcDf2).except(tgtDf2).isEmpty() && tgtDf2.except(srcDf1.union(srcDf2)).isEmpty());
    System.out.println("Done");
  }

  @Test
  void testHoodieMetadataSyncWithDeletePartitions() throws Exception {
    sourcePath1 = Paths.get(basePath(), "source1").toString();
    sourcePath2 = Paths.get(basePath(), "source2").toString();

    Properties props = HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(new Properties())
        .build();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig1)) {
      String instant = writeClient.startCommit();
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_1});
      List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
      JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
      writeClient.insert(dataset, instant);

      instant = writeClient.startCommit();
      dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_3});
      records = dataGen.generateInserts(instant, 10);
      dataset = jsc().parallelize(records);
      writeClient.insert(dataset, instant);

      String instant3 = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, sourceMetaClient1);
      writeClient.deletePartitions(Collections.singletonList(PARTITION_PATH_1), instant3);
    }

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);

    HoodieWriteConfig writeConfig2 = getHoodieWriteConfig(sourcePath2);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig2)) {
      String instant = writeClient.startCommit();
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_2});
      List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
      JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
      writeClient.insert(dataset, instant);

      String instant2 = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, sourceMetaClient2);
      records = dataGen.generateInserts(instant2, 10);
      dataset = jsc().parallelize(records);
      writeClient.insertOverwrite(dataset, instant2);
    }


    HoodieMetadataSync.Config cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath1;
    cfg.targetBasePath = targetPath;
    String latestCommit = sourceMetaClient1.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    HoodieMetadataSync metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath1).registerTempTable("srcTable1");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable1");

    Dataset<Row> srcDf1 = spark().sql("select * from srcTable1").drop("city_to_state");
    srcDf1.cache();
    Dataset<Row> tgtDf = spark().sql("select * from tgtTable1").drop("city_to_state");

    assertEquals(srcDf1.schema(), tgtDf.schema());
    assertTrue(srcDf1.except(tgtDf).isEmpty() && tgtDf.except(srcDf1).isEmpty());

    cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath2;
    cfg.targetBasePath = targetPath;
    latestCommit = sourceMetaClient2.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath2).registerTempTable("srcTable2");

    Dataset<Row> srcDf2 = spark().sql("select * from srcTable2").drop("city_to_state");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable2");

    Dataset<Row> tgtDf2 = spark().sql("select * from tgtTable2").drop("city_to_state");

    assertEquals(srcDf2.schema(), tgtDf2.schema());
    assertTrue(srcDf1.union(srcDf2).except(tgtDf2).isEmpty() && tgtDf2.except(srcDf1.union(srcDf2)).isEmpty());
    System.out.println("Done");
  }

  @Test
  void testHoodieMetadataSyncWithDelete() throws Exception {
    sourcePath1 = Paths.get(basePath(), "source1").toString();
    sourcePath2 = Paths.get(basePath(), "source2").toString();

    Properties props = HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(new Properties())
        .build();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig1)) {
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_1});
      for (int i = 0; i < 2; i++) {
        String instant = writeClient.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
        JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
        writeClient.insert(dataset, instant);
      }

      for (int i = 0; i < 2; i++) {
        String instant = writeClient.startCommit();
        List<HoodieRecord> deletes = dataGen.generateDeletes(instant, 5);
        List<HoodieKey> keys = deletes.stream().map(HoodieRecord::getKey).collect(Collectors.toList());
        writeClient.delete(jsc().parallelize(keys), instant);
      }
    }

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);

    HoodieWriteConfig writeConfig2 = getHoodieWriteConfig(sourcePath2);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig2)) {
      String instant = writeClient.startCommit();
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_2});
      List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
      JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
      writeClient.insert(dataset, instant);

    }

    HoodieMetadataSync.Config cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath1;
    cfg.targetBasePath = targetPath;
    String latestCommit = sourceMetaClient1.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    HoodieMetadataSync metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath1).registerTempTable("srcTable1");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable1");

    Dataset<Row> srcDf1 = spark().sql("select * from srcTable1").drop("city_to_state");
    srcDf1.cache();
    Dataset<Row> tgtDf = spark().sql("select * from tgtTable1").drop("city_to_state");

    spark().read().format("hudi").load(sourcePath1 + "/.hoodie/metadata").registerTempTable("srcMetadata1");
    Dataset<Row> srcMdtDf = spark().sql("select key, filesystemMetadata from srcMetadata1 where type=2");

    spark().read().format("hudi").load(targetPath + "/.hoodie/metadata").registerTempTable("tgtMetadata");
    Dataset<Row> tgtMdtDf = spark().sql("select filesystemMetadata from tgtMetadata where type=2");

    srcMdtDf.show();
    tgtMdtDf.show();
    assertEquals(srcDf1.schema(), tgtDf.schema());
    assertTrue(srcDf1.except(tgtDf).isEmpty() && tgtDf.except(srcDf1).isEmpty());

    cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath2;
    cfg.targetBasePath = targetPath;
    latestCommit = sourceMetaClient2.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath2).registerTempTable("srcTable2");

    Dataset<Row> srcDf2 = spark().sql("select * from srcTable2").drop("city_to_state");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable2");

    Dataset<Row> tgtDf2 = spark().sql("select * from tgtTable2").drop("city_to_state");

    spark().read().format("hudi").load(sourcePath2 + "/.hoodie/metadata").registerTempTable("srcMetadata2");
    Dataset<Row> srcMdtDf2 = spark().sql("select key, filesystemMetadata from srcMetadata2 where type=2");

    spark().read().format("hudi").load(targetPath + "/.hoodie/metadata").registerTempTable("tgtMetadata2");
    Dataset<Row> tgtMdtDf2 = spark().sql("select key, filesystemMetadata from tgtMetadata2 where type=2");

    srcMdtDf2.show();
    tgtMdtDf2.show();
    assertEquals(srcDf2.schema(), tgtDf2.schema());
    assertTrue(srcDf1.union(srcDf2).except(tgtDf2).isEmpty() && tgtDf2.except(srcDf1.union(srcDf2)).isEmpty());
    System.out.println("Done");
  }

  @Test
  void testHoodieMetadataSyncWithClean() throws Exception {
    sourcePath1 = Paths.get(basePath(), "source1").toString();
    sourcePath2 = Paths.get(basePath(), "source2").toString();

    Properties props = HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(new Properties())
        .build();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig1)) {
      for (int i = 0; i < 5; i++) {
        String instant = writeClient.startCommit();
        HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_1});
        List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
        JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
        writeClient.insert(dataset, instant);
      }
    }

    HoodieWriteConfig cleanConfig = getHoodieCleanConfig(sourcePath1);
    try(SparkRDDWriteClient cleanClient = getHoodieWriteClient(cleanConfig)) {
      cleanClient.clean();
    }

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);

    HoodieWriteConfig writeConfig2 = getHoodieWriteConfig(sourcePath2);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig2)) {
      for (int i = 0; i < 2; i++) {
        String instant = writeClient.startCommit();
        HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_2});
        List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
        JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
        writeClient.insert(dataset, instant);
      }
    }

    cleanConfig = getHoodieCleanConfig(sourcePath2);
    try(SparkRDDWriteClient cleanClient = getHoodieWriteClient(cleanConfig)) {
      cleanClient.clean();
    }

    HoodieMetadataSync.Config cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath1;
    cfg.targetBasePath = targetPath;
    String latestCommit = sourceMetaClient1.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    HoodieMetadataSync metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath1).registerTempTable("srcTable1");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").option("hoodie.metadata.base.path.override", cfg.sourceBasePath).load(cfg.targetBasePath).registerTempTable("tgtTable1");

    Dataset<Row> srcDf1 = spark().sql("select * from srcTable1").drop("city_to_state");
    srcDf1.cache();
    Dataset<Row> tgtDf = spark().sql("select * from tgtTable1").drop("city_to_state");

    spark().read().format("hudi").load(sourcePath1 + "/.hoodie/metadata").registerTempTable("srcMetadata1");
    Dataset<Row> srcMdtDf = spark().sql("select key, filesystemMetadata from srcMetadata1 where type=2");

    spark().read().format("hudi").load(targetPath + "/.hoodie/metadata").registerTempTable("tgtMetadata");
    Dataset<Row> tgtMdtDf = spark().sql("select filesystemMetadata from tgtMetadata where type=2");

    srcMdtDf.show();
    tgtMdtDf.show();
    assertEquals(srcDf1.schema(), tgtDf.schema());
    assertTrue(srcDf1.except(tgtDf).isEmpty() && tgtDf.except(srcDf1).isEmpty());

    cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath2;
    cfg.targetBasePath = targetPath;
    latestCommit = sourceMetaClient2.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath2).registerTempTable("srcTable2");

    Dataset<Row> srcDf2 = spark().sql("select * from srcTable2").drop("city_to_state");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable2");

    Dataset<Row> tgtDf2 = spark().sql("select * from tgtTable2").drop("city_to_state");

    spark().read().format("hudi").load(sourcePath2 + "/.hoodie/metadata").registerTempTable("srcMetadata2");
    Dataset<Row> srcMdtDf2 = spark().sql("select key, filesystemMetadata from srcMetadata2 where type=2");

    spark().read().format("hudi").load(targetPath + "/.hoodie/metadata").registerTempTable("tgtMetadata2");
    Dataset<Row> tgtMdtDf2 = spark().sql("select key, filesystemMetadata from tgtMetadata2 where type=2");

    srcMdtDf2.show();
    tgtMdtDf2.show();
    assertEquals(srcDf2.schema(), tgtDf2.schema());
    assertTrue(srcDf1.union(srcDf2).except(tgtDf2).isEmpty() && tgtDf2.except(srcDf1.union(srcDf2)).isEmpty());
    System.out.println("Done");
  }

  @Test
  void testHoodieMetadataSyncWithMDTTableServices() throws Exception {
    sourcePath1 = Paths.get(basePath(), "source1").toString();
    sourcePath2 = Paths.get(basePath(), "source2").toString();

    Properties props = HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(new Properties())
        .build();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig1)) {
      for (int i = 0; i < 10; i++) {
        String instant = writeClient.startCommit();
        HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_1});
        List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
        JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
        writeClient.insert(dataset, instant);
      }
    }

    HoodieMetadataSync.Config cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath1;
    cfg.targetBasePath = targetPath;
    String latestCommit = sourceMetaClient1.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    cfg.performTableMaintenance = true;
    HoodieMetadataSync metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath1).registerTempTable("srcTable1");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable1");

    Dataset<Row> srcDf1 = spark().sql("select * from srcTable1").drop("city_to_state");
    srcDf1.cache();
    Dataset<Row> tgtDf = spark().sql("select * from tgtTable1").drop("city_to_state");

    spark().read().format("hudi").load(sourcePath1 + "/.hoodie/metadata").registerTempTable("srcMetadata1");
    Dataset<Row> srcMdtDf = spark().sql("select key, filesystemMetadata from srcMetadata1 where type=2");

    spark().read().format("hudi").load(targetPath + "/.hoodie/metadata").registerTempTable("tgtMetadata");
    Dataset<Row> tgtMdtDf = spark().sql("select filesystemMetadata from tgtMetadata where type=2");

    srcMdtDf.show();
    tgtMdtDf.show();
    assertEquals(srcDf1.schema(), tgtDf.schema());
    assertTrue(srcDf1.except(tgtDf).isEmpty() && tgtDf.except(srcDf1).isEmpty());
  }

  @Test
  void testHoodieMetadataSync_ClusteringCommitIsNotArchived() throws Exception {
    sourcePath1 = Paths.get(basePath(), "source1").toString();
    sourcePath2 = Paths.get(basePath(), "source2").toString();

    Properties props = HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(new Properties())
        .build();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig1)) {
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_1});
      for (int i = 0; i < 2; i++) {
        String instant = writeClient.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
        JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
        writeClient.insert(dataset, instant);
      }

      HoodieClusteringJob.Config clusterConfig = buildHoodieClusteringUtilConfig(sourcePath1, true, "scheduleAndExecute", false);
      HoodieClusteringJob clusteringJob = new HoodieClusteringJob(jsc(), clusterConfig);
      clusteringJob.cluster(0);

      for (int i = 0; i < 8; i++) {
        String instant = writeClient.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
        JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
        writeClient.insert(dataset, instant);
      }
    }

    HoodieMetadataSync.Config cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath1;
    cfg.targetBasePath = targetPath;
    String latestCommit = sourceMetaClient1.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    cfg.performTableMaintenance = true;
    cfg.configs = Arrays.asList("");
    HoodieMetadataSync metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath1).registerTempTable("srcTable1");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable1");

    Dataset<Row> srcDf1 = spark().sql("select * from srcTable1").drop("city_to_state");
    srcDf1.cache();
    Dataset<Row> tgtDf = spark().sql("select * from tgtTable1").drop("city_to_state");

    spark().read().format("hudi").load(sourcePath1 + "/.hoodie/metadata").registerTempTable("srcMetadata1");
    Dataset<Row> srcMdtDf = spark().sql("select key, filesystemMetadata from srcMetadata1 where type=2");

    spark().read().format("hudi").load(targetPath + "/.hoodie/metadata").registerTempTable("tgtMetadata");
    Dataset<Row> tgtMdtDf = spark().sql("select filesystemMetadata from tgtMetadata where type=2");

    srcMdtDf.show();
    tgtMdtDf.show();
    assertEquals(srcDf1.schema(), tgtDf.schema());
    assertTrue(srcDf1.except(tgtDf).isEmpty() && tgtDf.except(srcDf1).isEmpty());
  }


  @Test
  void testHoodieMetadataSync_Bootstrap() throws Exception {
    sourcePath1 = Paths.get(basePath(), "source1").toString();
    sourcePath2 = Paths.get(basePath(), "source2").toString();

    Properties props = HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(new Properties())
        .build();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_1});
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig1)) {
      for (int i = 0; i < 3; i++) {
        String instant = writeClient.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
        JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
        writeClient.insert(dataset, instant);
      }
    }

    //perform clustering
    HoodieClusteringJob.Config clusterConfig = buildHoodieClusteringUtilConfig(sourcePath1, true, "scheduleAndExecute", false);
    HoodieClusteringJob clusteringJob = new HoodieClusteringJob(jsc(), clusterConfig);
    clusteringJob.cluster(0);

    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig1)) {
      for (int i = 0; i < 1; i++) {
        String instant = writeClient.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
        JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
        writeClient.insert(dataset, instant);
      }
    }

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);

    dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_2});
    HoodieWriteConfig writeConfig2 = getHoodieWriteConfig(sourcePath2);
    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig2)) {
      for (int i = 0; i < 1; i++) {
        String instant = writeClient.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
        JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
        writeClient.insert(dataset, instant);
      }
    }

    HoodieMetadataSync.Config cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath1;
    cfg.targetBasePath = targetPath;
    String latestCommit = sourceMetaClient1.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    cfg.boostrap = true;
    HoodieMetadataSync metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath1).registerTempTable("srcTable1");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable1");

    Dataset<Row> srcDf1 = spark().sql("select * from srcTable1").drop("city_to_state");
    srcDf1.cache();
    Dataset<Row> tgtDf = spark().sql("select * from tgtTable1").drop("city_to_state");

    spark().read().format("hudi").load(sourcePath1 + "/.hoodie/metadata").registerTempTable("srcMetadata1");
    Dataset<Row> srcMdtDf = spark().sql("select key, filesystemMetadata from srcMetadata1 where type=2");

    spark().read().format("hudi").load(targetPath + "/.hoodie/metadata").registerTempTable("tgtMetadata");
    Dataset<Row> tgtMdtDf = spark().sql("select filesystemMetadata from tgtMetadata where type=2");

    srcMdtDf.show();
    tgtMdtDf.show();
    assertEquals(srcDf1.schema(), tgtDf.schema());
    assertTrue(srcDf1.except(tgtDf).isEmpty() && tgtDf.except(srcDf1).isEmpty());

    cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath2;
    cfg.targetBasePath = targetPath;
    latestCommit = sourceMetaClient2.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    cfg.boostrap = true;
    metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath2).registerTempTable("srcTable2");

    Dataset<Row> srcDf2 = spark().sql("select * from srcTable2").drop("city_to_state");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable2");

    Dataset<Row> tgtDf2 = spark().sql("select * from tgtTable2").drop("city_to_state");

    spark().read().format("hudi").load(sourcePath2 + "/.hoodie/metadata").registerTempTable("srcMetadata2");
    Dataset<Row> srcMdtDf2 = spark().sql("select key, filesystemMetadata from srcMetadata2 where type=2");

    spark().read().format("hudi").load(targetPath + "/.hoodie/metadata").registerTempTable("tgtMetadata2");
    Dataset<Row> tgtMdtDf2 = spark().sql("select * from tgtMetadata2 where type=2");

    srcMdtDf2.show();
    tgtMdtDf2.show();
    assertEquals(srcDf2.schema(), tgtDf2.schema());
    assertTrue(srcDf1.union(srcDf2).except(tgtDf2).isEmpty() && tgtDf2.except(srcDf1.union(srcDf2)).isEmpty());
    System.out.println("Done");

    try(SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig2)) {
      for (int i = 0; i < 2; i++) {
        String instant = writeClient.startCommit();
        List<HoodieRecord> records = dataGen.generateUpdates(instant, 10);
        JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
        writeClient.insert(dataset, instant);
      }
    }

    cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath2;
    cfg.targetBasePath = targetPath;
    latestCommit = sourceMetaClient2.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    cfg.boostrap = false;
    metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath2).registerTempTable("srcTable3");

    Dataset<Row> srcDf3 = spark().sql("select * from srcTable3").drop("city_to_state");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tgtTable3");

    Dataset<Row> tgtDf3 = spark().sql("select * from tgtTable3").drop("city_to_state");

    spark().read().format("hudi").load(sourcePath2 + "/.hoodie/metadata").registerTempTable("srcMetadata3");
    Dataset<Row> srcMdtDf3 = spark().sql("select key, filesystemMetadata from srcMetadata3 where type = 2");

    spark().read().format("hudi").load(targetPath + "/.hoodie/metadata").registerTempTable("tgtMetadata3");
    Dataset<Row> tgtMdtDf3 = spark().sql("select * from tgtMetadata3 where type = 2");

    srcMdtDf2.show();
    tgtMdtDf2.show();
    assertEquals(srcDf3.schema(), tgtDf3.schema());
    assertTrue(srcDf1.union(srcDf3).except(tgtDf3).isEmpty() && tgtDf3.except(srcDf1.union(srcDf3)).isEmpty());
    System.out.println("Done");
  }

  private HoodieWriteConfig getHoodieCleanConfig(String basePath) {
    Properties props = new Properties();
    props.put(CLEANER_COMMITS_RETAINED.key(), "2");
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withEmbeddedTimelineServerEnabled(false)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withDeleteParallelism(2)
        .withCleanConfig(
            HoodieCleanConfig.newBuilder()
                .retainCommits(2)
                .withAsyncClean(false)
                .build())
        .forTable(TABLE_NAME)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withProps(props)
        .build();
  }
}
