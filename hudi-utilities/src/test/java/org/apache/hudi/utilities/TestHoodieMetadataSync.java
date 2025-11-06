package org.apache.hudi.utilities;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieMetadataSync extends SparkClientFunctionalTestHarness implements SparkProvider {

  static final Logger LOG = LoggerFactory.getLogger(TestHoodieMetadataSync.class);
  static final int NUM_RECORDS = 100;
  static final String COMMIT_TIME = "20200101000000";
  static final String PARTITION_PATH = "2020";
  static final String TABLE_NAME = "testing";

  private static final HoodieTestDataGenerator DATA_GENERATOR = new HoodieTestDataGenerator(0L);
  private HoodieTableMetaClient metaClient;
  private HoodieTableMetaClient sourceMetaClient;
  private HoodieTableMetaClient sourceMetaClient2;

  String sourcePath;
  String sourcePath2;
  String targetPath;

  @BeforeEach
  public void init() throws IOException {
    this.metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);

    // Initialize test data dirs
    sourcePath = Paths.get(basePath(), "source").toString();
    sourcePath2 = Paths.get(basePath(), "source_2").toString();
    targetPath = Paths.get(basePath(), "target").toString();

    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(TABLE_NAME)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(jsc().hadoopConfiguration(), sourcePath);

    // Prepare data as source Hudi dataset
    HoodieWriteConfig cfg = getHoodieWriteConfig(sourcePath);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(cfg)) {
      String commitTime = writeClient.startCommit();
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH});
      List<HoodieRecord> records = dataGen.generateInserts(commitTime, NUM_RECORDS);
      JavaRDD<HoodieRecord> recordsRDD = jsc().parallelize(records, 1);
      writeClient.bulkInsert(recordsRDD, commitTime);
    }

    sourceMetaClient = HoodieTableMetaClient.builder().setBasePath(sourcePath).setConf(jsc().hadoopConfiguration()).build();

    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(TABLE_NAME)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(jsc().hadoopConfiguration(), sourcePath2);

    // Prepare data as source Hudi dataset
    cfg = getHoodieWriteConfig(sourcePath2);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(cfg)) {
      String commitTime = writeClient.startCommit();
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH});
      List<HoodieRecord> records = dataGen.generateInserts(commitTime, NUM_RECORDS);
      JavaRDD<HoodieRecord> recordsRDD = jsc().parallelize(records, 1);
      writeClient.bulkInsert(recordsRDD, commitTime);
    }

    sourceMetaClient2 = HoodieTableMetaClient.builder().setBasePath(sourcePath2).setConf(jsc().hadoopConfiguration()).build();
  }

  private HoodieWriteConfig getHoodieWriteConfig(String basePath) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withEmbeddedTimelineServerEnabled(false)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withDeleteParallelism(2)
        .forTable(TABLE_NAME)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .build();
  }

  @AfterAll
  public static void cleanup() {
    DATA_GENERATOR.close();
  }

  @Test
  public void simpleTest() throws Exception {
    HoodieMetadataSync.Config cfg = new HoodieMetadataSync.Config();
    cfg.sourceBasePath = sourcePath;
    cfg.targetBasePath = targetPath;
    String latestCommit = sourceMetaClient.reloadActiveTimeline().lastInstant().get().getTimestamp();
    cfg.commitToSync = latestCommit;
    cfg.targetTableName = TABLE_NAME;
    cfg.sparkMaster = "local[2]";
    cfg.sparkMemory = "1g";
    HoodieMetadataSync metadataSync = new HoodieMetadataSync(jsc(), cfg);
    metadataSync.run();

    spark().read().format("hudi").load(sourcePath).registerTempTable("sTable1");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(cfg.targetBasePath).registerTempTable("tTable1");

    Dataset<Row> sDf1 = spark().sql("select * from sTable1").drop("city_to_state");
    sDf1.cache();

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

    Dataset<Row> sDf2 = spark().sql("select * from sTable2").drop("city_to_state");

    Dataset<Row> tDf2 = spark().sql("select * from tTable1").drop("city_to_state");

    assertEquals(0, sDf1.union(sDf2).except(tDf2).count());
    assertEquals(0, tDf.except(sDf1.union(sDf2)).count());
    System.out.println("Asdfasd");
  }

  @Test
  public void testReads() {
    String tPath = "/tmp/target_tbl/";
    String sPath = "/tmp/source_tbl/";

    spark().read().format("hudi").load(tPath).registerTempTable("sTbl");

    spark().read().format("hudi").option("hoodie.metadata.enable","true")
        .option("hoodie.metadata.enable.base.path.for.partitions","true").load(tPath).registerTempTable("tTbl");

    spark().sql("select * from sTbl limit 2").show(false);

    spark().sql("select * from tTbl limit 2").show(false);

  }
}
