package org.apache.hudi.utilities;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class TestHoodieSnapshotExporter {
  public SparkSession spark = null;
  public HoodieTestDataGenerator dataGen = null;
  public String basePath = null;
  public FileSystem fs = null;
  public Map commonOpts;
  public HoodieSnapshotExporter.Config cfg;

  @Before
  public void initialize() throws IOException {
    spark = SparkSession.builder()
        .appName("Hoodie Datasource test")
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate();
    dataGen = new HoodieTestDataGenerator();
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    basePath = folder.getRoot().getAbsolutePath();
    fs = FSUtils.getFs(basePath, spark.sparkContext().hadoopConfiguration());
    commonOpts = new HashMap();
    {
      commonOpts.put("hoodie.insert.shuffle.parallelism", "4");
      commonOpts.put("hoodie.upsert.shuffle.parallelism", "4");
      commonOpts.put(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key");
      commonOpts.put(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition");
      commonOpts.put(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp");
      commonOpts.put(HoodieWriteConfig.TABLE_NAME, "hoodie_test");
    }

    cfg = new HoodieSnapshotExporter.Config();
    {
      cfg.sourceBasePath = basePath;
      cfg.targetBasePath = basePath + "/target";
      cfg.outputFormat = "json";
      cfg.outputPartitionField = "partition";
    }
  }

  @After
  public void teardown() throws Exception {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testExporter() throws IOException {
    // Insert Operation
    List<String> records = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("000", 100));
    Dataset<Row> inputDF = spark.read().json(new JavaSparkContext(spark.sparkContext()).parallelize(records, 2));
    inputDF.write().format("hudi")
        .options(commonOpts)
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL())
        .mode(SaveMode.Overwrite)
        .save(basePath);
    long sourceCount = inputDF.count();

    HoodieSnapshotExporter hoodieSnapshotExporter = new HoodieSnapshotExporter();
    hoodieSnapshotExporter.export(spark, cfg);

    long targetCount = spark.read().json(cfg.targetBasePath).count();

    assertTrue(sourceCount == targetCount);
  }

  @Test
  public void testConsumerPartitioner() throws IOException {
    cfg.outputPartitioner = "";
    // Insert Operation
    List<String> records = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("000", 100));
    Dataset<Row> inputDF = spark.read().json(new JavaSparkContext(spark.sparkContext()).parallelize(records, 2));
    inputDF.write().format("hudi")
        .options(commonOpts)
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL())
        .mode(SaveMode.Overwrite)
        .save(basePath);
    long sourceCount = inputDF.count();

    HoodieSnapshotExporter hoodieSnapshotExporter = new HoodieSnapshotExporter();
    hoodieSnapshotExporter.export(spark, cfg);

    long targetCount = spark.read().json(cfg.targetBasePath).count();

    assertTrue(sourceCount == targetCount);
  }


}
