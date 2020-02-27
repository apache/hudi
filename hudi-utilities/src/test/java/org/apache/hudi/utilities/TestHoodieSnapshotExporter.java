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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieTestUtils;
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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestHoodieSnapshotExporter {
  private static String TEST_WRITE_TOKEN = "1-0-1";

  public SparkSession spark = null;
  public HoodieTestDataGenerator dataGen = null;
  public String basePath = null;
  public String outputPath = null;
  public String rootPath = null;
  public FileSystem fs = null;
  public Map commonOpts;
  public HoodieSnapshotExporter.Config cfg;
  private JavaSparkContext jsc = null;

  @Before
  public void initialize() throws IOException {
    spark = SparkSession.builder()
        .appName("Hoodie Datasource test")
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate();
    jsc = new JavaSparkContext(spark.sparkContext());
    dataGen = new HoodieTestDataGenerator();
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    basePath = folder.getRoot().getAbsolutePath();
    fs = FSUtils.getFs(basePath, spark.sparkContext().hadoopConfiguration());
    commonOpts = new HashMap();

    commonOpts.put("hoodie.insert.shuffle.parallelism", "4");
    commonOpts.put("hoodie.upsert.shuffle.parallelism", "4");
    commonOpts.put(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key");
    commonOpts.put(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition");
    commonOpts.put(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp");
    commonOpts.put(HoodieWriteConfig.TABLE_NAME, "hoodie_test");


    cfg = new HoodieSnapshotExporter.Config();

    cfg.basePath = basePath;
    cfg.outputPath = outputPath = basePath + "/target";
    cfg.outputFormat = "json";
    cfg.outputPartitionField = "partition";

  }

  @After
  public void cleanup() throws Exception {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testSnapshotExporter() throws IOException {
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

    long targetCount = spark.read().json(outputPath).count();

    assertTrue(sourceCount == targetCount);

    // Test snapshotPrefix
    long filterCount = inputDF.where("partition == '2015/03/16'").count();
    cfg.snapshotPrefix = "2015/03/16";
    hoodieSnapshotExporter.export(spark, cfg);
    long targetFilterCount = spark.read().json(outputPath).count();
    assertTrue(filterCount == targetFilterCount);

  }

  // for testEmptySnapshotCopy
  public void init() throws IOException {
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    rootPath = "file://" + folder.getRoot().getAbsolutePath();
    basePath = rootPath + "/" + HoodieTestUtils.RAW_TRIPS_TEST_NAME;
    outputPath = rootPath + "/output";

    final Configuration hadoopConf = HoodieTestUtils.getDefaultHadoopConf();
    fs = FSUtils.getFs(basePath, hadoopConf);
    HoodieTestUtils.init(hadoopConf, basePath);
  }

  @Test
  public void testEmptySnapshotCopy() throws IOException {
    init();
    // There is no real data (only .hoodie directory)
    assertEquals(fs.listStatus(new Path(basePath)).length, 1);
    assertFalse(fs.exists(new Path(outputPath)));

    // Do the snapshot
    HoodieSnapshotCopier copier = new HoodieSnapshotCopier();
    copier.snapshot(jsc, basePath, outputPath, true);

    // Nothing changed; we just bail out
    assertEquals(fs.listStatus(new Path(basePath)).length, 1);
    assertFalse(fs.exists(new Path(outputPath + "/_SUCCESS")));
  }

  // TODO - uncomment this after fixing test failures
  // @Test
  public void testSnapshotCopy() throws Exception {
    // Generate some commits and corresponding parquets
    String commitTime1 = "20160501010101";
    String commitTime2 = "20160502020601";
    String commitTime3 = "20160506030611";
    new File(basePath + "/.hoodie").mkdirs();
    new File(basePath + "/.hoodie/hoodie.properties").createNewFile();
    // Only first two have commit files
    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".inflight").createNewFile();

    // Some parquet files
    new File(basePath + "/2016/05/01/").mkdirs();
    new File(basePath + "/2016/05/02/").mkdirs();
    new File(basePath + "/2016/05/06/").mkdirs();
    HoodieTestDataGenerator.writePartitionMetadata(fs, new String[]{"2016/05/01", "2016/05/02", "2016/05/06"},
        basePath);
    // Make commit1
    File file11 = new File(basePath + "/2016/05/01/" + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, "id11"));
    file11.createNewFile();
    File file12 = new File(basePath + "/2016/05/02/" + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, "id12"));
    file12.createNewFile();
    File file13 = new File(basePath + "/2016/05/06/" + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, "id13"));
    file13.createNewFile();

    // Make commit2
    File file21 = new File(basePath + "/2016/05/01/" + FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, "id21"));
    file21.createNewFile();
    File file22 = new File(basePath + "/2016/05/02/" + FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, "id22"));
    file22.createNewFile();
    File file23 = new File(basePath + "/2016/05/06/" + FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, "id23"));
    file23.createNewFile();

    // Make commit3
    File file31 = new File(basePath + "/2016/05/01/" + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, "id31"));
    file31.createNewFile();
    File file32 = new File(basePath + "/2016/05/02/" + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, "id32"));
    file32.createNewFile();
    File file33 = new File(basePath + "/2016/05/06/" + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, "id33"));
    file33.createNewFile();

    // Do a snapshot copy
    HoodieSnapshotCopier copier = new HoodieSnapshotCopier();
    copier.snapshot(jsc, basePath, outputPath, false);

    // Check results
    assertTrue(fs.exists(new Path(outputPath + "/2016/05/01/" + file11.getName())));
    assertTrue(fs.exists(new Path(outputPath + "/2016/05/02/" + file12.getName())));
    assertTrue(fs.exists(new Path(outputPath + "/2016/05/06/" + file13.getName())));
    assertTrue(fs.exists(new Path(outputPath + "/2016/05/01/" + file21.getName())));
    assertTrue(fs.exists(new Path(outputPath + "/2016/05/02/" + file22.getName())));
    assertTrue(fs.exists(new Path(outputPath + "/2016/05/06/" + file23.getName())));
    assertFalse(fs.exists(new Path(outputPath + "/2016/05/01/" + file31.getName())));
    assertFalse(fs.exists(new Path(outputPath + "/2016/05/02/" + file32.getName())));
    assertFalse(fs.exists(new Path(outputPath + "/2016/05/06/" + file33.getName())));

    assertTrue(fs.exists(new Path(outputPath + "/.hoodie/" + commitTime1 + ".commit")));
    assertTrue(fs.exists(new Path(outputPath + "/.hoodie/" + commitTime2 + ".commit")));
    assertFalse(fs.exists(new Path(outputPath + "/.hoodie/" + commitTime3 + ".commit")));
    assertFalse(fs.exists(new Path(outputPath + "/.hoodie/" + commitTime3 + ".inflight")));
    assertTrue(fs.exists(new Path(outputPath + "/.hoodie/hoodie.properties")));

    assertTrue(fs.exists(new Path(outputPath + "/_SUCCESS")));
  }

}
