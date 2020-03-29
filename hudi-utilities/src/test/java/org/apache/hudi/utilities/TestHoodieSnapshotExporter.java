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

import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.common.HoodieClientTestHarness;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.config.HoodieIndexConfig;
import org.apache.hudi.common.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.utilities.HoodieSnapshotExporter.Config;
import org.apache.hudi.utilities.HoodieSnapshotExporter.OutputFormatValidator;
import org.apache.hudi.utilities.HoodieSnapshotExporter.Partitioner;
import org.apache.hudi.utilities.exception.HoodieSnapshotExporterException;

import com.beust.jcommander.ParameterException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Enclosed.class)
public class TestHoodieSnapshotExporter {

  static class ExporterTestHarness extends HoodieClientTestHarness {

    static final Logger LOG = LogManager.getLogger(ExporterTestHarness.class);
    static final int NUM_RECORDS = 100;
    static final String COMMIT_TIME = "20200101000000";
    static final String PARTITION_PATH = "2020";
    static final String TABLE_NAME = "testing";
    String sourcePath;
    String targetPath;

    @Before
    public void setUp() throws Exception {
      initSparkContexts();
      initDFS();
      dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH});

      // Initialize test data dirs
      sourcePath = dfsBasePath + "/source/";
      targetPath = dfsBasePath + "/target/";
      dfs.mkdirs(new Path(sourcePath));
      HoodieTableMetaClient
          .initTableType(jsc.hadoopConfiguration(), sourcePath, HoodieTableType.COPY_ON_WRITE, TABLE_NAME,
              HoodieAvroPayload.class.getName());

      // Prepare data as source Hudi dataset
      HoodieWriteConfig cfg = getHoodieWriteConfig(sourcePath);
      HoodieWriteClient hdfsWriteClient = new HoodieWriteClient(jsc, cfg);
      hdfsWriteClient.startCommitWithTime(COMMIT_TIME);
      List<HoodieRecord> records = dataGen.generateInserts(COMMIT_TIME, NUM_RECORDS);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      hdfsWriteClient.bulkInsert(recordsRDD, COMMIT_TIME);
      hdfsWriteClient.close();

      RemoteIterator<LocatedFileStatus> itr = dfs.listFiles(new Path(sourcePath), true);
      while (itr.hasNext()) {
        LOG.info(">>> Prepared test file: " + itr.next().getPath());
      }
    }

    @After
    public void tearDown() throws Exception {
      cleanupSparkContexts();
      cleanupDFS();
      cleanupTestDataGenerator();
    }

    private HoodieWriteConfig getHoodieWriteConfig(String basePath) {
      return HoodieWriteConfig.newBuilder()
          .withPath(basePath)
          .withEmbeddedTimelineServerEnabled(false)
          .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
          .withParallelism(2, 2)
          .withBulkInsertParallelism(2)
          .forTable(TABLE_NAME)
          .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(IndexType.BLOOM).build())
          .build();
    }
  }

  public static class TestHoodieSnapshotExporterForHudi extends ExporterTestHarness {

    private HoodieSnapshotExporter.Config cfg;

    @Before
    public void setUp() throws Exception {
      super.setUp();
      cfg = new Config();
      cfg.sourceBasePath = sourcePath;
      cfg.targetOutputPath = targetPath;
      cfg.outputFormat = OutputFormatValidator.HUDI;
    }

    @Test
    public void testExportAsHudi() throws IOException {
      new HoodieSnapshotExporter().export(jsc, cfg);

      // Check results
      assertTrue(dfs.exists(new Path(targetPath + "/.hoodie/" + COMMIT_TIME + ".clean")));
      assertTrue(dfs.exists(new Path(targetPath + "/.hoodie/" + COMMIT_TIME + ".clean.inflight")));
      assertTrue(dfs.exists(new Path(targetPath + "/.hoodie/" + COMMIT_TIME + ".clean.requested")));
      assertTrue(dfs.exists(new Path(targetPath + "/.hoodie/" + COMMIT_TIME + ".commit")));
      assertTrue(dfs.exists(new Path(targetPath + "/.hoodie/" + COMMIT_TIME + ".commit.requested")));
      assertTrue(dfs.exists(new Path(targetPath + "/.hoodie/" + COMMIT_TIME + ".inflight")));
      assertTrue(dfs.exists(new Path(targetPath + "/.hoodie/hoodie.properties")));
      String partition = targetPath + "/" + PARTITION_PATH;
      long numParquetFiles = Arrays.stream(dfs.listStatus(new Path(partition)))
          .filter(fileStatus -> fileStatus.getPath().toString().endsWith(".parquet"))
          .count();
      assertTrue("There should exist at least 1 parquet file.", numParquetFiles >= 1);
      assertEquals(NUM_RECORDS, sqlContext.read().parquet(partition).count());
      assertTrue(dfs.exists(new Path(partition + "/.hoodie_partition_metadata")));
      assertTrue(dfs.exists(new Path(targetPath + "/_SUCCESS")));
    }
  }

  public static class TestHoodieSnapshotExporterForEarlyAbort extends ExporterTestHarness {

    private HoodieSnapshotExporter.Config cfg;

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
      super.setUp();
      cfg = new Config();
      cfg.sourceBasePath = sourcePath;
      cfg.targetOutputPath = targetPath;
      cfg.outputFormat = OutputFormatValidator.HUDI;
    }

    @Test
    public void testExportWhenTargetPathExists() throws IOException {
      // make target output path present
      dfs.mkdirs(new Path(targetPath));

      // export
      exceptionRule.expect(HoodieSnapshotExporterException.class);
      exceptionRule.expectMessage("The target output path already exists.");
      new HoodieSnapshotExporter().export(jsc, cfg);
    }

    @Test
    public void testExportDatasetWithNoCommit() throws IOException {
      // delete commit files
      List<Path> commitFiles = Arrays.stream(dfs.listStatus(new Path(sourcePath + "/.hoodie")))
          .map(FileStatus::getPath)
          .filter(filePath -> filePath.getName().endsWith(".commit"))
          .collect(Collectors.toList());
      for (Path p : commitFiles) {
        dfs.delete(p, false);
      }

      // export
      exceptionRule.expect(HoodieSnapshotExporterException.class);
      exceptionRule.expectMessage("No commits present. Nothing to snapshot.");
      new HoodieSnapshotExporter().export(jsc, cfg);
    }

    @Test
    public void testExportDatasetWithNoPartition() throws IOException {
      // delete all source data
      dfs.delete(new Path(sourcePath + "/" + PARTITION_PATH), true);

      // export
      exceptionRule.expect(HoodieSnapshotExporterException.class);
      exceptionRule.expectMessage("The source dataset has 0 partition to snapshot.");
      new HoodieSnapshotExporter().export(jsc, cfg);
    }
  }

  @RunWith(Parameterized.class)
  public static class TestHoodieSnapshotExporterForNonHudi extends ExporterTestHarness {

    @Parameters
    public static Iterable<String[]> formats() {
      return Arrays.asList(new String[][] {{"json"}, {"parquet"}});
    }

    @Parameter
    public String format;

    @Test
    public void testExportAsNonHudi() throws IOException {
      HoodieSnapshotExporter.Config cfg = new Config();
      cfg.sourceBasePath = sourcePath;
      cfg.targetOutputPath = targetPath;
      cfg.outputFormat = format;
      new HoodieSnapshotExporter().export(jsc, cfg);
      assertEquals(NUM_RECORDS, sqlContext.read().format(format).load(targetPath).count());
      assertTrue(dfs.exists(new Path(targetPath + "/_SUCCESS")));
    }
  }

  public static class TestHoodieSnapshotExporterForRepartitioning extends ExporterTestHarness {

    private static final String PARTITION_NAME = "year";

    public static class UserDefinedPartitioner implements Partitioner {

      @Override
      public DataFrameWriter<Row> partition(Dataset<Row> source) {
        return source
            .withColumnRenamed(HoodieRecord.PARTITION_PATH_METADATA_FIELD, PARTITION_NAME)
            .repartition(new Column(PARTITION_NAME))
            .write()
            .partitionBy(PARTITION_NAME);
      }
    }

    private HoodieSnapshotExporter.Config cfg;

    @Before
    public void setUp() throws Exception {
      super.setUp();
      cfg = new Config();
      cfg.sourceBasePath = sourcePath;
      cfg.targetOutputPath = targetPath;
      cfg.outputFormat = "json";
    }

    @Test
    public void testExportWithPartitionField() throws IOException {
      // `driver` field is set in HoodieTestDataGenerator
      cfg.outputPartitionField = "driver";
      new HoodieSnapshotExporter().export(jsc, cfg);

      assertEquals(NUM_RECORDS, sqlContext.read().format("json").load(targetPath).count());
      assertTrue(dfs.exists(new Path(targetPath + "/_SUCCESS")));
      assertTrue(dfs.listStatus(new Path(targetPath)).length > 1);
    }

    @Test
    public void testExportForUserDefinedPartitioner() throws IOException {
      cfg.outputPartitioner = UserDefinedPartitioner.class.getName();
      new HoodieSnapshotExporter().export(jsc, cfg);

      assertEquals(NUM_RECORDS, sqlContext.read().format("json").load(targetPath).count());
      assertTrue(dfs.exists(new Path(targetPath + "/_SUCCESS")));
      assertTrue(dfs.exists(new Path(String.format("%s/%s=%s", targetPath, PARTITION_NAME, PARTITION_PATH))));
    }
  }

  @RunWith(Parameterized.class)
  public static class TestHoodieSnapshotExporterInputValidation {

    @Parameters
    public static Iterable<Object[]> data() {
      return Arrays.asList(new Object[][] {
          {"json", true}, {"parquet", true}, {"hudi", true},
          {"JSON", false}, {"foo", false}, {null, false}, {"", false}
      });
    }

    @Parameter
    public String format;
    @Parameter(1)
    public boolean isValid;

    @Test
    public void testValidateOutputFormat() {
      Throwable t = null;
      try {
        new OutputFormatValidator().validate(null, format);
      } catch (Exception e) {
        t = e;
      }
      if (isValid) {
        assertNull(t);
      } else {
        assertTrue(t instanceof ParameterException);
      }
    }
  }
}
