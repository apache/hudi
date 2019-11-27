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

import org.apache.hudi.HoodieReadClient;
import org.apache.hudi.HoodieWriteClient;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.minicluster.HdfsTestService;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHDFSParquetImporter implements Serializable {

  private static String dfsBasePath;
  private static HdfsTestService hdfsTestService;
  private static MiniDFSCluster dfsCluster;
  private static DistributedFileSystem dfs;

  @BeforeClass
  public static void initClass() throws Exception {
    hdfsTestService = new HdfsTestService();
    dfsCluster = hdfsTestService.start(true);

    // Create a temp folder as the base path
    dfs = dfsCluster.getFileSystem();
    dfsBasePath = dfs.getWorkingDirectory().toString();
    dfs.mkdirs(new Path(dfsBasePath));
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
    }
  }

  /**
   * Test successful data import with retries.
   */
  @Test
  public void testDatasetImportWithRetries() throws Exception {
    JavaSparkContext jsc = null;
    try {
      jsc = getJavaSparkContext();

      // Test root folder.
      String basePath = (new Path(dfsBasePath, Thread.currentThread().getStackTrace()[1].getMethodName())).toString();

      // Hoodie root folder
      Path hoodieFolder = new Path(basePath, "testTarget");

      // Create schema file.
      String schemaFile = new Path(basePath, "file.schema").toString();

      // Create generic records.
      Path srcFolder = new Path(basePath, "testSrc");
      createRecords(srcFolder);

      HDFSParquetImporter.Config cfg = getHDFSParquetImporterConfig(srcFolder.toString(), hoodieFolder.toString(),
          "testTable", "COPY_ON_WRITE", "_row_key", "timestamp", 1, schemaFile);
      AtomicInteger retry = new AtomicInteger(3);
      AtomicInteger fileCreated = new AtomicInteger(0);
      HDFSParquetImporter dataImporter = new HDFSParquetImporter(cfg) {
        @Override
        protected int dataImport(JavaSparkContext jsc) throws IOException {
          int ret = super.dataImport(jsc);
          if (retry.decrementAndGet() == 0) {
            fileCreated.incrementAndGet();
            createSchemaFile(schemaFile);
          }

          return ret;
        }
      };
      // Schema file is not created so this operation should fail.
      assertEquals(0, dataImporter.dataImport(jsc, retry.get()));
      assertEquals(retry.get(), -1);
      assertEquals(fileCreated.get(), 1);

      // Check if
      // 1. .commit file is present
      // 2. number of records in each partition == 24
      // 3. total number of partitions == 4;
      boolean isCommitFilePresent = false;
      Map<String, Long> recordCounts = new HashMap<String, Long>();
      RemoteIterator<LocatedFileStatus> hoodieFiles = dfs.listFiles(hoodieFolder, true);
      while (hoodieFiles.hasNext()) {
        LocatedFileStatus f = hoodieFiles.next();
        isCommitFilePresent = isCommitFilePresent || f.getPath().toString().endsWith(HoodieTimeline.COMMIT_EXTENSION);

        if (f.getPath().toString().endsWith("parquet")) {
          SQLContext sc = new SQLContext(jsc);
          String partitionPath = f.getPath().getParent().toString();
          long count = sc.read().parquet(f.getPath().toString()).count();
          if (!recordCounts.containsKey(partitionPath)) {
            recordCounts.put(partitionPath, 0L);
          }
          recordCounts.put(partitionPath, recordCounts.get(partitionPath) + count);
        }
      }
      assertTrue("commit file is missing", isCommitFilePresent);
      assertEquals("partition is missing", 4, recordCounts.size());
      for (Entry<String, Long> e : recordCounts.entrySet()) {
        assertEquals("missing records", 24, e.getValue().longValue());
      }
    } finally {
      if (jsc != null) {
        jsc.stop();
      }
    }
  }

  private void createRecords(Path srcFolder) throws ParseException, IOException {
    Path srcFile = new Path(srcFolder.toString(), "file1.parquet");
    long startTime = HoodieActiveTimeline.COMMIT_FORMATTER.parse("20170203000000").getTime() / 1000;
    List<GenericRecord> records = new ArrayList<GenericRecord>();
    for (long recordNum = 0; recordNum < 96; recordNum++) {
      records.add(HoodieTestDataGenerator.generateGenericRecord(Long.toString(recordNum), "rider-" + recordNum,
          "driver-" + recordNum, startTime + TimeUnit.HOURS.toSeconds(recordNum)));
    }
    ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(srcFile)
        .withSchema(HoodieTestDataGenerator.avroSchema).withConf(HoodieTestUtils.getDefaultHadoopConf()).build();
    for (GenericRecord record : records) {
      writer.write(record);
    }
    writer.close();
  }

  private void createSchemaFile(String schemaFile) throws IOException {
    FSDataOutputStream schemaFileOS = dfs.create(new Path(schemaFile));
    schemaFileOS.write(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA.getBytes());
    schemaFileOS.close();
  }

  /**
   * Tests for scheme file. 1. File is missing. 2. File has invalid data.
   */
  @Test
  public void testSchemaFile() throws Exception {
    JavaSparkContext jsc = null;
    try {
      jsc = getJavaSparkContext();

      // Test root folder.
      String basePath = (new Path(dfsBasePath, Thread.currentThread().getStackTrace()[1].getMethodName())).toString();
      // Hoodie root folder
      Path hoodieFolder = new Path(basePath, "testTarget");
      Path srcFolder = new Path(basePath.toString(), "srcTest");
      Path schemaFile = new Path(basePath.toString(), "missingFile.schema");
      HDFSParquetImporter.Config cfg = getHDFSParquetImporterConfig(srcFolder.toString(), hoodieFolder.toString(),
          "testTable", "COPY_ON_WRITE", "_row_key", "timestamp", 1, schemaFile.toString());
      HDFSParquetImporter dataImporter = new HDFSParquetImporter(cfg);
      // Should fail - return : -1.
      assertEquals(-1, dataImporter.dataImport(jsc, 0));

      dfs.create(schemaFile).write("Random invalid schema data".getBytes());
      // Should fail - return : -1.
      assertEquals(-1, dataImporter.dataImport(jsc, 0));

    } finally {
      if (jsc != null) {
        jsc.stop();
      }
    }
  }

  /**
   * Test for missing rowKey and partitionKey.
   */
  @Test
  public void testRowAndPartitionKey() throws Exception {
    JavaSparkContext jsc = null;
    try {
      jsc = getJavaSparkContext();

      // Test root folder.
      String basePath = (new Path(dfsBasePath, Thread.currentThread().getStackTrace()[1].getMethodName())).toString();
      // Hoodie root folder
      Path hoodieFolder = new Path(basePath, "testTarget");

      // Create generic records.
      Path srcFolder = new Path(basePath, "testSrc");
      createRecords(srcFolder);

      // Create schema file.
      Path schemaFile = new Path(basePath.toString(), "missingFile.schema");
      createSchemaFile(schemaFile.toString());

      HDFSParquetImporter dataImporter;
      HDFSParquetImporter.Config cfg;

      // Check for invalid row key.
      cfg = getHDFSParquetImporterConfig(srcFolder.toString(), hoodieFolder.toString(), "testTable", "COPY_ON_WRITE",
          "invalidRowKey", "timestamp", 1, schemaFile.toString());
      dataImporter = new HDFSParquetImporter(cfg);
      assertEquals(-1, dataImporter.dataImport(jsc, 0));

      // Check for invalid partition key.
      cfg = getHDFSParquetImporterConfig(srcFolder.toString(), hoodieFolder.toString(), "testTable", "COPY_ON_WRITE",
          "_row_key", "invalidTimeStamp", 1, schemaFile.toString());
      dataImporter = new HDFSParquetImporter(cfg);
      assertEquals(-1, dataImporter.dataImport(jsc, 0));

    } finally {
      if (jsc != null) {
        jsc.stop();
      }
    }
  }

  private HDFSParquetImporter.Config getHDFSParquetImporterConfig(String srcPath, String targetPath, String tableName,
      String tableType, String rowKey, String partitionKey, int parallelism, String schemaFile) {
    HDFSParquetImporter.Config cfg = new HDFSParquetImporter.Config();
    cfg.srcPath = srcPath;
    cfg.targetPath = targetPath;
    cfg.tableName = tableName;
    cfg.tableType = tableType;
    cfg.rowKey = rowKey;
    cfg.partitionKey = partitionKey;
    cfg.parallelism = parallelism;
    cfg.schemaFile = schemaFile;
    return cfg;
  }

  private JavaSparkContext getJavaSparkContext() {
    // Initialize a local spark env
    SparkConf sparkConf = new SparkConf().setAppName("TestConversionCommand").setMaster("local[1]");
    sparkConf = HoodieWriteClient.registerClasses(sparkConf);
    return new JavaSparkContext(HoodieReadClient.addHoodieSupport(sparkConf));
  }
}
