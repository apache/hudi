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

package org.apache.hudi.utilities.functional;

import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.HoodieSnapshotExporter;
import org.apache.hudi.utilities.HoodieSnapshotExporter.Config;
import org.apache.hudi.utilities.HoodieSnapshotExporter.OutputFormatValidator;
import org.apache.hudi.utilities.HoodieSnapshotExporter.Partitioner;
import org.apache.hudi.utilities.exception.HoodieSnapshotExporterException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestHoodieSnapshotExporter extends SparkClientFunctionalTestHarness {

  static final Logger LOG = LoggerFactory.getLogger(TestHoodieSnapshotExporter.class);
  static final int NUM_RECORDS = 100;
  static final String COMMIT_TIME = "20200101000000";
  static final String PARTITION_PATH = "2020";
  static final String TABLE_NAME = "testing";
  String sourcePath;
  String targetPath;
  HoodieStorage storage;

  @BeforeEach
  public void init() throws Exception {
    // Initialize test data dirs
    sourcePath = Paths.get(basePath(), "source").toString();
    targetPath = Paths.get(basePath(), "target").toString();
    storage = HoodieStorageUtils.getStorage(basePath(), HadoopFSUtils.getStorageConf(jsc().hadoopConfiguration()));

    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(TABLE_NAME)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(HadoopFSUtils.getStorageConfWithCopy(jsc().hadoopConfiguration()), sourcePath);

    // Prepare data as source Hudi dataset
    HoodieWriteConfig cfg = getHoodieWriteConfig(sourcePath);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(cfg)) {
      writeClient.startCommitWithTime(COMMIT_TIME);
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH});
      List<HoodieRecord> records = dataGen.generateInserts(COMMIT_TIME, NUM_RECORDS);
      JavaRDD<HoodieRecord> recordsRDD = jsc().parallelize(records, 1);
      writeClient.bulkInsert(recordsRDD, COMMIT_TIME);
    }
    List<StoragePathInfo> pathInfoList = storage.listFiles(new StoragePath(sourcePath));
    for (StoragePathInfo pathInfo : pathInfoList) {
      LOG.info(">>> Prepared test file: " + pathInfo.getPath());
    }
  }

  @AfterEach
  public void cleanUp() throws IOException {
    storage.close();
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
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(IndexType.BLOOM).build())
        .build();
  }

  @Nested
  public class TestHoodieSnapshotExporterForHudi {

    private HoodieSnapshotExporter.Config cfg;

    @BeforeEach
    public void setUp() {
      cfg = new Config();
      cfg.sourceBasePath = sourcePath;
      cfg.targetOutputPath = targetPath;
      cfg.outputFormat = OutputFormatValidator.HUDI;
    }

    @Test
    public void testExportAsHudi() throws IOException {
      new HoodieSnapshotExporter().export(jsc(), cfg);

      // Check results
      assertTrue(storage.exists(new StoragePath(targetPath + "/.hoodie/" + COMMIT_TIME + ".commit")));
      assertTrue(storage.exists(new StoragePath(targetPath + "/.hoodie/" + COMMIT_TIME + ".commit.requested")));
      assertTrue(storage.exists(new StoragePath(targetPath + "/.hoodie/" + COMMIT_TIME + ".inflight")));
      assertTrue(storage.exists(new StoragePath(targetPath + "/.hoodie/hoodie.properties")));
      String partition = targetPath + "/" + PARTITION_PATH;
      long numParquetFiles = storage.listDirectEntries(new StoragePath(partition)).stream()
          .filter(fileStatus -> fileStatus.getPath().toString().endsWith(".parquet"))
          .count();
      assertTrue(numParquetFiles >= 1, "There should exist at least 1 parquet file.");
      assertEquals(NUM_RECORDS, sqlContext().read().parquet(partition).count());
      assertTrue(storage.exists(new StoragePath(partition + "/.hoodie_partition_metadata")));
      assertTrue(storage.exists(new StoragePath(targetPath + "/_SUCCESS")));
    }
  }

  @Nested
  public class TestHoodieSnapshotExporterForEarlyAbort {

    private HoodieSnapshotExporter.Config cfg;

    @BeforeEach
    public void setUp() {
      cfg = new Config();
      cfg.sourceBasePath = sourcePath;
      cfg.targetOutputPath = targetPath;
      cfg.outputFormat = OutputFormatValidator.HUDI;
    }

    @Test
    public void testExportWhenTargetPathExists() throws IOException {
      // make target output path present
      storage.createDirectory(new StoragePath(targetPath));

      // export
      final Throwable thrown = assertThrows(HoodieSnapshotExporterException.class, () -> {
        new HoodieSnapshotExporter().export(jsc(), cfg);
      });
      assertEquals("The target output path already exists.", thrown.getMessage());
    }

    @Test
    public void testExportDatasetWithNoCommit() throws IOException {
      // delete commit files
      List<StoragePath> commitFiles =
          storage.listDirectEntries(new StoragePath(sourcePath + "/.hoodie")).stream()
              .map(StoragePathInfo::getPath)
              .filter(filePath -> filePath.getName().endsWith(".commit"))
              .collect(Collectors.toList());
      for (StoragePath p : commitFiles) {
        storage.deleteFile(p);
      }

      // export
      final Throwable thrown = assertThrows(HoodieSnapshotExporterException.class, () -> {
        new HoodieSnapshotExporter().export(jsc(), cfg);
      });
      assertEquals("No commits present. Nothing to snapshot.", thrown.getMessage());
    }

    @Test
    public void testExportDatasetWithNoPartition() throws IOException {
      // delete all source data
      storage.deleteDirectory(new StoragePath(sourcePath + "/" + PARTITION_PATH));
      // delete hudi metadata table too.
      storage.deleteDirectory(new StoragePath(cfg.sourceBasePath + "/" + ".hoodie/metadata"));

      // export
      final Throwable thrown = assertThrows(HoodieSnapshotExporterException.class, () -> {
        new HoodieSnapshotExporter().export(jsc(), cfg);
      });
      assertEquals("The source dataset has 0 partition to snapshot.", thrown.getMessage());
    }
  }

  @Nested
  public class TestHoodieSnapshotExporterForNonHudi {

    @ParameterizedTest
    @ValueSource(strings = {"json", "parquet", "orc"})
    public void testExportAsNonHudi(String format) throws IOException {
      // NOTE: Hudi doesn't support Orc in Spark < 3.0
      //       Please check HUDI-4496 for more details
      if ("orc".equals(format) && !HoodieSparkUtils.gteqSpark3_0()) {
        return;
      }

      HoodieSnapshotExporter.Config cfg = new Config();
      cfg.sourceBasePath = sourcePath;
      cfg.targetOutputPath = targetPath;
      cfg.outputFormat = format;
      new HoodieSnapshotExporter().export(jsc(), cfg);
      assertEquals(NUM_RECORDS, sqlContext().read().format(format).load(targetPath).count());
      assertTrue(storage.exists(new StoragePath(targetPath + "/_SUCCESS")));
    }
  }

  public static class UserDefinedPartitioner implements Partitioner {

    public static final String PARTITION_NAME = "year";

    @Override
    public DataFrameWriter<Row> partition(Dataset<Row> source) {
      return source
          .withColumnRenamed(HoodieRecord.PARTITION_PATH_METADATA_FIELD, PARTITION_NAME)
          .repartition(new Column(PARTITION_NAME))
          .write()
          .partitionBy(PARTITION_NAME);
    }
  }

  @Nested
  public class TestHoodieSnapshotExporterForRepartitioning {

    private HoodieSnapshotExporter.Config cfg;

    @BeforeEach
    public void setUp() {
      cfg = new Config();
      cfg.sourceBasePath = sourcePath;
      cfg.targetOutputPath = targetPath;
      cfg.outputFormat = "json";
    }

    @Test
    public void testExportWithPartitionField() throws IOException {
      // `driver` field is set in HoodieTestDataGenerator
      cfg.outputPartitionField = "driver";
      new HoodieSnapshotExporter().export(jsc(), cfg);

      assertEquals(NUM_RECORDS, sqlContext().read().format("json").load(targetPath).count());
      assertTrue(storage.exists(new StoragePath(targetPath + "/_SUCCESS")));
      assertTrue(storage.listDirectEntries(new StoragePath(targetPath)).size() > 1);
    }

    @Test
    public void testExportForUserDefinedPartitioner() throws IOException {
      cfg.outputPartitioner = UserDefinedPartitioner.class.getName();
      new HoodieSnapshotExporter().export(jsc(), cfg);

      assertEquals(NUM_RECORDS, sqlContext().read().format("json").load(targetPath).count());
      assertTrue(storage.exists(new StoragePath(targetPath + "/_SUCCESS")));
      assertTrue(
          storage.exists(new StoragePath(
              String.format("%s/%s=%s", targetPath, UserDefinedPartitioner.PARTITION_NAME,
                  PARTITION_PATH))));
    }
  }
}
