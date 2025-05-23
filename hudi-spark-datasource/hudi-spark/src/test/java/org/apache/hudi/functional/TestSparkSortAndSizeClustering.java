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

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSparkSortAndSizeClustering extends HoodieSparkClientTestHarness {


  private HoodieWriteConfig config;
  private HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0);

  public void setup(int maxFileSize) throws IOException {
    setup(maxFileSize, Collections.emptyMap());
  }

  public void setup(int maxFileSize, Map<String, String> options) throws IOException {
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initHoodieStorage();
    Properties props = getPropertiesForKeyGen(true);
    props.putAll(options);
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE, props);
    config = getConfigBuilder().withProps(props)
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetMaxFileSize(maxFileSize).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanPartitionFilterMode(ClusteringPlanPartitionFilterMode.RECENT_DAYS)
            .build())
        .build();

    writeClient = getHoodieWriteClient(config);
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  @Test
  public void testClusteringWithRDD() throws IOException {
    writeAndClustering(false);
  }

  @Test
  public void testClusteringWithRow() throws IOException {
    writeAndClustering(true);
  }

  public void writeAndClustering(boolean isRow) throws IOException {
    setup(102400);
    config.setValue("hoodie.datasource.write.row.writer.enable", String.valueOf(isRow));
    config.setValue("hoodie.metadata.enable", "false");
    config.setValue("hoodie.clustering.plan.strategy.daybased.lookback.partitions", "1");
    config.setValue("hoodie.clustering.plan.strategy.target.file.max.bytes", String.valueOf(1024 * 1024));
    config.setValue("hoodie.clustering.plan.strategy.max.bytes.per.group", String.valueOf(2 * 1024 * 1024));

    int numRecords = 1000;
    writeData(writeClient.createNewInstantTime(), numRecords, true);

    String clusteringTime = (String) writeClient.scheduleClustering(Option.empty()).get();
    HoodieClusteringPlan plan = ClusteringUtils.getClusteringPlan(
        metaClient, INSTANT_GENERATOR.getClusteringCommitRequestedInstant(clusteringTime)).map(Pair::getRight).get();

    List<HoodieClusteringGroup> inputGroups = plan.getInputGroups();
    assertEquals(1, inputGroups.size(), "Clustering plan will contain 1 input group");

    Integer outputFileGroups = plan.getInputGroups().get(0).getNumOutputFileGroups();
    assertEquals(2, outputFileGroups, "Clustering plan will generate 2 output groups");

    HoodieWriteMetadata writeMetadata = writeClient.cluster(clusteringTime, true);
    List<HoodieWriteStat> writeStats = (List<HoodieWriteStat>)writeMetadata.getWriteStats().get();
    assertEquals(2, writeStats.size(), "Clustering should write 2 files");

    List<Row> rows = readRecords();
    assertEquals(numRecords, rows.size());
    validateDecimalTypeAfterClustering(writeStats);
  }

  // Validate that clustering produces decimals in legacy format
  private void validateDecimalTypeAfterClustering(List<HoodieWriteStat> writeStats) {
    writeStats.stream().map(writeStat -> new StoragePath(metaClient.getBasePath(), writeStat.getPath())).forEach(writtenPath -> {
      MessageType schema = ParquetUtils.readMetadata(storage, writtenPath)
          .getFileMetaData().getSchema();
      int index = schema.getFieldIndex("height");
      Type decimalType = schema.getFields().get(index);
      assertEquals("DECIMAL", decimalType.getOriginalType().toString());
      assertEquals("FIXED_LEN_BYTE_ARRAY", decimalType.asPrimitiveType().getPrimitiveTypeName().toString());
    });
  }

  private List<WriteStatus> writeData(String commitTime, int totalRecords, boolean doCommit) {
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, totalRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    WriteClientTestUtils.startCommitWithTime(writeClient, commitTime);
    List<WriteStatus> writeStatues = writeClient.insert(writeRecords, commitTime).collect();
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatues);

    if (doCommit) {
      Assertions.assertTrue(writeClient.commitStats(commitTime, writeStatues.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
          Option.empty(), metaClient.getCommitActionType()));
    }

    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatues;
  }

  private List<Row> readRecords() {
    Dataset<Row> roViewDF = sparkSession.read().format("hudi").load(basePath);
    roViewDF.createOrReplaceTempView("clutering_table");
    return sparkSession.sqlContext().sql("select * from clutering_table").collectAsList();
  }

  public HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .forTable("clustering-table")
        .withEmbeddedTimelineServerEnabled(true);
  }
}
