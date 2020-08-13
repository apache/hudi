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

package org.apache.hudi.internal;

import org.apache.hudi.client.HoodieInternalWriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.apache.hudi.testutils.SparkDatasetTestUtils.ENCODER;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.STRUCT_TYPE;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.getConfigBuilder;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.getRandomRows;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.toInternalRows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests {@link HoodieDataSourceInternalWriter}.
 */
public class TestHoodieDataSourceInternalWriter extends HoodieClientTestHarness {

  private static final Random RANDOM = new Random();

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieDataSourceInternalWriter");
    initPath();
    initFileSystem();
    initTestDataGenerator();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void testDataSourceWriter() throws IOException {
    // init config and table
    HoodieWriteConfig cfg = getConfigBuilder(basePath).build();
    String instantTime = "001";
    // init writer
    HoodieDataSourceInternalWriter dataSourceInternalWriter =
        new HoodieDataSourceInternalWriter(instantTime, cfg, STRUCT_TYPE, sqlContext.sparkSession(), hadoopConf);
    DataWriter<InternalRow> writer = dataSourceInternalWriter.createWriterFactory().createDataWriter(0, RANDOM.nextLong(), RANDOM.nextLong());

    List<String> partitionPaths = Arrays.asList(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS);
    List<String> partitionPathsAbs = new ArrayList<>();
    for (String partitionPath : partitionPaths) {
      partitionPathsAbs.add(basePath + "/" + partitionPath + "/*");
    }

    int size = 10 + RANDOM.nextInt(1000);
    int batches = 5;
    Dataset<Row> totalInputRows = null;

    for (int j = 0; j < batches; j++) {
      String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[j % 3];
      Dataset<Row> inputRows = getRandomRows(sqlContext, size, partitionPath, false);
      writeRows(inputRows, writer);
      if (totalInputRows == null) {
        totalInputRows = inputRows;
      } else {
        totalInputRows = totalInputRows.union(inputRows);
      }
    }

    HoodieWriterCommitMessage commitMetadata = (HoodieWriterCommitMessage) writer.commit();
    List<HoodieWriterCommitMessage> commitMessages = new ArrayList<>();
    commitMessages.add(commitMetadata);
    dataSourceInternalWriter.commit(commitMessages.toArray(new HoodieWriterCommitMessage[0]));
    metaClient.reloadActiveTimeline();
    Dataset<Row> result = HoodieClientTestUtils.read(jsc, basePath, sqlContext, metaClient.getFs(), partitionPathsAbs.toArray(new String[0]));
    // verify output
    assertOutput(totalInputRows, result, instantTime);
    assertWriteStatuses(commitMessages.get(0).getWriteStatuses(), batches, size);
  }

  @Test
  public void testMultipleDataSourceWrites() throws IOException {
    // init config and table
    HoodieWriteConfig cfg = getConfigBuilder(basePath).build();
    int partitionCounter = 0;

    // execute N rounds
    for (int i = 0; i < 5; i++) {
      String instantTime = "00" + i;
      // init writer
      HoodieDataSourceInternalWriter dataSourceInternalWriter =
          new HoodieDataSourceInternalWriter(instantTime, cfg, STRUCT_TYPE, sqlContext.sparkSession(), hadoopConf);

      List<HoodieWriterCommitMessage> commitMessages = new ArrayList<>();
      Dataset<Row> totalInputRows = null;
      DataWriter<InternalRow> writer = dataSourceInternalWriter.createWriterFactory().createDataWriter(partitionCounter++, RANDOM.nextLong(), RANDOM.nextLong());

      int size = 10 + RANDOM.nextInt(1000);
      int batches = 5; // one batch per partition

      for (int j = 0; j < batches; j++) {
        String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[j % 3];
        Dataset<Row> inputRows = getRandomRows(sqlContext, size, partitionPath, false);
        writeRows(inputRows, writer);
        if (totalInputRows == null) {
          totalInputRows = inputRows;
        } else {
          totalInputRows = totalInputRows.union(inputRows);
        }
      }

      HoodieWriterCommitMessage commitMetadata = (HoodieWriterCommitMessage) writer.commit();
      commitMessages.add(commitMetadata);
      dataSourceInternalWriter.commit(commitMessages.toArray(new HoodieWriterCommitMessage[0]));
      metaClient.reloadActiveTimeline();

      Dataset<Row> result = HoodieClientTestUtils.readCommit(basePath, sqlContext, metaClient.getCommitTimeline(), instantTime);

      // verify output
      assertOutput(totalInputRows, result, instantTime);
      assertWriteStatuses(commitMessages.get(0).getWriteStatuses(), batches, size);
    }
  }

  @Test
  public void testLargeWrites() throws IOException {
    // init config and table
    HoodieWriteConfig cfg = getConfigBuilder(basePath).build();
    int partitionCounter = 0;

    // execute N rounds
    for (int i = 0; i < 3; i++) {
      String instantTime = "00" + i;
      // init writer
      HoodieDataSourceInternalWriter dataSourceInternalWriter =
          new HoodieDataSourceInternalWriter(instantTime, cfg, STRUCT_TYPE, sqlContext.sparkSession(), hadoopConf);

      List<HoodieWriterCommitMessage> commitMessages = new ArrayList<>();
      Dataset<Row> totalInputRows = null;
      DataWriter<InternalRow> writer = dataSourceInternalWriter.createWriterFactory().createDataWriter(partitionCounter++, RANDOM.nextLong(), RANDOM.nextLong());

      int size = 10000 + RANDOM.nextInt(10000);
      int batches = 3; // one batch per partition

      for (int j = 0; j < batches; j++) {
        String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[j % 3];
        Dataset<Row> inputRows = getRandomRows(sqlContext, size, partitionPath, false);
        writeRows(inputRows, writer);
        if (totalInputRows == null) {
          totalInputRows = inputRows;
        } else {
          totalInputRows = totalInputRows.union(inputRows);
        }
      }

      HoodieWriterCommitMessage commitMetadata = (HoodieWriterCommitMessage) writer.commit();
      commitMessages.add(commitMetadata);
      dataSourceInternalWriter.commit(commitMessages.toArray(new HoodieWriterCommitMessage[0]));
      metaClient.reloadActiveTimeline();

      Dataset<Row> result = HoodieClientTestUtils.readCommit(basePath, sqlContext, metaClient.getCommitTimeline(), instantTime);

      // verify output
      assertOutput(totalInputRows, result, instantTime);
      assertWriteStatuses(commitMessages.get(0).getWriteStatuses(), batches, size);
    }
  }

  /**
   * Tests that DataSourceWriter.abort() will abort the written records of interest write and commit batch1 write and abort batch2 Read of entire dataset should show only records from batch1.
   * commit batch1
   * abort batch2
   * verify only records from batch1 is available to read
   */
  @Test
  public void testAbort() throws IOException {
    // init config and table
    HoodieWriteConfig cfg = getConfigBuilder(basePath).build();

    String instantTime0 = "00" + 0;
    // init writer
    HoodieDataSourceInternalWriter dataSourceInternalWriter =
        new HoodieDataSourceInternalWriter(instantTime0, cfg, STRUCT_TYPE, sqlContext.sparkSession(), hadoopConf);
    DataWriter<InternalRow> writer = dataSourceInternalWriter.createWriterFactory().createDataWriter(0, RANDOM.nextLong(), RANDOM.nextLong());

    List<String> partitionPaths = Arrays.asList(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS);
    List<String> partitionPathsAbs = new ArrayList<>();
    for (String partitionPath : partitionPaths) {
      partitionPathsAbs.add(basePath + "/" + partitionPath + "/*");
    }

    int size = 10 + RANDOM.nextInt(100);
    int batches = 1;
    Dataset<Row> totalInputRows = null;

    for (int j = 0; j < batches; j++) {
      String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[j % 3];
      Dataset<Row> inputRows = getRandomRows(sqlContext, size, partitionPath, false);
      writeRows(inputRows, writer);
      if (totalInputRows == null) {
        totalInputRows = inputRows;
      } else {
        totalInputRows = totalInputRows.union(inputRows);
      }
    }

    HoodieWriterCommitMessage commitMetadata = (HoodieWriterCommitMessage) writer.commit();
    List<HoodieWriterCommitMessage> commitMessages = new ArrayList<>();
    commitMessages.add(commitMetadata);
    // commit 1st batch
    dataSourceInternalWriter.commit(commitMessages.toArray(new HoodieWriterCommitMessage[0]));
    metaClient.reloadActiveTimeline();
    Dataset<Row> result = HoodieClientTestUtils.read(jsc, basePath, sqlContext, metaClient.getFs(), partitionPathsAbs.toArray(new String[0]));
    // verify rows
    assertOutput(totalInputRows, result, instantTime0);
    assertWriteStatuses(commitMessages.get(0).getWriteStatuses(), batches, size);

    // 2nd batch. abort in the end
    String instantTime1 = "00" + 1;
    dataSourceInternalWriter =
        new HoodieDataSourceInternalWriter(instantTime1, cfg, STRUCT_TYPE, sqlContext.sparkSession(), hadoopConf);
    writer = dataSourceInternalWriter.createWriterFactory().createDataWriter(1, RANDOM.nextLong(), RANDOM.nextLong());

    for (int j = 0; j < batches; j++) {
      String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[j % 3];
      Dataset<Row> inputRows = getRandomRows(sqlContext, size, partitionPath, false);
      writeRows(inputRows, writer);
    }

    commitMetadata = (HoodieWriterCommitMessage) writer.commit();
    commitMessages = new ArrayList<>();
    commitMessages.add(commitMetadata);
    // commit 1st batch
    dataSourceInternalWriter.abort(commitMessages.toArray(new HoodieWriterCommitMessage[0]));
    metaClient.reloadActiveTimeline();
    result = HoodieClientTestUtils.read(jsc, basePath, sqlContext, metaClient.getFs(), partitionPathsAbs.toArray(new String[0]));
    // verify rows
    // only rows from first batch should be present
    assertOutput(totalInputRows, result, instantTime0);
  }

  private void writeRows(Dataset<Row> inputRows, DataWriter<InternalRow> writer) throws IOException {
    List<InternalRow> internalRows = toInternalRows(inputRows, ENCODER);
    // issue writes
    for (InternalRow internalRow : internalRows) {
      writer.write(internalRow);
    }
  }

  private void assertWriteStatuses(List<HoodieInternalWriteStatus> writeStatuses, int batches, int size) {
    assertEquals(batches, writeStatuses.size());
    int counter = 0;
    for (HoodieInternalWriteStatus writeStatus : writeStatuses) {
      assertEquals(writeStatus.getPartitionPath(), HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[counter % 3]);
      assertEquals(writeStatus.getTotalRecords(), size);
      assertEquals(writeStatus.getFailedRowsSize(), 0);
      assertEquals(writeStatus.getTotalErrorRecords(), 0);
      assertFalse(writeStatus.hasErrors());
      assertNull(writeStatus.getGlobalError());
      assertNotNull(writeStatus.getFileId());
      String fileId = writeStatus.getFileId();
      HoodieWriteStat writeStat = writeStatus.getStat();
      assertEquals(size, writeStat.getNumInserts());
      assertEquals(size, writeStat.getNumWrites());
      assertEquals(fileId, writeStat.getFileId());
      assertEquals(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[counter++ % 3], writeStat.getPartitionPath());
      assertEquals(0, writeStat.getNumDeletes());
      assertEquals(0, writeStat.getNumUpdateWrites());
      assertEquals(0, writeStat.getTotalWriteErrors());
    }
  }

  private void assertOutput(Dataset<Row> expectedRows, Dataset<Row> actualRows, String instantTime) {
    // verify 3 meta fields that are filled in within create handle
    actualRows.collectAsList().forEach(entry -> {
      assertEquals(entry.get(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD)).toString(), instantTime);
      assertFalse(entry.isNullAt(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.FILENAME_METADATA_FIELD)));
      assertFalse(entry.isNullAt(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD)));
    });

    // after trimming 2 of the meta fields, rest of the fields should match
    Dataset<Row> trimmedExpected = expectedRows.drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD);
    Dataset<Row> trimmedActual = actualRows.drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD);
    assertEquals(0, trimmedActual.except(trimmedExpected).count());
  }
}
