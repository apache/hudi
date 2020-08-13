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
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.hudi.testutils.SparkDatasetTestUtils.ENCODER;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.STRUCT_TYPE;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.getConfigBuilder;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.getInternalRowWithError;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.getRandomRows;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.toInternalRows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit tests {@link HoodieBulkInsertDataInternalWriter}.
 */
public class TestHoodieBulkInsertDataInternalWriter extends HoodieClientTestHarness {

  private static final Random RANDOM = new Random();

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieBulkInsertDataInternalWriter");
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
  public void testDataInternalWriter() throws IOException {
    // init config and table
    HoodieWriteConfig cfg = getConfigBuilder(basePath).build();
    HoodieTable table = HoodieTable.create(metaClient, cfg, hadoopConf);
    // execute N rounds
    for (int i = 0; i < 5; i++) {
      String instantTime = "00" + i;
      // init writer
      HoodieBulkInsertDataInternalWriter writer = new HoodieBulkInsertDataInternalWriter(table, cfg, instantTime, RANDOM.nextInt(100000), RANDOM.nextLong(), RANDOM.nextLong(), STRUCT_TYPE);

      int size = 10 + RANDOM.nextInt(1000);
      // write N rows to partition1, N rows to partition2 and N rows to partition3 ... Each batch should create a new RowCreateHandle and a new file
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
      List<String> fileAbsPaths = new ArrayList<>();
      List<String> fileNames = new ArrayList<>();

      // verify write statuses
      assertWriteStatuses(commitMetadata.getWriteStatuses(), batches, size, fileAbsPaths, fileNames);

      // verify rows
      Dataset<Row> result = sqlContext.read().parquet(fileAbsPaths.toArray(new String[0]));
      assertOutput(totalInputRows, result, instantTime, fileNames);
    }
  }


  /**
   * Issue some corrupted or wrong schematized InternalRow after few valid InternalRows so that global error is thrown. write batch 1 of valid records write batch2 of invalid records which is expected
   * to throw Global Error. Verify global error is set appropriately and only first batch of records are written to disk.
   */
  @Test
  public void testGlobalFailure() throws IOException {
    // init config and table
    HoodieWriteConfig cfg = getConfigBuilder(basePath).build();
    HoodieTable table = HoodieTable.create(metaClient, cfg, hadoopConf);
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];

    String instantTime = "001";
    HoodieBulkInsertDataInternalWriter writer = new HoodieBulkInsertDataInternalWriter(table, cfg, instantTime, RANDOM.nextInt(100000), RANDOM.nextLong(), RANDOM.nextLong(), STRUCT_TYPE);

    int size = 10 + RANDOM.nextInt(100);
    int totalFailures = 5;
    // Generate first batch of valid rows
    Dataset<Row> inputRows = getRandomRows(sqlContext, size / 2, partitionPath, false);
    List<InternalRow> internalRows = toInternalRows(inputRows, ENCODER);

    // generate some failures rows
    for (int i = 0; i < totalFailures; i++) {
      internalRows.add(getInternalRowWithError(partitionPath));
    }

    // generate 2nd batch of valid rows
    Dataset<Row> inputRows2 = getRandomRows(sqlContext, size / 2, partitionPath, false);
    internalRows.addAll(toInternalRows(inputRows2, ENCODER));

    // issue writes
    try {
      for (InternalRow internalRow : internalRows) {
        writer.write(internalRow);
      }
      fail("Should have failed");
    } catch (Throwable e) {
      // expected
    }

    HoodieWriterCommitMessage commitMetadata = (HoodieWriterCommitMessage) writer.commit();

    List<String> fileAbsPaths = new ArrayList<>();
    List<String> fileNames = new ArrayList<>();
    // verify write statuses
    assertWriteStatuses(commitMetadata.getWriteStatuses(), 1, size / 2, fileAbsPaths, fileNames);

    // verify rows
    Dataset<Row> result = sqlContext.read().parquet(fileAbsPaths.toArray(new String[0]));
    assertOutput(inputRows, result, instantTime, fileNames);
  }

  private void writeRows(Dataset<Row> inputRows, HoodieBulkInsertDataInternalWriter writer) throws IOException {
    List<InternalRow> internalRows = toInternalRows(inputRows, ENCODER);
    // issue writes
    for (InternalRow internalRow : internalRows) {
      writer.write(internalRow);
    }
  }

  private void assertWriteStatuses(List<HoodieInternalWriteStatus> writeStatuses, int batches, int size, List<String> fileAbsPaths, List<String> fileNames) {
    assertEquals(batches, writeStatuses.size());
    int counter = 0;
    for (HoodieInternalWriteStatus writeStatus : writeStatuses) {
      // verify write status
      assertEquals(writeStatus.getTotalRecords(), size);
      assertNull(writeStatus.getGlobalError());
      assertEquals(writeStatus.getFailedRowsSize(), 0);
      assertNotNull(writeStatus.getFileId());
      String fileId = writeStatus.getFileId();
      assertEquals(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[counter % 3], writeStatus.getPartitionPath());
      fileAbsPaths.add(basePath + "/" + writeStatus.getStat().getPath());
      fileNames.add(writeStatus.getStat().getPath().substring(writeStatus.getStat().getPath().lastIndexOf('/') + 1));
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

  private void assertOutput(Dataset<Row> expectedRows, Dataset<Row> actualRows, String instantTime, List<String> fileNames) {
    // verify 3 meta fields that are filled in within create handle
    actualRows.collectAsList().forEach(entry -> {
      assertEquals(entry.get(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD)).toString(), instantTime);
      assertFalse(entry.isNullAt(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.FILENAME_METADATA_FIELD)));
      assertTrue(fileNames.contains(entry.get(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.FILENAME_METADATA_FIELD))));
      assertFalse(entry.isNullAt(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD)));
    });

    // after trimming 2 of the meta fields, rest of the fields should match
    Dataset<Row> trimmedExpected = expectedRows.drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD);
    Dataset<Row> trimmedActual = actualRows.drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD);
    assertEquals(0, trimmedActual.except(trimmedExpected).count());
  }
}
