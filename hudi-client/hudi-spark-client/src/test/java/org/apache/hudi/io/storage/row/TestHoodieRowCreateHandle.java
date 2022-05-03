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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.client.HoodieInternalWriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.SparkDatasetTestUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit tests {@link HoodieRowCreateHandle}.
 */
@SuppressWarnings("checkstyle:LineLength")
public class TestHoodieRowCreateHandle extends HoodieClientTestHarness {

  private static final Random RANDOM = new Random();

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieRowCreateHandle");
    initPath();
    initFileSystem();
    initTestDataGenerator();
    initMetaClient();
    initTimelineService();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  public void testRowCreateHandle(boolean populateMetaFields) throws Exception {
    // init config and table
    HoodieWriteConfig cfg =
        SparkDatasetTestUtils.getConfigBuilder(basePath, timelineServicePort).build();
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    List<String> fileNames = new ArrayList<>();
    List<String> fileAbsPaths = new ArrayList<>();

    Dataset<Row> totalInputRows = null;
    // one round per partition
    for (int i = 0; i < 5; i++) {
      String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[i % 3];

      // init some args
      String fileId = UUID.randomUUID().toString();
      String instantTime = "000";

      HoodieRowCreateHandle handle = new HoodieRowCreateHandle(table, cfg, partitionPath, fileId, instantTime,
          RANDOM.nextInt(100000), RANDOM.nextLong(), RANDOM.nextLong(), SparkDatasetTestUtils.STRUCT_TYPE, populateMetaFields);
      int size = 10 + RANDOM.nextInt(1000);
      // Generate inputs
      Dataset<Row> inputRows = SparkDatasetTestUtils.getRandomRows(sqlContext, size, partitionPath, false);
      if (totalInputRows == null) {
        totalInputRows = inputRows;
      } else {
        totalInputRows = totalInputRows.union(inputRows);
      }

      // issue writes
      HoodieInternalWriteStatus writeStatus = writeAndGetWriteStatus(inputRows, handle);

      fileAbsPaths.add(basePath + "/" + writeStatus.getStat().getPath());
      fileNames.add(handle.getFileName());
      // verify output
      assertOutput(writeStatus, size, fileId, partitionPath, instantTime, totalInputRows, fileNames, fileAbsPaths, populateMetaFields);
    }
  }

  /**
   * Issue some corrupted or wrong schematized InternalRow after few valid InternalRows so that global error is thrown. write batch 1 of valid records write batch 2 of invalid records Global Error
   * should be thrown.
   */
  @Test
  public void testGlobalFailure() throws Exception {
    // init config and table
    HoodieWriteConfig cfg =
        SparkDatasetTestUtils.getConfigBuilder(basePath, timelineServicePort).build();
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];

    // init some args
    String fileId = UUID.randomUUID().toString();
    String instantTime = "000";

    HoodieRowCreateHandle handle =
        new HoodieRowCreateHandle(table, cfg, partitionPath, fileId, instantTime, RANDOM.nextInt(100000), RANDOM.nextLong(), RANDOM.nextLong(), SparkDatasetTestUtils.STRUCT_TYPE, true);
    int size = 10 + RANDOM.nextInt(1000);
    int totalFailures = 5;
    // Generate first batch of valid rows
    Dataset<Row> inputRows = SparkDatasetTestUtils.getRandomRows(sqlContext, size / 2, partitionPath, false);
    List<InternalRow> internalRows = SparkDatasetTestUtils.toInternalRows(inputRows, SparkDatasetTestUtils.ENCODER);

    // generate some failures rows
    for (int i = 0; i < totalFailures; i++) {
      internalRows.add(SparkDatasetTestUtils.getInternalRowWithError(partitionPath));
    }

    // generate 2nd batch of valid rows
    Dataset<Row> inputRows2 = SparkDatasetTestUtils.getRandomRows(sqlContext, size / 2, partitionPath, false);
    internalRows.addAll(SparkDatasetTestUtils.toInternalRows(inputRows2, SparkDatasetTestUtils.ENCODER));

    // issue writes
    try {
      for (InternalRow internalRow : internalRows) {
        handle.write(internalRow);
      }
      fail("Should have failed");
    } catch (Throwable e) {
      // expected
    }
    // close the create handle
    HoodieInternalWriteStatus writeStatus = handle.close();

    List<String> fileNames = new ArrayList<>();
    fileNames.add(handle.getFileName());
    // verify write status
    assertNotNull(writeStatus.getGlobalError());
    assertTrue(writeStatus.getGlobalError().getMessage().contains("java.lang.String cannot be cast to org.apache.spark.unsafe.types.UTF8String"));
    assertEquals(writeStatus.getFileId(), fileId);
    assertEquals(writeStatus.getPartitionPath(), partitionPath);

    // verify rows
    Dataset<Row> result = sqlContext.read().parquet(basePath + "/" + partitionPath);
    // passing only first batch of inputRows since after first batch global error would have been thrown
    assertRows(inputRows, result, instantTime, fileNames, true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInstantiationFailure(boolean enableMetadataTable) {
    // init config and table
    HoodieWriteConfig cfg = SparkDatasetTestUtils.getConfigBuilder(basePath, timelineServicePort)
        .withPath("/dummypath/abc/")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
        .build();

    try {
      HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
      new HoodieRowCreateHandle(table, cfg, " def", UUID.randomUUID().toString(), "001", RANDOM.nextInt(100000), RANDOM.nextLong(), RANDOM.nextLong(), SparkDatasetTestUtils.STRUCT_TYPE, true);
      fail("Should have thrown exception");
    } catch (HoodieInsertException ioe) {
      // expected without metadata table
      if (enableMetadataTable) {
        fail("Should have thrown TableNotFoundException");
      }
    } catch (TableNotFoundException e) {
      // expected with metadata table
      if (!enableMetadataTable) {
        fail("Should have thrown HoodieInsertException");
      }
    }
  }

  private HoodieInternalWriteStatus writeAndGetWriteStatus(Dataset<Row> inputRows, HoodieRowCreateHandle handle)
      throws Exception {
    List<InternalRow> internalRows = SparkDatasetTestUtils.toInternalRows(inputRows, SparkDatasetTestUtils.ENCODER);
    // issue writes
    for (InternalRow internalRow : internalRows) {
      handle.write(internalRow);
    }
    // close the create handle
    return handle.close();
  }

  private void assertOutput(HoodieInternalWriteStatus writeStatus, int size, String fileId, String partitionPath,
                            String instantTime, Dataset<Row> inputRows, List<String> filenames, List<String> fileAbsPaths, boolean populateMetaFields) {
    assertEquals(writeStatus.getPartitionPath(), partitionPath);
    assertEquals(writeStatus.getTotalRecords(), size);
    assertEquals(writeStatus.getFailedRowsSize(), 0);
    assertEquals(writeStatus.getTotalErrorRecords(), 0);
    assertFalse(writeStatus.hasErrors());
    assertNull(writeStatus.getGlobalError());
    assertEquals(writeStatus.getFileId(), fileId);
    HoodieWriteStat writeStat = writeStatus.getStat();
    assertEquals(size, writeStat.getNumInserts());
    assertEquals(size, writeStat.getNumWrites());
    assertEquals(fileId, writeStat.getFileId());
    assertEquals(partitionPath, writeStat.getPartitionPath());
    assertEquals(0, writeStat.getNumDeletes());
    assertEquals(0, writeStat.getNumUpdateWrites());
    assertEquals(0, writeStat.getTotalWriteErrors());

    // verify rows
    Dataset<Row> result = sqlContext.read().parquet(fileAbsPaths.toArray(new String[0]));
    assertRows(inputRows, result, instantTime, filenames, populateMetaFields);
  }

  private void assertRows(Dataset<Row> expectedRows, Dataset<Row> actualRows, String instantTime, List<String> filenames, boolean populateMetaFields) {
    // verify 3 meta fields that are filled in within create handle
    actualRows.collectAsList().forEach(entry -> {
      String commitTime = entry.getString(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD));
      String fileName = entry.getString(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.FILENAME_METADATA_FIELD));
      String seqId = entry.getString(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD));

      if (populateMetaFields) {
        assertEquals(instantTime, commitTime);
        assertFalse(StringUtils.isNullOrEmpty(seqId));
        assertTrue(filenames.contains(fileName));
      } else {
        assertEquals("", commitTime);
        assertEquals("", seqId);
        assertEquals("", fileName);
      }
    });

    // after trimming 2 of the meta fields, rest of the fields should match
    Dataset<Row> trimmedExpected = expectedRows.drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD);
    Dataset<Row> trimmedActual = actualRows.drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD);
    assertEquals(0, trimmedActual.except(trimmedExpected).count());
  }
}
