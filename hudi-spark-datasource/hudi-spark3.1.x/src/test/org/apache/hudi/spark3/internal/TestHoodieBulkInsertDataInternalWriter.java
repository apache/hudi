/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.spark3.internal;

import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.internal.HoodieBulkInsertInternalWriterTestBase;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.testutils.SparkDatasetTestUtils.ENCODER;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.STRUCT_TYPE;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.getInternalRowWithError;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.getRandomRows;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.toInternalRows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit tests {@link HoodieBulkInsertDataInternalWriter}.
 */
public class TestHoodieBulkInsertDataInternalWriter extends
    HoodieBulkInsertInternalWriterTestBase {

  private static Stream<Arguments> configParams() {
    Object[][] data = new Object[][] {
        {true, true},
        {true, false},
        {false, true},
        {false, false}
    };
    return Stream.of(data).map(Arguments::of);
  }

  private static Stream<Arguments> bulkInsertTypeParams() {
    Object[][] data = new Object[][] {
        {true},
        {false}
    };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("configParams")
  public void testDataInternalWriter(boolean sorted, boolean populateMetaFields) throws Exception {
    // init config and table
    HoodieWriteConfig cfg = getWriteConfig(populateMetaFields);
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    // execute N rounds
    for (int i = 0; i < 2; i++) {
      String instantTime = "00" + i;
      // init writer
      HoodieBulkInsertDataInternalWriter writer = new HoodieBulkInsertDataInternalWriter(table, cfg, instantTime, RANDOM.nextInt(100000),
          RANDOM.nextLong(), STRUCT_TYPE, populateMetaFields, sorted);

      int size = 10 + RANDOM.nextInt(1000);
      // write N rows to partition1, N rows to partition2 and N rows to partition3 ... Each batch should create a new RowCreateHandle and a new file
      int batches = 3;
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
      Option<List<String>> fileAbsPaths = Option.of(new ArrayList<>());
      Option<List<String>> fileNames = Option.of(new ArrayList<>());

      // verify write statuses
      assertWriteStatuses(commitMetadata.getWriteStatuses(), batches, size, sorted, fileAbsPaths, fileNames, false);

      // verify rows
      Dataset<Row> result = sqlContext.read().parquet(fileAbsPaths.get().toArray(new String[0]));
      assertOutput(totalInputRows, result, instantTime, fileNames, populateMetaFields);
    }
  }


  /**
   * Issue some corrupted or wrong schematized InternalRow after few valid InternalRows so that global error is thrown. write batch 1 of valid records write batch2 of invalid records which is expected
   * to throw Global Error. Verify global error is set appropriately and only first batch of records are written to disk.
   */
  @Test
  public void testGlobalFailure() throws Exception {
    // init config and table
    HoodieWriteConfig cfg = getWriteConfig(true);
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];

    String instantTime = "001";
    HoodieBulkInsertDataInternalWriter writer = new HoodieBulkInsertDataInternalWriter(table, cfg, instantTime, RANDOM.nextInt(100000),
        RANDOM.nextLong(), STRUCT_TYPE, true, false);

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

    Option<List<String>> fileAbsPaths = Option.of(new ArrayList<>());
    Option<List<String>> fileNames = Option.of(new ArrayList<>());
    // verify write statuses
    assertWriteStatuses(commitMetadata.getWriteStatuses(), 1, size / 2, fileAbsPaths, fileNames);

    // verify rows
    Dataset<Row> result = sqlContext.read().parquet(fileAbsPaths.get().toArray(new String[0]));
    assertOutput(inputRows, result, instantTime, fileNames, true);
  }

  private void writeRows(Dataset<Row> inputRows, HoodieBulkInsertDataInternalWriter writer)
      throws Exception {
    List<InternalRow> internalRows = toInternalRows(inputRows, ENCODER);
    // issue writes
    for (InternalRow internalRow : internalRows) {
      writer.write(internalRow);
    }
  }
}
