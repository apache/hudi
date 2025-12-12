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

package org.apache.hudi.testutils;

import org.apache.hudi.HoodieDataSourceHelpers;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test utils for data source tests.
 */
public class DataSourceTestUtils {

  private static final Random RANDOM = new Random(0xDAADDEED);

  public static Schema getStructTypeExampleSchema() throws IOException {
    return new Schema.Parser().parse(FileIOUtils.readAsUTFString(DataSourceTestUtils.class.getResourceAsStream("/exampleSchema.txt")));
  }

  public static Schema getStructTypeExampleEvolvedSchema() throws IOException {
    return new Schema.Parser().parse(FileIOUtils.readAsUTFString(DataSourceTestUtils.class.getResourceAsStream("/exampleEvolvedSchema.txt")));
  }

  public static List<Row> generateRandomRows(int count) {
    List<Row> toReturn = new ArrayList<>();
    List<String> partitions = Arrays.asList(
        DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH);
    for (int i = 0; i < count; i++) {
      Object[] values = new Object[4];
      values[0] = HoodieTestDataGenerator.genPseudoRandomUUID(RANDOM).toString();
      values[1] = partitions.get(i % 3);
      values[2] = new Date().getTime();
      values[3] = false;
      toReturn.add(RowFactory.create(values));
    }
    return toReturn;
  }

  public static List<Row> generateRandomRowsByPartition(int count, String partition) {
    List<Row> toReturn = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Object[] values = new Object[4];
      values[0] = HoodieTestDataGenerator.genPseudoRandomUUID(RANDOM).toString();
      values[1] = partition;
      values[2] = new Date().getTime();
      values[3] = false;
      toReturn.add(RowFactory.create(values));
    }
    return toReturn;
  }

  public static List<Row> generateUpdates(List<Row> records, int count) {
    List<Row> toReturn = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Object[] values = new Object[4];
      values[0] = records.get(i).getString(0);
      values[1] = records.get(i).getAs(1);
      values[2] = new Date().getTime();
      values[3] = false;
      toReturn.add(RowFactory.create(values));
    }
    return toReturn;
  }

  public static List<Row> getUniqueRows(List<Row> inserts, int count) {
    List<Row> toReturn = new ArrayList<>();
    int soFar = 0;
    int curIndex = 0;
    while (soFar < count) {
      if (!toReturn.contains(inserts.get(curIndex))) {
        toReturn.add(inserts.get(curIndex));
        soFar++;
      }
      curIndex++;
    }
    return toReturn;
  }

  public static List<Row> generateRandomRowsEvolvedSchema(int count) {
    List<Row> toReturn = new ArrayList<>();
    List<String> partitions = Arrays.asList(new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH});
    for (int i = 0; i < count; i++) {
      Object[] values = new Object[5];
      values[0] = UUID.randomUUID().toString();
      values[1] = partitions.get(RANDOM.nextInt(3));
      values[2] = new Date().getTime();
      values[3] = false;
      values[4] = UUID.randomUUID().toString();
      toReturn.add(RowFactory.create(values));
    }
    return toReturn;
  }

  public static List<Row> updateRowsWithUpdatedTs(Dataset<Row> inputDf) {
    return updateRowsWithUpdatedTs(inputDf, false, false);
  }

  public static List<Row> updateRowsWithUpdatedTs(Dataset<Row> inputDf, Boolean lowerTs, Boolean updatePartitionPath) {
    List<Row> input = inputDf.collectAsList();
    List<Row> rows = new ArrayList<>();
    for (Row row : input) {
      Object[] values = new Object[4];
      values[0] = row.getAs("_row_key");
      String partition = row.getAs("partition");
      if (updatePartitionPath) {
        values[1] = partition.equals(DEFAULT_FIRST_PARTITION_PATH) ? DEFAULT_SECOND_PARTITION_PATH :
            (partition.equals(DEFAULT_SECOND_PARTITION_PATH) ? DEFAULT_THIRD_PARTITION_PATH : DEFAULT_FIRST_PARTITION_PATH);
      } else {
        values[1] = partition;
      }
      values[2] = ((Long) row.getAs("ts")) + (lowerTs ? (-1 - RANDOM.nextInt(1000)) : RANDOM.nextInt(1000));
      values[3] = false;
      rows.add(RowFactory.create(values));
    }
    return rows;
  }

  /**
   * Test if there is only log files exists in the table.
   */
  public static boolean isLogFileOnly(String basePath) throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(basePath);
    FileSystem fs = path.getFileSystem(conf);
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, true);
    while (files.hasNext()) {
      LocatedFileStatus file = files.next();
      // skip meta folder
      if (file.isFile() && !file.getPath().toString().contains(HoodieTableMetaClient.METAFOLDER_NAME + StoragePath.SEPARATOR)) {
        if (HadoopFSUtils.isBaseFile(file.getPath())) {
          return false;
        }
      }
    }
    return true;
  }

  public static String latestCommitCompletionTime(FileSystem fs, String basePath) {
    return HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, basePath)
        .getLatestCompletionTime().orElse(null);
  }

  public static String latestCommitCompletionTime(HoodieStorage storage, String basePath) {
    return HoodieDataSourceHelpers.allCompletedCommitsCompactions(storage, basePath)
        .getLatestCompletionTime().orElse(null);
  }

  public static String latestCommitRequestTime(HoodieStorage storage, String basePath) {
    return HoodieDataSourceHelpers.allCompletedCommitsCompactions(storage, basePath)
        .lastInstant().map(instant -> instant.requestedTime()).orElse(null);
  }

  public static String latestDeltaCommitCompletionTime(HoodieStorage storage, String basePath) {
    return HoodieDataSourceHelpers.allCompletedCommitsCompactions(storage, basePath)
        .filter(instant -> HoodieTimeline.DELTA_COMMIT_ACTION.equals(instant.getAction()))
        .getLatestCompletionTime().orElse(null);
  }

  public static String latestDeltaCommitRequest(HoodieStorage storage, String basePath) {
    return HoodieDataSourceHelpers.allCompletedCommitsCompactions(storage, basePath)
        .filter(instant -> HoodieTimeline.DELTA_COMMIT_ACTION.equals(instant.getAction()))
        .lastInstant().map(instant -> instant.requestedTime()).orElse(null);
  }

  public static void validateCommitMetadata(HoodieCommitMetadata commitMetadata, String previousCommit, long expectedTotalRecordsWritten, long expectedTotalUpdatedRecords,
                                            long expectedTotalInsertedRecords, long expectedTotalDeletedRecords) {
    long totalRecordsWritten = 0;
    long totalDeletedRecords = 0;
    long totalUpdatedRecords = 0;
    long totalInsertedRecords = 0;
    for (HoodieWriteStat writeStat : commitMetadata.getWriteStats()) {
      totalRecordsWritten += writeStat.getNumWrites();
      totalDeletedRecords += writeStat.getNumDeletes();
      totalUpdatedRecords += writeStat.getNumUpdateWrites();
      totalInsertedRecords += writeStat.getNumInserts();
      assertEquals(previousCommit, writeStat.getPrevCommit());
      assertNotNull(writeStat.getFileId());
      assertNotNull(writeStat.getPath());
      assertTrue(writeStat.getFileSizeInBytes() > 0);
      assertTrue(writeStat.getTotalWriteBytes() > 0);
      if (commitMetadata.getOperationType() == WriteOperationType.COMPACT) {
        assertTrue(writeStat.getTotalLogBlocks() > 0);
        assertTrue(writeStat.getTotalLogSizeCompacted() > 0);
        assertTrue(writeStat.getTotalLogFilesCompacted() > 0);
        assertTrue(writeStat.getTotalLogRecords() > 0);
      }
    }
    assertEquals(expectedTotalRecordsWritten, totalRecordsWritten);
    assertEquals(expectedTotalUpdatedRecords, totalUpdatedRecords);
    assertEquals(expectedTotalInsertedRecords, totalInsertedRecords);
    assertEquals(expectedTotalDeletedRecords, totalDeletedRecords);
  }
}
