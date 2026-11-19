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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies hoodie.write.updates.as.deletes.and.inserts through the Spark write path: updates on a
 * merge-on-read table decompose into positional deletes plus inserts, repeated updates of the same
 * keys resolve to a single live copy, and log files carry only deletes.
 */
@Tag("functional")
public class TestSparkUpdatesAsDeletesAndInserts extends SparkClientFunctionalTestHarness {

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"EIGHT", "NINE", "TEN"})
  void testMergeOnReadUpdatesDecomposeIntoDeletesAndInserts(HoodieTableVersion tableVersion) throws IOException {
    Properties tableProperties = new Properties();
    tableProperties.setProperty(HoodieTableConfig.VERSION.key(), String.valueOf(tableVersion.versionCode()));
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, tableProperties);
    HoodieWriteConfig config = getConfigBuilder(true, HoodieIndex.IndexType.SIMPLE)
        .withWriteUpdatesAsDeletesAndInserts(true)
        .withWriteTableVersion(tableVersion.versionCode())
        .withAutoUpgradeVersion(false)
        .build();
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String insertTime = client.startCommit();
      List<HoodieRecord> inserts = dataGen.generateInserts(insertTime, 100);
      JavaRDD<WriteStatus> insertStatuses = client.upsert(jsc().parallelize(inserts, 1), insertTime);
      client.commit(insertTime, insertStatuses);

      // Repeated rounds update every key: from the second round on, the simple index sees a key
      // in its tombstoned old file group and its current one, and each round must still produce
      // exactly one positional delete and one insert per key.
      for (int round = 0; round < 2; round++) {
        String updateTime = client.startCommit();
        List<HoodieRecord> updates = dataGen.generateUniqueUpdates(updateTime, 100);
        List<WriteStatus> statuses = client.upsert(jsc().parallelize(updates, 1), updateTime).collect();
        client.commit(updateTime, jsc().parallelize(statuses, 1));
        long deletes = statuses.stream().mapToLong(status -> status.getStat().getNumDeletes()).sum();
        long insertCount = statuses.stream().mapToLong(status -> status.getStat().getNumInserts()).sum();
        long updateWrites = statuses.stream().mapToLong(status -> status.getStat().getNumUpdateWrites()).sum();
        assertEquals(100, deletes, "each key must be deleted exactly once per round");
        assertEquals(100, insertCount, "each key must be re-inserted exactly once per round");
        assertEquals(0, updateWrites, "no update record must be written in this mode");
      }

      // Every log file belongs to a file slice with a base file and holds only delete blocks whose
      // positions reference that base file; despite the small-file limit, no update was merged into
      // an existing base file in place. Version 10 writes native delete log files, earlier versions
      // inline log files, so the blocks are read through the log reader.
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertEquals(tableVersion, metaClient.getTableConfig().getTableVersion());
      HoodieSchema schema = HoodieSchema.parse(config.getSchema());
      long deleteRecords = 0;
      HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
          context(), metaClient, metaClient.getActiveTimeline().filterCompletedInstants());
      try {
        List<FileSlice> slicesWithLogs = Arrays.stream(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS)
            .flatMap(fsView::getLatestFileSlices)
            .filter(FileSlice::hasLogFiles)
            .collect(Collectors.toList());
        assertTrue(!slicesWithLogs.isEmpty(), "the deletes must land in log files");
        for (FileSlice slice : slicesWithLogs) {
          assertTrue(slice.getBaseFile().isPresent(),
              "a file group with delete logs must have a base file");
          List<String> baseFileKeys = readRecordKeys(config, slice.getBaseFile().get());
          for (HoodieLogFile logFile : slice.getLogFiles().collect(Collectors.toList())) {
            assertEquals(tableVersion.greaterThanOrEquals(HoodieTableVersion.TEN),
                FSUtils.isNativeDeleteLogFile(logFile.getFileName()), "unexpected log file layout: " + logFile.getFileName());
            long logFileDeletes = deleteRecords;
            try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(metaClient, logFile, schema)) {
              while (reader.hasNext()) {
                HoodieLogBlock block = reader.next();
                assertTrue(block instanceof HoodieDeleteBlock,
                    "expected only delete blocks but got: " + block.getClass().getSimpleName() + " in " + logFile.getFileName());
                HoodieDeleteBlock deleteBlock = (HoodieDeleteBlock) block;
                DeleteRecord[] deletes = deleteBlock.getRecordsToDelete();
                List<Long> positions = deleteBlock.getRecordPositionList();
                assertEquals(deletes.length, positions.size(),
                    "every delete record must carry a position");
                for (int i = 0; i < positions.size(); i++) {
                  long position = positions.get(i);
                  assertTrue(position >= 0 && position < baseFileKeys.size(), "delete record position out of range: " + position);
                  assertEquals(deletes[i].getRecordKey(), baseFileKeys.get((int) position),
                      "the base file row at a delete position must hold the deleted record key");
                }
                assertEquals(slice.getBaseFile().get().getCommitTime(), deleteBlock.getBaseFileInstantTimeOfPositions(),
                    "positions must reference the file slice's base file");
                deleteRecords += positions.size();
              }
            }
            assertTrue(deleteRecords > logFileDeletes, "log file must hold deletes: " + logFile.getFileName());
          }
        }
      } finally {
        fsView.close();
      }
      // Both rounds tombstone 100 records each in file groups that remain the latest slices.
      assertEquals(200, deleteRecords, "each update must produce exactly one positional delete");
    }
  }

  /**
   * Returns the record keys of a base file in row order, so that a key's index is its row position.
   */
  private List<String> readRecordKeys(HoodieWriteConfig config, HoodieBaseFile baseFile) throws IOException {
    List<String> keys = new ArrayList<>();
    try (HoodieFileReader reader = HoodieIOFactory.getIOFactory(hoodieStorage())
        .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
        .getFileReader(config, new StoragePath(baseFile.getPath()));
         ClosableIterator<String> keyIterator = reader.getRecordKeyIterator()) {
      keyIterator.forEachRemaining(keys::add);
    }
    return keys;
  }
}
