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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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

  @Test
  public void testMergeOnReadUpdatesDecomposeIntoDeletesAndInserts() throws IOException {
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ);
    HoodieWriteConfig config = getConfigBuilder(true, HoodieIndex.IndexType.SIMPLE)
        .withWriteUpdatesAsDeletesAndInserts(true)
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

      // Every log file belongs to a file slice with a base file and holds only deletes; despite
      // the small-file limit, no update was merged into an existing base file in place.
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
          context(), metaClient, metaClient.getActiveTimeline().filterCompletedInstants());
      try {
        List<FileSlice> slicesWithLogs = Arrays.stream(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS)
            .flatMap(fsView::getLatestFileSlices)
            .filter(FileSlice::hasLogFiles)
            .collect(Collectors.toList());
        assertTrue(!slicesWithLogs.isEmpty(), "the deletes must land in log files");
        slicesWithLogs.forEach(slice -> {
          assertTrue(slice.getBaseFile().isPresent(),
              "a file group with delete logs must have a base file");
          slice.getLogFiles().forEach(logFile ->
              assertTrue(logFile.getFileName().contains(".deletes."),
                  "log files must only contain deletes but got: " + logFile.getFileName()));
        });
      } finally {
        fsView.close();
      }
    }
  }
}
