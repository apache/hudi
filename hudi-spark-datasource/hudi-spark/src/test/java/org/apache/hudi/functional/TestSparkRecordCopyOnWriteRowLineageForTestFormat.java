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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.functional.TestSparkCopyOnWriteRowLineageForTestFormat;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.RowLineageTestUtils.NO_ROW_LINEAGE;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.assertNewRowLineage;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.assertRowLineageChangesWithCommitSeqNo;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.deriveRowLineage;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.readStoredRowLineage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs {@link TestSparkCopyOnWriteRowLineageForTestFormat} with Spark records, which the merge reads through
 * the Spark parquet reader of the Spark version adapter, and compacts a merge-on-read table with Spark records.
 */
@Tag("functional")
class TestSparkRecordCopyOnWriteRowLineageForTestFormat extends TestSparkCopyOnWriteRowLineageForTestFormat {

  private static final String PARTITION = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;

  @Override
  protected HoodieRecordType getRecordType() {
    return HoodieRecordType.SPARK;
  }

  /**
   * Compaction merges the updates and deletes in the log files of a merge-on-read file group into a new base file,
   * which keeps the row lineage of the records no log file changes.
   */
  @Test
  void testCompactionKeepsRowLineageOfUnchangedRecords() throws IOException {
    HoodieTableMetaClient metaClient = initTable(HoodieTableType.MERGE_ON_READ);
    String tableBasePath = metaClient.getBasePath().toString();
    HoodieWriteConfig config = buildConfig(metaClient, HoodieRecordType.SPARK, 1024 * 1024 * 1024L, HoodieClusteringConfig.newBuilder().build());
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION});
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      List<HoodieRecord> inserts = dataGen.generateInsertsForPartition("001", 20, PARTITION);
      write(client, toRecordType(inserts, HoodieRecordType.SPARK), false);
      HoodieBaseFile baseFile1 = getOnlyLatestBaseFile(metaClient);
      Map<String, Pair<Long, Long>> rowLineage1 = deriveRowLineage(storageConf(), tableBasePath, baseFile1);

      List<HoodieRecord> updates = Arrays.asList(inserts.get(1), inserts.get(7), inserts.get(13));
      List<HoodieRecord> deletes = Arrays.asList(inserts.get(4), inserts.get(10));
      List<HoodieRecord> records = new ArrayList<>(dataGen.generateUpdates("002", updates));
      records.addAll(dataGen.generateDeletesFromExistingRecords(deletes));
      write(client, toRecordType(records, HoodieRecordType.SPARK));
      assertEquals(baseFile1.getFileName(), getOnlyLatestBaseFile(metaClient).getFileName());

      Option<String> compactionTime = client.scheduleCompaction(Option.empty());
      assertTrue(compactionTime.isPresent());
      client.commitCompaction(compactionTime.get(), client.compact(compactionTime.get()), Option.empty());
      HoodieBaseFile baseFile2 = getOnlyLatestBaseFile(metaClient);
      assertEquals(baseFile1.getFileId(), baseFile2.getFileId());
      assertNotEquals(baseFile1.getFileName(), baseFile2.getFileName());

      Map<String, Pair<Long, Long>> expectedStored = new LinkedHashMap<>(rowLineage1);
      expectedStored.keySet().removeAll(keys(deletes));
      keys(updates).forEach(key -> expectedStored.put(key, NO_ROW_LINEAGE));
      assertEquals(expectedStored, readStoredRowLineage(storageConf(), baseFile2));
      assertNewRowLineage(tableBasePath, baseFile2, deriveRowLineage(storageConf(), tableBasePath, baseFile2), keys(updates), rowLineage1.values());
      assertRowLineageChangesWithCommitSeqNo(storageConf(), tableBasePath, baseFile1, baseFile2, 15, 3);
    }
  }
}
