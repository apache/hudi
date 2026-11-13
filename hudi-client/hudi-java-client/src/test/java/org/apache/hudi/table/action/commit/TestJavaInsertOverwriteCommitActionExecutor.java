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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieJavaTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.GenericRecordValidationTestUtils;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestJavaInsertOverwriteCommitActionExecutor extends HoodieJavaClientTestHarness {

  @ParameterizedTest
  @CsvSource({"COPY_ON_WRITE,false", "COPY_ON_WRITE,true", "MERGE_ON_READ,false", "MERGE_ON_READ,true"})
  public void testInsertOverwriteDoesNotWriteIntoReplacedSmallFile(HoodieTableType tableType, boolean overwriteTable) throws Exception {
    initMetaClient(tableType);
    dataGen = new HoodieTestDataGenerator(new String[] {DEFAULT_FIRST_PARTITION_PATH});
    HoodieWriteConfig config = getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.SIMPLE).build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, insertTime);
    assertTrue(client.commit(insertTime, client.insert(dataGen.generateInserts(insertTime, 10), insertTime)));

    String overwriteTime = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, overwriteTime, HoodieTimeline.REPLACE_COMMIT_ACTION);
    HoodieJavaTable table = HoodieJavaTable.create(config, context, HoodieTableMetaClient.reload(metaClient));
    client.preWrite(overwriteTime,
        overwriteTable ? WriteOperationType.INSERT_OVERWRITE_TABLE : WriteOperationType.INSERT_OVERWRITE, table.getMetaClient());
    List<HoodieRecord> replacement = dataGen.generateInserts(overwriteTime, 5);
    HoodieWriteMetadata<List<WriteStatus>> result = overwriteTable
        ? table.insertOverwriteTable(context, overwriteTime, replacement)
        : table.insertOverwrite(context, overwriteTime, replacement);
    assertTrue(client.commit(overwriteTime, result.getWriteStatuses(), Option.empty(),
        HoodieTimeline.REPLACE_COMMIT_ACTION, result.getPartitionToReplaceFileIds()));

    Set<String> replacedFileIds = new HashSet<>(result.getPartitionToReplaceFileIds().get(DEFAULT_FIRST_PARTITION_PATH));
    assertFalse(replacedFileIds.isEmpty());
    for (WriteStatus status : result.getWriteStatuses()) {
      assertFalse(replacedFileIds.contains(status.getFileId()),
          "Insert overwrite wrote into file group " + status.getFileId() + ", which it replaces");
    }
    Set<String> expectedKeys = replacement.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet());
    assertEquals(expectedKeys, GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, dataGen).keySet());
  }
}
