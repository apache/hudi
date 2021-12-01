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

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;

@Tag("functional")
public class TestHoodieMetadataBootstrap extends TestHoodieMetadataBase {

  private static final Logger LOG = LogManager.getLogger(TestHoodieMetadataBootstrap.class);

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataBootstrapInsertUpsert(HoodieTableType tableType) throws Exception {
    init(tableType, false);
    doPreBootstrapWriteOperation(testTable, INSERT, "0000001");
    doPreBootstrapWriteOperation(testTable, "0000002");
    if (tableType == MERGE_ON_READ) {
      doPrebootstrapCompaction(testTable, "0000003");
    }
    bootstrapAndVerify();
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataBootstrapInsertUpsertClean(HoodieTableType tableType) throws Exception {
    init(tableType, false);
    doPreBootstrapWriteOperation(testTable, INSERT, "0000001");
    doPreBootstrapWriteOperation(testTable, "0000002");
    doPreBootstrapClean(testTable, "0000003", Arrays.asList("0000001"));
    if (tableType == MERGE_ON_READ) {
      doPrebootstrapCompaction(testTable, "0000004");
    }
    doPreBootstrapWriteOperation(testTable, "0000005");
    bootstrapAndVerify();
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataBootstrapInsertUpsertRollback(HoodieTableType tableType) throws Exception {
    init(tableType, false);
    doPreBootstrapWriteOperation(testTable, INSERT, "0000001");
    doPreBootstrapWriteOperation(testTable, "0000002");
    doPreBootstrapRollback(testTable, "0000003", "0000002");
    if (tableType == MERGE_ON_READ) {
      doPrebootstrapCompaction(testTable, "0000004");
    }
    bootstrapAndVerify();
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataBootstrapInsertUpsertCluster(HoodieTableType tableType) throws Exception {
    init(tableType, false);
    doPreBootstrapWriteOperation(testTable, INSERT, "0000001");
    doPreBootstrapWriteOperation(testTable, "0000002");
    doPreBootstrapCluster(testTable, "0000003");
    if (tableType == MERGE_ON_READ) {
      doPrebootstrapCompaction(testTable, "0000004");
    }
    bootstrapAndVerify();
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataBootstrapLargeCommitList(HoodieTableType tableType) throws Exception {
    init(tableType, false);
    for (int i = 1; i < 25; i += 7) {
      String commitTime1 = ((i > 9) ? ("00000") : ("000000")) + i;
      String commitTime2 = ((i > 9) ? ("00000") : ("000000")) + (i + 1);
      String commitTime3 = ((i > 9) ? ("00000") : ("000000")) + (i + 2);
      String commitTime4 = ((i > 9) ? ("00000") : ("000000")) + (i + 3);
      String commitTime5 = ((i > 9) ? ("00000") : ("000000")) + (i + 4);
      String commitTime6 = ((i > 9) ? ("00000") : ("000000")) + (i + 5);
      String commitTime7 = ((i > 9) ? ("00000") : ("000000")) + (i + 6);
      doPreBootstrapWriteOperation(testTable, INSERT, commitTime1);
      doPreBootstrapWriteOperation(testTable, commitTime2);
      doPreBootstrapClean(testTable, commitTime3, Arrays.asList(commitTime1));
      doPreBootstrapWriteOperation(testTable, commitTime4);
      if (tableType == MERGE_ON_READ) {
        doPrebootstrapCompaction(testTable, commitTime5);
      }
      doPreBootstrapWriteOperation(testTable, commitTime6);
      doPreBootstrapRollback(testTable, commitTime7, commitTime6);
    }
    bootstrapAndVerify();
  }

  @Test
  public void testMetadataBootstrapInflightCommit() throws Exception {
    HoodieTableType tableType = COPY_ON_WRITE;
    init(tableType, false);

    doPreBootstrapWriteOperation(testTable, INSERT, "0000001");
    doPreBootstrapWriteOperation(testTable, "0000002");
    // add an inflight commit
    HoodieCommitMetadata inflightCommitMeta = testTable.doWriteOperation("00000007", UPSERT, emptyList(),
        asList("p1", "p2"), 2, true, true);
    // bootstrap and following validation should fail. bootstrap should not happen.
    bootstrapAndVerifyFailure();

    // once the commit is complete, metadata should get fully synced.
    // in prod code path, SparkHoodieBackedTableMetadataWriter.create() will be called for every commit,
    // which may not be the case here if we directly call HoodieBackedTableMetadataWriter.update()
    // hence lets first move the commit to complete and invoke sync directly
    ((HoodieMetadataTestTable) testTable).moveInflightCommitToComplete("00000007", inflightCommitMeta, true);
    syncTableMetadata(writeConfig);
    validateMetadata(testTable);
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataBootstrapArchival(HoodieTableType tableType) throws Exception {
    init(tableType, false);
    writeConfig = getWriteConfig(2, 4);
    for (int i = 1; i < 13; i += 7) {
      String commitTime1 = ((i > 9) ? ("00000") : ("000000")) + i;
      String commitTime2 = ((i > 9) ? ("00000") : ("000000")) + (i + 1);
      String commitTime3 = ((i > 9) ? ("00000") : ("000000")) + (i + 2);
      String commitTime4 = ((i > 9) ? ("00000") : ("000000")) + (i + 3);
      String commitTime5 = ((i > 9) ? ("00000") : ("000000")) + (i + 4);
      String commitTime6 = ((i > 9) ? ("00000") : ("000000")) + (i + 5);
      String commitTime7 = ((i > 9) ? ("00000") : ("000000")) + (i + 6);
      doPreBootstrapWriteOperation(testTable, INSERT, commitTime1);
      doPreBootstrapWriteOperation(testTable, commitTime2);
      doPreBootstrapClean(testTable, commitTime3, Arrays.asList(commitTime1));
      doPreBootstrapWriteOperation(testTable, commitTime4);
      if (tableType == MERGE_ON_READ) {
        doPrebootstrapCompaction(testTable, commitTime5);
      }
      doPreBootstrapWriteOperation(testTable, commitTime6);
      doPreBootstrapRollback(testTable, commitTime7, commitTime6);
    }
    // archive and then bootstrap
    archiveDataTable(writeConfig, metaClient);
    bootstrapAndVerify();
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataBootstrapAfterRestore(HoodieTableType tableType) throws Exception {
    init(tableType, false);
    testRestore(false);
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataBootstrapAfterRestoreAndUpserts(HoodieTableType tableType) throws Exception {
    init(tableType, false);
    testRestore(true);
  }

  private void testRestore(boolean addUpsertsAfterRestore) throws Exception {
    doPreBootstrapWriteOperation(testTable, INSERT, "0000001");
    doPreBootstrapWriteOperation(testTable, "0000002");
    if (tableType == MERGE_ON_READ) {
      doPrebootstrapCompaction(testTable, "0000003");
    }
    doPreBootstrapWriteOperation(testTable, "0000004");
    doPreBootstrapWriteOperation(testTable, "0000005");
    doPreBootstrapWriteOperation(testTable, "0000006");
    doPreBootstrapRestore(testTable, "0000007", "0000004");

    if (addUpsertsAfterRestore) {
      doPreBootstrapWriteOperation(testTable, "0000008");
      doPreBootstrapWriteOperation(testTable, "0000009");
      if (tableType == MERGE_ON_READ) {
        doPrebootstrapCompaction(testTable, "0000010");
      }
    }
    bootstrapAndVerify();
  }

  private void bootstrapAndVerify() throws Exception {
    writeConfig = getWriteConfig(true, true);
    initWriteConfigAndMetatableWriter(writeConfig, true);
    syncTableMetadata(writeConfig);
    validateMetadata(testTable);
    // after bootstrap do two writes and validate its still functional.
    doWriteInsertAndUpsert(testTable);
    validateMetadata(testTable);
  }

  private void bootstrapAndVerifyFailure() throws Exception {
    writeConfig = getWriteConfig(true, true);
    initWriteConfigAndMetatableWriter(writeConfig, true);
    syncTableMetadata(writeConfig);
    try {
      validateMetadata(testTable);
      Assertions.fail("Should have failed");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  private void doWriteInsertAndUpsert(HoodieTestTable testTable) throws Exception {
    doWriteInsertAndUpsert(testTable, "0000100", "0000101");
  }

  private HoodieWriteConfig getWriteConfig(int minArchivalCommits, int maxArchivalCommits) throws Exception {
    return HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(minArchivalCommits, maxArchivalCommits).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .forTable("test-trip-table").build();
  }

  @Override
  protected HoodieTableType getTableType() {
    return tableType;
  }
}
