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

package org.apache.hudi.util;

import org.apache.hudi.client.SecondaryIndexStats;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.testutils.HoodieFlinkClientTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link WriteStatusMerger}.
 */
public class TestWriteStatusMerger extends HoodieFlinkClientTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initFileSystem();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void testMergeWriteStatusWithoutErrors() {
    // Create first write status
    WriteStatus writeStatus1 = new WriteStatus(false, 0.0);
    writeStatus1.setFileId("file1");
    writeStatus1.setPartitionPath("partition1");

    HoodieWriteStat writeStat1 = new HoodieWriteStat();
    writeStat1.setFileId("file1");
    writeStat1.setPath("path1");
    writeStat1.setNumWrites(10);
    writeStat1.setTotalWriteBytes(1000);
    writeStatus1.setStat(writeStat1);

    writeStatus1.setTotalRecords(10);
    writeStatus1.setTotalErrorRecords(0);

    // Add some written record delegates
    HoodieRecordDelegate delegate1 = HoodieRecordDelegate.create("key1", "partition1");
    HoodieRecordDelegate delegate2 = HoodieRecordDelegate.create("key2", "partition1");
    writeStatus1.addRecordDelegate(delegate1);
    writeStatus1.addRecordDelegate(delegate2);

    // Add secondary index stats
    writeStatus1.getIndexStats().addSecondaryIndexStats("idx1", "key1", "value1", false);
    writeStatus1.getIndexStats().addSecondaryIndexStats("idx2", "key2", "value2", false);

    // Create second write status
    WriteStatus writeStatus2 = new WriteStatus(false, 0.0);
    writeStatus2.setFileId("file1"); // Same file id
    writeStatus2.setPartitionPath("partition1"); // Same partition path

    HoodieWriteStat writeStat2 = new HoodieWriteStat();
    writeStat2.setFileId("file1");
    writeStat2.setPath("path2");
    writeStat2.setNumWrites(15);
    writeStat2.setTotalWriteBytes(1500);
    writeStatus2.setStat(writeStat2);

    writeStatus2.setTotalRecords(15);
    writeStatus2.setTotalErrorRecords(0);

    // Add some written record delegates
    HoodieRecordDelegate delegate3 = HoodieRecordDelegate.create("key3", "partition1");
    HoodieRecordDelegate delegate4 = HoodieRecordDelegate.create("key4", "partition1");
    writeStatus2.addRecordDelegate(delegate3);
    writeStatus2.addRecordDelegate(delegate4);

    // Add secondary index stats
    writeStatus2.getIndexStats().addSecondaryIndexStats("idx1", "key3", "value3", false);
    writeStatus2.getIndexStats().addSecondaryIndexStats("idx3", "key4", "value4", false);

    // Merge the write statuses
    WriteStatus mergedStatus = WriteStatusMerger.merge(writeStatus1, writeStatus2);

    // Verify merged status
    assertEquals("file1", mergedStatus.getFileId());
    assertEquals("partition1", mergedStatus.getPartitionPath());
    assertEquals(25, mergedStatus.getTotalRecords()); // 10 + 15
    assertEquals(0, mergedStatus.getTotalErrorRecords());
    assertFalse(mergedStatus.hasErrors());
    assertFalse(mergedStatus.hasGlobalError());
    assertNull(mergedStatus.getGlobalError());

    // Verify merged write stat
    HoodieWriteStat mergedStat = mergedStatus.getStat();
    assertNotNull(mergedStat);
    assertEquals("file1", mergedStat.getFileId());
    assertEquals("path2", mergedStat.getPath()); // path from the 2nd write stat
    assertEquals(25, mergedStat.getNumWrites()); // 10 + 15
    assertEquals(2500, mergedStat.getTotalWriteBytes()); // 1000 + 1500

    // Verify merged record delegates
    List<HoodieRecordDelegate> writtenRecordDelegates = mergedStatus.getIndexStats().getWrittenRecordDelegates();
    assertEquals(4, writtenRecordDelegates.size()); // 2 + 2
    assertTrue(writtenRecordDelegates.contains(delegate1));
    assertTrue(writtenRecordDelegates.contains(delegate2));
    assertTrue(writtenRecordDelegates.contains(delegate3));
    assertTrue(writtenRecordDelegates.contains(delegate4));

    // Verify merged secondary index stats
    Map<String, List<SecondaryIndexStats>> secondaryIndexStats = mergedStatus.getIndexStats().getSecondaryIndexStats();
    assertEquals(3, secondaryIndexStats.size()); // idx1, idx2, idx3
    assertEquals(2, secondaryIndexStats.get("idx1").size()); // key1, key3
    assertEquals(1, secondaryIndexStats.get("idx2").size()); // key2
    assertEquals(1, secondaryIndexStats.get("idx3").size()); // key4
  }

  @Test
  public void testMergeWriteStatusWithErrors() {
    // Create first write status with errors
    WriteStatus writeStatus1 = new WriteStatus(false, 0.0);
    writeStatus1.setFileId("file1");
    writeStatus1.setPartitionPath("partition1");

    HoodieWriteStat writeStat1 = new HoodieWriteStat();
    writeStat1.setFileId("file1");
    writeStat1.setPath("path1");
    writeStat1.setNumWrites(10);
    writeStat1.setTotalWriteErrors(2);
    writeStatus1.setStat(writeStat1);

    writeStatus1.setTotalRecords(12);
    writeStatus1.setTotalErrorRecords(2);

    // Add errors
    HoodieKey errorKey1 = new HoodieKey("errorKey1", "partition1");
    Exception error1 = new Exception("Error 1");
    writeStatus1.getErrors().put(errorKey1, error1);

    HoodieRecordDelegate failedRecord1 = HoodieRecordDelegate.create("errorKey1", "partition1");
    writeStatus1.getFailedRecords().add(
        org.apache.hudi.common.util.collection.Pair.of(failedRecord1, error1));

    // Create second write status with errors
    WriteStatus writeStatus2 = new WriteStatus(false, 0.0);
    writeStatus2.setFileId("file1");
    writeStatus2.setPartitionPath("partition1");

    HoodieWriteStat writeStat2 = new HoodieWriteStat();
    writeStat2.setFileId("file1");
    writeStat2.setPath("path2");
    writeStat2.setNumWrites(15);
    writeStat2.setTotalWriteErrors(3);
    writeStatus2.setStat(writeStat2);

    writeStatus2.setTotalRecords(18);
    writeStatus2.setTotalErrorRecords(3);

    // Add errors
    HoodieKey errorKey2 = new HoodieKey("errorKey2", "partition1");
    Exception error2 = new Exception("Error 2");
    writeStatus2.getErrors().put(errorKey2, error2);

    HoodieRecordDelegate failedRecord2 = HoodieRecordDelegate.create("errorKey2", "partition1");
    writeStatus2.getFailedRecords().add(
        org.apache.hudi.common.util.collection.Pair.of(failedRecord2, error2));

    // Merge the write statuses
    WriteStatus mergedStatus = WriteStatusMerger.merge(writeStatus1, writeStatus2);

    // Verify merged status
    assertEquals("file1", mergedStatus.getFileId());
    assertEquals("partition1", mergedStatus.getPartitionPath());
    assertEquals(30, mergedStatus.getTotalRecords()); // 12 + 18
    assertEquals(5, mergedStatus.getTotalErrorRecords()); // 2 + 3
    assertTrue(mergedStatus.hasErrors());
    assertFalse(mergedStatus.hasGlobalError());
    assertNull(mergedStatus.getGlobalError());

    // Verify merged write stat
    HoodieWriteStat mergedStat = mergedStatus.getStat();
    assertNotNull(mergedStat);
    assertEquals("file1", mergedStat.getFileId());
    assertEquals("path2", mergedStat.getPath()); // path from the 2nd write stat
    assertEquals(25, mergedStat.getNumWrites()); // 10 + 15
    assertEquals(5, mergedStat.getTotalWriteErrors()); // 2 + 3

    // Verify merged errors
    Map<HoodieKey, Throwable> errors = mergedStatus.getErrors();
    assertEquals(2, errors.size());
    assertTrue(errors.containsKey(errorKey1));
    assertTrue(errors.containsKey(errorKey2));

    // Verify failed records
    assertEquals(2, mergedStatus.getFailedRecords().size());
    assertTrue(mergedStatus.getFailedRecords().stream()
        .anyMatch(pair -> pair.getLeft().getRecordKey().equals("errorKey1")));
    assertTrue(mergedStatus.getFailedRecords().stream()
        .anyMatch(pair -> pair.getLeft().getRecordKey().equals("errorKey2")));
  }

  @Test
  public void testMergeWriteStatusWithGlobalError() {
    // Create first write status with global error
    WriteStatus writeStatus1 = new WriteStatus(false, 0.0);
    writeStatus1.setFileId("file1");
    writeStatus1.setPartitionPath("partition1");

    Exception globalError1 = new Exception("Global Error 1");
    writeStatus1.setGlobalError(globalError1);

    HoodieWriteStat writeStat1 = new HoodieWriteStat();
    writeStat1.setFileId("file1");
    writeStat1.setPath("path1");
    writeStatus1.setStat(writeStat1);

    // Create second write status with global error
    WriteStatus writeStatus2 = new WriteStatus(false, 0.0);
    writeStatus2.setFileId("file1");
    writeStatus2.setPartitionPath("partition1");

    Exception globalError2 = new Exception("Global Error 2");
    writeStatus2.setGlobalError(globalError2);

    HoodieWriteStat writeStat2 = new HoodieWriteStat();
    writeStat2.setFileId("file1");
    writeStat2.setPath("path1");
    writeStatus2.setStat(writeStat2);

    // Merge the write statuses
    WriteStatus mergedStatus = WriteStatusMerger.merge(writeStatus1, writeStatus2);

    // Verify that the second global error overwrites the first one
    assertTrue(mergedStatus.hasGlobalError());
    assertEquals(globalError2, mergedStatus.getGlobalError());
  }

  @Test
  public void testMergeWriteStatusFirstHasGlobalError() {
    // Create first write status with global error
    WriteStatus writeStatus1 = new WriteStatus(false, 0.0);
    writeStatus1.setFileId("file1");
    writeStatus1.setPartitionPath("partition1");

    Exception globalError1 = new Exception("Global Error 1");
    writeStatus1.setGlobalError(globalError1);

    HoodieWriteStat writeStat1 = new HoodieWriteStat();
    writeStat1.setFileId("file1");
    writeStat1.setPath("path1");
    writeStatus1.setStat(writeStat1);

    // Create second write status without global error
    WriteStatus writeStatus2 = new WriteStatus(false, 0.0);
    writeStatus2.setFileId("file1");
    writeStatus2.setPartitionPath("partition1");

    HoodieWriteStat writeStat2 = new HoodieWriteStat();
    writeStat2.setFileId("file1");
    writeStat2.setPath("path2");
    writeStatus2.setStat(writeStat2);

    // Merge the write statuses
    WriteStatus mergedStatus = WriteStatusMerger.merge(writeStatus1, writeStatus2);

    // Verify that the first global error is preserved
    assertTrue(mergedStatus.hasGlobalError());
    assertEquals(globalError1, mergedStatus.getGlobalError());
  }

  @Test
  public void testMergeWriteStatusSecondHasGlobalError() {
    // Create first write status without global error
    WriteStatus writeStatus1 = new WriteStatus(false, 0.0);
    writeStatus1.setFileId("file1");
    writeStatus1.setPartitionPath("partition1");

    HoodieWriteStat writeStat1 = new HoodieWriteStat();
    writeStat1.setFileId("file1");
    writeStat1.setPath("path1");
    writeStatus1.setStat(writeStat1);

    // Create second write status with global error
    WriteStatus writeStatus2 = new WriteStatus(false, 0.0);
    writeStatus2.setFileId("file1");
    writeStatus2.setPartitionPath("partition1");

    Exception globalError2 = new Exception("Global Error 2");
    writeStatus2.setGlobalError(globalError2);

    HoodieWriteStat writeStat2 = new HoodieWriteStat();
    writeStat2.setFileId("file1");
    writeStat2.setPath("path2");
    writeStatus2.setStat(writeStat2);

    // Merge the write statuses
    WriteStatus mergedStatus = WriteStatusMerger.merge(writeStatus1, writeStatus2);

    // Verify that the second global error is used
    assertTrue(mergedStatus.hasGlobalError());
    assertEquals(globalError2, mergedStatus.getGlobalError());
  }
}