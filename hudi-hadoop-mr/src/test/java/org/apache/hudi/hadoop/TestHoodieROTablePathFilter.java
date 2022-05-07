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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieROTablePathFilter extends HoodieCommonTestHarness {

  private HoodieROTablePathFilter pathFilter;
  private HoodieTestTable testTable;

  @BeforeEach
  public void setUp() throws Exception {
    initMetaClient();
    pathFilter = new HoodieROTablePathFilter(metaClient.getHadoopConf());
    testTable = HoodieTestTable.of(metaClient);
  }

  @Test
  public void testHoodiePaths() throws Exception {
    final String p1 = "2017/01/01";
    final String p2 = "2017/01/02";
    testTable.addCommit("001")
        .withBaseFilesInPartition(p1, "f1", "f2")
        .withBaseFilesInPartition(p2, "f3")
        .addCommit("002")
        .withBaseFilesInPartition(p1, "f2")
        .addInflightCommit("003")
        .withBaseFilesInPartition(p2, "f3")
        .addRequestedCompaction("004");

    assertTrue(pathFilter.accept(testTable.forCommit("002").getBaseFilePath(p1, "f2")));
    assertFalse(pathFilter.accept(testTable.forCommit("003").getBaseFilePath(p2, "f3")));
    assertFalse(pathFilter.accept(testTable.forCommit("003").getBaseFilePath(p1, "f3")));

    assertFalse(pathFilter.accept(testTable.getCommitFilePath("001")));
    assertFalse(pathFilter.accept(testTable.getCommitFilePath("002")));
    assertFalse(pathFilter.accept(testTable.getInflightCommitFilePath("003")));
    assertFalse(pathFilter.accept(testTable.getRequestedCompactionFilePath("004")));
    assertFalse(pathFilter.accept(new Path("file:///" + basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/")));
    assertFalse(pathFilter.accept(new Path("file:///" + basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/hoodie.properties")));
    assertFalse(pathFilter.accept(new Path("file:///" + basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME)));

    assertEquals(1, pathFilter.metaClientCache.size());
    assertEquals(0, pathFilter.nonHoodiePathCache.size(), "NonHoodiePathCache size should be 0");
  }

  @Test
  public void testNonHoodiePaths() throws IOException {
    java.nio.file.Path path1 = Paths.get(basePath, "nonhoodiefolder");
    Files.createDirectories(path1);
    assertTrue(pathFilter.accept(new Path(path1.toUri())));

    java.nio.file.Path path2 = Paths.get(basePath, "nonhoodiefolder/somefile");
    Files.createFile(path2);
    assertTrue(pathFilter.accept(new Path(path2.toUri())));
    assertEquals(2, pathFilter.nonHoodiePathCache.size(), "NonHoodiePathCache size should be 2");
  }

  @Test
  public void testPartitionPathsAsNonHoodiePaths() throws Exception {
    final String p1 = "2017/01/01";
    final String p2 = "2017/01/02";
    testTable.addCommit("001").getFileIdsWithBaseFilesInPartitions(p1, p2);
    Path partitionPath1 = testTable.getPartitionPath(p1).getParent();
    Path partitionPath2 = testTable.getPartitionPath(p2).getParent();
    assertTrue(pathFilter.accept(partitionPath1), "Directories should be accepted");
    assertTrue(pathFilter.accept(partitionPath2), "Directories should be accepted");
    assertEquals(2, pathFilter.nonHoodiePathCache.size(), "NonHoodiePathCache size should be 2");
  }
}
