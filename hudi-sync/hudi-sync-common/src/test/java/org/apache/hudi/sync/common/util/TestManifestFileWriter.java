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

package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS;
import static org.apache.hudi.sync.common.util.ManifestFileWriter.fetchLatestBaseFilesForAllPartitions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestManifestFileWriter extends HoodieCommonTestHarness {

  @BeforeEach
  public void setUp() throws IOException {
    initMetaClient();
  }

  @Test
  public void testMultiLevelPartitionedTable() throws Exception {
    // Generate 10 files under each partition
    createTestDataForPartitionedTable(metaClient, 10);
    ManifestFileWriter manifestFileWriter = ManifestFileWriter.builder().setMetaClient(metaClient).build();
    assertEquals(30, fetchLatestBaseFilesForAllPartitions(metaClient, false, false, false).count());
  }

  @Test
  public void testCreateManifestFile() throws Exception {
    // Generate 10 files under each partition
    createTestDataForPartitionedTable(metaClient, 3);
    ManifestFileWriter manifestFileWriter = ManifestFileWriter.builder().setMetaClient(metaClient).build();
    manifestFileWriter.writeManifestFile(false);
    StoragePath manifestFilePath = manifestFileWriter.getManifestFilePath(false);
    try (InputStream is = metaClient.getStorage().open(manifestFilePath)) {
      List<String> expectedLines = FileIOUtils.readAsUTFStringLines(is);
      assertEquals(9, expectedLines.size(), "there should be 9 base files in total; 3 per partition.");
      expectedLines.forEach(line -> assertFalse(line.contains(basePath)));
    }
  }

  @Test
  public void testCreateManifestFileWithAbsolutePath() throws Exception {
    // Generate 10 files under each partition
    createTestDataForPartitionedTable(metaClient, 3);
    ManifestFileWriter manifestFileWriter = ManifestFileWriter.builder().setMetaClient(metaClient).build();
    manifestFileWriter.writeManifestFile(true);
    StoragePath manifestFilePath = manifestFileWriter.getManifestFilePath(true);
    try (InputStream is = metaClient.getStorage().open(manifestFilePath)) {
      List<String> expectedLines = FileIOUtils.readAsUTFStringLines(is);
      assertEquals(9, expectedLines.size(), "there should be 9 base files in total; 3 per partition.");
      expectedLines.forEach(line -> assertTrue(line.startsWith(metaClient.getStorage().getScheme() + ":" + basePath)));
    }
  }

  private static void createTestDataForPartitionedTable(HoodieTableMetaClient metaClient, int numFilesPerPartition) throws Exception {
    final String instantTime = "100";
    HoodieTestTable testTable = HoodieTestTable.of(metaClient).addCommit(instantTime);
    for (String partition : DEFAULT_PARTITION_PATHS) {
      testTable.withPartitionMetaFiles(partition)
          .withBaseFilesInPartition(partition, IntStream.range(0, numFilesPerPartition).toArray());
    }
  }

  @Test
  public void getManifestSourceUri() {
    ManifestFileWriter manifestFileWriter = ManifestFileWriter.builder().setMetaClient(metaClient).build();
    String sourceUri = manifestFileWriter.getManifestSourceUri(false);
    assertEquals(new Path(basePath, ".hoodie/manifest/*").toUri().toString(), sourceUri);

    sourceUri = manifestFileWriter.getManifestSourceUri(true);
    assertEquals(new Path(basePath, ".hoodie/absolute-path-manifest/*").toUri().toString(), sourceUri);
  }
}
