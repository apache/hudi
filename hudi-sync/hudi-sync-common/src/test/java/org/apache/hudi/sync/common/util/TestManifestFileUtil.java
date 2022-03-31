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

import org.apache.hudi.common.fs.FSUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;

public class TestManifestFileUtil extends HoodieCommonTestHarness {

  private static final List<String> MULTI_LEVEL_PARTITIONS = Arrays.asList("2019/01", "2020/01", "2021/01");
  private static HoodieTestTable hoodieTestTable;

  @BeforeEach
  public void setUp() throws IOException {
    initMetaClient();
    hoodieTestTable = HoodieTestTable.of(metaClient);
  }

  @Test
  public void testMultiLevelPartitionedTable() throws Exception {
    // Generate 10 files under each partition
    createTestDataForPartitionedTable(10);
    ManifestFileUtil manifestFileUtil = ManifestFileUtil.builder().setConf(metaClient.getHadoopConf()).setBasePath(basePath).build();
    Assertions.assertEquals(30, manifestFileUtil.fetchLatestBaseFilesForAllPartitions().count());
  }

  @Test
  public void testCreateManifestFile() throws Exception {
    // Generate 10 files under each partition
    createTestDataForPartitionedTable(10);
    ManifestFileUtil mainfestFileUtil = ManifestFileUtil.builder().setConf(metaClient.getHadoopConf()).setBasePath(basePath).build();
    try {
      mainfestFileUtil.writeManifestFile();
      Assertions.assertTrue(FSUtils.getFileSize(metaClient.getFs(), new Path(mainfestFileUtil.getManifestFilePath())) > 0);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void createTestDataForPartitionedTable(int numOfFiles) throws Exception {
    String instant = "100";
    hoodieTestTable = hoodieTestTable.addCommit(instant);
    // Generate 10 files under each partition
    MULTI_LEVEL_PARTITIONS.forEach(p -> {
      try {
        hoodieTestTable = hoodieTestTable.withPartitionMetaFiles(p)
            .withBaseFilesInPartition(p, IntStream.range(0, numOfFiles).toArray());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
