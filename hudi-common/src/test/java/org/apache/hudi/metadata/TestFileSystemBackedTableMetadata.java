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

package org.apache.hudi.metadata;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestFileSystemBackedTableMetadata extends HoodieCommonTestHarness {

  private static final String DEFAULT_PARTITION = "";
  private static final List<String> DATE_PARTITIONS = Arrays.asList("2019/01/01", "2020/01/02", "2021/03/01");
  private static final List<String> ONE_LEVEL_PARTITIONS = Arrays.asList("2019", "2020", "2021");
  private static final List<String> MULTI_LEVEL_PARTITIONS = Arrays.asList("2019/01", "2020/01", "2021/01");
  private static HoodieTestTable hoodieTestTable;

  @BeforeEach
  public void setUp() throws IOException {
    initMetaClient();
    hoodieTestTable = HoodieTestTable.of(metaClient);
  }

  @AfterEach
  public void tearDown() throws IOException {
    metaClient.getFs().delete(new Path(metaClient.getBasePath()), true);
  }

  /**
   * Test non partition hoodie table.
   * @throws Exception
   */
  @Test
  public void testNonPartitionedTable() throws Exception {
    // Generate 10 files under basepath
    hoodieTestTable.addCommit("100").withBaseFilesInPartition(DEFAULT_PARTITION, IntStream.range(0, 10).toArray());
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
    FileSystemBackedTableMetadata fileSystemBackedTableMetadata =
        new FileSystemBackedTableMetadata(localEngineContext, new SerializableConfiguration(metaClient.getHadoopConf()), basePath, false);
    Assertions.assertEquals(0, fileSystemBackedTableMetadata.getAllPartitionPaths().size());
    Assertions.assertEquals(10, fileSystemBackedTableMetadata.getAllFilesInPartition(new Path(basePath)).length);
    Assertions.assertEquals(10, fileSystemBackedTableMetadata.getAllFilesInPartitions(
        Collections.singletonList(basePath)).get(basePath).length);
  }

  /**
   * Test listing of partitions result for date based partitions.
   * @throws Exception
   */
  @Test
  public void testDatePartitionedTable() throws Exception {
    String instant = "100";
    hoodieTestTable = hoodieTestTable.addCommit(instant);
    // Generate 10 files under each partition
    DATE_PARTITIONS.stream().forEach(p -> {
      try {
        hoodieTestTable = hoodieTestTable.withBaseFilesInPartition(p, IntStream.range(0, 10).toArray());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
    FileSystemBackedTableMetadata fileSystemBackedTableMetadata =
        new FileSystemBackedTableMetadata(localEngineContext, new SerializableConfiguration(metaClient.getHadoopConf()), basePath, true);
    Assertions.assertEquals(3, fileSystemBackedTableMetadata.getAllPartitionPaths().size());
    Assertions.assertEquals(10, fileSystemBackedTableMetadata.getAllFilesInPartition(new Path(basePath + "/" + DATE_PARTITIONS.get(0))).length);

    List<String> fullPartitionPaths = DATE_PARTITIONS.stream().map(p -> basePath + "/" + p).collect(Collectors.toList());
    Map<String, FileStatus[]> partitionToFilesMap = fileSystemBackedTableMetadata.getAllFilesInPartitions(fullPartitionPaths);
    for (String p : fullPartitionPaths) {
      Assertions.assertEquals(10, partitionToFilesMap.get(p).length);
    }
  }

  /**
   * Test listing of partitions result for date based partitions with assumeDataPartitioning = false.
   * @throws Exception
   */
  @Test
  public void testDatePartitionedTableWithAssumeDateIsFalse() throws Exception {
    String instant = "100";
    hoodieTestTable = hoodieTestTable.addCommit(instant);
    // Generate 10 files under each partition
    DATE_PARTITIONS.stream().forEach(p -> {
      try {
        hoodieTestTable = hoodieTestTable
            .withPartitionMetaFiles(p)
            .withBaseFilesInPartition(p, IntStream.range(0, 10).toArray());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
    FileSystemBackedTableMetadata fileSystemBackedTableMetadata =
        new FileSystemBackedTableMetadata(localEngineContext, new SerializableConfiguration(metaClient.getHadoopConf()), basePath, false);
    Assertions.assertEquals(3, fileSystemBackedTableMetadata.getAllPartitionPaths().size());

    List<String> fullPartitionPaths = DATE_PARTITIONS.stream().map(p -> basePath + "/" + p).collect(Collectors.toList());
    Map<String, FileStatus[]> partitionToFilesMap = fileSystemBackedTableMetadata.getAllFilesInPartitions(fullPartitionPaths);
    for (String p : fullPartitionPaths) {
      Assertions.assertEquals(10, partitionToFilesMap.get(p).length);
    }
  }

  @Test
  public void testOneLevelPartitionedTable() throws Exception {
    String instant = "100";
    hoodieTestTable = hoodieTestTable.addCommit(instant);
    // Generate 10 files under each partition
    ONE_LEVEL_PARTITIONS.stream().forEach(p -> {
      try {
        hoodieTestTable = hoodieTestTable.withPartitionMetaFiles(p)
            .withBaseFilesInPartition(p, IntStream.range(0, 10).toArray());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
    FileSystemBackedTableMetadata fileSystemBackedTableMetadata =
        new FileSystemBackedTableMetadata(localEngineContext, new SerializableConfiguration(metaClient.getHadoopConf()), basePath, false);
    Assertions.assertEquals(3, fileSystemBackedTableMetadata.getAllPartitionPaths().size());
    Assertions.assertEquals(10, fileSystemBackedTableMetadata.getAllFilesInPartition(new Path(basePath + "/" + ONE_LEVEL_PARTITIONS.get(0))).length);

    List<String> fullPartitionPaths = ONE_LEVEL_PARTITIONS.stream().map(p -> basePath + "/" + p).collect(Collectors.toList());
    Map<String, FileStatus[]> partitionToFilesMap = fileSystemBackedTableMetadata.getAllFilesInPartitions(fullPartitionPaths);
    for (String p : fullPartitionPaths) {
      Assertions.assertEquals(10, partitionToFilesMap.get(p).length);
    }
  }

  @Test
  public void testMultiLevelPartitionedTable() throws Exception {
    String instant = "100";
    hoodieTestTable = hoodieTestTable.addCommit(instant);
    // Generate 10 files under each partition
    MULTI_LEVEL_PARTITIONS.stream().forEach(p -> {
      try {
        hoodieTestTable = hoodieTestTable.withPartitionMetaFiles(p)
            .withBaseFilesInPartition(p, IntStream.range(0, 10).toArray());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
    FileSystemBackedTableMetadata fileSystemBackedTableMetadata =
        new FileSystemBackedTableMetadata(localEngineContext, new SerializableConfiguration(metaClient.getHadoopConf()), basePath, false);
    Assertions.assertEquals(3, fileSystemBackedTableMetadata.getAllPartitionPaths().size());
    Assertions.assertEquals(10, fileSystemBackedTableMetadata.getAllFilesInPartition(new Path(basePath + "/" + MULTI_LEVEL_PARTITIONS.get(0))).length);

    List<String> fullPartitionPaths = MULTI_LEVEL_PARTITIONS.stream().map(p -> basePath + "/" + p).collect(Collectors.toList());
    Map<String, FileStatus[]> partitionToFilesMap = fileSystemBackedTableMetadata.getAllFilesInPartitions(fullPartitionPaths);
    for (String p : fullPartitionPaths) {
      Assertions.assertEquals(10, partitionToFilesMap.get(p).length);
    }
  }

  @Test
  public void testMultiLevelEmptyPartitionTable() throws Exception {
    String instant = "100";
    hoodieTestTable = hoodieTestTable.addCommit(instant);
    // Generate 10 files under each partition
    MULTI_LEVEL_PARTITIONS.stream().forEach(p -> {
      try {
        hoodieTestTable = hoodieTestTable.withPartitionMetaFiles(p);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
    FileSystemBackedTableMetadata fileSystemBackedTableMetadata =
        new FileSystemBackedTableMetadata(localEngineContext, new SerializableConfiguration(metaClient.getHadoopConf()), basePath, false);
    Assertions.assertEquals(3, fileSystemBackedTableMetadata.getAllPartitionPaths().size());
    Assertions.assertEquals(0, fileSystemBackedTableMetadata.getAllFilesInPartition(new Path(basePath + "/" + MULTI_LEVEL_PARTITIONS.get(0))).length);

    List<String> fullPartitionPaths = MULTI_LEVEL_PARTITIONS.stream().map(p -> basePath + "/" + p).collect(Collectors.toList());
    Map<String, FileStatus[]> partitionToFilesMap = fileSystemBackedTableMetadata.getAllFilesInPartitions(fullPartitionPaths);
    for (String p : fullPartitionPaths) {
      Assertions.assertEquals(0, partitionToFilesMap.get(p).length);
    }
  }

}
