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

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataKeyGenerator;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestHoodieBackedTableMetadata extends TestHoodieMetadataBase {

  private static final Logger LOG = LogManager.getLogger(TestHoodieBackedTableMetadata.class);

  @Test
  public void testTableOperations() throws Exception {
    HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
    init(tableType);
    doWriteInsertAndUpsert(testTable);

    // trigger an upsert
    doWriteOperation(testTable, "0000003");
    verifyBaseMetadataTable();
  }

  private void doWriteInsertAndUpsert(HoodieTestTable testTable) throws Exception {
    doWriteInsertAndUpsert(testTable, "0000001", "0000002");
  }

  private void verifyBaseMetadataTable() throws IOException {
    HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(context, writeConfig.getMetadataConfig(), writeConfig.getBasePath(), writeConfig.getSpillableMapBasePath(), false);
    assertTrue(tableMetadata.enabled());
    List<java.nio.file.Path> fsPartitionPaths = testTable.getAllPartitionPaths();
    List<String> fsPartitions = new ArrayList<>();
    fsPartitionPaths.forEach(entry -> fsPartitions.add(entry.getFileName().toString()));
    List<String> metadataPartitions = tableMetadata.getAllPartitionPaths();

    Collections.sort(fsPartitions);
    Collections.sort(metadataPartitions);

    assertEquals(fsPartitions.size(), metadataPartitions.size(), "Partitions should match");
    assertEquals(fsPartitions, metadataPartitions, "Partitions should match");

    // Files within each partition should match
    HoodieTable table = HoodieSparkTable.create(writeConfig, context, true);
    TableFileSystemView tableView = table.getHoodieView();
    List<String> fullPartitionPaths = fsPartitions.stream().map(partition -> basePath + "/" + partition).collect(Collectors.toList());
    Map<String, FileStatus[]> partitionToFilesMap = tableMetadata.getAllFilesInPartitions(fullPartitionPaths);
    assertEquals(fsPartitions.size(), partitionToFilesMap.size());

    fsPartitions.forEach(partition -> {
      try {
        validateFilesPerPartition(testTable, tableMetadata, tableView, partitionToFilesMap, partition);
      } catch (IOException e) {
        fail("Exception should not be raised: " + e);
      }
    });
  }

  /**
   * Verify if the Metadata table is constructed with table properties including
   * the right key generator class name.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataTableKeyGenerator(final HoodieTableType tableType) throws Exception {
    init(tableType);

    HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(context,
        writeConfig.getMetadataConfig(), writeConfig.getBasePath(), writeConfig.getSpillableMapBasePath(), false);

    assertEquals(HoodieTableMetadataKeyGenerator.class.getCanonicalName(),
        tableMetadata.getMetadataMetaClient().getTableConfig().getKeyGeneratorClassName());
  }

  /**
   * [HUDI-2852] Table metadata returns empty for non-exist partition.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testNotExistPartition(final HoodieTableType tableType) throws Exception {
    init(tableType);
    HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(context,
        writeConfig.getMetadataConfig(), writeConfig.getBasePath(), writeConfig.getSpillableMapBasePath(), false);
    FileStatus[] allFilesInPartition =
        tableMetadata.getAllFilesInPartition(new Path(writeConfig.getBasePath() + "dummy"));
    assertEquals(allFilesInPartition.length, 0);
  }
}
