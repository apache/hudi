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

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.testutils.AbstractShellIntegrationTest;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestTable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link UpgradeOrDowngradeCommand}.
 */
public class TestUpgradeDowngradeCommand extends AbstractShellIntegrationTest {

  private String tablePath;

  @BeforeEach
  public void init() throws Exception {
    String tableName = "test_table";
    tablePath = Paths.get(basePath, tableName).toString();
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");
    metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
    //Create some commits files and parquet files
    HoodieTestTable.of(metaClient)
        .withPartitionMetaFiles(DEFAULT_PARTITION_PATHS)
        .addCommit("100")
        .withBaseFilesInPartition(DEFAULT_FIRST_PARTITION_PATH, "file-1")
        .withBaseFilesInPartition(DEFAULT_SECOND_PARTITION_PATH, "file-2")
        .withBaseFilesInPartition(DEFAULT_THIRD_PARTITION_PATH, "file-3")
        .addInflightCommit("101")
        .withBaseFilesInPartition(DEFAULT_FIRST_PARTITION_PATH, "file-1")
        .withBaseFilesInPartition(DEFAULT_SECOND_PARTITION_PATH, "file-2")
        .withBaseFilesInPartition(DEFAULT_THIRD_PARTITION_PATH, "file-3")
        .withMarkerFile(DEFAULT_FIRST_PARTITION_PATH, "file-1", IOType.MERGE)
        .withMarkerFile(DEFAULT_SECOND_PARTITION_PATH, "file-2", IOType.MERGE)
        .withMarkerFile(DEFAULT_THIRD_PARTITION_PATH, "file-3", IOType.MERGE);
  }

  @Test
  public void testDowngradeCommand() throws Exception {
    // update hoodie.table.version to 1
    metaClient.getTableConfig().setTableVersion(HoodieTableVersion.ONE);
    try (FSDataOutputStream os = metaClient.getFs().create(new Path(metaClient.getMetaPath() + "/" + HoodieTableConfig.HOODIE_PROPERTIES_FILE), true)) {
      metaClient.getTableConfig().getProperties().store(os, "");
    }
    metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());

    // verify marker files for inflight commit exists
    for (String partitionPath : DEFAULT_PARTITION_PATHS) {
      assertEquals(1, FileCreateUtils.getTotalMarkerFileCount(tablePath, partitionPath, "101", IOType.MERGE));
    }

    SparkMain.upgradeOrDowngradeTable(jsc, tablePath, HoodieTableVersion.ZERO.name());

    // verify hoodie.table.version got downgraded
    metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());

    // verify hoodie.table.version
    assertEquals(metaClient.getTableConfig().getTableVersion().versionCode(), HoodieTableVersion.ZERO.versionCode());
    assertTableVersionFromPropertyFile();

    // verify marker files are non existant
    for (String partitionPath : DEFAULT_PARTITION_PATHS) {
      assertEquals(0, FileCreateUtils.getTotalMarkerFileCount(tablePath, partitionPath, "101", IOType.MERGE));
    }
  }

  private void assertTableVersionFromPropertyFile() throws IOException {
    Path propertyFile = new Path(metaClient.getMetaPath() + "/" + HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    // Load the properties and verify
    FSDataInputStream fsDataInputStream = metaClient.getFs().open(propertyFile);
    Properties prop = new Properties();
    prop.load(fsDataInputStream);
    fsDataInputStream.close();
    assertEquals(Integer.toString(HoodieTableVersion.ZERO.versionCode()), prop.getProperty(HoodieTableConfig.HOODIE_TABLE_VERSION_PROP_NAME));
  }
}
