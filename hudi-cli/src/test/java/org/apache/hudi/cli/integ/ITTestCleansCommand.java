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

package org.apache.hudi.cli.integ;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.commands.TableCommand;
import org.apache.hudi.cli.testutils.AbstractShellIntegrationTest;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ITTestCleansCommand extends AbstractShellIntegrationTest {

  private String tablePath;
  private URL propsFilePath;

  @BeforeEach
  public void init() throws IOException {
    HoodieCLI.conf = jsc.hadoopConfiguration();

    String tableName = "test_table";
    tablePath = basePath + File.separator + tableName;
    propsFilePath = this.getClass().getClassLoader().getResource("clean.properties");

    // Create table and connect
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload",
        "org.apache.hudi.common.bootstrap.index.HFileBasedBootstrapIndex");

    Configuration conf = HoodieCLI.conf;

    metaClient = HoodieCLI.getTableMetaClient();
    // Create four commits
    for (int i = 100; i < 104; i++) {
      String timestamp = String.valueOf(i);
      // Requested Compaction
      HoodieTestCommitMetadataGenerator.createCompactionAuxiliaryMetadata(tablePath,
          new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, timestamp), conf);
      // Inflight Compaction
      HoodieTestCommitMetadataGenerator.createCompactionAuxiliaryMetadata(tablePath,
          new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, timestamp), conf);
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, timestamp, conf);
    }
  }

  /**
   * Test case for cleans run.
   */
  @Test
  public void testRunClean() throws IOException {
    // First, there should none of clean instant.
    assertEquals(0, metaClient.getActiveTimeline().reload().getCleanerTimeline().getInstants().count());

    // Check properties file exists.
    assertNotNull(propsFilePath, "Not found properties file");

    // Create partition metadata
    Files.createFile(Paths.get(tablePath,
        HoodieTestCommitMetadataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE));
    Files.createFile(Paths.get(tablePath,
        HoodieTestCommitMetadataGenerator.DEFAULT_SECOND_PARTITION_PATH,
        HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE));

    CommandResult cr = getShell().executeCommand("cleans run --sparkMaster local --propsFilePath " + propsFilePath.toString());
    assertTrue(cr.isSuccess());

    // After run clean, there should have 1 clean instant
    assertEquals(1, metaClient.getActiveTimeline().reload().getCleanerTimeline().getInstants().count(),
        "Loaded 1 clean and the count should match");
  }
}
