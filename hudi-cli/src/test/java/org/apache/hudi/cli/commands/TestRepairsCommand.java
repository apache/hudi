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
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.testutils.AbstractShellIntegrationTest;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link RepairsCommand}.
 */
public class TestRepairsCommand extends AbstractShellIntegrationTest {

  private String tablePath;

  @BeforeEach
  public void init() throws IOException {
    String tableName = "test_table";
    tablePath = basePath + File.separator + tableName;

    // Create table and connect
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");
  }

  /**
   * Test case for dry run 'repair addpartitionmeta'.
   */
  @Test
  public void testAddPartitionMetaWithDryRun() throws IOException {
    // create commit instant
    Files.createFile(Paths.get(tablePath + "/.hoodie/100.commit"));

    // create partition path
    String partition1 = tablePath + File.separator + HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    String partition2 = tablePath + File.separator + HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
    String partition3 = tablePath + File.separator + HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
    assertTrue(fs.mkdirs(new Path(partition1)));
    assertTrue(fs.mkdirs(new Path(partition2)));
    assertTrue(fs.mkdirs(new Path(partition3)));

    // default is dry run.
    CommandResult cr = getShell().executeCommand("repair addpartitionmeta");
    assertTrue(cr.isSuccess());

    // expected all 'No'.
    String[][] rows = FSUtils.getAllPartitionFoldersThreeLevelsDown(fs, tablePath)
        .stream()
        .map(partition -> new String[]{partition, "No", "None"})
        .toArray(String[][]::new);
    String expected = HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_PARTITION_PATH,
        HoodieTableHeaderFields.HEADER_METADATA_PRESENT, HoodieTableHeaderFields.HEADER_ACTION}, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(expected, got);
  }

  /**
   * Test case for real run 'repair addpartitionmeta'.
   */
  @Test
  public void testAddPartitionMetaWithRealRun() throws IOException {
    // create commit instant
    Files.createFile(Paths.get(tablePath + "/.hoodie/100.commit"));

    // create partition path
    String partition1 = tablePath + File.separator + HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    String partition2 = tablePath + File.separator + HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
    String partition3 = tablePath + File.separator + HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
    assertTrue(fs.mkdirs(new Path(partition1)));
    assertTrue(fs.mkdirs(new Path(partition2)));
    assertTrue(fs.mkdirs(new Path(partition3)));

    CommandResult cr = getShell().executeCommand("repair addpartitionmeta --dryrun false");
    assertTrue(cr.isSuccess());

    List<String> paths = FSUtils.getAllPartitionFoldersThreeLevelsDown(fs, tablePath);
    // after dry run, the action will be 'Repaired'
    String[][] rows = paths.stream()
        .map(partition -> new String[]{partition, "No", "Repaired"})
        .toArray(String[][]::new);
    String expected = HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_PARTITION_PATH,
        HoodieTableHeaderFields.HEADER_METADATA_PRESENT, HoodieTableHeaderFields.HEADER_ACTION}, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(expected, got);

    cr = getShell().executeCommand("repair addpartitionmeta");

    // after real run, Metadata is present now.
    rows = paths.stream()
        .map(partition -> new String[]{partition, "Yes", "None"})
        .toArray(String[][]::new);
    expected = HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_PARTITION_PATH,
        HoodieTableHeaderFields.HEADER_METADATA_PRESENT, HoodieTableHeaderFields.HEADER_ACTION}, rows);
    expected = removeNonWordAndStripSpace(expected);
    got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(expected, got);
  }

  /**
   * Test case for 'repair overwrite-hoodie-props'.
   */
  @Test
  public void testOverwriteHoodieProperties() throws IOException {
    URL newProps = this.getClass().getClassLoader().getResource("table-config.properties");
    assertNotNull(newProps, "New property file must exist");

    CommandResult cr = getShell().executeCommand("repair overwrite-hoodie-props --new-props-file " + newProps.getPath());
    assertTrue(cr.isSuccess());

    Map<String, String> oldProps = HoodieCLI.getTableMetaClient().getTableConfig().getProps();

    // after overwrite, the stored value in .hoodie is equals to which read from properties.
    Map<String, String> result = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient()).getTableConfig().getProps();
    Properties expectProps = new Properties();
    expectProps.load(new FileInputStream(new File(newProps.getPath())));

    Map<String, String> expected = expectProps.entrySet().stream()
        .collect(Collectors.toMap(e -> String.valueOf(e.getKey()), e -> String.valueOf(e.getValue())));
    assertEquals(expected, result);

    // check result
    List<String> allPropsStr = Arrays.asList("hoodie.table.name", "hoodie.table.type", "hoodie.table.version",
        "hoodie.archivelog.folder", "hoodie.timeline.layout.version");
    String[][] rows = allPropsStr.stream().sorted().map(key -> new String[]{key,
        oldProps.getOrDefault(key, "null"), result.getOrDefault(key, "null")})
        .toArray(String[][]::new);
    String expect = HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_HOODIE_PROPERTY,
        HoodieTableHeaderFields.HEADER_OLD_VALUE, HoodieTableHeaderFields.HEADER_NEW_VALUE}, rows);
    expect = removeNonWordAndStripSpace(expect);
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(expect, got);
  }

  /**
   * Test case for 'repair corrupted clean files'.
   */
  @Test
  public void testRemoveCorruptedPendingCleanAction() throws IOException {
    HoodieCLI.conf = jsc.hadoopConfiguration();

    Configuration conf = HoodieCLI.conf;

    metaClient = HoodieCLI.getTableMetaClient();

    // Create four requested files
    for (int i = 100; i < 104; i++) {
      String timestamp = String.valueOf(i);
      // Write corrupted requested Clean File
      HoodieTestCommitMetadataGenerator.createEmptyCleanRequestedFile(tablePath, timestamp, conf);
    }

    // reload meta client
    metaClient = HoodieTableMetaClient.reload(metaClient);
    // first, there are four instants
    assertEquals(4, metaClient.getActiveTimeline().filterInflightsAndRequested().getInstants().count());

    CommandResult cr = getShell().executeCommand("repair corrupted clean files");
    assertTrue(cr.isSuccess());

    // reload meta client
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertEquals(0, metaClient.getActiveTimeline().filterInflightsAndRequested().getInstants().count());
  }
}
