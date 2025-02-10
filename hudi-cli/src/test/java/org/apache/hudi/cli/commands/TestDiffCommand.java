/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Cases for {@link DiffCommand}.
 */
@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestDiffCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;
  private String tableName;
  private String tablePath;

  @BeforeEach
  public void init() {
    tableName = tableName();
    tablePath = tablePath(tableName);
  }

  @Test
  public void testDiffFile() throws Exception {
    // create COW table.
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, HoodieAvroPayload.class.getName());

    StorageConfiguration<?> conf = HoodieCLI.conf;

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    HoodieStorage storage = HoodieStorageUtils.getStorage(basePath(), storageConf());
    HoodieTestDataGenerator.writePartitionMetadataDeprecated(storage,
        HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, tablePath);

    // Create four commits
    Set<String> commits = new HashSet<>();
    for (int i = 100; i < 104; i++) {
      String timestamp = String.valueOf(i);
      commits.add(timestamp);
      // Requested Compaction
      HoodieTestCommitMetadataGenerator.createCompactionAuxiliaryMetadata(tablePath,
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, timestamp), conf);
      // Inflight Compaction
      HoodieTestCommitMetadataGenerator.createCompactionAuxiliaryMetadata(tablePath,
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, timestamp), conf);

      Map<String, String> extraCommitMetadata =
          Collections.singletonMap(HoodieCommitMetadata.SCHEMA_KEY, HoodieTestTable.PHONY_TABLE_SCHEMA);
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, timestamp, conf, fileId1, fileId2,
          Option.empty(), Option.empty(), extraCommitMetadata, false);
    }

    HoodieTableMetaClient.reload(metaClient);

    Object result =  shell.evaluate(() -> String.format("diff file --fileId %s", fileId1));
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));
    String expected = generateExpectDataWithExtraMetadata(commits, fileId1, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  private String generateExpectDataWithExtraMetadata(Set<String> commits, String fileId, String partition) {
    List<Comparable[]> rows = new ArrayList<>();
    commits.stream().sorted(Comparator.reverseOrder()).forEach(commit -> rows.add(new Comparable[] {
        HoodieTimeline.COMMIT_ACTION,
        commit,
        partition,
        fileId,
        HoodieTestCommitMetadataGenerator.DEFAULT_PRE_COMMIT,
        HoodieTestCommitMetadataGenerator.DEFAULT_NUM_WRITES,
        "0",
        "0",
        HoodieTestCommitMetadataGenerator.DEFAULT_NUM_UPDATE_WRITES,
        "0",
        HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_LOG_BLOCKS,
        "0",
        "0",
        HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_LOG_RECORDS,
        "0",
        HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_WRITE_BYTES}));

    final Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN, entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString()))));

    final TableHeader header = HoodieTableHeaderFields.getTableHeaderWithExtraMetadata();

    return HoodiePrintHelper.print(header, fieldNameToConverterMap, "", false, -1, false, rows);
  }
}
