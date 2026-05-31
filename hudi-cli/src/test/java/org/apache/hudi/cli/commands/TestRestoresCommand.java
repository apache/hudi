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

import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestRestoresCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;

  @BeforeEach
  public void init() throws Exception {
    String tableName = tableName();
    String tablePath = tablePath(tableName);
    new TableCommand().createTable(
            tablePath, tableName, HoodieTableType.MERGE_ON_READ.name(),
            "", HoodieTableVersion.current().versionCode(), "org.apache.hudi.common.model.HoodieAvroPayload");
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
    //Create some commits files and base files
    Map<String, String> partitionAndFileId = new HashMap<String, String>() {
      {
        put(DEFAULT_FIRST_PARTITION_PATH, "file-1");
        put(DEFAULT_SECOND_PARTITION_PATH, "file-2");
        put(DEFAULT_THIRD_PARTITION_PATH, "file-3");
      }
    };

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(tablePath)
            .withMetadataConfig(
                    // Column Stats Index is disabled, since these tests construct tables which are
                    // not valid (empty commit metadata, etc)
                    HoodieMetadataConfig.newBuilder()
                            .withMetadataIndexColumnStats(false)
                            .enable(false)
                            .build()
            )
            .withRollbackUsingMarkers(false)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
            .build();

    try (HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(metaClient.getStorageConf(), config, context)) {
      HoodieTestTable hoodieTestTable = HoodieMetadataTestTable.of(metaClient, metadataWriter, Option.of(context))
          .withPartitionMetaFiles(DEFAULT_PARTITION_PATHS)
          .addCommit("100", Option.of("100001"), Option.empty())
          .withBaseFilesInPartitions(partitionAndFileId).getLeft()
          .addCommit("101", Option.of("101001"), Option.empty());

      hoodieTestTable.addCommit("102", Option.of("102001"), Option.empty()).withBaseFilesInPartitions(partitionAndFileId);
      HoodieSavepointMetadata savepointMetadata2 = hoodieTestTable.doSavepoint("102");
      hoodieTestTable.addSavepointCommit("102", Option.of("102002"), savepointMetadata2);

      hoodieTestTable.addCommit("103", Option.of("103001"), Option.empty()).withBaseFilesInPartitions(partitionAndFileId);

      try (BaseHoodieWriteClient client = new SparkRDDWriteClient(context(), config)) {
        client.rollback("103");
        client.restoreToSavepoint("102");
      }

      hoodieTestTable.addCommit("105", Option.of("105001"), Option.empty()).withBaseFilesInPartitions(partitionAndFileId);
      HoodieSavepointMetadata savepointMetadata = hoodieTestTable.doSavepoint("105");
      hoodieTestTable.addSavepointCommit("105", Option.of("105002"), savepointMetadata);

      hoodieTestTable.addCommit("106", Option.of("106001"), Option.empty()).withBaseFilesInPartitions(partitionAndFileId);

      try (BaseHoodieWriteClient client = new SparkRDDWriteClient(context(), config)) {
        client.restoreToSavepoint("105");
      }
    }
  }

  @Disabled("TODO: HUDI-7614 - HoodieRestore failing with v9 tables")
  @Test
  public void testShowRestores() {
    Object result = shell.evaluate(() -> "show restores");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // get restored instants
    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    Stream<HoodieInstant> restores = activeTimeline.getRestoreTimeline().filterCompletedInstants().getInstantsAsStream();

    List<Comparable[]> rows = new ArrayList<>();
    restores.sorted().forEach(instant -> {
      try {
        HoodieRestoreMetadata metadata = activeTimeline.readRestoreMetadata(instant);
        metadata.getInstantsToRollback().forEach(c -> {
          Comparable[] row = new Comparable[4];
          row[0] = metadata.getStartRestoreTime();
          row[1] = c;
          row[2] = metadata.getTimeTakenInMillis();
          row[3] = HoodieInstant.State.COMPLETED.toString();
          rows.add(row);
        });
      } catch (IOException e) {
        e.printStackTrace();
      }
    });

    TableHeader header = new TableHeader()
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TIME_TOKEN_MILLIS)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_STATE);
    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false,
            -1, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  @Disabled("TODO: HUDI-7614 - HoodieRestore failing with v9 tables")
  @Test
  public void testShowRestore() throws IOException {
    // get instant
    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    Stream<HoodieInstant> restores = activeTimeline.getRestoreTimeline().filterCompletedInstants().getInstantsAsStream();
    HoodieInstant instant = restores.findFirst().orElse(null);
    assertNotNull(instant, "The instant can not be null.");

    Object result = shell.evaluate(() -> "show restore --instant " + instant.requestedTime());
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // get metadata of instant
    HoodieRestoreMetadata instantMetadata = activeTimeline.readRestoreMetadata(instant);

    // generate expected result
    TableHeader header = new TableHeader()
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TIME_TOKEN_MILLIS)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_STATE);

    List<Comparable[]> rows = new ArrayList<>();
    instantMetadata.getInstantsToRollback().forEach((String rolledbackInstant) -> {
      Comparable[] row = new Comparable[4];
      row[0] = instantMetadata.getStartRestoreTime();
      row[1] = rolledbackInstant;
      row[2] = instantMetadata.getTimeTakenInMillis();
      row[3] = HoodieInstant.State.COMPLETED.toString();
      rows.add(row);
    });
    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1,
            false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

}
