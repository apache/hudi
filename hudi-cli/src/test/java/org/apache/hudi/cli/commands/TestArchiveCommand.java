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
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("functional")
public class TestArchiveCommand extends CLIFunctionalTestHarness {

  @Test
  public void testArchiving() throws Exception {
    HoodieCLI.conf = storageConf();

    // Create table and connect
    String tableName = tableName();
    String tablePath = tablePath(tableName);

    new TableCommand().createTable(
        tablePath, tableName,
        "COPY_ON_WRITE", "", HoodieTableVersion.current().versionCode(), "org.apache.hudi.common.model.HoodieAvroPayload");

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();

    // Create six commits
    for (int i = 100; i < 106; i++) {
      String timestamp = String.valueOf(i);
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, timestamp, storageConf());
    }

    // The shell command "trigger archival" launches SparkMain in a separate spark-submit
    // process, which needs SPARK_HOME and the jars packaged under target/lib (see the
    // ITTest classes), neither of which exists when the functional suite runs. Invoke the
    // entry point that process dispatches to for the ARCHIVE command instead, with the
    // arguments the shell invocation used:
    // trigger archival --minCommits 2 --maxCommits 3 --commitsRetainedByCleaner 1 --enableMetadata false
    assertEquals(0, SparkMain.archive(jsc(), 2, 3, 1, false, tablePath));

    metaClient = HoodieTableMetaClient.reload(metaClient);

    // get instants in the active timeline only returns the latest state of the commit
    // therefore we expect 2 instants because minCommits is 2
    assertEquals(2, metaClient.getActiveTimeline().countInstants());

    // 6 commits - 2 kept in the active timeline = 4 archived. The LSM archived timeline
    // of table version 8 and above holds a single entry per instant (the legacy log
    // format archived requested, inflight and completed as separate entries), so the
    // archived timeline counts 4 instants.
    assertEquals(4, metaClient.getArchivedTimeline().countInstants());
  }

}
