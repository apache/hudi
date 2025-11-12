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
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestArchiveCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;

  @Test
  public void testArchiving() throws Exception {
    HoodieCLI.conf = storageConf();

    // Create table and connect
    String tableName = tableName();
    String tablePath = tablePath(tableName);

    new TableCommand().createTable(
        tablePath, tableName,
        "COPY_ON_WRITE", "", 1, "org.apache.hudi.common.model.HoodieAvroPayload");

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();

    // Create six commits
    for (int i = 100; i < 106; i++) {
      String timestamp = String.valueOf(i);
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, timestamp, storageConf());
    }

    Object cmdResult = shell.evaluate(() -> "trigger archival --minCommits 2 --maxCommits 3 --commitsRetainedByCleaner 1 --enableMetadata false");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cmdResult));
    metaClient = HoodieTableMetaClient.reload(metaClient);

    //get instants in the active timeline only returns the latest state of the commit
    //therefore we expect 2 instants because minCommits is 2
    assertEquals(2, metaClient.getActiveTimeline().countInstants());

    //get instants in the archived timeline returns all instants in the commit
    //therefore we expect 12 instants because 6 commits - 2 commits in active timeline = 4 in archived
    //since each commit is completed, there are 3 instances per commit (requested, inflight, completed)
    //and 3 instances per commit * 4 commits = 12 instances
    assertEquals(12, metaClient.getArchivedTimeline().countInstants());
  }

}

