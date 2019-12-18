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

import org.apache.hudi.HoodieWriteClient;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * CLI command to display savepoint options.
 */
@Component
public class SavepointsCommand implements CommandMarker {

  @CliCommand(value = "savepoints show", help = "Show the savepoints")
  public String showSavepoints() throws IOException {
    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getSavePointTimeline().filterCompletedInstants();
    List<HoodieInstant> commits = timeline.getReverseOrderedInstants().collect(Collectors.toList());
    String[][] rows = new String[commits.size()][];
    for (int i = 0; i < commits.size(); i++) {
      HoodieInstant commit = commits.get(i);
      rows[i] = new String[] {commit.getTimestamp()};
    }
    return HoodiePrintHelper.print(new String[] {"SavepointTime"}, rows);
  }

  @CliCommand(value = "savepoint create", help = "Savepoint a commit")
  public String savepoint(@CliOption(key = {"commit"}, help = "Commit to savepoint") final String commitTime,
      @CliOption(key = {"user"}, unspecifiedDefaultValue = "default", help = "User who is creating the savepoint") final String user,
      @CliOption(key = {"comments"}, unspecifiedDefaultValue = "default", help = "Comments for creating the savepoint") final String comments)
      throws Exception {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitTimeline().filterCompletedInstants();
    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTime);

    if (!timeline.containsInstant(commitInstant)) {
      return "Commit " + commitTime + " not found in Commits " + timeline;
    }

    JavaSparkContext jsc = SparkUtil.initJavaSparkConf("Create Savepoint");
    HoodieWriteClient client = createHoodieClient(jsc, metaClient.getBasePath());
    String result;
    if (client.savepoint(commitTime, user, comments)) {
      // Refresh the current
      refreshMetaClient();
      result = String.format("The commit \"%s\" has been savepointed.", commitTime);
    } else {
      result = String.format("Failed: Could not savepoint commit \"%s\".", commitTime);
    }
    jsc.close();
    return result;
  }

  @CliCommand(value = "savepoint rollback", help = "Savepoint a commit")
  public String rollbackToSavepoint(
      @CliOption(key = {"savepoint"}, help = "Savepoint to rollback") final String commitTime,
      @CliOption(key = {"sparkProperties"}, help = "Spark Properites File Path") final String sparkPropertiesPath)
      throws Exception {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    if (metaClient.getActiveTimeline().getSavePointTimeline().filterCompletedInstants().empty()) {
      throw new HoodieException("There are no completed instants to run rollback");
    }
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitTimeline().filterCompletedInstants();
    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTime);

    if (!timeline.containsInstant(commitInstant)) {
      return "Commit " + commitTime + " not found in Commits " + timeline;
    }

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkMain.SparkCommand.ROLLBACK_TO_SAVEPOINT.toString(), commitTime,
        metaClient.getBasePath());
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    // Refresh the current
    refreshMetaClient();
    if (exitCode != 0) {
      return "Savepoint " + commitTime + " failed to roll back";
    }
    return "Savepoint " + commitTime + " rolled back";
  }

  @CliCommand(value = "savepoints refresh", help = "Refresh the savepoints")
  public String refreshMetaClient() throws IOException {
    HoodieCLI.refreshTableMetadata();
    return "Metadata for table " + HoodieCLI.getTableMetaClient().getTableConfig().getTableName() + " refreshed.";
  }

  private static HoodieWriteClient createHoodieClient(JavaSparkContext jsc, String basePath) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();
    return new HoodieWriteClient(jsc, config, false);
  }

}
