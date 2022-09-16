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
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.exception.HoodieException;

import org.apache.spark.launcher.SparkLauncher;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * CLI command to display savepoint options.
 */
@Component
public class SavepointsCommand implements CommandMarker {

  @CliCommand(value = "savepoints show", help = "Show the savepoints")
  public String showSavepoints() {
    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getSavePointTimeline().filterCompletedInstants();
    List<HoodieInstant> commits = timeline.getReverseOrderedInstants().collect(Collectors.toList());
    String[][] rows = new String[commits.size()][];
    for (int i = 0; i < commits.size(); i++) {
      HoodieInstant commit = commits.get(i);
      rows[i] = new String[] {commit.getTimestamp()};
    }
    return HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_SAVEPOINT_TIME}, rows);
  }

  @CliCommand(value = "savepoint create", help = "Savepoint a commit")
  public String savepoint(
      @CliOption(key = {"commit"}, help = "Commit to savepoint") final String commitTime,
      @CliOption(key = {"user"}, unspecifiedDefaultValue = "default",
          help = "User who is creating the savepoint") final String user,
      @CliOption(key = {"comments"}, unspecifiedDefaultValue = "default",
          help = "Comments for creating the savepoint") final String comments,
      @CliOption(key = {"sparkProperties"}, help = "Spark Properties File Path") final String sparkPropertiesPath,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "", help = "Spark Master") String master,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory)
      throws Exception {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    if (!activeTimeline.getCommitsTimeline().filterCompletedInstants().containsInstant(commitTime)) {
      return "Commit " + commitTime + " not found in Commits " + activeTimeline;
    }

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkMain.SparkCommand.SAVEPOINT.toString(), master, sparkMemory, commitTime,
        user, comments, metaClient.getBasePath());
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    // Refresh the current
    HoodieCLI.refreshTableMetadata();
    if (exitCode != 0) {
      return String.format("Failed: Could not create savepoint \"%s\".", commitTime);
    }
    return String.format("The commit \"%s\" has been savepointed.", commitTime);
  }

  @CliCommand(value = "savepoint rollback", help = "Savepoint a commit")
  public String rollbackToSavepoint(
      @CliOption(key = {"savepoint"}, help = "Savepoint to rollback") final String instantTime,
      @CliOption(key = {"sparkProperties"}, help = "Spark Properties File Path") final String sparkPropertiesPath,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "", help = "Spark Master") String master,
      @CliOption(key = {"lazyFailedWritesCleanPolicy"}, help = "True if FailedWriteCleanPolicy is lazy",
          unspecifiedDefaultValue = "false") final String lazyFailedWritesCleanPolicy,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory)
      throws Exception {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    if (metaClient.getActiveTimeline().getSavePointTimeline().filterCompletedInstants().empty()) {
      throw new HoodieException("There are no completed instants to run rollback");
    }
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> instants = timeline.getInstants().filter(instant -> instant.getTimestamp().equals(instantTime)).collect(Collectors.toList());

    if (instants.isEmpty()) {
      return "Commit " + instantTime + " not found in Commits " + timeline;
    }

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkMain.SparkCommand.ROLLBACK_TO_SAVEPOINT.toString(), master, sparkMemory,
        instantTime, metaClient.getBasePath(), lazyFailedWritesCleanPolicy);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    // Refresh the current
    HoodieCLI.refreshTableMetadata();
    if (exitCode != 0) {
      return String.format("Savepoint \"%s\" failed to roll back", instantTime);
    }
    return String.format("Savepoint \"%s\" rolled back", instantTime);
  }

  @CliCommand(value = "savepoint delete", help = "Delete the savepoint")
  public String deleteSavepoint(
      @CliOption(key = {"commit"}, help = "Delete a savepoint") final String instantTime,
      @CliOption(key = {"sparkProperties"}, help = "Spark Properties File Path") final String sparkPropertiesPath,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "", help = "Spark Master") String master,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory)
      throws Exception {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    HoodieTimeline completedInstants = metaClient.getActiveTimeline().getSavePointTimeline().filterCompletedInstants();
    if (completedInstants.empty()) {
      throw new HoodieException("There are no completed savepoint to run delete");
    }
    HoodieInstant savePoint = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, instantTime);

    if (!completedInstants.containsInstant(savePoint)) {
      return "Commit " + instantTime + " not found in Commits " + completedInstants;
    }

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkMain.SparkCommand.DELETE_SAVEPOINT.toString(), master, sparkMemory, instantTime,
        metaClient.getBasePath());
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    // Refresh the current
    HoodieCLI.refreshTableMetadata();
    if (exitCode != 0) {
      return String.format("Failed: Could not delete savepoint \"%s\".", instantTime);
    }
    return String.format("Savepoint \"%s\" deleted.", instantTime);
  }
}
