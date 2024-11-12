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
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.util.List;
import java.util.stream.Collectors;

/**
 * CLI command to display savepoint options.
 */
@ShellComponent
public class SavepointsCommand {

  @ShellMethod(key = "savepoints show", value = "Show the savepoints")
  public String showSavepoints() {
    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getSavePointTimeline().filterCompletedInstants();
    List<HoodieInstant> commits = timeline.getReverseOrderedInstants().collect(Collectors.toList());
    String[][] rows = new String[commits.size()][];
    for (int i = 0; i < commits.size(); i++) {
      HoodieInstant commit = commits.get(i);
      rows[i] = new String[] {commit.requestedTime()};
    }
    return HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_SAVEPOINT_TIME}, rows);
  }

  @ShellMethod(key = "savepoint create", value = "Savepoint a commit")
  public String savepoint(
      @ShellOption(value = {"--commit"}, help = "Commit to savepoint") final String commitTime,
      @ShellOption(value = {"--user"}, defaultValue = "default",
          help = "User who is creating the savepoint") final String user,
      @ShellOption(value = {"--comments"}, defaultValue = "default",
          help = "Comments for creating the savepoint") final String comments,
      @ShellOption(value = {"--sparkProperties"}, help = "Spark Properties File Path",
          defaultValue = "") final String sparkPropertiesPath,
      @ShellOption(value = "--sparkMaster", defaultValue = "", help = "Spark Master") String master,
      @ShellOption(value = "--sparkMemory", defaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory)
      throws Exception {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    if (!activeTimeline.getCommitsTimeline().filterCompletedInstants().containsInstant(commitTime)) {
      return String.format("Commit %s not found in Commits %s", commitTime, activeTimeline);
    }

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    SparkMain.addAppArgs(sparkLauncher, SparkMain.SparkCommand.SAVEPOINT, master, sparkMemory, commitTime,
        user, comments, HoodieCLI.basePath);
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

  @ShellMethod(key = "savepoint rollback", value = "Savepoint a commit")
  public String rollbackToSavepoint(
      @ShellOption(value = {"--savepoint"}, help = "Savepoint to rollback") final String instantTime,
      @ShellOption(value = {"--sparkProperties"}, help = "Spark Properties File Path",
          defaultValue = "") final String sparkPropertiesPath,
      @ShellOption(value = "--sparkMaster", defaultValue = "", help = "Spark Master") String master,
      @ShellOption(value = {"--lazyFailedWritesCleanPolicy"}, help = "True if FailedWriteCleanPolicy is lazy",
          defaultValue = "false") final String lazyFailedWritesCleanPolicy,
      @ShellOption(value = "--sparkMemory", defaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory)
      throws Exception {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    if (metaClient.getActiveTimeline().getSavePointTimeline().filterCompletedInstants().empty()) {
      throw new HoodieException("There are no completed instants to run rollback");
    }
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> instants = timeline.getInstantsAsStream().filter(instant -> instant.requestedTime().equals(instantTime)).collect(Collectors.toList());

    if (instants.isEmpty()) {
      return String.format("Commit %s not found in Commits %s", instantTime, timeline);
    }

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    SparkMain.addAppArgs(sparkLauncher, SparkMain.SparkCommand.ROLLBACK_TO_SAVEPOINT, master, sparkMemory,
        instantTime, HoodieCLI.basePath, lazyFailedWritesCleanPolicy);
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

  @ShellMethod(key = "savepoint delete", value = "Delete the savepoint")
  public String deleteSavepoint(
      @ShellOption(value = {"--commit"}, help = "Delete a savepoint") final String instantTime,
      @ShellOption(value = {"--sparkProperties"}, help = "Spark Properties File Path",
          defaultValue = "") final String sparkPropertiesPath,
      @ShellOption(value = "--sparkMaster", defaultValue = "", help = "Spark Master") String master,
      @ShellOption(value = "--sparkMemory", defaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory)
      throws Exception {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    HoodieTimeline completedInstants = metaClient.getActiveTimeline().getSavePointTimeline().filterCompletedInstants();
    if (completedInstants.empty()) {
      throw new HoodieException("There are no completed savepoint to run delete");
    }
    HoodieInstant savePoint = metaClient.createNewInstant(HoodieInstant.State.COMPLETED,
        HoodieTimeline.SAVEPOINT_ACTION, instantTime);

    if (!completedInstants.containsInstant(savePoint)) {
      return String.format("Commit %s not found in Commits %s", instantTime, completedInstants);
    }

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    SparkMain.addAppArgs(sparkLauncher, SparkMain.SparkCommand.DELETE_SAVEPOINT, master, sparkMemory, instantTime,
        HoodieCLI.basePath);
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
