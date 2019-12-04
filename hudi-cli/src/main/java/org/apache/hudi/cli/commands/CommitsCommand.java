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
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.NumericUtils;

import org.apache.spark.launcher.SparkLauncher;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * CLI command to display commits options.
 */
@Component
public class CommitsCommand implements CommandMarker {

  @CliAvailabilityIndicator({"commits show"})
  public boolean isShowAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliAvailabilityIndicator({"commits refresh"})
  public boolean isRefreshAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliAvailabilityIndicator({"commit rollback"})
  public boolean isRollbackAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliAvailabilityIndicator({"commit show"})
  public boolean isCommitShowAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliCommand(value = "commits show", help = "Show the commits")
  public String showCommits(
      @CliOption(key = {"limit"}, mandatory = false, help = "Limit commits",
          unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {

    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> commits = timeline.getReverseOrderedInstants().collect(Collectors.toList());
    List<Comparable[]> rows = new ArrayList<>();
    for (int i = 0; i < commits.size(); i++) {
      HoodieInstant commit = commits.get(i);
      HoodieCommitMetadata commitMetadata =
          HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(commit).get(), HoodieCommitMetadata.class);
      rows.add(new Comparable[] {commit.getTimestamp(), commitMetadata.fetchTotalBytesWritten(),
          commitMetadata.fetchTotalFilesInsert(), commitMetadata.fetchTotalFilesUpdated(),
          commitMetadata.fetchTotalPartitionsWritten(), commitMetadata.fetchTotalRecordsWritten(),
          commitMetadata.fetchTotalUpdateRecordsWritten(), commitMetadata.fetchTotalWriteErrors()});
    }

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put("Total Bytes Written", entry -> {
      return NumericUtils.humanReadableByteCount((Double.valueOf(entry.toString())));
    });

    TableHeader header = new TableHeader().addTableHeaderField("CommitTime").addTableHeaderField("Total Bytes Written")
        .addTableHeaderField("Total Files Added").addTableHeaderField("Total Files Updated")
        .addTableHeaderField("Total Partitions Written").addTableHeaderField("Total Records Written")
        .addTableHeaderField("Total Update Records Written").addTableHeaderField("Total Errors");
    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }

  @CliCommand(value = "commits refresh", help = "Refresh the commits")
  public String refreshCommits() throws IOException {
    HoodieCLI.refreshTableMetadata();
    return "Metadata for table " + HoodieCLI.tableMetadata.getTableConfig().getTableName() + " refreshed.";
  }

  @CliCommand(value = "commit rollback", help = "Rollback a commit")
  public String rollbackCommit(@CliOption(key = {"commit"}, help = "Commit to rollback") final String commitTime,
      @CliOption(key = {"sparkProperties"}, help = "Spark Properites File Path") final String sparkPropertiesPath)
      throws Exception {
    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTime);

    if (!timeline.containsInstant(commitInstant)) {
      return "Commit " + commitTime + " not found in Commits " + timeline;
    }

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkMain.SparkCommand.ROLLBACK.toString(), commitTime,
        HoodieCLI.tableMetadata.getBasePath());
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    // Refresh the current
    refreshCommits();
    if (exitCode != 0) {
      return "Commit " + commitTime + " failed to roll back";
    }
    return "Commit " + commitTime + " rolled back";
  }

  @CliCommand(value = "commit showpartitions", help = "Show partition level details of a commit")
  public String showCommitPartitions(@CliOption(key = {"commit"}, help = "Commit to show") final String commitTime,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws Exception {

    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTime);

    if (!timeline.containsInstant(commitInstant)) {
      return "Commit " + commitTime + " not found in Commits " + timeline;
    }
    HoodieCommitMetadata meta = HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(commitInstant).get(),
        HoodieCommitMetadata.class);
    List<Comparable[]> rows = new ArrayList<>();
    for (Map.Entry<String, List<HoodieWriteStat>> entry : meta.getPartitionToWriteStats().entrySet()) {
      String path = entry.getKey();
      List<HoodieWriteStat> stats = entry.getValue();
      long totalFilesAdded = 0;
      long totalFilesUpdated = 0;
      long totalRecordsUpdated = 0;
      long totalRecordsInserted = 0;
      long totalBytesWritten = 0;
      long totalWriteErrors = 0;
      for (HoodieWriteStat stat : stats) {
        if (stat.getPrevCommit().equals(HoodieWriteStat.NULL_COMMIT)) {
          totalFilesAdded += 1;
          totalRecordsInserted += stat.getNumWrites();
        } else {
          totalFilesUpdated += 1;
          totalRecordsUpdated += stat.getNumUpdateWrites();
        }
        totalBytesWritten += stat.getTotalWriteBytes();
        totalWriteErrors += stat.getTotalWriteErrors();
      }
      rows.add(new Comparable[] {path, totalFilesAdded, totalFilesUpdated, totalRecordsInserted, totalRecordsUpdated,
          totalBytesWritten, totalWriteErrors});
    }

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put("Total Bytes Written", entry -> {
      return NumericUtils.humanReadableByteCount((Long.valueOf(entry.toString())));
    });

    TableHeader header = new TableHeader().addTableHeaderField("Partition Path")
        .addTableHeaderField("Total Files Added").addTableHeaderField("Total Files Updated")
        .addTableHeaderField("Total Records Inserted").addTableHeaderField("Total Records Updated")
        .addTableHeaderField("Total Bytes Written").addTableHeaderField("Total Errors");

    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }

  @CliCommand(value = "commit showfiles", help = "Show file level details of a commit")
  public String showCommitFiles(@CliOption(key = {"commit"}, help = "Commit to show") final String commitTime,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws Exception {

    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTime);

    if (!timeline.containsInstant(commitInstant)) {
      return "Commit " + commitTime + " not found in Commits " + timeline;
    }
    HoodieCommitMetadata meta = HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(commitInstant).get(),
        HoodieCommitMetadata.class);
    List<Comparable[]> rows = new ArrayList<>();
    for (Map.Entry<String, List<HoodieWriteStat>> entry : meta.getPartitionToWriteStats().entrySet()) {
      String path = entry.getKey();
      List<HoodieWriteStat> stats = entry.getValue();
      for (HoodieWriteStat stat : stats) {
        rows.add(new Comparable[] {path, stat.getFileId(), stat.getPrevCommit(), stat.getNumUpdateWrites(),
            stat.getNumWrites(), stat.getTotalWriteBytes(), stat.getTotalWriteErrors(), stat.getFileSizeInBytes()});
      }
    }

    TableHeader header = new TableHeader().addTableHeaderField("Partition Path").addTableHeaderField("File ID")
        .addTableHeaderField("Previous Commit").addTableHeaderField("Total Records Updated")
        .addTableHeaderField("Total Records Written").addTableHeaderField("Total Bytes Written")
        .addTableHeaderField("Total Errors").addTableHeaderField("File Size");

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
  }

  @CliAvailabilityIndicator({"commits compare"})
  public boolean isCompareCommitsAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliCommand(value = "commits compare", help = "Compare commits with another Hoodie dataset")
  public String compareCommits(@CliOption(key = {"path"}, help = "Path of the dataset to compare to") final String path)
      throws Exception {

    HoodieTableMetaClient target = new HoodieTableMetaClient(HoodieCLI.conf, path);
    HoodieTimeline targetTimeline = target.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    HoodieTableMetaClient source = HoodieCLI.tableMetadata;
    HoodieTimeline sourceTimeline = source.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    String targetLatestCommit =
        targetTimeline.getInstants().iterator().hasNext() ? "0" : targetTimeline.lastInstant().get().getTimestamp();
    String sourceLatestCommit =
        sourceTimeline.getInstants().iterator().hasNext() ? "0" : sourceTimeline.lastInstant().get().getTimestamp();

    if (sourceLatestCommit != null
        && HoodieTimeline.compareTimestamps(targetLatestCommit, sourceLatestCommit, HoodieTimeline.GREATER)) {
      // source is behind the target
      List<String> commitsToCatchup = targetTimeline.findInstantsAfter(sourceLatestCommit, Integer.MAX_VALUE)
          .getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
      return "Source " + source.getTableConfig().getTableName() + " is behind by " + commitsToCatchup.size()
          + " commits. Commits to catch up - " + commitsToCatchup;
    } else {
      List<String> commitsToCatchup = sourceTimeline.findInstantsAfter(targetLatestCommit, Integer.MAX_VALUE)
          .getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
      return "Source " + source.getTableConfig().getTableName() + " is ahead by " + commitsToCatchup.size()
          + " commits. Commits to catch up - " + commitsToCatchup;
    }
  }

  @CliAvailabilityIndicator({"commits sync"})
  public boolean isSyncCommitsAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliCommand(value = "commits sync", help = "Compare commits with another Hoodie dataset")
  public String syncCommits(@CliOption(key = {"path"}, help = "Path of the dataset to compare to") final String path)
      throws Exception {
    HoodieCLI.syncTableMetadata = new HoodieTableMetaClient(HoodieCLI.conf, path);
    HoodieCLI.state = HoodieCLI.CLIState.SYNC;
    return "Load sync state between " + HoodieCLI.tableMetadata.getTableConfig().getTableName() + " and "
        + HoodieCLI.syncTableMetadata.getTableConfig().getTableName();
  }

}
