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
import org.apache.hudi.cli.utils.CommitUtil;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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

  private String printCommits(HoodieDefaultTimeline timeline,
                              final Integer limit, final String sortByField,
                              final boolean descending,
                              final boolean headerOnly,
                              final String tempTableName) throws IOException {
    final List<Comparable[]> rows = new ArrayList<>();

    final List<HoodieInstant> commits = timeline.getCommitsTimeline().filterCompletedInstants()
            .getInstants().collect(Collectors.toList());
    // timeline can be read from multiple files. So sort is needed instead of reversing the collection
    Collections.sort(commits, HoodieInstant.COMPARATOR.reversed());

    for (int i = 0; i < commits.size(); i++) {
      final HoodieInstant commit = commits.get(i);
      final HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
              timeline.getInstantDetails(commit).get(),
              HoodieCommitMetadata.class);
      rows.add(new Comparable[]{commit.getTimestamp(),
              commitMetadata.fetchTotalBytesWritten(),
              commitMetadata.fetchTotalFilesInsert(),
              commitMetadata.fetchTotalFilesUpdated(),
              commitMetadata.fetchTotalPartitionsWritten(),
              commitMetadata.fetchTotalRecordsWritten(),
              commitMetadata.fetchTotalUpdateRecordsWritten(),
              commitMetadata.fetchTotalWriteErrors()});
    }

    final Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put("Total Bytes Written", entry -> {
      return NumericUtils.humanReadableByteCount((Double.valueOf(entry.toString())));
    });

    final TableHeader header = new TableHeader()
            .addTableHeaderField("CommitTime")
            .addTableHeaderField("Total Bytes Written")
            .addTableHeaderField("Total Files Added")
            .addTableHeaderField("Total Files Updated")
            .addTableHeaderField("Total Partitions Written")
            .addTableHeaderField("Total Records Written")
            .addTableHeaderField("Total Update Records Written")
            .addTableHeaderField("Total Errors");

    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending,
            limit, headerOnly, rows, tempTableName);
  }

  private String printCommitsWithMetadata(HoodieDefaultTimeline timeline,
                              final Integer limit, final String sortByField,
                              final boolean descending,
                              final boolean headerOnly,
                              final String tempTableName) throws IOException {
    final List<Comparable[]> rows = new ArrayList<>();

    final List<HoodieInstant> commits = timeline.getCommitsTimeline().filterCompletedInstants()
            .getInstants().collect(Collectors.toList());
    // timeline can be read from multiple files. So sort is needed instead of reversing the collection
    Collections.sort(commits, HoodieInstant.COMPARATOR.reversed());

    for (int i = 0; i < commits.size(); i++) {
      final HoodieInstant commit = commits.get(i);
      final HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
              timeline.getInstantDetails(commit).get(),
              HoodieCommitMetadata.class);

      for (Map.Entry<String, List<HoodieWriteStat>> partitionWriteStat :
              commitMetadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat hoodieWriteStat : partitionWriteStat.getValue()) {
          rows.add(new Comparable[]{ commit.getAction(), commit.getTimestamp(), hoodieWriteStat.getPartitionPath(),
                  hoodieWriteStat.getFileId(), hoodieWriteStat.getPrevCommit(), hoodieWriteStat.getNumWrites(),
                  hoodieWriteStat.getNumInserts(), hoodieWriteStat.getNumDeletes(),
                  hoodieWriteStat.getNumUpdateWrites(), hoodieWriteStat.getTotalWriteErrors(),
                  hoodieWriteStat.getTotalLogBlocks(), hoodieWriteStat.getTotalCorruptLogBlock(),
                  hoodieWriteStat.getTotalRollbackBlocks(), hoodieWriteStat.getTotalLogRecords(),
                  hoodieWriteStat.getTotalUpdatedRecordsCompacted(), hoodieWriteStat.getTotalWriteBytes()
          });
        }
      }
    }

    final Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put("Total Bytes Written", entry -> {
      return NumericUtils.humanReadableByteCount((Double.valueOf(entry.toString())));
    });

    TableHeader header = new TableHeader().addTableHeaderField("Action").addTableHeaderField("Instant")
            .addTableHeaderField("Partition").addTableHeaderField("File Id").addTableHeaderField("Prev Instant")
            .addTableHeaderField("Num Writes").addTableHeaderField("Num Inserts").addTableHeaderField("Num Deletes")
            .addTableHeaderField("Num Update Writes").addTableHeaderField("Total Write Errors")
            .addTableHeaderField("Total Log Blocks").addTableHeaderField("Total Corrupt LogBlocks")
            .addTableHeaderField("Total Rollback Blocks").addTableHeaderField("Total Log Records")
            .addTableHeaderField("Total Updated Records Compacted").addTableHeaderField("Total Write Bytes");

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending,
            limit, headerOnly, rows, tempTableName);
  }

  @CliCommand(value = "commits show", help = "Show the commits")
  public String showCommits(
      @CliOption(key = {"includeExtraMetadata"}, help = "Include extra metadata",
          unspecifiedDefaultValue = "false") final boolean includeExtraMetadata,
      @CliOption(key = {"createView"}, mandatory = false, help = "view name to store output table",
          unspecifiedDefaultValue = "") final String exportTableName,
      @CliOption(key = {"limit"}, help = "Limit commits",
          unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {

    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    if (includeExtraMetadata) {
      return printCommitsWithMetadata(activeTimeline, limit, sortByField, descending, headerOnly, exportTableName);
    } else  {
      return printCommits(activeTimeline, limit, sortByField, descending, headerOnly, exportTableName);
    }
  }

  @CliCommand(value = "commits showarchived", help = "Show the archived commits")
  public String showArchivedCommits(
          @CliOption(key = {"includeExtraMetadata"}, help = "Include extra metadata",
                  unspecifiedDefaultValue = "false") final boolean includeExtraMetadata,
          @CliOption(key = {"createView"}, mandatory = false, help = "view name to store output table",
                  unspecifiedDefaultValue = "") final String exportTableName,
          @CliOption(key = {"startTs"},  mandatory = false, help = "start time for commits, default: now - 10 days")
          String startTs,
          @CliOption(key = {"endTs"},  mandatory = false, help = "end time for commits, default: now - 1 day")
          String endTs,
          @CliOption(key = {"limit"}, mandatory = false, help = "Limit commits", unspecifiedDefaultValue = "-1")
          final Integer limit,
          @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "")
          final String sortByField,
          @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false")
          final boolean descending,
          @CliOption(key = {"headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false")
          final boolean headerOnly)
          throws IOException {
    if (StringUtils.isNullOrEmpty(startTs)) {
      startTs = CommitUtil.getTimeDaysAgo(10);
    }
    if (StringUtils.isNullOrEmpty(endTs)) {
      endTs = CommitUtil.getTimeDaysAgo(1);
    }
    HoodieArchivedTimeline archivedTimeline = HoodieCLI.getTableMetaClient().getArchivedTimeline();
    try {
      archivedTimeline.loadInstantDetailsInMemory(startTs, endTs);
      HoodieDefaultTimeline timelineRange = archivedTimeline.findInstantsInRange(startTs, endTs);
      if (includeExtraMetadata) {
        return printCommitsWithMetadata(timelineRange, limit, sortByField, descending, headerOnly, exportTableName);
      } else  {
        return printCommits(timelineRange, limit, sortByField, descending, headerOnly, exportTableName);
      }
    } finally {
      // clear the instant details from memory after printing to reduce usage
      archivedTimeline.clearInstantDetailsFromMemory(startTs, endTs);
    }
  }

  @CliCommand(value = "commits refresh", help = "Refresh the commits")
  public String refreshCommits() throws IOException {
    HoodieCLI.refreshTableMetadata();
    return "Metadata for table " + HoodieCLI.getTableMetaClient().getTableConfig().getTableName() + " refreshed.";
  }

  @CliCommand(value = "commit rollback", help = "Rollback a commit")
  public String rollbackCommit(@CliOption(key = {"commit"}, help = "Commit to rollback") final String instantTime,
      @CliOption(key = {"sparkProperties"}, help = "Spark Properties File Path") final String sparkPropertiesPath)
      throws Exception {
    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    HoodieTimeline completedTimeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    HoodieTimeline filteredTimeline = completedTimeline.filter(instant -> instant.getTimestamp().equals(instantTime));
    if (filteredTimeline.empty()) {
      return "Commit " + instantTime + " not found in Commits " + completedTimeline;
    }

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkMain.SparkCommand.ROLLBACK.toString(), instantTime,
        HoodieCLI.getTableMetaClient().getBasePath());
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    // Refresh the current
    refreshCommits();
    if (exitCode != 0) {
      return "Commit " + instantTime + " failed to roll back";
    }
    return "Commit " + instantTime + " rolled back";
  }

  @CliCommand(value = "commit showpartitions", help = "Show partition level details of a commit")
  public String showCommitPartitions(
      @CliOption(key = {"createView"}, mandatory = false, help = "view name to store output table",
          unspecifiedDefaultValue = "") final String exportTableName,
      @CliOption(key = {"commit"}, help = "Commit to show") final String instantTime,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws Exception {

    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, instantTime);

    if (!timeline.containsInstant(commitInstant)) {
      return "Commit " + instantTime + " not found in Commits " + timeline;
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
        } else {
          totalFilesUpdated += 1;
          totalRecordsUpdated += stat.getNumUpdateWrites();
        }
        totalRecordsInserted += stat.getNumInserts();
        totalBytesWritten += stat.getTotalWriteBytes();
        totalWriteErrors += stat.getTotalWriteErrors();
      }
      rows.add(new Comparable[] {path, totalFilesAdded, totalFilesUpdated, totalRecordsInserted, totalRecordsUpdated,
          totalBytesWritten, totalWriteErrors});
    }

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put("Total Bytes Written", entry -> NumericUtils.humanReadableByteCount((Long.parseLong(entry.toString()))));

    TableHeader header = new TableHeader().addTableHeaderField("Partition Path")
        .addTableHeaderField("Total Files Added").addTableHeaderField("Total Files Updated")
        .addTableHeaderField("Total Records Inserted").addTableHeaderField("Total Records Updated")
        .addTableHeaderField("Total Bytes Written").addTableHeaderField("Total Errors");

    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending,
        limit, headerOnly, rows, exportTableName);
  }

  @CliCommand(value = "commit showfiles", help = "Show file level details of a commit")
  public String showCommitFiles(
      @CliOption(key = {"createView"}, mandatory = false, help = "view name to store output table",
          unspecifiedDefaultValue = "") final String exportTableName,
      @CliOption(key = {"commit"}, help = "Commit to show") final String instantTime,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws Exception {

    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, instantTime);

    if (!timeline.containsInstant(commitInstant)) {
      return "Commit " + instantTime + " not found in Commits " + timeline;
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

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending,
        limit, headerOnly, rows, exportTableName);
  }

  @CliCommand(value = "commits compare", help = "Compare commits with another Hoodie table")
  public String compareCommits(@CliOption(key = {"path"}, help = "Path of the table to compare to") final String path)
      throws Exception {

    HoodieTableMetaClient source = HoodieCLI.getTableMetaClient();
    HoodieTableMetaClient target = new HoodieTableMetaClient(HoodieCLI.conf, path);
    HoodieTimeline targetTimeline = target.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
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

  @CliCommand(value = "commits sync", help = "Compare commits with another Hoodie table")
  public String syncCommits(@CliOption(key = {"path"}, help = "Path of the table to compare to") final String path) {
    HoodieCLI.syncTableMetadata = new HoodieTableMetaClient(HoodieCLI.conf, path);
    HoodieCLI.state = HoodieCLI.CLIState.SYNC;
    return "Load sync state between " + HoodieCLI.getTableMetaClient().getTableConfig().getTableName() + " and "
        + HoodieCLI.syncTableMetadata.getTableConfig().getTableName();
  }
}
