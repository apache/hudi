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
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.utils.CommitUtil;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import org.apache.spark.launcher.SparkLauncher;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN, entry -> {
      return NumericUtils.humanReadableByteCount((Double.valueOf(entry.toString())));
    });

    final TableHeader header = new TableHeader()
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_COMMIT_TIME)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_ADDED)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_UPDATED)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_PARTITIONS_WRITTEN)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_WRITTEN)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_UPDATE_RECORDS_WRITTEN)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_ERRORS);

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
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN, entry -> {
      return NumericUtils.humanReadableByteCount((Double.valueOf(entry.toString())));
    });

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_ACTION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_ID)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_PREVIOUS_COMMIT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_NUM_WRITES)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_NUM_INSERTS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_NUM_DELETES)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_NUM_UPDATE_WRITES)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_ERRORS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_LOG_BLOCKS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_CORRUPT_LOG_BLOCKS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_ROLLBACK_BLOCKS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_LOG_RECORDS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_UPDATED_RECORDS_COMPACTED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN);

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

  @CliCommand(value = "commit rollback", help = "Rollback a commit")
  public String rollbackCommit(@CliOption(key = {"commit"}, help = "Commit to rollback") final String instantTime,
      @CliOption(key = {"sparkProperties"}, help = "Spark Properties File Path") final String sparkPropertiesPath,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "", help = "Spark Master") String master,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "4G",
         help = "Spark executor memory") final String sparkMemory)
      throws Exception {
    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    HoodieTimeline completedTimeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    HoodieTimeline filteredTimeline = completedTimeline.filter(instant -> instant.getTimestamp().equals(instantTime));
    if (filteredTimeline.empty()) {
      return "Commit " + instantTime + " not found in Commits " + completedTimeline;
    }

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkMain.SparkCommand.ROLLBACK.toString(), master, sparkMemory, instantTime,
        HoodieCLI.getTableMetaClient().getBasePath());
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    // Refresh the current
    HoodieCLI.refreshTableMetadata();
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

    Option<HoodieInstant> hoodieInstantOption = getCommitForInstant(timeline, instantTime);
    Option<HoodieCommitMetadata> commitMetadataOptional = getHoodieCommitMetadata(timeline, hoodieInstantOption);

    if (!commitMetadataOptional.isPresent()) {
      return "Commit " + instantTime + " not found in Commits " + timeline;
    }

    HoodieCommitMetadata meta = commitMetadataOptional.get();
    List<Comparable[]> rows = new ArrayList<>();
    for (Map.Entry<String, List<HoodieWriteStat>> entry : meta.getPartitionToWriteStats().entrySet()) {
      String action = hoodieInstantOption.get().getAction();
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
      rows.add(new Comparable[] {action, path, totalFilesAdded, totalFilesUpdated, totalRecordsInserted, totalRecordsUpdated,
          totalBytesWritten, totalWriteErrors});
    }

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN, entry ->
        NumericUtils.humanReadableByteCount((Long.parseLong(entry.toString()))));

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_ACTION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION_PATH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_ADDED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_UPDATED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_INSERTED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_UPDATED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_ERRORS);

    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending,
        limit, headerOnly, rows, exportTableName);
  }

  @CliCommand(value = "commit show_write_stats", help = "Show write stats of a commit")
  public String showWriteStats(
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

    Option<HoodieInstant> hoodieInstantOption = getCommitForInstant(timeline, instantTime);
    Option<HoodieCommitMetadata> commitMetadataOptional = getHoodieCommitMetadata(timeline, hoodieInstantOption);

    if (!commitMetadataOptional.isPresent()) {
      return "Commit " + instantTime + " not found in Commits " + timeline;
    }

    HoodieCommitMetadata meta = commitMetadataOptional.get();

    String action = hoodieInstantOption.get().getAction();
    long recordsWritten = meta.fetchTotalRecordsWritten();
    long bytesWritten = meta.fetchTotalBytesWritten();
    long avgRecSize = (long) Math.ceil((1.0 * bytesWritten) / recordsWritten);
    List<Comparable[]> rows = new ArrayList<>();
    rows.add(new Comparable[] {action, bytesWritten, recordsWritten, avgRecSize});

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN, entry ->
        NumericUtils.humanReadableByteCount((Long.parseLong(entry.toString()))));

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_ACTION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN_COMMIT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_WRITTEN_COMMIT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_AVG_REC_SIZE_COMMIT);

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

    Option<HoodieInstant> hoodieInstantOption = getCommitForInstant(timeline, instantTime);
    Option<HoodieCommitMetadata> commitMetadataOptional = getHoodieCommitMetadata(timeline, hoodieInstantOption);

    if (!commitMetadataOptional.isPresent()) {
      return "Commit " + instantTime + " not found in Commits " + timeline;
    }

    HoodieCommitMetadata meta = commitMetadataOptional.get();
    List<Comparable[]> rows = new ArrayList<>();
    for (Map.Entry<String, List<HoodieWriteStat>> entry : meta.getPartitionToWriteStats().entrySet()) {
      String action = hoodieInstantOption.get().getAction();
      String path = entry.getKey();
      List<HoodieWriteStat> stats = entry.getValue();
      for (HoodieWriteStat stat : stats) {
        rows.add(new Comparable[] {action, path, stat.getFileId(), stat.getPrevCommit(), stat.getNumUpdateWrites(),
            stat.getNumWrites(), stat.getTotalWriteBytes(), stat.getTotalWriteErrors(), stat.getFileSizeInBytes()});
      }
    }

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_ACTION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION_PATH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_ID)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_PREVIOUS_COMMIT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_UPDATED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_ERRORS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_SIZE);

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending,
        limit, headerOnly, rows, exportTableName);
  }

  @CliCommand(value = "commits compare", help = "Compare commits with another Hoodie table")
  public String compareCommits(@CliOption(key = {"path"}, help = "Path of the table to compare to") final String path) {

    HoodieTableMetaClient source = HoodieCLI.getTableMetaClient();
    HoodieTableMetaClient target = HoodieTableMetaClient.builder().setConf(HoodieCLI.conf).setBasePath(path).build();
    HoodieTimeline targetTimeline = target.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    HoodieTimeline sourceTimeline = source.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    String targetLatestCommit =
        targetTimeline.getInstants().iterator().hasNext() ? targetTimeline.lastInstant().get().getTimestamp() : "0";
    String sourceLatestCommit =
        sourceTimeline.getInstants().iterator().hasNext() ? sourceTimeline.lastInstant().get().getTimestamp() : "0";

    if (sourceLatestCommit != null
        && HoodieTimeline.compareTimestamps(targetLatestCommit, HoodieTimeline.GREATER_THAN, sourceLatestCommit)) {
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
    HoodieCLI.syncTableMetadata = HoodieTableMetaClient.builder().setConf(HoodieCLI.conf).setBasePath(path).build();
    HoodieCLI.state = HoodieCLI.CLIState.SYNC;
    return "Load sync state between " + HoodieCLI.getTableMetaClient().getTableConfig().getTableName() + " and "
        + HoodieCLI.syncTableMetadata.getTableConfig().getTableName();
  }

  /*
  Checks whether a commit or replacecommit action exists in the timeline.
  * */
  private Option<HoodieInstant> getCommitForInstant(HoodieTimeline timeline, String instantTime) throws IOException {
    List<HoodieInstant> instants = Arrays.asList(
            new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, instantTime),
            new HoodieInstant(false, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime),
            new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime));

    Option<HoodieInstant> hoodieInstant = Option.fromJavaOptional(instants.stream().filter(timeline::containsInstant).findAny());

    return hoodieInstant;
  }

  private Option<HoodieCommitMetadata> getHoodieCommitMetadata(HoodieTimeline timeline, Option<HoodieInstant> hoodieInstant) throws IOException {
    if (hoodieInstant.isPresent()) {
      if (hoodieInstant.get().getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
        return Option.of(HoodieReplaceCommitMetadata.fromBytes(timeline.getInstantDetails(hoodieInstant.get()).get(),
            HoodieReplaceCommitMetadata.class));
      }
      return Option.of(HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(hoodieInstant.get()).get(),
              HoodieCommitMetadata.class));
    }

    return Option.empty();
  }
}
