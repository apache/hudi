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

import org.apache.hudi.util.HoodieInflightCommitsUtil;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparator;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.cli.utils.CommitUtil.getTimeDaysAgo;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.common.table.timeline.TimelineUtils.getCommitMetadata;
import static org.apache.hudi.common.table.timeline.TimelineUtils.getTimeline;

/**
 * CLI command to display commits options.
 */
@ShellComponent
public class CommitsCommand {

  private String printCommits(HoodieTimeline timeline,
                              final Integer limit,
                              final String sortByField,
                              final boolean descending,
                              final boolean headerOnly,
                              final String tempTableName) throws IOException {
    final List<Comparable[]> rows = new ArrayList<>();
    InstantComparator instantComparator = HoodieCLI.getTableMetaClient().getTimelineLayout().getInstantComparator();

    final List<HoodieInstant> commits = timeline.getCommitsTimeline().filterCompletedInstants()
        .getInstantsAsStream().sorted(instantComparator.requestedTimeOrderedComparator().reversed()).collect(Collectors.toList());

    for (final HoodieInstant commit : commits) {
      final HoodieCommitMetadata commitMetadata = timeline.readCommitMetadata(commit);
      rows.add(new Comparable[] {commit.requestedTime(),
          commitMetadata.fetchTotalBytesWritten(),
          commitMetadata.fetchTotalFilesInsert(),
          commitMetadata.fetchTotalFilesUpdated(),
          commitMetadata.fetchTotalPartitionsWritten(),
          commitMetadata.fetchTotalRecordsWritten(),
          commitMetadata.fetchTotalUpdateRecordsWritten(),
          commitMetadata.fetchTotalWriteErrors()});
    }

    final Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(
        HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN,
        entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString()))));

    final TableHeader header = HoodieTableHeaderFields.getTableHeader();

    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending,
        limit, headerOnly, rows, tempTableName);
  }

  private String printCommitsWithMetadata(HoodieTimeline timeline,
                                          final Integer limit, final String sortByField,
                                          final boolean descending,
                                          final boolean headerOnly,
                                          final String tempTableName,
                                          final String partition) throws IOException {
    final List<Comparable[]> rows = new ArrayList<>();
    InstantComparator instantComparator = HoodieCLI.getTableMetaClient().getTimelineLayout().getInstantComparator();

    final List<HoodieInstant> commits = timeline.getCommitsTimeline().filterCompletedInstants()
        .getInstantsAsStream().sorted(instantComparator.requestedTimeOrderedComparator().reversed()).collect(Collectors.toList());

    for (final HoodieInstant commit : commits) {
      final HoodieCommitMetadata commitMetadata = timeline.readCommitMetadata(commit);

      for (Map.Entry<String, List<HoodieWriteStat>> partitionWriteStat :
          commitMetadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat hoodieWriteStat : partitionWriteStat.getValue()) {
          if (StringUtils.isNullOrEmpty(partition) || partition.equals(hoodieWriteStat.getPartitionPath())) {
            rows.add(new Comparable[] {commit.getAction(), commit.requestedTime(), hoodieWriteStat.getPartitionPath(),
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
    }

    final Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(
        HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN,
        entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString()))));

    return HoodiePrintHelper.print(HoodieTableHeaderFields.getTableHeaderWithExtraMetadata(),
        fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows, tempTableName);
  }

  @ShellMethod(key = "commits show", value = "Show the commits")
  public String showCommits(
      @ShellOption(value = {"--includeExtraMetadata"}, help = "Include extra metadata",
          defaultValue = "false") final boolean includeExtraMetadata,
      @ShellOption(value = {"--createView"}, help = "view name to store output table",
              defaultValue = "") final String exportTableName,
      @ShellOption(value = {"--limit"}, help = "Limit commits",
              defaultValue = "-1") final Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
              defaultValue = "false") final boolean headerOnly,
      @ShellOption(value = {"--partition"}, help = "Partition value", defaultValue = ShellOption.NULL) final String partition,
      @ShellOption(value = {"--includeArchivedTimeline"}, help = "Include archived commits as well",
              defaultValue = "false") final boolean includeArchivedTimeline)
      throws IOException {

    HoodieTimeline timeline = getTimeline(HoodieCLI.getTableMetaClient(), includeArchivedTimeline);
    if (includeExtraMetadata) {
      return printCommitsWithMetadata(timeline, limit, sortByField, descending, headerOnly, exportTableName, partition);
    } else {
      return printCommits(timeline, limit, sortByField, descending, headerOnly, exportTableName);
    }
  }

  @ShellMethod(key = "commits showarchived", value = "Show the archived commits")
  public String showArchivedCommits(
      @ShellOption(value = {"--includeExtraMetadata"}, help = "Include extra metadata",
          defaultValue = "false") final boolean includeExtraMetadata,
      @ShellOption(value = {"--createView"}, help = "view name to store output table",
          defaultValue = "") final String exportTableName,
      @ShellOption(value = {"--startTs"}, defaultValue = ShellOption.NULL, help = "start time for commits, default: now - 10 days")
          String startTs,
      @ShellOption(value = {"--endTs"}, defaultValue = ShellOption.NULL, help = "end time for commits, default: now - 1 day")
          String endTs,
      @ShellOption(value = {"--limit"}, help = "Limit commits", defaultValue = "-1") final Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only", defaultValue = "false") final boolean headerOnly,
      @ShellOption(value = {"--partition"}, help = "Partition value", defaultValue = ShellOption.NULL) final String partition)
      throws IOException {
    if (StringUtils.isNullOrEmpty(startTs)) {
      startTs = getTimeDaysAgo(10);
    }
    if (StringUtils.isNullOrEmpty(endTs)) {
      endTs = getTimeDaysAgo(1);
    }
    HoodieArchivedTimeline archivedTimeline = HoodieCLI.getTableMetaClient().getArchivedTimeline();
    try {
      archivedTimeline.loadInstantDetailsInMemory(startTs, endTs);
      HoodieTimeline timelineRange = archivedTimeline.findInstantsInRange(startTs, endTs);
      if (includeExtraMetadata) {
        return printCommitsWithMetadata(timelineRange, limit, sortByField, descending, headerOnly, exportTableName, partition);
      } else {
        return printCommits(timelineRange, limit, sortByField, descending, headerOnly, exportTableName);
      }
    } finally {
      // clear the instant details from memory after printing to reduce usage
      archivedTimeline.clearInstantDetailsFromMemory(startTs, endTs);
    }
  }

  @ShellMethod(key = "commit showpartitions", value = "Show partition level details of a commit")
  public String showCommitPartitions(
      @ShellOption(value = {"--createView"}, help = "view name to store output table",
          defaultValue = "") final String exportTableName,
      @ShellOption(value = {"--commit"}, help = "Commit to show") final String instantTime,
      @ShellOption(value = {"--limit"}, help = "Limit commits", defaultValue = "-1") final Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
          defaultValue = "false") final boolean headerOnly,
      @ShellOption(value = {"--includeArchivedTimeline"}, help = "Include archived commits as well",
              defaultValue = "false") final boolean includeArchivedTimeline)
      throws Exception {

    HoodieTimeline defaultTimeline = getTimeline(HoodieCLI.getTableMetaClient(), includeArchivedTimeline);
    HoodieTimeline timeline = defaultTimeline.getCommitsTimeline().filterCompletedInstants();

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

  @ShellMethod(key = "commit show_write_stats", value = "Show write stats of a commit")
  public String showWriteStats(
      @ShellOption(value = {"--createView"}, help = "view name to store output table",
          defaultValue = "") final String exportTableName,
      @ShellOption(value = {"--commit"}, help = "Commit to show") final String instantTime,
      @ShellOption(value = {"--limit"}, help = "Limit commits", defaultValue = "-1") final Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
          defaultValue = "false") final boolean headerOnly,
      @ShellOption(value = {"--includeArchivedTimeline"}, help = "Include archived commits as well",
              defaultValue = "false") final boolean includeArchivedTimeline)
      throws Exception {

    HoodieTimeline defaultTimeline = getTimeline(HoodieCLI.getTableMetaClient(), includeArchivedTimeline);
    HoodieTimeline timeline = defaultTimeline.getCommitsTimeline().filterCompletedInstants();

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

  @ShellMethod(key = "commit showfiles", value = "Show file level details of a commit")
  public String showCommitFiles(
      @ShellOption(value = {"--createView"}, help = "view name to store output table",
          defaultValue = "") final String exportTableName,
      @ShellOption(value = {"--commit"}, help = "Commit to show") final String instantTime,
      @ShellOption(value = {"--limit"}, help = "Limit commits", defaultValue = "-1") final Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
          defaultValue = "false") final boolean headerOnly,
      @ShellOption(value = {"--includeArchivedTimeline"}, help = "Include archived commits as well",
              defaultValue = "false") final boolean includeArchivedTimeline)
      throws Exception {

    HoodieTimeline defaultTimeline = getTimeline(HoodieCLI.getTableMetaClient(), includeArchivedTimeline);
    HoodieTimeline timeline = defaultTimeline.getCommitsTimeline().filterCompletedInstants();

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

  @ShellMethod(key = "commits show_infights", value = "Show inflight instants that are left longer than a certain duration")
  public String showInflightCommits(
      @ShellOption(value = {"durationInMins"}, help = "Commit to show", defaultValue = "0") final long durationInMins) {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();

    // Fetch inflight commits.
    List<HoodieInstant> inflightInstants = HoodieInflightCommitsUtil
        .inflightWriteCommitsOlderThan(metaClient, durationInMins, true);

    // Create a table out of inflight commits.
    List<String[]> data = new ArrayList<>();
    inflightInstants.forEach(instant ->
        data.add(new String[]{instant.requestedTime(), instant.getAction(), instant.getState().name()}));
    String[] header = new String[]{HoodieTableHeaderFields.HEADER_COMMIT_TIME,
        HoodieTableHeaderFields.HEADER_ACTION, HoodieTableHeaderFields.HEADER_STATE};
    if (data.isEmpty()) {
      return "No inflight instants are found.";
    } else {
      return HoodiePrintHelper.print(header, data.toArray(new String[0][]));
    }
  }

  @ShellMethod(key = "commits compare", value = "Compare commits with another Hoodie table")
  public String compareCommits(@ShellOption(value = {"--path"}, help = "Path of the table to compare to") final String path) {

    HoodieTableMetaClient source = HoodieCLI.getTableMetaClient();
    HoodieTableMetaClient target = HoodieTableMetaClient.builder()
        .setConf(HoodieCLI.conf.newInstance()).setBasePath(path).build();
    HoodieTimeline targetTimeline = target.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    HoodieTimeline sourceTimeline = source.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    String targetLatestCommit =
        targetTimeline.getInstantsAsStream().iterator().hasNext() ? targetTimeline.lastInstant().get().requestedTime() : "0";
    String sourceLatestCommit =
        sourceTimeline.getInstantsAsStream().iterator().hasNext() ? sourceTimeline.lastInstant().get().requestedTime() : "0";

    if (sourceLatestCommit != null
        && compareTimestamps(targetLatestCommit, GREATER_THAN, sourceLatestCommit)) {
      // source is behind the target
      List<String> commitsToCatchup = targetTimeline.findInstantsAfter(sourceLatestCommit, Integer.MAX_VALUE)
          .getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
      return "Source " + source.getTableConfig().getTableName() + " is behind by " + commitsToCatchup.size()
          + " commits. Commits to catch up - " + commitsToCatchup;
    } else {
      List<String> commitsToCatchup = sourceTimeline.findInstantsAfter(targetLatestCommit, Integer.MAX_VALUE)
          .getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
      return "Source " + source.getTableConfig().getTableName() + " is ahead by " + commitsToCatchup.size()
          + " commits. Commits to catch up - " + commitsToCatchup;
    }
  }

  @ShellMethod(key = "commits sync", value = "Sync commits with another Hoodie table")
  public String syncCommits(@ShellOption(value = {"--path"}, help = "Path of the table to sync to") final String path) {
    HoodieCLI.syncTableMetadata = HoodieTableMetaClient.builder()
        .setConf(HoodieCLI.conf.newInstance()).setBasePath(path).build();
    HoodieCLI.state = HoodieCLI.CLIState.SYNC;
    return "Load sync state between " + HoodieCLI.getTableMetaClient().getTableConfig().getTableName() + " and "
        + HoodieCLI.syncTableMetadata.getTableConfig().getTableName();
  }

  /*
  Checks whether a commit or replacecommit action exists in the timeline.
  * */
  private Option<HoodieInstant> getCommitForInstant(HoodieTimeline timeline, String instantTime) {
    List<HoodieInstant> instants = Arrays.asList(
        HoodieCLI.getTableMetaClient().createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, instantTime),
        HoodieCLI.getTableMetaClient().createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime),
        HoodieCLI.getTableMetaClient().createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime));

    return Option.fromJavaOptional(instants.stream().filter(timeline::containsInstant).findAny());
  }

  private Option<HoodieCommitMetadata> getHoodieCommitMetadata(HoodieTimeline timeline, Option<HoodieInstant> hoodieInstant) throws IOException {
    if (hoodieInstant.isPresent()) {
      return Option.of(getCommitMetadata(hoodieInstant.get(), timeline));
    }
    return Option.empty();
  }
}
