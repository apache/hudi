/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.Option;

import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.cli.utils.CommitUtil.getTimeDaysAgo;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Given a file id or partition value, this command line utility tracks the changes to the file group or partition across range of commits.
 * Usage: diff file --fileId <fileId>
 */
@ShellComponent
public class DiffCommand {

  private static final BiFunction<HoodieWriteStat, String, Boolean> FILE_ID_CHECKER = (writeStat, fileId) -> fileId.equals(writeStat.getFileId());
  private static final BiFunction<HoodieWriteStat, String, Boolean> PARTITION_CHECKER = (writeStat, partitionPath) -> partitionPath.equals(writeStat.getPartitionPath());

  @ShellMethod(key = "diff file", value = "Check how file differs across range of commits")
  public String diffFile(
      @ShellOption(value = {"--fileId"}, help = "File ID to diff across range of commits") String fileId,
      @ShellOption(value = {"--startTs"}, help = "start time for compactions, default: now - 10 days",
              defaultValue = ShellOption.NULL) String startTs,
      @ShellOption(value = {"--endTs"}, help = "end time for compactions, default: now - 1 day",
              defaultValue = ShellOption.NULL) String endTs,
      @ShellOption(value = {"--limit"}, help = "Limit compactions", defaultValue = "-1") final Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only", defaultValue = "false") final boolean headerOnly,
      @ShellOption(value = {"--includeArchivedTimeline"}, help = "Include archived commits as well",
          defaultValue = "false") final boolean includeArchivedTimeline) throws IOException {
    HoodieDefaultTimeline timeline = getTimelineInRange(startTs, endTs, includeArchivedTimeline);
    return printCommitsWithMetadataForFileId(timeline, limit, sortByField, descending, headerOnly, "", fileId);
  }

  @ShellMethod(key = "diff partition", value = "Check how file differs across range of commits. It is meant to be used only for partitioned tables.")
  public String diffPartition(
      @ShellOption(value = {"--partitionPath"}, help = "Relative partition path to diff across range of commits") String partitionPath,
      @ShellOption(value = {"--startTs"}, help = "start time for compactions, default: now - 10 days",
              defaultValue = ShellOption.NULL) String startTs,
      @ShellOption(value = {"--endTs"}, help = "end time for compactions, default: now - 1 day",
              defaultValue = ShellOption.NULL) String endTs,
      @ShellOption(value = {"--limit"}, help = "Limit compactions", defaultValue = "-1") final Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only", defaultValue = "false") final boolean headerOnly,
      @ShellOption(value = {"--includeArchivedTimeline"}, help = "Include archived commits as well",
          defaultValue = "false") final boolean includeArchivedTimeline) throws IOException {
    HoodieDefaultTimeline timeline = getTimelineInRange(startTs, endTs, includeArchivedTimeline);
    return printCommitsWithMetadataForPartition(timeline, limit, sortByField, descending, headerOnly, "", partitionPath);
  }

  private HoodieDefaultTimeline getTimelineInRange(String startTs, String endTs, boolean includeArchivedTimeline) {
    if (isNullOrEmpty(startTs)) {
      startTs = getTimeDaysAgo(10);
    }
    if (isNullOrEmpty(endTs)) {
      endTs = getTimeDaysAgo(1);
    }
    checkArgument(nonEmpty(startTs), "startTs is null or empty");
    checkArgument(nonEmpty(endTs), "endTs is null or empty");
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    if (includeArchivedTimeline) {
      HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
      archivedTimeline.loadInstantDetailsInMemory(startTs, endTs);
      return archivedTimeline.findInstantsInRange(startTs, endTs).mergeTimeline(activeTimeline);
    }
    return activeTimeline;
  }

  private String printCommitsWithMetadataForFileId(HoodieDefaultTimeline timeline,
                                                   final Integer limit,
                                                   final String sortByField,
                                                   final boolean descending,
                                                   final boolean headerOnly,
                                                   final String tempTableName,
                                                   final String fileId) throws IOException {
    return printDiffWithMetadata(timeline, limit, sortByField, descending, headerOnly, tempTableName, fileId, FILE_ID_CHECKER);
  }

  private String printCommitsWithMetadataForPartition(HoodieDefaultTimeline timeline,
                                                      final Integer limit,
                                                      final String sortByField,
                                                      final boolean descending,
                                                      final boolean headerOnly,
                                                      final String tempTableName,
                                                      final String partition) throws IOException {
    return printDiffWithMetadata(timeline, limit, sortByField, descending, headerOnly, tempTableName, partition, PARTITION_CHECKER);
  }

  private String printDiffWithMetadata(HoodieDefaultTimeline timeline, Integer limit, String sortByField, boolean descending, boolean headerOnly, String tempTableName, String diffEntity,
                                       BiFunction<HoodieWriteStat, String, Boolean> diffEntityChecker) throws IOException {
    List<Comparable[]> rows = new ArrayList<>();
    List<HoodieInstant> commits = timeline.getCommitsTimeline().filterCompletedInstants()
        .getInstants().sorted(HoodieInstant.COMPARATOR.reversed()).collect(Collectors.toList());

    for (final HoodieInstant commit : commits) {
      Option<byte[]> instantDetails = timeline.getInstantDetails(commit);
      if (instantDetails.isPresent()) {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(instantDetails.get(), HoodieCommitMetadata.class);
        for (Map.Entry<String, List<HoodieWriteStat>> partitionWriteStat :
            commitMetadata.getPartitionToWriteStats().entrySet()) {
          for (HoodieWriteStat hoodieWriteStat : partitionWriteStat.getValue()) {
            populateRows(rows, commit, hoodieWriteStat, diffEntity, diffEntityChecker);
          }
        }
      }
    }

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(
        HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN,
        entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString()))));

    return HoodiePrintHelper.print(HoodieTableHeaderFields.getTableHeaderWithExtraMetadata(),
        fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows, tempTableName);
  }

  private void populateRows(List<Comparable[]> rows, HoodieInstant commit, HoodieWriteStat hoodieWriteStat,
                            String value, BiFunction<HoodieWriteStat, String, Boolean> checker) {
    if (checker.apply(hoodieWriteStat, value)) {
      rows.add(new Comparable[] {
          commit.getAction(),
          commit.getTimestamp(),
          hoodieWriteStat.getPartitionPath(),
          hoodieWriteStat.getFileId(),
          hoodieWriteStat.getPrevCommit(),
          hoodieWriteStat.getNumWrites(),
          hoodieWriteStat.getNumInserts(),
          hoodieWriteStat.getNumDeletes(),
          hoodieWriteStat.getNumUpdateWrites(),
          hoodieWriteStat.getTotalWriteErrors(),
          hoodieWriteStat.getTotalLogBlocks(),
          hoodieWriteStat.getTotalCorruptLogBlock(),
          hoodieWriteStat.getTotalRollbackBlocks(),
          hoodieWriteStat.getTotalLogRecords(),
          hoodieWriteStat.getTotalUpdatedRecordsCompacted(),
          hoodieWriteStat.getTotalWriteBytes()
      });
    }
  }
}
