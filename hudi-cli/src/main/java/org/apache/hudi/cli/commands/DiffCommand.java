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
import org.apache.hudi.common.util.StringUtils;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.shell.core.CommandMarker;
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

import static org.apache.hudi.cli.utils.TimelineUtil.getTimeDaysAgo;

/**
 * Given a file id or partition value, this command line utility tracks the changes to the file group or partition across range of commits.
 * Usage: diff file --fileId <fileId>
 */
@Component
public class DiffCommand implements CommandMarker {

  private static final Logger LOG = LogManager.getLogger(DiffCommand.class);

  @CliCommand(value = "diff file", help = "Check how file differs across range of commits")
  public String diffFile(
      @CliOption(key = {"fileId"}, help = "File ID to diff across range of commits", mandatory = true) String fileId,
      @CliOption(key = {"startTs"}, help = "start time for compactions, default: now - 10 days") String startTs,
      @CliOption(key = {"endTs"}, help = "end time for compactions, default: now - 1 day") String endTs,
      @CliOption(key = {"limit"}, help = "Limit compactions", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false") final boolean headerOnly,
      @CliOption(key = {"includeArchivedTimeline"}, help = "Include archived commits as well",
          unspecifiedDefaultValue = "false") final boolean includeArchivedTimeline) throws IOException {
    HoodieDefaultTimeline timeline = getTimelineInRange(startTs, endTs, includeArchivedTimeline);
    return printCommitsWithMetadataForFileId(timeline, limit, sortByField, descending, headerOnly, "", fileId);
  }

  @CliCommand(value = "diff partition", help = "Check how file differs across range of commits")
  public String diffPartition(
      @CliOption(key = {"partitionPath"}, help = "Relative partition path to diff across range of commits", mandatory = true) String partitionPath,
      @CliOption(key = {"startTs"}, help = "start time for compactions, default: now - 10 days") String startTs,
      @CliOption(key = {"endTs"}, help = "end time for compactions, default: now - 1 day") String endTs,
      @CliOption(key = {"limit"}, help = "Limit compactions", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false") final boolean headerOnly,
      @CliOption(key = {"includeArchivedTimeline"}, help = "Include archived commits as well",
          unspecifiedDefaultValue = "false") final boolean includeArchivedTimeline) throws IOException {
    HoodieDefaultTimeline timeline = getTimelineInRange(startTs, endTs, includeArchivedTimeline);
    return printCommitsWithMetadataForPartition(timeline, limit, sortByField, descending, headerOnly, "", partitionPath);
  }

  private HoodieDefaultTimeline getTimelineInRange(String startTs, String endTs, boolean includeArchivedTimeline) {
    if (StringUtils.isNullOrEmpty(startTs)) {
      startTs = getTimeDaysAgo(10);
    }
    if (StringUtils.isNullOrEmpty(endTs)) {
      endTs = getTimeDaysAgo(1);
    }
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
    final List<Comparable[]> rows = new ArrayList<>();

    final List<HoodieInstant> commits = timeline.getCommitsTimeline().filterCompletedInstants()
        .getInstants().sorted(HoodieInstant.COMPARATOR.reversed()).collect(Collectors.toList());

    for (final HoodieInstant commit : commits) {
      if (timeline.getInstantDetails(commit).isPresent()) {
        final HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            timeline.getInstantDetails(commit).get(),
            HoodieCommitMetadata.class);

        for (Map.Entry<String, List<HoodieWriteStat>> partitionWriteStat :
            commitMetadata.getPartitionToWriteStats().entrySet()) {
          for (HoodieWriteStat hoodieWriteStat : partitionWriteStat.getValue()) {
            if (fileId.equals(hoodieWriteStat.getFileId())) {
              rows.add(new Comparable[] {commit.getAction(), commit.getTimestamp(), hoodieWriteStat.getPartitionPath(),
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
    }

    final Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(
        HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN,
        entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString()))));

    return HoodiePrintHelper.print(HoodieTableHeaderFields.getTableHeaderWithExtraMetadata(),
        fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows, tempTableName);
  }

  private String printCommitsWithMetadataForPartition(HoodieDefaultTimeline timeline,
                                                      final Integer limit, final String sortByField,
                                                      final boolean descending,
                                                      final boolean headerOnly,
                                                      final String tempTableName,
                                                      final String partition) throws IOException {
    final List<Comparable[]> rows = new ArrayList<>();

    final List<HoodieInstant> commits = timeline.getCommitsTimeline().filterCompletedInstants()
        .getInstants().sorted(HoodieInstant.COMPARATOR.reversed()).collect(Collectors.toList());

    for (final HoodieInstant commit : commits) {
      if (timeline.getInstantDetails(commit).isPresent()) {
        final HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            timeline.getInstantDetails(commit).get(),
            HoodieCommitMetadata.class);

        for (Map.Entry<String, List<HoodieWriteStat>> partitionWriteStat :
            commitMetadata.getPartitionToWriteStats().entrySet()) {
          for (HoodieWriteStat hoodieWriteStat : partitionWriteStat.getValue()) {
            if (partition.equals(hoodieWriteStat.getPartitionPath())) {
              rows.add(new Comparable[] {commit.getAction(), commit.getTimestamp(), hoodieWriteStat.getPartitionPath(),
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
    }

    final Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(
        HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN,
        entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString()))));

    return HoodiePrintHelper.print(HoodieTableHeaderFields.getTableHeaderWithExtraMetadata(),
        fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows, tempTableName);
  }
}
