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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * CLI command to display file system options.
 */
@Component
public class FileSystemViewCommand implements CommandMarker {

  @CliCommand(value = "show fsview all", help = "Show entire file-system view")
  public String showAllFileSlices(
      @CliOption(key = {"pathRegex"}, help = "regex to select files, eg: 2016/08/02",
          unspecifiedDefaultValue = "*/*/*") String globRegex,
      @CliOption(key = {"readOptimizedOnly"}, help = "Only display read-optimized view",
          unspecifiedDefaultValue = "false") boolean readOptimizedOnly,
      @CliOption(key = {"maxInstant"}, help = "File-Slices upto this instant are displayed",
          unspecifiedDefaultValue = "") String maxInstant,
      @CliOption(key = {"includeMax"}, help = "Include Max Instant",
          unspecifiedDefaultValue = "false") boolean includeMaxInstant,
      @CliOption(key = {"includeInflight"}, help = "Include Inflight Instants",
          unspecifiedDefaultValue = "false") boolean includeInflight,
      @CliOption(key = {"excludeCompaction"}, help = "Exclude compaction Instants",
          unspecifiedDefaultValue = "false") boolean excludeCompaction,
      @CliOption(key = {"limit"}, help = "Limit rows to be displayed", unspecifiedDefaultValue = "-1") Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {

    HoodieTableFileSystemView fsView = buildFileSystemView(globRegex, maxInstant, readOptimizedOnly, includeMaxInstant,
        includeInflight, excludeCompaction);
    List<Comparable[]> rows = new ArrayList<>();
    fsView.getAllFileGroups().forEach(fg -> fg.getAllFileSlices().forEach(fs -> {
      int idx = 0;
      // For ReadOptimized Views, do not display any delta-file related columns
      Comparable[] row = new Comparable[readOptimizedOnly ? 5 : 8];
      row[idx++] = fg.getPartitionPath();
      row[idx++] = fg.getFileGroupId().getFileId();
      row[idx++] = fs.getBaseInstantTime();
      row[idx++] = fs.getDataFile().isPresent() ? fs.getDataFile().get().getPath() : "";
      row[idx++] = fs.getDataFile().isPresent() ? fs.getDataFile().get().getFileSize() : -1;
      if (!readOptimizedOnly) {
        row[idx++] = fs.getLogFiles().count();
        row[idx++] = fs.getLogFiles().mapToLong(lf -> lf.getFileSize()).sum();
        row[idx++] = fs.getLogFiles().collect(Collectors.toList()).toString();
      }
      rows.add(row);
    }));
    Function<Object, String> converterFunction =
        entry -> NumericUtils.humanReadableByteCount((Double.valueOf(entry.toString())));
    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put("Total Delta File Size", converterFunction);
    fieldNameToConverterMap.put("Data-File Size", converterFunction);

    TableHeader header = new TableHeader().addTableHeaderField("Partition").addTableHeaderField("FileId")
        .addTableHeaderField("Base-Instant").addTableHeaderField("Data-File").addTableHeaderField("Data-File Size");
    if (!readOptimizedOnly) {
      header = header.addTableHeaderField("Num Delta Files").addTableHeaderField("Total Delta File Size")
          .addTableHeaderField("Delta Files");
    }
    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }

  @CliCommand(value = "show fsview latest", help = "Show latest file-system view")
  public String showLatestFileSlices(
      @CliOption(key = {"partitionPath"}, help = "A valid paritition path", mandatory = true) String partition,
      @CliOption(key = {"readOptimizedOnly"}, help = "Only display read-optimized view",
          unspecifiedDefaultValue = "false") boolean readOptimizedOnly,
      @CliOption(key = {"maxInstant"}, help = "File-Slices upto this instant are displayed",
          unspecifiedDefaultValue = "") String maxInstant,
      @CliOption(key = {"merge"}, help = "Merge File Slices due to pending compaction",
          unspecifiedDefaultValue = "true") final boolean merge,
      @CliOption(key = {"includeMax"}, help = "Include Max Instant",
          unspecifiedDefaultValue = "false") boolean includeMaxInstant,
      @CliOption(key = {"includeInflight"}, help = "Include Inflight Instants",
          unspecifiedDefaultValue = "false") boolean includeInflight,
      @CliOption(key = {"excludeCompaction"}, help = "Exclude compaction Instants",
          unspecifiedDefaultValue = "false") boolean excludeCompaction,
      @CliOption(key = {"limit"}, help = "Limit rows to be displayed", unspecifiedDefaultValue = "-1") Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {

    HoodieTableFileSystemView fsView = buildFileSystemView(partition, maxInstant, readOptimizedOnly, includeMaxInstant,
        includeInflight, excludeCompaction);
    List<Comparable[]> rows = new ArrayList<>();

    final Stream<FileSlice> fileSliceStream;
    if (!merge) {
      fileSliceStream = fsView.getLatestFileSlices(partition);
    } else {
      if (maxInstant.isEmpty()) {
        maxInstant = HoodieCLI.tableMetadata.getActiveTimeline().filterCompletedAndCompactionInstants().lastInstant()
            .get().getTimestamp();
      }
      fileSliceStream = fsView.getLatestMergedFileSlicesBeforeOrOn(partition, maxInstant);
    }

    fileSliceStream.forEach(fs -> {
      int idx = 0;
      Comparable[] row = new Comparable[readOptimizedOnly ? 5 : 13];
      row[idx++] = partition;
      row[idx++] = fs.getFileId();
      row[idx++] = fs.getBaseInstantTime();
      row[idx++] = fs.getDataFile().isPresent() ? fs.getDataFile().get().getPath() : "";

      long dataFileSize = fs.getDataFile().isPresent() ? fs.getDataFile().get().getFileSize() : -1;
      row[idx++] = dataFileSize;

      if (!readOptimizedOnly) {
        row[idx++] = fs.getLogFiles().count();
        row[idx++] = fs.getLogFiles().mapToLong(lf -> lf.getFileSize()).sum();
        long logFilesScheduledForCompactionTotalSize =
            fs.getLogFiles().filter(lf -> lf.getBaseCommitTime().equals(fs.getBaseInstantTime()))
                .mapToLong(lf -> lf.getFileSize()).sum();
        row[idx++] = logFilesScheduledForCompactionTotalSize;

        long logFilesUnscheduledTotalSize =
            fs.getLogFiles().filter(lf -> !lf.getBaseCommitTime().equals(fs.getBaseInstantTime()))
                .mapToLong(lf -> lf.getFileSize()).sum();
        row[idx++] = logFilesUnscheduledTotalSize;

        double logSelectedForCompactionToBaseRatio =
            dataFileSize > 0 ? logFilesScheduledForCompactionTotalSize / (dataFileSize * 1.0) : -1;
        row[idx++] = logSelectedForCompactionToBaseRatio;
        double logUnscheduledToBaseRatio = dataFileSize > 0 ? logFilesUnscheduledTotalSize / (dataFileSize * 1.0) : -1;
        row[idx++] = logUnscheduledToBaseRatio;

        row[idx++] = fs.getLogFiles().filter(lf -> lf.getBaseCommitTime().equals(fs.getBaseInstantTime()))
            .collect(Collectors.toList()).toString();
        row[idx++] = fs.getLogFiles().filter(lf -> !lf.getBaseCommitTime().equals(fs.getBaseInstantTime()))
            .collect(Collectors.toList()).toString();
      }
      rows.add(row);
    });

    Function<Object, String> converterFunction =
        entry -> NumericUtils.humanReadableByteCount((Double.valueOf(entry.toString())));
    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put("Data-File Size", converterFunction);
    if (!readOptimizedOnly) {
      fieldNameToConverterMap.put("Total Delta Size", converterFunction);
      fieldNameToConverterMap.put("Delta Size - compaction scheduled", converterFunction);
      fieldNameToConverterMap.put("Delta Size - compaction unscheduled", converterFunction);
    }

    TableHeader header = new TableHeader().addTableHeaderField("Partition").addTableHeaderField("FileId")
        .addTableHeaderField("Base-Instant").addTableHeaderField("Data-File").addTableHeaderField("Data-File Size");

    if (!readOptimizedOnly) {
      header = header.addTableHeaderField("Num Delta Files").addTableHeaderField("Total Delta Size")
          .addTableHeaderField("Delta Size - compaction scheduled")
          .addTableHeaderField("Delta Size - compaction unscheduled")
          .addTableHeaderField("Delta To Base Ratio - compaction scheduled")
          .addTableHeaderField("Delta To Base Ratio - compaction unscheduled")
          .addTableHeaderField("Delta Files - compaction scheduled")
          .addTableHeaderField("Delta Files - compaction unscheduled");
    }
    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }

  /**
   * Build File System View.
   * 
   * @param globRegex Path Regex
   * @param maxInstant Max Instants to be used for displaying file-instants
   * @param readOptimizedOnly Include only read optimized view
   * @param includeMaxInstant Include Max instant
   * @param includeInflight Include inflight instants
   * @param excludeCompaction Exclude Compaction instants
   * @return
   * @throws IOException
   */
  private HoodieTableFileSystemView buildFileSystemView(String globRegex, String maxInstant, boolean readOptimizedOnly,
      boolean includeMaxInstant, boolean includeInflight, boolean excludeCompaction) throws IOException {
    HoodieTableMetaClient metaClient =
        new HoodieTableMetaClient(HoodieCLI.tableMetadata.getHadoopConf(), HoodieCLI.tableMetadata.getBasePath(), true);
    FileSystem fs = HoodieCLI.fs;
    String globPath = String.format("%s/%s/*", HoodieCLI.tableMetadata.getBasePath(), globRegex);
    FileStatus[] statuses = fs.globStatus(new Path(globPath));
    Stream<HoodieInstant> instantsStream = null;

    HoodieTimeline timeline = null;
    if (readOptimizedOnly) {
      timeline = metaClient.getActiveTimeline().getCommitTimeline();
    } else if (excludeCompaction) {
      timeline = metaClient.getActiveTimeline().getCommitsTimeline();
    } else {
      timeline = metaClient.getActiveTimeline().getCommitsAndCompactionTimeline();
    }

    if (!includeInflight) {
      timeline = timeline.filterCompletedInstants();
    }

    instantsStream = timeline.getInstants();

    if (!maxInstant.isEmpty()) {
      final BiPredicate<String, String> predicate;
      if (includeMaxInstant) {
        predicate = HoodieTimeline.GREATER_OR_EQUAL;
      } else {
        predicate = HoodieTimeline.GREATER;
      }
      instantsStream = instantsStream.filter(is -> predicate.test(maxInstant, is.getTimestamp()));
    }

    HoodieTimeline filteredTimeline = new HoodieDefaultTimeline(instantsStream,
        (Function<HoodieInstant, Option<byte[]>> & Serializable) metaClient.getActiveTimeline()::getInstantDetails);
    return new HoodieTableFileSystemView(metaClient, filteredTimeline, statuses);
  }
}
