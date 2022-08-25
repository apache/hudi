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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
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
          unspecifiedDefaultValue = "") String globRegex,
      @CliOption(key = {"baseFileOnly"}, help = "Only display base files view",
          unspecifiedDefaultValue = "false") boolean baseFileOnly,
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

    globRegex = globRegex == null ? "" : globRegex;
    // TODO: There is a bug in spring shell, if we pass */*/* to pathRegex, the last '/' will be lost, pathRegex will be */**
    if (globRegex.endsWith("**")) {
      globRegex = globRegex.replace("**", "*/*");
    }

    HoodieTableFileSystemView fsView = buildFileSystemView(globRegex, maxInstant, baseFileOnly, includeMaxInstant,
        includeInflight, excludeCompaction);
    List<Comparable[]> rows = new ArrayList<>();
    fsView.getAllFileGroups().forEach(fg -> fg.getAllFileSlices().forEach(fs -> {
      int idx = 0;
      // For base file only Views, do not display any delta-file related columns
      Comparable[] row = new Comparable[baseFileOnly ? 5 : 8];
      row[idx++] = fg.getPartitionPath();
      row[idx++] = fg.getFileGroupId().getFileId();
      row[idx++] = fs.getBaseInstantTime();
      row[idx++] = fs.getBaseFile().isPresent() ? fs.getBaseFile().get().getPath() : "";
      row[idx++] = fs.getBaseFile().isPresent() ? fs.getBaseFile().get().getFileSize() : -1;
      if (!baseFileOnly) {
        row[idx++] = fs.getLogFiles().count();
        row[idx++] = fs.getLogFiles().mapToLong(HoodieLogFile::getFileSize).sum();
        row[idx++] = fs.getLogFiles().collect(Collectors.toList()).toString();
      }
      rows.add(row);
    }));
    Function<Object, String> converterFunction =
        entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString())));
    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_DELTA_FILE_SIZE, converterFunction);
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_DATA_FILE_SIZE, converterFunction);

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_ID)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_BASE_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DATA_FILE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DATA_FILE_SIZE);
    if (!baseFileOnly) {
      header = header.addTableHeaderField(HoodieTableHeaderFields.HEADER_NUM_DELTA_FILES)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_DELTA_FILE_SIZE)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELTA_FILES);
    }
    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }

  @CliCommand(value = "show fsview latest", help = "Show latest file-system view")
  public String showLatestFileSlices(
      @CliOption(key = {"partitionPath"}, help = "A valid partition path", unspecifiedDefaultValue = "") String partition,
      @CliOption(key = {"baseFileOnly"}, help = "Only display base file view",
          unspecifiedDefaultValue = "false") boolean baseFileOnly,
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

    HoodieTableFileSystemView fsView = buildFileSystemView(partition, maxInstant, baseFileOnly, includeMaxInstant,
        includeInflight, excludeCompaction);
    List<Comparable[]> rows = new ArrayList<>();

    final Stream<FileSlice> fileSliceStream;
    if (!merge) {
      fileSliceStream = fsView.getLatestFileSlices(partition);
    } else {
      if (maxInstant.isEmpty()) {
        maxInstant = HoodieCLI.getTableMetaClient().getActiveTimeline().filterCompletedAndCompactionInstants().lastInstant()
            .get().getTimestamp();
      }
      fileSliceStream = fsView.getLatestMergedFileSlicesBeforeOrOn(partition, maxInstant);
    }

    fileSliceStream.forEach(fs -> {
      int idx = 0;
      Comparable[] row = new Comparable[baseFileOnly ? 5 : 13];
      row[idx++] = partition;
      row[idx++] = fs.getFileId();
      row[idx++] = fs.getBaseInstantTime();
      row[idx++] = fs.getBaseFile().isPresent() ? fs.getBaseFile().get().getPath() : "";

      long dataFileSize = fs.getBaseFile().isPresent() ? fs.getBaseFile().get().getFileSize() : -1;
      row[idx++] = dataFileSize;

      if (!baseFileOnly) {
        row[idx++] = fs.getLogFiles().count();
        row[idx++] = fs.getLogFiles().mapToLong(HoodieLogFile::getFileSize).sum();
        long logFilesScheduledForCompactionTotalSize =
            fs.getLogFiles().filter(lf -> lf.getBaseCommitTime().equals(fs.getBaseInstantTime()))
                .mapToLong(HoodieLogFile::getFileSize).sum();
        row[idx++] = logFilesScheduledForCompactionTotalSize;

        long logFilesUnscheduledTotalSize =
            fs.getLogFiles().filter(lf -> !lf.getBaseCommitTime().equals(fs.getBaseInstantTime()))
                .mapToLong(HoodieLogFile::getFileSize).sum();
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
        entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString())));
    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_DATA_FILE_SIZE, converterFunction);
    if (!baseFileOnly) {
      fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_DELTA_SIZE, converterFunction);
      fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_DELTA_SIZE_SCHEDULED, converterFunction);
      fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_DELTA_SIZE_UNSCHEDULED, converterFunction);
    }

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_ID)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_BASE_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DATA_FILE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DATA_FILE_SIZE);

    if (!baseFileOnly) {
      header = header.addTableHeaderField(HoodieTableHeaderFields.HEADER_NUM_DELTA_FILES)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_DELTA_SIZE)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELTA_SIZE_SCHEDULED)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELTA_SIZE_UNSCHEDULED)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELTA_BASE_SCHEDULED)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELTA_BASE_UNSCHEDULED)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELTA_FILES_SCHEDULED)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELTA_FILES_UNSCHEDULED);
    }
    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }

  /**
   * Build File System View.
   *
   * @param globRegex         Path Regex
   * @param maxInstant        Max Instants to be used for displaying file-instants
   * @param basefileOnly      Include only base file view
   * @param includeMaxInstant Include Max instant
   * @param includeInflight   Include inflight instants
   * @param excludeCompaction Exclude Compaction instants
   * @return
   * @throws IOException
   */
  private HoodieTableFileSystemView buildFileSystemView(String globRegex, String maxInstant, boolean basefileOnly,
                                                        boolean includeMaxInstant, boolean includeInflight, boolean excludeCompaction) throws IOException {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(client.getHadoopConf()).setBasePath(client.getBasePath()).setLoadActiveTimelineOnLoad(true).build();
    FileSystem fs = HoodieCLI.fs;
    String globPath = String.format("%s/%s/*", client.getBasePath(), globRegex);
    List<FileStatus> statuses = FSUtils.getGlobStatusExcludingMetaFolder(fs, new Path(globPath));
    Stream<HoodieInstant> instantsStream;

    HoodieTimeline timeline;
    if (basefileOnly) {
      timeline = metaClient.getActiveTimeline().getCommitTimeline();
    } else if (excludeCompaction) {
      timeline = metaClient.getActiveTimeline().getCommitsTimeline();
    } else {
      timeline = metaClient.getActiveTimeline().getWriteTimeline();
    }

    if (!includeInflight) {
      timeline = timeline.filterCompletedInstants();
    }

    instantsStream = timeline.getInstants();

    if (!maxInstant.isEmpty()) {
      final BiPredicate<String, String> predicate;
      if (includeMaxInstant) {
        predicate = HoodieTimeline.GREATER_THAN_OR_EQUALS;
      } else {
        predicate = HoodieTimeline.GREATER_THAN;
      }
      instantsStream = instantsStream.filter(is -> predicate.test(maxInstant, is.getTimestamp()));
    }

    HoodieTimeline filteredTimeline = new HoodieDefaultTimeline(instantsStream,
        (Function<HoodieInstant, Option<byte[]>> & Serializable) metaClient.getActiveTimeline()::getInstantDetails);
    return new HoodieTableFileSystemView(metaClient, filteredTimeline, statuses.toArray(new FileStatus[0]));
  }
}
