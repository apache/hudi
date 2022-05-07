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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.NumericUtils;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * CLI command to displays stats options.
 */
@Component
public class StatsCommand implements CommandMarker {

  public static final int MAX_FILES = 1000000;

  @CliCommand(value = "stats wa", help = "Write Amplification. Ratio of how many records were upserted to how many "
      + "records were actually written")
  public String writeAmplificationStats(
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {

    long totalRecordsUpserted = 0;
    long totalRecordsWritten = 0;

    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitTimeline().filterCompletedInstants();

    List<Comparable[]> rows = new ArrayList<>();
    DecimalFormat df = new DecimalFormat("#.00");
    for (HoodieInstant instantTime : timeline.getInstants().collect(Collectors.toList())) {
      String waf = "0";
      HoodieCommitMetadata commit = HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(instantTime).get(),
          HoodieCommitMetadata.class);
      if (commit.fetchTotalUpdateRecordsWritten() > 0) {
        waf = df.format((float) commit.fetchTotalRecordsWritten() / commit.fetchTotalUpdateRecordsWritten());
      }
      rows.add(new Comparable[] {instantTime.getTimestamp(), commit.fetchTotalUpdateRecordsWritten(),
          commit.fetchTotalRecordsWritten(), waf});
      totalRecordsUpserted += commit.fetchTotalUpdateRecordsWritten();
      totalRecordsWritten += commit.fetchTotalRecordsWritten();
    }
    String waf = "0";
    if (totalRecordsUpserted > 0) {
      waf = df.format((float) totalRecordsWritten / totalRecordsUpserted);
    }
    rows.add(new Comparable[] {"Total", totalRecordsUpserted, totalRecordsWritten, waf});

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_COMMIT_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_UPSERTED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_WRITE_AMPLIFICATION_FACTOR);
    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
  }

  public Comparable[] printFileSizeHistogram(String instantTime, Snapshot s) {
    return new Comparable[] {instantTime, s.getMin(), s.getValue(0.1), s.getMedian(), s.getMean(), s.get95thPercentile(),
        s.getMax(), s.size(), s.getStdDev()};
  }

  @CliCommand(value = "stats filesizes", help = "File Sizes. Display summary stats on sizes of files")
  public String fileSizeStats(
      @CliOption(key = {"partitionPath"}, help = "regex to select files, eg: 2016/08/02",
          unspecifiedDefaultValue = "*/*/*") final String globRegex,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {

    FileSystem fs = HoodieCLI.fs;
    String globPath = String.format("%s/%s/*", HoodieCLI.getTableMetaClient().getBasePath(), globRegex);
    List<FileStatus> statuses = FSUtils.getGlobStatusExcludingMetaFolder(fs, new Path(globPath));

    // max, min, #small files < 10MB, 50th, avg, 95th
    Histogram globalHistogram = new Histogram(new UniformReservoir(MAX_FILES));
    HashMap<String, Histogram> commitHistoMap = new HashMap<>();
    for (FileStatus fileStatus : statuses) {
      String instantTime = FSUtils.getCommitTime(fileStatus.getPath().getName());
      long sz = fileStatus.getLen();
      if (!commitHistoMap.containsKey(instantTime)) {
        commitHistoMap.put(instantTime, new Histogram(new UniformReservoir(MAX_FILES)));
      }
      commitHistoMap.get(instantTime).update(sz);
      globalHistogram.update(sz);
    }

    List<Comparable[]> rows = new ArrayList<>();
    for (Map.Entry<String, Histogram> entry : commitHistoMap.entrySet()) {
      Snapshot s = entry.getValue().getSnapshot();
      rows.add(printFileSizeHistogram(entry.getKey(), s));
    }
    Snapshot s = globalHistogram.getSnapshot();
    rows.add(printFileSizeHistogram("ALL", s));

    TableHeader header = new TableHeader()
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_COMMIT_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_MIN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_10TH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_50TH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_AVG)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_95TH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_MAX)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_NUM_FILES)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_STD_DEV);
    return HoodiePrintHelper.print(header, getFieldNameToConverterMap(), sortByField, descending, limit, headerOnly, rows);
  }

  public Map<String, Function<Object, String>> getFieldNameToConverterMap() {
    Function<Object, String> converterFunction =
        entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString())));
    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put("Min", converterFunction);
    fieldNameToConverterMap.put("10th", converterFunction);
    fieldNameToConverterMap.put("50th", converterFunction);
    fieldNameToConverterMap.put("avg", converterFunction);
    fieldNameToConverterMap.put("95th", converterFunction);
    fieldNameToConverterMap.put("Max", converterFunction);
    fieldNameToConverterMap.put("StdDev", converterFunction);
    return fieldNameToConverterMap;
  }
}
