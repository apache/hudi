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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.NumericUtils;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
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

  private static final int MAX_FILES = 1000000;

  @CliAvailabilityIndicator({"stats wa"})
  public boolean isWriteAmpAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

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

    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitTimeline().filterCompletedInstants();

    List<Comparable[]> rows = new ArrayList<>();
    int i = 0;
    DecimalFormat df = new DecimalFormat("#.00");
    for (HoodieInstant commitTime : timeline.getInstants().collect(Collectors.toList())) {
      String waf = "0";
      HoodieCommitMetadata commit = HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(commitTime).get(),
          HoodieCommitMetadata.class);
      if (commit.fetchTotalUpdateRecordsWritten() > 0) {
        waf = df.format((float) commit.fetchTotalRecordsWritten() / commit.fetchTotalUpdateRecordsWritten());
      }
      rows.add(new Comparable[] {commitTime.getTimestamp(), commit.fetchTotalUpdateRecordsWritten(),
          commit.fetchTotalRecordsWritten(), waf});
      totalRecordsUpserted += commit.fetchTotalUpdateRecordsWritten();
      totalRecordsWritten += commit.fetchTotalRecordsWritten();
    }
    String waf = "0";
    if (totalRecordsUpserted > 0) {
      waf = df.format((float) totalRecordsWritten / totalRecordsUpserted);
    }
    rows.add(new Comparable[] {"Total", totalRecordsUpserted, totalRecordsWritten, waf});

    TableHeader header = new TableHeader().addTableHeaderField("CommitTime").addTableHeaderField("Total Upserted")
        .addTableHeaderField("Total Written").addTableHeaderField("Write Amplifiation Factor");
    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
  }

  private Comparable[] printFileSizeHistogram(String commitTime, Snapshot s) {
    return new Comparable[] {commitTime, s.getMin(), s.getValue(0.1), s.getMedian(), s.getMean(), s.get95thPercentile(),
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
    String globPath = String.format("%s/%s/*", HoodieCLI.tableMetadata.getBasePath(), globRegex);
    FileStatus[] statuses = fs.globStatus(new Path(globPath));

    // max, min, #small files < 10MB, 50th, avg, 95th
    Histogram globalHistogram = new Histogram(new UniformReservoir(MAX_FILES));
    HashMap<String, Histogram> commitHistoMap = new HashMap<String, Histogram>();
    for (FileStatus fileStatus : statuses) {
      String commitTime = FSUtils.getCommitTime(fileStatus.getPath().getName());
      long sz = fileStatus.getLen();
      if (!commitHistoMap.containsKey(commitTime)) {
        commitHistoMap.put(commitTime, new Histogram(new UniformReservoir(MAX_FILES)));
      }
      commitHistoMap.get(commitTime).update(sz);
      globalHistogram.update(sz);
    }

    List<Comparable[]> rows = new ArrayList<>();
    int ind = 0;
    for (String commitTime : commitHistoMap.keySet()) {
      Snapshot s = commitHistoMap.get(commitTime).getSnapshot();
      rows.add(printFileSizeHistogram(commitTime, s));
    }
    Snapshot s = globalHistogram.getSnapshot();
    rows.add(printFileSizeHistogram("ALL", s));

    Function<Object, String> converterFunction =
        entry -> NumericUtils.humanReadableByteCount((Double.valueOf(entry.toString())));
    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put("Min", converterFunction);
    fieldNameToConverterMap.put("10th", converterFunction);
    fieldNameToConverterMap.put("50th", converterFunction);
    fieldNameToConverterMap.put("avg", converterFunction);
    fieldNameToConverterMap.put("95th", converterFunction);
    fieldNameToConverterMap.put("Max", converterFunction);
    fieldNameToConverterMap.put("StdDev", converterFunction);

    TableHeader header = new TableHeader().addTableHeaderField("CommitTime").addTableHeaderField("Min")
        .addTableHeaderField("10th").addTableHeaderField("50th").addTableHeaderField("avg").addTableHeaderField("95th")
        .addTableHeaderField("Max").addTableHeaderField("NumFiles").addTableHeaderField("StdDev");
    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }
}
