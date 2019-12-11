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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.AvroUtils;

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
import java.util.stream.Collectors;

/**
 * CLI command to show cleans options.
 */
@Component
public class CleansCommand implements CommandMarker {

  @CliAvailabilityIndicator({"cleans show"})
  public boolean isShowAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliAvailabilityIndicator({"cleans refresh"})
  public boolean isRefreshAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliAvailabilityIndicator({"clean showpartitions"})
  public boolean isCommitShowAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliCommand(value = "cleans show", help = "Show the cleans")
  public String showCleans(
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {

    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCleanerTimeline().filterCompletedInstants();
    List<HoodieInstant> cleans = timeline.getReverseOrderedInstants().collect(Collectors.toList());
    List<Comparable[]> rows = new ArrayList<>();
    for (int i = 0; i < cleans.size(); i++) {
      HoodieInstant clean = cleans.get(i);
      HoodieCleanMetadata cleanMetadata =
          AvroUtils.deserializeHoodieCleanMetadata(timeline.getInstantDetails(clean).get());
      rows.add(new Comparable[] {clean.getTimestamp(), cleanMetadata.getEarliestCommitToRetain(),
          cleanMetadata.getTotalFilesDeleted(), cleanMetadata.getTimeTakenInMillis()});
    }

    TableHeader header =
        new TableHeader().addTableHeaderField("CleanTime").addTableHeaderField("EarliestCommandRetained")
            .addTableHeaderField("Total Files Deleted").addTableHeaderField("Total Time Taken");
    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
  }

  @CliCommand(value = "cleans refresh", help = "Refresh the commits")
  public String refreshCleans() throws IOException {
    HoodieCLI.refreshTableMetadata();
    return "Metadata for table " + HoodieCLI.tableMetadata.getTableConfig().getTableName() + " refreshed.";
  }

  @CliCommand(value = "clean showpartitions", help = "Show partition level details of a clean")
  public String showCleanPartitions(@CliOption(key = {"clean"}, help = "clean to show") final String commitTime,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws Exception {

    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCleanerTimeline().filterCompletedInstants();
    HoodieInstant cleanInstant = new HoodieInstant(false, HoodieTimeline.CLEAN_ACTION, commitTime);

    if (!timeline.containsInstant(cleanInstant)) {
      return "Clean " + commitTime + " not found in metadata " + timeline;
    }

    HoodieCleanMetadata cleanMetadata =
        AvroUtils.deserializeHoodieCleanMetadata(timeline.getInstantDetails(cleanInstant).get());
    List<Comparable[]> rows = new ArrayList<>();
    for (Map.Entry<String, HoodieCleanPartitionMetadata> entry : cleanMetadata.getPartitionMetadata().entrySet()) {
      String path = entry.getKey();
      HoodieCleanPartitionMetadata stats = entry.getValue();
      String policy = stats.getPolicy();
      Integer totalSuccessDeletedFiles = stats.getSuccessDeleteFiles().size();
      Integer totalFailedDeletedFiles = stats.getFailedDeleteFiles().size();
      rows.add(new Comparable[] {path, policy, totalSuccessDeletedFiles, totalFailedDeletedFiles});
    }

    TableHeader header = new TableHeader().addTableHeaderField("Partition Path").addTableHeaderField("Cleaning policy")
        .addTableHeaderField("Total Files Successfully Deleted").addTableHeaderField("Total Failed Deletions");
    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);

  }
}
