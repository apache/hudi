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
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.utilities.UtilHelpers;

import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

/**
 * CLI command to show cleans options.
 */
@Component
public class CleansCommand implements CommandMarker {

  @CliCommand(value = "cleans show", help = "Show the cleans")
  public String showCleans(
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {

    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCleanerTimeline().filterCompletedInstants();
    List<HoodieInstant> cleans = timeline.getReverseOrderedInstants().collect(Collectors.toList());
    List<Comparable[]> rows = new ArrayList<>();
    for (HoodieInstant clean : cleans) {
      HoodieCleanMetadata cleanMetadata =
          TimelineMetadataUtils.deserializeHoodieCleanMetadata(timeline.getInstantDetails(clean).get());
      rows.add(new Comparable[] {clean.getTimestamp(), cleanMetadata.getEarliestCommitToRetain(),
          cleanMetadata.getTotalFilesDeleted(), cleanMetadata.getTimeTakenInMillis()});
    }

    TableHeader header =
        new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_CLEAN_TIME)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_EARLIEST_COMMAND_RETAINED)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_DELETED)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_TIME_TAKEN);
    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
  }

  @CliCommand(value = "clean showpartitions", help = "Show partition level details of a clean")
  public String showCleanPartitions(
      @CliOption(key = {"clean"}, help = "clean to show") final String instantTime,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws Exception {

    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCleanerTimeline().filterCompletedInstants();
    HoodieInstant cleanInstant = new HoodieInstant(false, HoodieTimeline.CLEAN_ACTION, instantTime);

    if (!timeline.containsInstant(cleanInstant)) {
      return "Clean " + instantTime + " not found in metadata " + timeline;
    }

    HoodieCleanMetadata cleanMetadata =
        TimelineMetadataUtils.deserializeHoodieCleanMetadata(timeline.getInstantDetails(cleanInstant).get());
    List<Comparable[]> rows = new ArrayList<>();
    for (Map.Entry<String, HoodieCleanPartitionMetadata> entry : cleanMetadata.getPartitionMetadata().entrySet()) {
      String path = entry.getKey();
      HoodieCleanPartitionMetadata stats = entry.getValue();
      String policy = stats.getPolicy();
      int totalSuccessDeletedFiles = stats.getSuccessDeleteFiles().size();
      int totalFailedDeletedFiles = stats.getFailedDeleteFiles().size();
      rows.add(new Comparable[] {path, policy, totalSuccessDeletedFiles, totalFailedDeletedFiles});
    }

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION_PATH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_CLEANING_POLICY)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_SUCCESSFULLY_DELETED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FAILED_DELETIONS);
    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);

  }

  @CliCommand(value = "cleans run", help = "run clean")
  public String runClean(
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory,
      @CliOption(key = "propsFilePath", help = "path to properties file on localfs or dfs with configurations for hoodie client for cleaning",
          unspecifiedDefaultValue = "") final String propsFilePath,
      @CliOption(key = "hoodieConfigs", help = "Any configuration that can be set in the properties file can be passed here in the form of an array",
          unspecifiedDefaultValue = "") final String[] configs,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "", help = "Spark Master ") String master) throws IOException, InterruptedException, URISyntaxException {
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();

    String sparkPropertiesPath =
        Utils.getDefaultPropertiesFile(JavaConverters.mapAsScalaMapConverter(System.getenv()).asScala());
    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);

    String cmd = SparkMain.SparkCommand.CLEAN.toString();
    sparkLauncher.addAppArgs(cmd, master, sparkMemory, metaClient.getBasePath(), propsFilePath);
    UtilHelpers.validateAndAddProperties(configs, sparkLauncher);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      return "Failed to clean hoodie dataset";
    }
    return "Cleaned hoodie dataset";
  }
}
