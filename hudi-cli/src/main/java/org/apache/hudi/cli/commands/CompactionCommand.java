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

import org.apache.hudi.CompactionAdminClient.RenameOpResult;
import org.apache.hudi.CompactionAdminClient.ValidationOpResult;
import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.commands.SparkMain.SparkCommand;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * CLI command to display compaction related options.
 */
@Component
public class CompactionCommand implements CommandMarker {

  private static Logger log = LogManager.getLogger(CompactionCommand.class);

  private static final String TMP_DIR = "/tmp/";

  @CliAvailabilityIndicator({"compactions show all", "compaction show", "compaction run", "compaction schedule"})
  public boolean isAvailable() {
    return (HoodieCLI.tableMetadata != null)
        && (HoodieCLI.tableMetadata.getTableType() == HoodieTableType.MERGE_ON_READ);
  }

  @CliCommand(value = "compactions show all", help = "Shows all compactions that are in active timeline")
  public String compactionsAll(
      @CliOption(key = {"includeExtraMetadata"}, help = "Include extra metadata",
          unspecifiedDefaultValue = "false") final boolean includeExtraMetadata,
      @CliOption(key = {"limit"}, mandatory = false, help = "Limit commits",
          unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {
    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsAndCompactionTimeline();
    HoodieTimeline commitTimeline = activeTimeline.getCommitTimeline().filterCompletedInstants();
    Set<String> committed = commitTimeline.getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toSet());

    List<HoodieInstant> instants = timeline.getReverseOrderedInstants().collect(Collectors.toList());
    List<Comparable[]> rows = new ArrayList<>();
    for (int i = 0; i < instants.size(); i++) {
      HoodieInstant instant = instants.get(i);
      HoodieCompactionPlan workload = null;
      if (!instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION)) {
        try {
          // This could be a completed compaction. Assume a compaction request file is present but skip if fails
          workload = AvroUtils.deserializeCompactionPlan(activeTimeline
              .getInstantAuxiliaryDetails(HoodieTimeline.getCompactionRequestedInstant(instant.getTimestamp())).get());
        } catch (HoodieIOException ioe) {
          // SKIP
        }
      } else {
        workload = AvroUtils.deserializeCompactionPlan(activeTimeline
            .getInstantAuxiliaryDetails(HoodieTimeline.getCompactionRequestedInstant(instant.getTimestamp())).get());
      }

      if (null != workload) {
        HoodieInstant.State state = instant.getState();
        if (committed.contains(instant.getTimestamp())) {
          state = State.COMPLETED;
        }
        if (includeExtraMetadata) {
          rows.add(new Comparable[] {instant.getTimestamp(), state.toString(),
              workload.getOperations() == null ? 0 : workload.getOperations().size(),
              workload.getExtraMetadata().toString()});
        } else {
          rows.add(new Comparable[] {instant.getTimestamp(), state.toString(),
              workload.getOperations() == null ? 0 : workload.getOperations().size()});
        }
      }
    }

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    TableHeader header = new TableHeader().addTableHeaderField("Compaction Instant Time").addTableHeaderField("State")
        .addTableHeaderField("Total FileIds to be Compacted");
    if (includeExtraMetadata) {
      header = header.addTableHeaderField("Extra Metadata");
    }
    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }

  @CliCommand(value = "compaction show", help = "Shows compaction details for a specific compaction instant")
  public String compactionShow(
      @CliOption(key = "instant", mandatory = true,
          help = "Base path for the target hoodie dataset") final String compactionInstantTime,
      @CliOption(key = {"limit"}, mandatory = false, help = "Limit commits",
          unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws Exception {
    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieCompactionPlan workload = AvroUtils.deserializeCompactionPlan(activeTimeline
        .getInstantAuxiliaryDetails(HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime)).get());

    List<Comparable[]> rows = new ArrayList<>();
    if ((null != workload) && (null != workload.getOperations())) {
      for (HoodieCompactionOperation op : workload.getOperations()) {
        rows.add(new Comparable[] {op.getPartitionPath(), op.getFileId(), op.getBaseInstantTime(), op.getDataFilePath(),
            op.getDeltaFilePaths().size(), op.getMetrics() == null ? "" : op.getMetrics().toString()});
      }
    }

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    TableHeader header = new TableHeader().addTableHeaderField("Partition Path").addTableHeaderField("File Id")
        .addTableHeaderField("Base Instant").addTableHeaderField("Data File Path")
        .addTableHeaderField("Total Delta Files").addTableHeaderField("getMetrics");
    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }

  @CliCommand(value = "compaction schedule", help = "Schedule Compaction")
  public String scheduleCompact(@CliOption(key = "sparkMemory", unspecifiedDefaultValue = "1G",
      help = "Spark executor memory") final String sparkMemory) throws Exception {
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    // First get a compaction instant time and pass it to spark launcher for scheduling compaction
    String compactionInstantTime = HoodieActiveTimeline.createNewCommitTime();

    if (HoodieCLI.tableMetadata.getTableType() == HoodieTableType.MERGE_ON_READ) {
      String sparkPropertiesPath =
          Utils.getDefaultPropertiesFile(scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
      SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
      sparkLauncher.addAppArgs(SparkCommand.COMPACT_SCHEDULE.toString(), HoodieCLI.tableMetadata.getBasePath(),
          HoodieCLI.tableMetadata.getTableConfig().getTableName(), compactionInstantTime, sparkMemory);
      Process process = sparkLauncher.launch();
      InputStreamConsumer.captureOutput(process);
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        return "Failed to run compaction for " + compactionInstantTime;
      }
      return "Compaction successfully completed for " + compactionInstantTime;
    } else {
      throw new Exception("Compactions can only be run for table type : MERGE_ON_READ");
    }
  }

  @CliCommand(value = "compaction run", help = "Run Compaction for given instant time")
  public String compact(
      @CliOption(key = {"parallelism"}, mandatory = true,
          help = "Parallelism for hoodie compaction") final String parallelism,
      @CliOption(key = "schemaFilePath", mandatory = true,
          help = "Path for Avro schema file") final String schemaFilePath,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory,
      @CliOption(key = "retry", unspecifiedDefaultValue = "1", help = "Number of retries") final String retry,
      @CliOption(key = "compactionInstant", mandatory = false,
          help = "Base path for the target hoodie dataset") String compactionInstantTime)
      throws Exception {
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    if (HoodieCLI.tableMetadata.getTableType() == HoodieTableType.MERGE_ON_READ) {
      if (null == compactionInstantTime) {
        // pick outstanding one with lowest timestamp
        Option<String> firstPendingInstant =
            HoodieCLI.tableMetadata.reloadActiveTimeline().filterCompletedAndCompactionInstants()
                .filter(instant -> instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION)).firstInstant()
                .map(HoodieInstant::getTimestamp);
        if (!firstPendingInstant.isPresent()) {
          return "NO PENDING COMPACTION TO RUN";
        }
        compactionInstantTime = firstPendingInstant.get();
      }

      String sparkPropertiesPath =
          Utils.getDefaultPropertiesFile(scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
      SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
      sparkLauncher.addAppArgs(SparkCommand.COMPACT_RUN.toString(), HoodieCLI.tableMetadata.getBasePath(),
          HoodieCLI.tableMetadata.getTableConfig().getTableName(), compactionInstantTime, parallelism, schemaFilePath,
          sparkMemory, retry);
      Process process = sparkLauncher.launch();
      InputStreamConsumer.captureOutput(process);
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        return "Failed to run compaction for " + compactionInstantTime;
      }
      return "Compaction successfully completed for " + compactionInstantTime;
    } else {
      throw new Exception("Compactions can only be run for table type : MERGE_ON_READ");
    }
  }

  private static String getTmpSerializerFile() {
    return TMP_DIR + UUID.randomUUID().toString() + ".ser";
  }

  private <T> T deSerializeOperationResult(String inputP, FileSystem fs) throws Exception {
    Path inputPath = new Path(inputP);
    FSDataInputStream fsDataInputStream = fs.open(inputPath);
    ObjectInputStream in = new ObjectInputStream(fsDataInputStream);
    try {
      T result = (T) in.readObject();
      log.info("Result : " + result);
      return result;
    } finally {
      in.close();
      fsDataInputStream.close();
    }
  }

  @CliCommand(value = "compaction validate", help = "Validate Compaction")
  public String validateCompaction(
      @CliOption(key = "instant", mandatory = true, help = "Compaction Instant") String compactionInstant,
      @CliOption(key = {"parallelism"}, unspecifiedDefaultValue = "3", help = "Parallelism") String parallelism,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "", help = "Spark Master ") String master,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "2G", help = "executor memory") String sparkMemory,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") boolean headerOnly)
      throws Exception {
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    String outputPathStr = getTmpSerializerFile();
    Path outputPath = new Path(outputPathStr);
    String output = null;
    if (HoodieCLI.tableMetadata.getTableType() == HoodieTableType.MERGE_ON_READ) {
      try {
        String sparkPropertiesPath = Utils
            .getDefaultPropertiesFile(scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
        SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
        sparkLauncher.addAppArgs(SparkCommand.COMPACT_VALIDATE.toString(), HoodieCLI.tableMetadata.getBasePath(),
            compactionInstant, outputPathStr, parallelism, master, sparkMemory);
        Process process = sparkLauncher.launch();
        InputStreamConsumer.captureOutput(process);
        int exitCode = process.waitFor();
        if (exitCode != 0) {
          return "Failed to validate compaction for " + compactionInstant;
        }
        List<ValidationOpResult> res = deSerializeOperationResult(outputPathStr, HoodieCLI.fs);
        boolean valid = res.stream().map(r -> r.isSuccess()).reduce(Boolean::logicalAnd).orElse(true);
        String message = "\n\n\t COMPACTION PLAN " + (valid ? "VALID" : "INVALID") + "\n\n";
        List<Comparable[]> rows = new ArrayList<>();
        res.stream().forEach(r -> {
          Comparable[] row = new Comparable[] {r.getOperation().getFileId(), r.getOperation().getBaseInstantTime(),
              r.getOperation().getDataFileName().isPresent() ? r.getOperation().getDataFileName().get() : "",
              r.getOperation().getDeltaFileNames().size(), r.isSuccess(),
              r.getException().isPresent() ? r.getException().get().getMessage() : ""};
          rows.add(row);
        });

        Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
        TableHeader header = new TableHeader().addTableHeaderField("File Id").addTableHeaderField("Base Instant Time")
            .addTableHeaderField("Base Data File").addTableHeaderField("Num Delta Files").addTableHeaderField("Valid")
            .addTableHeaderField("Error");

        output = message + HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit,
            headerOnly, rows);
      } finally {
        // Delete tmp file used to serialize result
        if (HoodieCLI.fs.exists(outputPath)) {
          HoodieCLI.fs.delete(outputPath, false);
        }
      }
      return output;
    } else {
      throw new Exception("Compactions can only be run for table type : MERGE_ON_READ");
    }
  }

  @CliCommand(value = "compaction unschedule", help = "Unschedule Compaction")
  public String unscheduleCompaction(
      @CliOption(key = "instant", mandatory = true, help = "Compaction Instant") String compactionInstant,
      @CliOption(key = {"parallelism"}, unspecifiedDefaultValue = "3", help = "Parallelism") String parallelism,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "", help = "Spark Master ") String master,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "2G", help = "executor memory") String sparkMemory,
      @CliOption(key = {"skipValidation"}, help = "skip validation", unspecifiedDefaultValue = "false") boolean skipV,
      @CliOption(key = {"dryRun"}, help = "Dry Run Mode", unspecifiedDefaultValue = "false") boolean dryRun,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") boolean headerOnly)
      throws Exception {
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    String outputPathStr = getTmpSerializerFile();
    Path outputPath = new Path(outputPathStr);
    String output = "";
    if (HoodieCLI.tableMetadata.getTableType() == HoodieTableType.MERGE_ON_READ) {
      try {
        String sparkPropertiesPath = Utils
            .getDefaultPropertiesFile(scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
        SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
        sparkLauncher.addAppArgs(SparkCommand.COMPACT_UNSCHEDULE_PLAN.toString(), HoodieCLI.tableMetadata.getBasePath(),
            compactionInstant, outputPathStr, parallelism, master, sparkMemory, Boolean.valueOf(skipV).toString(),
            Boolean.valueOf(dryRun).toString());
        Process process = sparkLauncher.launch();
        InputStreamConsumer.captureOutput(process);
        int exitCode = process.waitFor();
        if (exitCode != 0) {
          return "Failed to unschedule compaction for " + compactionInstant;
        }
        List<RenameOpResult> res = deSerializeOperationResult(outputPathStr, HoodieCLI.fs);
        output =
            getRenamesToBePrinted(res, limit, sortByField, descending, headerOnly, "unschedule pending compaction");
      } finally {
        // Delete tmp file used to serialize result
        if (HoodieCLI.fs.exists(outputPath)) {
          HoodieCLI.fs.delete(outputPath, false);
        }
      }
      return output;
    } else {
      throw new Exception("Compactions can only be run for table type : MERGE_ON_READ");
    }
  }

  @CliCommand(value = "compaction unscheduleFileId", help = "UnSchedule Compaction for a fileId")
  public String unscheduleCompactFile(
      @CliOption(key = "fileId", mandatory = true, help = "File Id") final String fileId,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "", help = "Spark Master ") String master,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "2G", help = "executor memory") String sparkMemory,
      @CliOption(key = {"skipValidation"}, help = "skip validation", unspecifiedDefaultValue = "false") boolean skipV,
      @CliOption(key = {"dryRun"}, help = "Dry Run Mode", unspecifiedDefaultValue = "false") boolean dryRun,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") boolean descending,
      @CliOption(key = {"headeronly"}, help = "Header Only", unspecifiedDefaultValue = "false") boolean headerOnly)
      throws Exception {
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    String outputPathStr = getTmpSerializerFile();
    Path outputPath = new Path(outputPathStr);
    String output = "";
    if (HoodieCLI.tableMetadata.getTableType() == HoodieTableType.MERGE_ON_READ) {
      try {
        String sparkPropertiesPath = Utils
            .getDefaultPropertiesFile(scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
        SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
        sparkLauncher.addAppArgs(SparkCommand.COMPACT_UNSCHEDULE_FILE.toString(), HoodieCLI.tableMetadata.getBasePath(),
            fileId, outputPathStr, "1", master, sparkMemory, Boolean.valueOf(skipV).toString(),
            Boolean.valueOf(dryRun).toString());
        Process process = sparkLauncher.launch();
        InputStreamConsumer.captureOutput(process);
        int exitCode = process.waitFor();
        if (exitCode != 0) {
          return "Failed to unschedule compaction for file " + fileId;
        }
        List<RenameOpResult> res = deSerializeOperationResult(outputPathStr, HoodieCLI.fs);
        output = getRenamesToBePrinted(res, limit, sortByField, descending, headerOnly,
            "unschedule file from pending compaction");
      } finally {
        // Delete tmp file used to serialize result
        if (HoodieCLI.fs.exists(outputPath)) {
          HoodieCLI.fs.delete(outputPath, false);
        }
      }
      return output;
    } else {
      throw new Exception("Compactions can only be run for table type : MERGE_ON_READ");
    }
  }

  @CliCommand(value = "compaction repair", help = "Renames the files to make them consistent with the timeline as "
      + "dictated by Hoodie metadata. Use when compaction unschedule fails partially.")
  public String repairCompaction(
      @CliOption(key = "instant", mandatory = true, help = "Compaction Instant") String compactionInstant,
      @CliOption(key = {"parallelism"}, unspecifiedDefaultValue = "3", help = "Parallelism") String parallelism,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "", help = "Spark Master ") String master,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "2G", help = "executor memory") String sparkMemory,
      @CliOption(key = {"dryRun"}, help = "Dry Run Mode", unspecifiedDefaultValue = "false") boolean dryRun,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") boolean headerOnly)
      throws Exception {
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);
    String outputPathStr = getTmpSerializerFile();
    Path outputPath = new Path(outputPathStr);
    String output = "";
    if (HoodieCLI.tableMetadata.getTableType() == HoodieTableType.MERGE_ON_READ) {
      try {
        String sparkPropertiesPath = Utils
            .getDefaultPropertiesFile(scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
        SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
        sparkLauncher.addAppArgs(SparkCommand.COMPACT_REPAIR.toString(), HoodieCLI.tableMetadata.getBasePath(),
            compactionInstant, outputPathStr, parallelism, master, sparkMemory, Boolean.valueOf(dryRun).toString());
        Process process = sparkLauncher.launch();
        InputStreamConsumer.captureOutput(process);
        int exitCode = process.waitFor();
        if (exitCode != 0) {
          return "Failed to unschedule compaction for " + compactionInstant;
        }
        List<RenameOpResult> res = deSerializeOperationResult(outputPathStr, HoodieCLI.fs);
        output = getRenamesToBePrinted(res, limit, sortByField, descending, headerOnly, "repair compaction");
      } finally {
        // Delete tmp file used to serialize result
        if (HoodieCLI.fs.exists(outputPath)) {
          HoodieCLI.fs.delete(outputPath, false);
        }
      }
      return output;
    } else {
      throw new Exception("Compactions can only be run for table type : MERGE_ON_READ");
    }
  }

  private String getRenamesToBePrinted(List<RenameOpResult> res, Integer limit, String sortByField, boolean descending,
      boolean headerOnly, String operation) {

    Option<Boolean> result =
        Option.fromJavaOptional(res.stream().map(r -> r.isExecuted() && r.isSuccess()).reduce(Boolean::logicalAnd));
    if (result.isPresent()) {
      System.out.println("There were some file renames that needed to be done to " + operation);

      if (result.get()) {
        System.out.println("All renames successfully completed to " + operation + " done !!");
      } else {
        System.out
            .println("Some renames failed. DataSet could be in inconsistent-state. " + "Try running compaction repair");
      }

      List<Comparable[]> rows = new ArrayList<>();
      res.stream().forEach(r -> {
        Comparable[] row =
            new Comparable[] {r.getOperation().fileId, r.getOperation().srcPath, r.getOperation().destPath,
                r.isExecuted(), r.isSuccess(), r.getException().isPresent() ? r.getException().get().getMessage() : ""};
        rows.add(row);
      });

      Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
      TableHeader header = new TableHeader().addTableHeaderField("File Id").addTableHeaderField("Source File Path")
          .addTableHeaderField("Destination File Path").addTableHeaderField("Rename Executed?")
          .addTableHeaderField("Rename Succeeded?").addTableHeaderField("Error");

      return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
    } else {
      return "No File renames needed to " + operation + ". Operation successful.";
    }
  }
}
