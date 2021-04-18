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

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.commands.SparkMain.SparkCommand;
import org.apache.hudi.cli.utils.CommitUtil;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.client.CompactionAdminClient.RenameOpResult;
import org.apache.hudi.client.CompactionAdminClient.ValidationOpResult;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.action.compact.OperationResult;
import org.apache.hudi.utilities.UtilHelpers;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.springframework.shell.core.CommandMarker;
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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * CLI command to display compaction related options.
 */
@Component
public class CompactionCommand implements CommandMarker {

  private static final Logger LOG = LogManager.getLogger(CompactionCommand.class);

  private static final String TMP_DIR = "/tmp/";

  private HoodieTableMetaClient checkAndGetMetaClient() {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    if (client.getTableType() != HoodieTableType.MERGE_ON_READ) {
      throw new HoodieException("Compactions can only be run for table type : MERGE_ON_READ");
    }
    return client;
  }

  @CliCommand(value = "compactions show all", help = "Shows all compactions that are in active timeline")
  public String compactionsAll(
      @CliOption(key = {"includeExtraMetadata"}, help = "Include extra metadata",
          unspecifiedDefaultValue = "false") final boolean includeExtraMetadata,
      @CliOption(key = {"limit"}, help = "Limit commits",
          unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly) {
    HoodieTableMetaClient client = checkAndGetMetaClient();
    HoodieActiveTimeline activeTimeline = client.getActiveTimeline();
    return printAllCompactions(activeTimeline,
            compactionPlanReader(this::readCompactionPlanForActiveTimeline, activeTimeline),
            includeExtraMetadata, sortByField, descending, limit, headerOnly);
  }

  @CliCommand(value = "compaction show", help = "Shows compaction details for a specific compaction instant")
  public String compactionShow(
      @CliOption(key = "instant", mandatory = true,
          help = "Base path for the target hoodie table") final String compactionInstantTime,
      @CliOption(key = {"limit"}, help = "Limit commits",
          unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws Exception {
    HoodieTableMetaClient client = checkAndGetMetaClient();
    HoodieActiveTimeline activeTimeline = client.getActiveTimeline();
    HoodieCompactionPlan compactionPlan = TimelineMetadataUtils.deserializeCompactionPlan(
        activeTimeline.readCompactionPlanAsBytes(
            HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime)).get());

    return printCompaction(compactionPlan, sortByField, descending, limit, headerOnly);
  }

  @CliCommand(value = "compactions showarchived", help = "Shows compaction details for specified time window")
  public String compactionsShowArchived(
          @CliOption(key = {"includeExtraMetadata"}, help = "Include extra metadata",
                  unspecifiedDefaultValue = "false") final boolean includeExtraMetadata,
          @CliOption(key = {"startTs"},  mandatory = false, help = "start time for compactions, default: now - 10 days")
                  String startTs,
          @CliOption(key = {"endTs"},  mandatory = false, help = "end time for compactions, default: now - 1 day")
                  String endTs,
          @CliOption(key = {"limit"}, help = "Limit compactions",
                  unspecifiedDefaultValue = "-1") final Integer limit,
          @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
          @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
          @CliOption(key = {"headeronly"}, help = "Print Header Only",
                  unspecifiedDefaultValue = "false") final boolean headerOnly) {
    if (StringUtils.isNullOrEmpty(startTs)) {
      startTs = CommitUtil.getTimeDaysAgo(10);
    }
    if (StringUtils.isNullOrEmpty(endTs)) {
      endTs = CommitUtil.getTimeDaysAgo(1);
    }

    HoodieTableMetaClient client = checkAndGetMetaClient();
    HoodieArchivedTimeline archivedTimeline = client.getArchivedTimeline();
    archivedTimeline.loadCompactionDetailsInMemory(startTs, endTs);
    try {
      return printAllCompactions(archivedTimeline,
              compactionPlanReader(this::readCompactionPlanForArchivedTimeline, archivedTimeline),
              includeExtraMetadata, sortByField, descending, limit, headerOnly);
    } finally {
      archivedTimeline.clearInstantDetailsFromMemory(startTs, endTs);
    }
  }

  @CliCommand(value = "compaction showarchived", help = "Shows compaction details for a specific compaction instant")
  public String compactionShowArchived(
          @CliOption(key = "instant", mandatory = true,
                  help = "instant time") final String compactionInstantTime,
          @CliOption(key = {"limit"}, help = "Limit commits",
                  unspecifiedDefaultValue = "-1") final Integer limit,
          @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
          @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
          @CliOption(key = {"headeronly"}, help = "Print Header Only",
                  unspecifiedDefaultValue = "false") final boolean headerOnly)
          throws Exception {
    HoodieTableMetaClient client = checkAndGetMetaClient();
    HoodieArchivedTimeline archivedTimeline = client.getArchivedTimeline();
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED,
            HoodieTimeline.COMPACTION_ACTION, compactionInstantTime);
    try {
      archivedTimeline.loadCompactionDetailsInMemory(compactionInstantTime);
      HoodieCompactionPlan compactionPlan = TimelineMetadataUtils.deserializeAvroRecordMetadata(
              archivedTimeline.getInstantDetails(instant).get(), HoodieCompactionPlan.getClassSchema());
      return printCompaction(compactionPlan, sortByField, descending, limit, headerOnly);
    } finally {
      archivedTimeline.clearInstantDetailsFromMemory(compactionInstantTime);
    }
  }

  @CliCommand(value = "compaction schedule", help = "Schedule Compaction")
  public String scheduleCompact(@CliOption(key = "sparkMemory", unspecifiedDefaultValue = "1G",
      help = "Spark executor memory") final String sparkMemory,
      @CliOption(key = "propsFilePath", help = "path to properties file on localfs or dfs with configurations for hoodie client for compacting",
          unspecifiedDefaultValue = "") final String propsFilePath,
      @CliOption(key = "hoodieConfigs", help = "Any configuration that can be set in the properties file can be passed here in the form of an array",
          unspecifiedDefaultValue = "") final String[] configs,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "local", help = "Spark Master") String master)
      throws Exception {
    HoodieTableMetaClient client = checkAndGetMetaClient();
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    // First get a compaction instant time and pass it to spark launcher for scheduling compaction
    String compactionInstantTime = HoodieActiveTimeline.createNewInstantTime();

    String sparkPropertiesPath =
        Utils.getDefaultPropertiesFile(scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    String cmd = SparkCommand.COMPACT_SCHEDULE.toString();
    sparkLauncher.addAppArgs(cmd, master, sparkMemory, client.getBasePath(),
        client.getTableConfig().getTableName(), compactionInstantTime, propsFilePath);
    UtilHelpers.validateAndAddProperties(configs, sparkLauncher);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      return "Failed to run compaction for " + compactionInstantTime;
    }
    return "Attempted to schedule compaction for " + compactionInstantTime;
  }

  @CliCommand(value = "compaction run", help = "Run Compaction for given instant time")
  public String compact(
      @CliOption(key = {"parallelism"}, mandatory = true,
          help = "Parallelism for hoodie compaction") final String parallelism,
      @CliOption(key = "schemaFilePath", mandatory = true,
          help = "Path for Avro schema file") final String schemaFilePath,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "local",
          help = "Spark Master") String master,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory,
      @CliOption(key = "retry", unspecifiedDefaultValue = "1", help = "Number of retries") final String retry,
      @CliOption(key = "compactionInstant", help = "Base path for the target hoodie table") String compactionInstantTime,
      @CliOption(key = "propsFilePath", help = "path to properties file on localfs or dfs with configurations for hoodie client for compacting",
        unspecifiedDefaultValue = "") final String propsFilePath,
      @CliOption(key = "hoodieConfigs", help = "Any configuration that can be set in the properties file can be passed here in the form of an array",
        unspecifiedDefaultValue = "") final String[] configs)
      throws Exception {
    HoodieTableMetaClient client = checkAndGetMetaClient();
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    if (null == compactionInstantTime) {
      // pick outstanding one with lowest timestamp
      Option<String> firstPendingInstant =
          client.reloadActiveTimeline().filterCompletedAndCompactionInstants()
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
    sparkLauncher.addAppArgs(SparkCommand.COMPACT_RUN.toString(), master, sparkMemory, client.getBasePath(),
        client.getTableConfig().getTableName(), compactionInstantTime, parallelism, schemaFilePath,
        retry, propsFilePath);
    UtilHelpers.validateAndAddProperties(configs, sparkLauncher);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      return "Failed to run compaction for " + compactionInstantTime;
    }
    return "Compaction successfully completed for " + compactionInstantTime;
  }

  /**
   * Prints all compaction details.
   */
  private String printAllCompactions(HoodieDefaultTimeline timeline,
                                     Function<HoodieInstant, HoodieCompactionPlan> compactionPlanReader,
                                     boolean includeExtraMetadata,
                                     String sortByField,
                                     boolean descending,
                                     int limit,
                                     boolean headerOnly) {

    Stream<HoodieInstant> instantsStream = timeline.getWriteTimeline().getReverseOrderedInstants();
    List<Pair<HoodieInstant, HoodieCompactionPlan>> compactionPlans = instantsStream
            .map(instant -> Pair.of(instant, compactionPlanReader.apply(instant)))
            .filter(pair -> pair.getRight() != null)
            .collect(Collectors.toList());

    Set<String> committedInstants = timeline.getCommitTimeline().filterCompletedInstants()
            .getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toSet());

    List<Comparable[]> rows = new ArrayList<>();
    for (Pair<HoodieInstant, HoodieCompactionPlan> compactionPlan : compactionPlans) {
      HoodieCompactionPlan plan = compactionPlan.getRight();
      HoodieInstant instant = compactionPlan.getLeft();
      final HoodieInstant.State state;
      if (committedInstants.contains(instant.getTimestamp())) {
        state = HoodieInstant.State.COMPLETED;
      } else {
        state = instant.getState();
      }

      if (includeExtraMetadata) {
        rows.add(new Comparable[] {instant.getTimestamp(), state.toString(),
                plan.getOperations() == null ? 0 : plan.getOperations().size(),
                plan.getExtraMetadata().toString()});
      } else {
        rows.add(new Comparable[] {instant.getTimestamp(), state.toString(),
                plan.getOperations() == null ? 0 : plan.getOperations().size()});
      }
    }

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    TableHeader header = new TableHeader()
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_COMPACTION_INSTANT_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_STATE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_TO_BE_COMPACTED);
    if (includeExtraMetadata) {
      header = header.addTableHeaderField(HoodieTableHeaderFields.HEADER_EXTRA_METADATA);
    }
    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }

  /**
   * Compaction reading is different for different timelines. Create partial function to override special logic.
   * We can make these read methods part of HoodieDefaultTimeline and override where necessary. But the
   * BiFunction below has 'hacky' exception blocks, so restricting it to CLI.
   */
  private <T extends HoodieDefaultTimeline, U extends HoodieInstant, V extends HoodieCompactionPlan>
      Function<HoodieInstant, HoodieCompactionPlan> compactionPlanReader(
          BiFunction<T, HoodieInstant, HoodieCompactionPlan> f, T timeline) {

    return (y) -> f.apply(timeline, y);
  }

  private HoodieCompactionPlan readCompactionPlanForArchivedTimeline(HoodieArchivedTimeline archivedTimeline,
                                                                     HoodieInstant instant) {
    // filter inflight compaction
    if (HoodieTimeline.COMPACTION_ACTION.equals(instant.getAction())
        && HoodieInstant.State.INFLIGHT.equals(instant.getState())) {
      try {
        return TimelineMetadataUtils.deserializeAvroRecordMetadata(archivedTimeline.getInstantDetails(instant).get(),
            HoodieCompactionPlan.getClassSchema());
      } catch (Exception e) {
        throw new HoodieException(e.getMessage(), e);
      }
    } else {
      return null;
    }
  }

  /**
   * TBD Can we make this part of HoodieActiveTimeline or a utility class.
   */
  private HoodieCompactionPlan readCompactionPlanForActiveTimeline(HoodieActiveTimeline activeTimeline,
                                                                   HoodieInstant instant) {
    try {
      if (!HoodieTimeline.COMPACTION_ACTION.equals(instant.getAction())) {
        try {
          // This could be a completed compaction. Assume a compaction request file is present but skip if fails
          return TimelineMetadataUtils.deserializeCompactionPlan(
                  activeTimeline.readCompactionPlanAsBytes(
                          HoodieTimeline.getCompactionRequestedInstant(instant.getTimestamp())).get());
        } catch (HoodieIOException ioe) {
          // SKIP
          return null;
        }
      } else {
        return TimelineMetadataUtils.deserializeCompactionPlan(activeTimeline.readCompactionPlanAsBytes(
                HoodieTimeline.getCompactionRequestedInstant(instant.getTimestamp())).get());
      }
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  protected String printCompaction(HoodieCompactionPlan compactionPlan,
                                 String sortByField,
                                 boolean descending,
                                 int limit,
                                 boolean headerOnly) {
    List<Comparable[]> rows = new ArrayList<>();
    if ((null != compactionPlan) && (null != compactionPlan.getOperations())) {
      for (HoodieCompactionOperation op : compactionPlan.getOperations()) {
        rows.add(new Comparable[]{op.getPartitionPath(), op.getFileId(), op.getBaseInstantTime(), op.getDataFilePath(),
                op.getDeltaFilePaths().size(), op.getMetrics() == null ? "" : op.getMetrics().toString()});
      }
    }

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    TableHeader header = new TableHeader()
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION_PATH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_ID)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_BASE_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DATA_FILE_PATH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_DELTA_FILES)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_METRICS);
    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
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
      LOG.info("Result : " + result);
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
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "local", help = "Spark Master") String master,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "2G", help = "executor memory") String sparkMemory,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") boolean headerOnly)
      throws Exception {
    HoodieTableMetaClient client = checkAndGetMetaClient();
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    String outputPathStr = getTmpSerializerFile();
    Path outputPath = new Path(outputPathStr);
    String output;
    try {
      String sparkPropertiesPath = Utils
          .getDefaultPropertiesFile(scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
      SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
      sparkLauncher.addAppArgs(SparkCommand.COMPACT_VALIDATE.toString(), master, sparkMemory, client.getBasePath(),
          compactionInstant, outputPathStr, parallelism);
      Process process = sparkLauncher.launch();
      InputStreamConsumer.captureOutput(process);
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        return "Failed to validate compaction for " + compactionInstant;
      }
      List<ValidationOpResult> res = deSerializeOperationResult(outputPathStr, HoodieCLI.fs);
      boolean valid = res.stream().map(OperationResult::isSuccess).reduce(Boolean::logicalAnd).orElse(true);
      String message = "\n\n\t COMPACTION PLAN " + (valid ? "VALID" : "INVALID") + "\n\n";
      List<Comparable[]> rows = new ArrayList<>();
      res.forEach(r -> {
        Comparable[] row = new Comparable[] {r.getOperation().getFileId(), r.getOperation().getBaseInstantTime(),
            r.getOperation().getDataFileName().isPresent() ? r.getOperation().getDataFileName().get() : "",
            r.getOperation().getDeltaFileNames().size(), r.isSuccess(),
            r.getException().isPresent() ? r.getException().get().getMessage() : ""};
        rows.add(row);
      });

      Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
      TableHeader header = new TableHeader()
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_ID)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_BASE_INSTANT_TIME)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_BASE_DATA_FILE)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_NUM_DELTA_FILES)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_VALID)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_ERROR);

      output = message + HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit,
          headerOnly, rows);
    } finally {
      // Delete tmp file used to serialize result
      if (HoodieCLI.fs.exists(outputPath)) {
        HoodieCLI.fs.delete(outputPath, false);
      }
    }
    return output;
  }

  @CliCommand(value = "compaction unschedule", help = "Unschedule Compaction")
  public String unscheduleCompaction(
      @CliOption(key = "instant", mandatory = true, help = "Compaction Instant") String compactionInstant,
      @CliOption(key = {"parallelism"}, unspecifiedDefaultValue = "3", help = "Parallelism") String parallelism,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "local", help = "Spark Master") String master,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "2G", help = "executor memory") String sparkMemory,
      @CliOption(key = {"skipValidation"}, help = "skip validation", unspecifiedDefaultValue = "false") boolean skipV,
      @CliOption(key = {"dryRun"}, help = "Dry Run Mode", unspecifiedDefaultValue = "false") boolean dryRun,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") boolean headerOnly)
      throws Exception {
    HoodieTableMetaClient client = checkAndGetMetaClient();
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    String outputPathStr = getTmpSerializerFile();
    Path outputPath = new Path(outputPathStr);
    String output;
    try {
      String sparkPropertiesPath = Utils
          .getDefaultPropertiesFile(scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
      SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
      sparkLauncher.addAppArgs(SparkCommand.COMPACT_UNSCHEDULE_PLAN.toString(), master, sparkMemory, client.getBasePath(),
          compactionInstant, outputPathStr, parallelism, Boolean.valueOf(skipV).toString(),
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
  }

  @CliCommand(value = "compaction unscheduleFileId", help = "UnSchedule Compaction for a fileId")
  public String unscheduleCompactFile(
      @CliOption(key = "fileId", mandatory = true, help = "File Id") final String fileId,
      @CliOption(key = "partitionPath", mandatory = true, help = "partition path") final String partitionPath,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "local", help = "Spark Master") String master,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "2G", help = "executor memory") String sparkMemory,
      @CliOption(key = {"skipValidation"}, help = "skip validation", unspecifiedDefaultValue = "false") boolean skipV,
      @CliOption(key = {"dryRun"}, help = "Dry Run Mode", unspecifiedDefaultValue = "false") boolean dryRun,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") boolean descending,
      @CliOption(key = {"headeronly"}, help = "Header Only", unspecifiedDefaultValue = "false") boolean headerOnly)
      throws Exception {
    HoodieTableMetaClient client = checkAndGetMetaClient();
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    String outputPathStr = getTmpSerializerFile();
    Path outputPath = new Path(outputPathStr);
    String output;
    try {
      String sparkPropertiesPath = Utils
          .getDefaultPropertiesFile(scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
      SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
      sparkLauncher.addAppArgs(SparkCommand.COMPACT_UNSCHEDULE_FILE.toString(), master, sparkMemory, client.getBasePath(),
          fileId, partitionPath, outputPathStr, "1", Boolean.valueOf(skipV).toString(),
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
  }

  @CliCommand(value = "compaction repair", help = "Renames the files to make them consistent with the timeline as "
      + "dictated by Hoodie metadata. Use when compaction unschedule fails partially.")
  public String repairCompaction(
      @CliOption(key = "instant", mandatory = true, help = "Compaction Instant") String compactionInstant,
      @CliOption(key = {"parallelism"}, unspecifiedDefaultValue = "3", help = "Parallelism") String parallelism,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "local", help = "Spark Master") String master,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "2G", help = "executor memory") String sparkMemory,
      @CliOption(key = {"dryRun"}, help = "Dry Run Mode", unspecifiedDefaultValue = "false") boolean dryRun,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") boolean headerOnly)
      throws Exception {
    HoodieTableMetaClient client = checkAndGetMetaClient();
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    String outputPathStr = getTmpSerializerFile();
    Path outputPath = new Path(outputPathStr);
    String output;
    try {
      String sparkPropertiesPath = Utils
          .getDefaultPropertiesFile(scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
      SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
      sparkLauncher.addAppArgs(SparkCommand.COMPACT_REPAIR.toString(), master, sparkMemory, client.getBasePath(),
          compactionInstant, outputPathStr, parallelism, Boolean.valueOf(dryRun).toString());
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
        System.out.println("Some renames failed. table could be in inconsistent-state. Try running compaction repair");
      }

      List<Comparable[]> rows = new ArrayList<>();
      res.forEach(r -> {
        Comparable[] row =
            new Comparable[] {r.getOperation().fileId, r.getOperation().srcPath, r.getOperation().destPath,
                r.isExecuted(), r.isSuccess(), r.getException().isPresent() ? r.getException().get().getMessage() : ""};
        rows.add(row);
      });

      Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
      TableHeader header = new TableHeader()
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_ID)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_SOURCE_FILE_PATH)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_DESTINATION_FILE_PATH)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_RENAME_EXECUTED)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_RENAME_SUCCEEDED)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_ERROR);

      return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
    } else {
      return "No File renames needed to " + operation + ". Operation successful.";
    }
  }
}
