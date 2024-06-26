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
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.AvroRuntimeException;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.sql.hudi.DeDupeType;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

/**
 * CLI command to display and trigger repair options.
 */
@ShellComponent
public class RepairsCommand {

  private static final Logger LOG = LoggerFactory.getLogger(RepairsCommand.class);
  public static final String DEDUPLICATE_RETURN_PREFIX = "Deduplicated files placed in:  ";

  @ShellMethod(key = "repair deduplicate",
      value = "De-duplicate a partition path contains duplicates & produce repaired files to replace with")
  public String deduplicate(
      @ShellOption(value = {"--duplicatedPartitionPath"}, defaultValue = "", help = "Partition Path containing the duplicates")
      final String duplicatedPartitionPath,
      @ShellOption(value = {"--repairedOutputPath"}, help = "Location to place the repaired files")
      final String repairedOutputPath,
      @ShellOption(value = {"--sparkProperties"}, help = "Spark Properties File Path",
          defaultValue = "") String sparkPropertiesPath,
      @ShellOption(value = "--sparkMaster", defaultValue = "", help = "Spark Master") String master,
      @ShellOption(value = "--sparkMemory", defaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory,
      @ShellOption(value = {"--dryrun"},
          help = "Should we actually remove duplicates or just run and store result to repairedOutputPath",
          defaultValue = "true") final boolean dryRun,
      @ShellOption(value = {"--dedupeType"}, help = "Valid values are - insert_type, update_type and upsert_type",
          defaultValue = "insert_type") final String dedupeType)
      throws Exception {
    if (!DeDupeType.values().contains(DeDupeType.withName(dedupeType))) {
      throw new IllegalArgumentException("Please provide valid dedupe type!");
    }
    if (StringUtils.isNullOrEmpty(sparkPropertiesPath)) {
      sparkPropertiesPath =
          Utils.getDefaultPropertiesFile(JavaConverters.mapAsScalaMapConverter(System.getenv()).asScala());
    }

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkMain.SparkCommand.DEDUPLICATE.toString(), master, sparkMemory,
        duplicatedPartitionPath, repairedOutputPath, HoodieCLI.basePath, String.valueOf(dryRun), dedupeType);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();

    if (exitCode != 0) {
      return "Deduplication failed!";
    }
    if (dryRun) {
      return DEDUPLICATE_RETURN_PREFIX + repairedOutputPath;
    } else {
      return DEDUPLICATE_RETURN_PREFIX + duplicatedPartitionPath;
    }
  }

  @ShellMethod(key = "repair addpartitionmeta", value = "Add partition metadata to a table, if not present")
  public String addPartitionMeta(
      @ShellOption(value = {"--dryrun"}, help = "Should we actually add or just print what would be done",
          defaultValue = "true") final boolean dryRun)
      throws IOException {

    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    String latestCommit =
        client.getActiveTimeline().getCommitAndReplaceTimeline().lastInstant().get().getTimestamp();
    List<String> partitionPaths =
        FSUtils.getAllPartitionFoldersThreeLevelsDown(HoodieCLI.storage, HoodieCLI.basePath);
    StoragePath basePath = client.getBasePath();
    String[][] rows = new String[partitionPaths.size()][];

    int ind = 0;
    for (String partition : partitionPaths) {
      StoragePath partitionPath = FSUtils.constructAbsolutePath(basePath, partition);
      String[] row = new String[3];
      row[0] = partition;
      row[1] = "Yes";
      row[2] = "None";
      if (!HoodiePartitionMetadata.hasPartitionMetadata(HoodieCLI.storage, partitionPath)) {
        row[1] = "No";
        if (!dryRun) {
          HoodiePartitionMetadata partitionMetadata =
              new HoodiePartitionMetadata(HoodieCLI.storage, latestCommit, basePath, partitionPath,
                  client.getTableConfig().getPartitionMetafileFormat());
          partitionMetadata.trySave();
          row[2] = "Repaired";
        }
      }
      rows[ind++] = row;
    }

    return HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_PARTITION_PATH,
        HoodieTableHeaderFields.HEADER_METADATA_PRESENT, HoodieTableHeaderFields.HEADER_ACTION}, rows);
  }

  @ShellMethod(key = "repair overwrite-hoodie-props",
          value = "Overwrite hoodie.properties with provided file. Risky operation. Proceed with caution!")
  public String overwriteHoodieProperties(
      @ShellOption(value = {"--new-props-file"},
              help = "Path to a properties file on local filesystem to overwrite the table's hoodie.properties with")
      final String overwriteFilePath) throws IOException {

    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    Properties newProps = new Properties();
    try (FileInputStream fileInputStream = new FileInputStream(overwriteFilePath)) {
      newProps.load(fileInputStream);
    }
    Map<String, String> oldProps = client.getTableConfig().propsMap();
    HoodieTableConfig.create(client.getStorage(), client.getMetaPath(), newProps);
    // reload new props as checksum would have been added
    newProps =
        HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient()).getTableConfig().getProps();

    TreeSet<String> allPropKeys = new TreeSet<>();
    allPropKeys.addAll(
        newProps.keySet().stream().map(Object::toString).collect(Collectors.toSet()));
    allPropKeys.addAll(oldProps.keySet());

    String[][] rows = new String[allPropKeys.size()][];
    int ind = 0;
    for (String propKey : allPropKeys) {
      String[] row = new String[] {
          propKey,
          oldProps.getOrDefault(propKey, "null"),
          newProps.getOrDefault(propKey, "null").toString()
      };
      rows[ind++] = row;
    }
    return HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_HOODIE_PROPERTY,
        HoodieTableHeaderFields.HEADER_OLD_VALUE, HoodieTableHeaderFields.HEADER_NEW_VALUE}, rows);
  }

  @ShellMethod(key = "repair corrupted clean files", value = "repair corrupted clean files")
  public void removeCorruptedPendingCleanAction() {

    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    HoodieTimeline cleanerTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline().getCleanerTimeline();
    LOG.info("Inspecting pending clean metadata in timeline for corrupted files");
    cleanerTimeline.filterInflightsAndRequested().getInstants().forEach(instant -> {
      try {
        CleanerUtils.getCleanerPlan(client, instant);
      } catch (AvroRuntimeException e) {
        LOG.warn("Corruption found. Trying to remove corrupted clean instant file: " + instant);
        HoodieActiveTimeline.deleteInstantFile(client.getStorage(), client.getMetaPath(),
            instant);
      } catch (IOException ioe) {
        if (ioe.getMessage().contains("Not an Avro data file")) {
          LOG.warn("Corruption found. Trying to remove corrupted clean instant file: " + instant);
          HoodieActiveTimeline.deleteInstantFile(client.getStorage(), client.getMetaPath(),
              instant);
        } else {
          throw new HoodieIOException(ioe.getMessage(), ioe);
        }
      }
    });
  }

  @ShellMethod(key = "repair show empty commit metadata", value = "show failed commits")
  public void showFailedCommits() {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    HoodieActiveTimeline activeTimeline =  metaClient.getActiveTimeline();
    activeTimeline.filterCompletedInstants().getInstantsAsStream().filter(activeTimeline::isEmpty).forEach(hoodieInstant -> LOG.warn("Empty Commit: " + hoodieInstant.toString()));
  }

  @ShellMethod(key = "repair migrate-partition-meta", value = "Migrate all partition meta file currently stored in text format "
      + "to be stored in base file format. See HoodieTableConfig#PARTITION_METAFILE_USE_DATA_FORMAT.")
  public String migratePartitionMeta(
      @ShellOption(value = {"--dryrun"}, help = "dry run without modifying anything.", defaultValue = "true")
      final boolean dryRun)
      throws IOException {

    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(HoodieCLI.conf);
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    List<String> partitionPaths = FSUtils.getAllPartitionPaths(engineContext, client.getStorage(), client.getBasePath(), false, false);
    StoragePath basePath = client.getBasePath();

    String[][] rows = new String[partitionPaths.size()][];
    int ind = 0;
    for (String partitionPath : partitionPaths) {
      StoragePath partition =
          FSUtils.constructAbsolutePath(client.getBasePath(), partitionPath);
      Option<StoragePath> textFormatFile =
          HoodiePartitionMetadata.textFormatMetaPathIfExists(HoodieCLI.storage, partition);
      Option<StoragePath> baseFormatFile =
          HoodiePartitionMetadata.baseFormatMetaPathIfExists(HoodieCLI.storage, partition);
      String latestCommit =
          client.getActiveTimeline().getCommitAndReplaceTimeline().lastInstant().get().getTimestamp();

      String[] row = new String[] {
          partitionPath,
          String.valueOf(textFormatFile.isPresent()),
          String.valueOf(baseFormatFile.isPresent()),
          textFormatFile.isPresent() ? "MIGRATE" : "NONE"
      };

      if (!dryRun) {
        if (!baseFormatFile.isPresent()) {
          HoodiePartitionMetadata partitionMetadata =
              new HoodiePartitionMetadata(HoodieCLI.storage, latestCommit, basePath, partition,
                  Option.of(client.getTableConfig().getBaseFileFormat()));
          partitionMetadata.trySave();
        }

        // delete it, in case we failed midway last time.
        textFormatFile.ifPresent(path -> {
          try {
            HoodieCLI.storage.deleteFile(path);
          } catch (IOException e) {
            throw new HoodieIOException(e.getMessage(), e);
          }
        });

        row[3] = "MIGRATED";
      }

      rows[ind++] = row;
    }

    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.key(), "true");
    HoodieTableConfig.update(HoodieCLI.storage, client.getMetaPath(), props);

    return HoodiePrintHelper.print(new String[] {
        HoodieTableHeaderFields.HEADER_PARTITION_PATH,
        HoodieTableHeaderFields.HEADER_TEXT_METAFILE_PRESENT,
        HoodieTableHeaderFields.HEADER_BASE_METAFILE_PRESENT,
        HoodieTableHeaderFields.HEADER_ACTION
    }, rows);
  }

  @ShellMethod(key = "repair deprecated partition",
      value = "Repair deprecated partition (\"default\"). Re-writes data from the deprecated partition into " + PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH)
  public String repairDeprecatePartition(
      @ShellOption(value = {"--sparkProperties"}, help = "Spark Properties File Path",
          defaultValue = "") String sparkPropertiesPath,
      @ShellOption(value = "--sparkMaster", defaultValue = "", help = "Spark Master") String master,
      @ShellOption(value = "--sparkMemory", defaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory) throws Exception {
    if (StringUtils.isNullOrEmpty(sparkPropertiesPath)) {
      sparkPropertiesPath =
          Utils.getDefaultPropertiesFile(JavaConverters.mapAsScalaMapConverter(System.getenv()).asScala());
    }

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkMain.SparkCommand.REPAIR_DEPRECATED_PARTITION.toString(), master, sparkMemory,
        HoodieCLI.basePath);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();

    if (exitCode != 0) {
      return "Deduplication failed!";
    }
    return "Repair succeeded";
  }

  @ShellMethod(key = "rename partition",
      value = "Rename partition. Usage: rename partition --oldPartition <oldPartition> --newPartition <newPartition>")
  public String renamePartition(
      @ShellOption(value = {"--oldPartition"}, help = "Partition value to be renamed") String oldPartition,
      @ShellOption(value = {"--newPartition"}, help = "New partition value after rename") String newPartition,
      @ShellOption(value = {"--sparkProperties"}, help = "Spark Properties File Path",
          defaultValue = "") String sparkPropertiesPath,
      @ShellOption(value = "--sparkMaster", defaultValue = "", help = "Spark Master") String master,
      @ShellOption(value = "--sparkMemory", defaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory) throws Exception {
    if (StringUtils.isNullOrEmpty(sparkPropertiesPath)) {
      sparkPropertiesPath =
          Utils.getDefaultPropertiesFile(JavaConverters.mapAsScalaMapConverter(System.getenv()).asScala());
    }

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkMain.SparkCommand.RENAME_PARTITION.toString(), master, sparkMemory,
        HoodieCLI.basePath, oldPartition, newPartition);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();

    if (exitCode != 0) {
      return "rename partition failed!";
    }
    return "rename partition succeeded";
  }
}
