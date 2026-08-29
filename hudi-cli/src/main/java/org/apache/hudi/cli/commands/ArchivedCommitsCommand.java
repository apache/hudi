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

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieCommitMetadata;
import org.apache.hudi.avro.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.commands.SparkMain.SparkCommand;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Reader;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.util.JavaScalaConverters.convertJavaPropertiesToScalaMap;

/**
 * CLI command to display archived commits and stats if available.
 */
@ShellComponent
@Slf4j
public class ArchivedCommitsCommand {
  @ShellMethod(key = "trigger archival", value = "trigger archival")
  public String triggerArchival(
      @ShellOption(value = {"--minCommits"},
        help = "Minimum number of instants to retain in the active timeline. See hoodie.keep.min.commits",
        defaultValue = "20") int minCommits,
      @ShellOption(value = {"--maxCommits"},
          help = "Maximum number of instants to retain in the active timeline. See hoodie.keep.max.commits",
          defaultValue = "30") int maxCommits,
      @ShellOption(value = {"--commitsRetainedByCleaner"}, help = "Number of commits to retain, without cleaning",
          defaultValue = "10") int retained,
      @ShellOption(value = {"--enableMetadata"},
          help = "Enable the internal metadata table which serves table metadata like level file listings",
          defaultValue = "true") boolean enableMetadata,
      @ShellOption(value = "--sparkMemory", defaultValue = "1G",
          help = "Spark executor memory") final String sparkMemory,
      @ShellOption(value = "--sparkMaster", defaultValue = "local", help = "Spark Master") String master) throws Exception {
    String sparkPropertiesPath =
        Utils.getDefaultPropertiesFile(convertJavaPropertiesToScalaMap(System.getProperties()));
    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    SparkMain.addAppArgs(sparkLauncher, SparkCommand.ARCHIVE, master, sparkMemory, Integer.toString(minCommits), Integer.toString(maxCommits),
        Integer.toString(retained), Boolean.toString(enableMetadata), HoodieCLI.basePath);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      return "Failed to trigger archival";
    }
    return "Archival successfully triggered";
  }

  @ShellMethod(key = "show archived commit stats", value = "Read commits from archived files and show file group details")
  public String showArchivedCommits(
      @ShellOption(value = {"--archiveFolderPattern"},
          help = "Archive Folder, a folder under the meta path holding archive files in the"
              + " legacy log format written before table version 8. When absent, the table's"
              + " archived timeline is read in whichever format the table version mandates",
          defaultValue = "") String folder,
      @ShellOption(value = {"--limit"}, help = "Limit commits", defaultValue = "10") final Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
              defaultValue = "false") final boolean headerOnly)
      throws IOException {
    System.out.println("===============> Showing only " + limit + " archived commits <===============");
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    List<Comparable[]> allStats;
    if (folder != null && !folder.isEmpty()) {
      allStats = readCommitStatsFromLegacyArchive(metaClient, new StoragePath(metaClient.getMetaPath(), folder));
    } else {
      allStats = readCommitStatsFromArchivedTimeline(metaClient);
    }
    TableHeader header = new TableHeader().addTableHeaderField("action").addTableHeaderField("instant")
        .addTableHeaderField("partition").addTableHeaderField("file_id").addTableHeaderField("prev_instant")
        .addTableHeaderField("num_writes").addTableHeaderField("num_inserts").addTableHeaderField("num_deletes")
        .addTableHeaderField("num_update_writes").addTableHeaderField("total_log_files")
        .addTableHeaderField("total_log_blocks").addTableHeaderField("total_corrupt_log_blocks")
        .addTableHeaderField("total_rollback_blocks").addTableHeaderField("total_log_records")
        .addTableHeaderField("total_updated_records_compacted").addTableHeaderField("total_write_bytes")
        .addTableHeaderField("total_write_errors");

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, allStats);
  }

  @ShellMethod(key = "show archived commits", value = "Read commits from archived files and show details")
  public String showCommits(
      @ShellOption(value = {"--skipMetadata"}, help = "Skip displaying commit metadata",
          defaultValue = "true") boolean skipMetadata,
      @ShellOption(value = {"--limit"}, help = "Limit commits", defaultValue = "10") final Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
              defaultValue = "false") final boolean headerOnly) {

    System.out.println("===============> Showing only " + limit + " archived commits <===============");
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
    List<Comparable[]> allCommits;
    try {
      if (!skipMetadata) {
        archivedTimeline.loadCompletedInstantDetailsInMemory();
      }
      allCommits = archivedTimeline.getInstants().stream()
          .filter(HoodieInstant::isCompleted)
          .map(instant -> readArchivedCommit(archivedTimeline, instant, skipMetadata))
          .collect(Collectors.toList());
    } finally {
      if (!skipMetadata) {
        // free the metadata that was loaded in memory for this command
        archivedTimeline.getInstants().forEach(
            instant -> archivedTimeline.clearInstantDetailsFromMemory(instant.requestedTime()));
      }
    }

    TableHeader header = new TableHeader().addTableHeaderField("CommitTime").addTableHeaderField("CommitType");

    if (!skipMetadata) {
      header = header.addTableHeaderField("CommitDetails");
    }

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, allCommits);
  }

  /**
   * Reads the write stats of the archived commit and delta commit instants through the
   * archived timeline, which resolves the archive format from the table version (LSM
   * timeline for table version 8 and above, log format before that).
   */
  private List<Comparable[]> readCommitStatsFromArchivedTimeline(HoodieTableMetaClient metaClient) {
    HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
    try {
      archivedTimeline.loadCompletedInstantDetailsInMemory();
      return archivedTimeline.getInstants().stream()
          .filter(HoodieInstant::isCompleted)
          .filter(instant -> HoodieTimeline.COMMIT_ACTION.equals(instant.getAction())
              || HoodieTimeline.DELTA_COMMIT_ACTION.equals(instant.getAction()))
          .flatMap(instant -> readWriteStatRows(archivedTimeline, instant))
          .collect(Collectors.toList());
    } finally {
      // free the metadata that was loaded in memory for this command
      archivedTimeline.getInstants().forEach(
          instant -> archivedTimeline.clearInstantDetailsFromMemory(instant.requestedTime()));
    }
  }

  private Stream<Comparable[]> readWriteStatRows(HoodieArchivedTimeline archivedTimeline, HoodieInstant instant) {
    HoodieCommitMetadata metadata;
    try {
      metadata = archivedTimeline.readCommitMetadataToAvro(instant);
    } catch (IOException e) {
      throw new HoodieException("Failed to read the archived commit metadata of instant " + instant, e);
    }
    if (metadata == null || metadata.getPartitionToWriteStats() == null) {
      return Stream.empty();
    }
    final String action = instant.getAction();
    final String instantTime = instant.requestedTime();
    return metadata.getPartitionToWriteStats().values().stream()
        .flatMap(List::stream)
        .map(writeStat -> new Comparable[] {action, instantTime, writeStat.getPartitionPath(),
            writeStat.getFileId(), writeStat.getPrevCommit(), writeStat.getNumWrites(),
            writeStat.getNumInserts(), writeStat.getNumDeletes(), writeStat.getNumUpdateWrites(),
            writeStat.getTotalLogFiles(), writeStat.getTotalLogBlocks(), writeStat.getTotalCorruptLogBlock(),
            writeStat.getTotalRollbackBlocks(), writeStat.getTotalLogRecords(),
            writeStat.getTotalUpdatedRecordsCompacted(), writeStat.getTotalWriteBytes(),
            writeStat.getTotalWriteErrors()});
  }

  /**
   * Reads the write stats of the archived commit and delta commit instants from the given
   * folder of archive files in the legacy log format written before table version 8.
   */
  private List<Comparable[]> readCommitStatsFromLegacyArchive(HoodieTableMetaClient metaClient, StoragePath archivePath) throws IOException {
    HoodieStorage storage = metaClient.getStorage();
    List<StoragePathInfo> pathInfoList = storage.globEntries(archivePath);
    List<Comparable[]> allStats = new ArrayList<>();
    for (StoragePathInfo pathInfo : pathInfoList) {
      // read the archived file
      try (Reader reader = HoodieLogFormat.newReader(metaClient, new HoodieLogFile(pathInfo.getPath()),
          HoodieSchema.fromAvroSchema(HoodieArchivedMetaEntry.getClassSchema()))) {
        List<IndexedRecord> readRecords = new ArrayList<>();
        // read the avro blocks
        while (reader.hasNext()) {
          HoodieAvroDataBlock blk = (HoodieAvroDataBlock) reader.next();
          blk.getRecordIterator(HoodieRecordType.AVRO).forEachRemaining(r -> readRecords.add((IndexedRecord) r.getData()));
        }
        List<Comparable[]> readCommits = readRecords.stream().map(r -> (GenericRecord) r)
            .filter(r -> r.get("actionType").toString().equals(HoodieTimeline.COMMIT_ACTION)
                || r.get("actionType").toString().equals(HoodieTimeline.DELTA_COMMIT_ACTION))
            .flatMap(r -> {
              HoodieCommitMetadata metadata = (HoodieCommitMetadata) SpecificData.get()
                  .deepCopy(HoodieCommitMetadata.SCHEMA$, r.get("hoodieCommitMetadata"));
              final String instantTime = r.get("commitTime").toString();
              final String action = r.get("actionType").toString();
              return metadata.getPartitionToWriteStats().values().stream().flatMap(hoodieWriteStats -> hoodieWriteStats.stream().map(hoodieWriteStat -> {
                List<Comparable> row = new ArrayList<>();
                row.add(action);
                row.add(instantTime);
                row.add(hoodieWriteStat.getPartitionPath());
                row.add(hoodieWriteStat.getFileId());
                row.add(hoodieWriteStat.getPrevCommit());
                row.add(hoodieWriteStat.getNumWrites());
                row.add(hoodieWriteStat.getNumInserts());
                row.add(hoodieWriteStat.getNumDeletes());
                row.add(hoodieWriteStat.getNumUpdateWrites());
                row.add(hoodieWriteStat.getTotalLogFiles());
                row.add(hoodieWriteStat.getTotalLogBlocks());
                row.add(hoodieWriteStat.getTotalCorruptLogBlock());
                row.add(hoodieWriteStat.getTotalRollbackBlocks());
                row.add(hoodieWriteStat.getTotalLogRecords());
                row.add(hoodieWriteStat.getTotalUpdatedRecordsCompacted());
                row.add(hoodieWriteStat.getTotalWriteBytes());
                row.add(hoodieWriteStat.getTotalWriteErrors());
                return row;
              })).map(rowList -> rowList.toArray(new Comparable[0]));
            }).collect(Collectors.toList());
        allStats.addAll(readCommits);
      }
    }
    return allStats;
  }

  private Comparable[] readArchivedCommit(HoodieArchivedTimeline archivedTimeline, HoodieInstant instant, boolean skipMetadata) {
    List<Comparable> commitDetails = new ArrayList<>();
    commitDetails.add(instant.requestedTime());
    commitDetails.add(instant.getAction());
    if (!skipMetadata) {
      commitDetails.add(readArchivedMetadataString(archivedTimeline, instant));
    }
    return commitDetails.toArray(new Comparable[commitDetails.size()]);
  }

  private String readArchivedMetadataString(HoodieArchivedTimeline archivedTimeline, HoodieInstant instant) {
    Option<byte[]> details = archivedTimeline.getInstantDetails(instant);
    if (!details.isPresent() || details.get().length == 0) {
      // instants can be archived with no metadata, e.g. from an empty completed
      // meta file that a writer failure left behind
      return "{}";
    }
    try {
      switch (instant.getAction()) {
        case HoodieTimeline.CLEAN_ACTION:
          return archivedTimeline.readCleanMetadata(instant).toString();
        case HoodieTimeline.COMMIT_ACTION:
        case HoodieTimeline.DELTA_COMMIT_ACTION:
          return sortPartitions(archivedTimeline.readCommitMetadataToAvro(instant)).toString();
        case HoodieTimeline.ROLLBACK_ACTION:
          return archivedTimeline.readRollbackMetadata(instant).toString();
        case HoodieTimeline.SAVEPOINT_ACTION:
          return archivedTimeline.readSavepointMetadata(instant).toString();
        case HoodieTimeline.COMPACTION_ACTION:
        case HoodieTimeline.LOG_COMPACTION_ACTION:
          return archivedTimeline.readCompactionPlan(instant).toString();
        case HoodieTimeline.REPLACE_COMMIT_ACTION:
        case HoodieTimeline.CLUSTERING_ACTION:
          return sortPartitions(archivedTimeline.readReplaceCommitMetadataToAvro(instant)).toString();
        default:
          throw new HoodieException("Unexpected action type: " + instant.getAction());
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to read the archived metadata of instant " + instant, e);
    }
  }

  /**
   * Orders the partition keyed maps of the metadata so that the rendering does not
   * depend on the iteration order of the map the Avro decoder happens to build.
   */
  private static HoodieCommitMetadata sortPartitions(HoodieCommitMetadata metadata) {
    metadata.setPartitionToWriteStats(sortByKey(metadata.getPartitionToWriteStats()));
    return metadata;
  }

  private static HoodieReplaceCommitMetadata sortPartitions(HoodieReplaceCommitMetadata metadata) {
    metadata.setPartitionToWriteStats(sortByKey(metadata.getPartitionToWriteStats()));
    metadata.setPartitionToReplaceFileIds(sortByKey(metadata.getPartitionToReplaceFileIds()));
    return metadata;
  }

  private static <T> Map<String, T> sortByKey(Map<String, T> partitionKeyedMap) {
    return partitionKeyedMap == null ? null : new TreeMap<>(partitionKeyedMap);
  }
}
