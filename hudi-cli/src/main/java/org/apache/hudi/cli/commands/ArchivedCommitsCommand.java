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
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.commands.SparkMain.SparkCommand;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Reader;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.HoodieStorageUtils;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * CLI command to display archived commits and stats if available.
 */
@ShellComponent
public class ArchivedCommitsCommand {
  private static final Logger LOG = LoggerFactory.getLogger(ArchivedCommitsCommand.class);
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
        Utils.getDefaultPropertiesFile(scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    String cmd = SparkCommand.ARCHIVE.toString();
    sparkLauncher.addAppArgs(cmd, master, sparkMemory, Integer.toString(minCommits), Integer.toString(maxCommits),
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
      @ShellOption(value = {"--archiveFolderPattern"}, help = "Archive Folder", defaultValue = "") String folder,
      @ShellOption(value = {"--limit"}, help = "Limit commits", defaultValue = "10") final Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
              defaultValue = "false") final boolean headerOnly)
      throws IOException {
    System.out.println("===============> Showing only " + limit + " archived commits <===============");
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    StoragePath archivePath = folder != null && !folder.isEmpty()
        ? new StoragePath(metaClient.getMetaPath(), folder)
        : new StoragePath(metaClient.getArchivePath(), ".commits_.archive*");
    HoodieStorage storage = metaClient.getStorage();
    List<StoragePathInfo> pathInfoList = storage.globEntries(archivePath);
    List<Comparable[]> allStats = new ArrayList<>();
    for (StoragePathInfo pathInfo : pathInfoList) {
      // read the archived file
      try (Reader reader = HoodieLogFormat.newReader(storage, new HoodieLogFile(pathInfo.getPath()),
          HoodieArchivedMetaEntry.getClassSchema())) {
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
              defaultValue = "false") final boolean headerOnly)
      throws IOException {

    System.out.println("===============> Showing only " + limit + " archived commits <===============");
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    StoragePath basePath = metaClient.getBasePath();
    StoragePath archivePath =
        new StoragePath(metaClient.getArchivePath() + "/.commits_.archive*");
    List<StoragePathInfo> pathInfoList =
        HoodieStorageUtils.getStorage(basePath, HoodieCLI.conf).globEntries(archivePath);
    List<Comparable[]> allCommits = new ArrayList<>();
    for (StoragePathInfo pathInfo : pathInfoList) {
      // read the archived file
      try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(HoodieStorageUtils.getStorage(basePath, HoodieCLI.conf),
          new HoodieLogFile(pathInfo.getPath()), HoodieArchivedMetaEntry.getClassSchema())) {
        List<IndexedRecord> readRecords = new ArrayList<>();
        // read the avro blocks
        while (reader.hasNext()) {
          HoodieAvroDataBlock blk = (HoodieAvroDataBlock) reader.next();
          try (ClosableIterator<HoodieRecord<IndexedRecord>> recordItr = blk.getRecordIterator(HoodieRecordType.AVRO)) {
            recordItr.forEachRemaining(r -> readRecords.add(r.getData()));
          }
        }
        List<Comparable[]> readCommits = readRecords.stream().map(r -> (GenericRecord) r)
            .map(r -> readCommit(r, skipMetadata)).collect(Collectors.toList());
        allCommits.addAll(readCommits);
      }
    }

    TableHeader header = new TableHeader().addTableHeaderField("CommitTime").addTableHeaderField("CommitType");

    if (!skipMetadata) {
      header = header.addTableHeaderField("CommitDetails");
    }

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, allCommits);
  }

  private Comparable[] commitDetail(GenericRecord record, String metadataName, boolean skipMetadata) {
    List<Object> commitDetails = new ArrayList<>();
    commitDetails.add(record.get("commitTime"));
    commitDetails.add(record.get("actionType").toString());
    if (!skipMetadata) {
      commitDetails.add(Option.ofNullable(record.get(metadataName)).orElse("{}").toString());
    }
    return commitDetails.toArray(new Comparable[commitDetails.size()]);
  }

  private Comparable[] readCommit(GenericRecord record, boolean skipMetadata) {
    String actionType = record.get("actionType").toString();
    switch (actionType) {
      case HoodieTimeline.CLEAN_ACTION:
        return commitDetail(record, "hoodieCleanMetadata", skipMetadata);
      case HoodieTimeline.COMMIT_ACTION:
      case HoodieTimeline.DELTA_COMMIT_ACTION:
        return commitDetail(record, "hoodieCommitMetadata", skipMetadata);
      case HoodieTimeline.ROLLBACK_ACTION:
        return commitDetail(record, "hoodieRollbackMetadata", skipMetadata);
      case HoodieTimeline.SAVEPOINT_ACTION:
        return commitDetail(record, "hoodieSavePointMetadata", skipMetadata);
      case HoodieTimeline.COMPACTION_ACTION:
        return commitDetail(record, "hoodieCompactionMetadata", skipMetadata);
      case HoodieTimeline.REPLACE_COMMIT_ACTION:
        return commitDetail(record, "hoodieReplaceCommitMetadata", skipMetadata);
      default: {
        throw new HoodieException("Unexpected action type: " + actionType);
      }
    }
  }

}
