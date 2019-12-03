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
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Reader;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.util.FSUtils;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * CLI command to display archived commits and stats if available.
 */
@Component
public class ArchivedCommitsCommand implements CommandMarker {

  @CliAvailabilityIndicator({"show archived commits"})
  public boolean isShowArchivedCommitAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliCommand(value = "show archived commit stats", help = "Read commits from archived files and show details")
  public String showArchivedCommits(
      @CliOption(key = {"archiveFolderPattern"}, help = "Archive Folder", unspecifiedDefaultValue = "") String folder,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {
    System.out.println("===============> Showing only " + limit + " archived commits <===============");
    String basePath = HoodieCLI.tableMetadata.getBasePath();
    Path archivePath = new Path(basePath + "/.hoodie/.commits_.archive*");
    if (folder != null && !folder.isEmpty()) {
      archivePath = new Path(basePath + "/.hoodie/" + folder);
    }
    FileStatus[] fsStatuses = FSUtils.getFs(basePath, HoodieCLI.conf).globStatus(archivePath);
    List<Comparable[]> allStats = new ArrayList<>();
    for (FileStatus fs : fsStatuses) {
      // read the archived file
      Reader reader = HoodieLogFormat.newReader(FSUtils.getFs(basePath, HoodieCLI.conf),
          new HoodieLogFile(fs.getPath()), HoodieArchivedMetaEntry.getClassSchema());

      List<IndexedRecord> readRecords = new ArrayList<>();
      // read the avro blocks
      while (reader.hasNext()) {
        HoodieAvroDataBlock blk = (HoodieAvroDataBlock) reader.next();
        List<IndexedRecord> records = blk.getRecords();
        readRecords.addAll(records);
      }
      List<Comparable[]> readCommits = readRecords.stream().map(r -> (GenericRecord) r)
          .filter(r -> r.get("actionType").toString().equals(HoodieTimeline.COMMIT_ACTION)
              || r.get("actionType").toString().equals(HoodieTimeline.DELTA_COMMIT_ACTION))
          .flatMap(r -> {
            HoodieCommitMetadata metadata = (HoodieCommitMetadata) SpecificData.get()
                .deepCopy(HoodieCommitMetadata.SCHEMA$, r.get("hoodieCommitMetadata"));
            final String instantTime = r.get("commitTime").toString();
            final String action = r.get("actionType").toString();
            return metadata.getPartitionToWriteStats().values().stream().flatMap(hoodieWriteStats -> {
              return hoodieWriteStats.stream().map(hoodieWriteStat -> {
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
              });
            }).map(rowList -> rowList.toArray(new Comparable[0]));
          }).collect(Collectors.toList());
      allStats.addAll(readCommits);
      reader.close();
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

  @CliCommand(value = "show archived commits", help = "Read commits from archived files and show details")
  public String showCommits(
      @CliOption(key = {"skipMetadata"}, help = "Skip displaying commit metadata",
          unspecifiedDefaultValue = "true") boolean skipMetadata,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "10") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {

    System.out.println("===============> Showing only " + limit + " archived commits <===============");
    String basePath = HoodieCLI.tableMetadata.getBasePath();
    FileStatus[] fsStatuses =
        FSUtils.getFs(basePath, HoodieCLI.conf).globStatus(new Path(basePath + "/.hoodie/.commits_.archive*"));
    List<Comparable[]> allCommits = new ArrayList<>();
    for (FileStatus fs : fsStatuses) {
      // read the archived file
      HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(FSUtils.getFs(basePath, HoodieCLI.conf),
          new HoodieLogFile(fs.getPath()), HoodieArchivedMetaEntry.getClassSchema());

      List<IndexedRecord> readRecords = new ArrayList<>();
      // read the avro blocks
      while (reader.hasNext()) {
        HoodieAvroDataBlock blk = (HoodieAvroDataBlock) reader.next();
        List<IndexedRecord> records = blk.getRecords();
        readRecords.addAll(records);
      }
      List<Comparable[]> readCommits = readRecords.stream().map(r -> (GenericRecord) r)
          .map(r -> readCommit(r, skipMetadata)).collect(Collectors.toList());
      allCommits.addAll(readCommits);
      reader.close();
    }

    TableHeader header = new TableHeader().addTableHeaderField("CommitTime").addTableHeaderField("CommitType");

    if (!skipMetadata) {
      header = header.addTableHeaderField("CommitDetails");
    }

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, allCommits);
  }

  private Comparable[] readCommit(GenericRecord record, boolean skipMetadata) {
    List<Object> commitDetails = new ArrayList<>();
    try {
      switch (record.get("actionType").toString()) {
        case HoodieTimeline.CLEAN_ACTION: {
          commitDetails.add(record.get("commitTime"));
          commitDetails.add(record.get("actionType").toString());
          if (!skipMetadata) {
            commitDetails.add(record.get("hoodieCleanMetadata").toString());
          }
          break;
        }
        case HoodieTimeline.COMMIT_ACTION: {
          commitDetails.add(record.get("commitTime"));
          commitDetails.add(record.get("actionType").toString());
          if (!skipMetadata) {
            commitDetails.add(record.get("hoodieCommitMetadata").toString());
          }
          break;
        }
        case HoodieTimeline.DELTA_COMMIT_ACTION: {
          commitDetails.add(record.get("commitTime"));
          commitDetails.add(record.get("actionType").toString());
          if (!skipMetadata) {
            commitDetails.add(record.get("hoodieCommitMetadata").toString());
          }
          break;
        }
        case HoodieTimeline.ROLLBACK_ACTION: {
          commitDetails.add(record.get("commitTime"));
          commitDetails.add(record.get("actionType").toString());
          if (!skipMetadata) {
            commitDetails.add(record.get("hoodieRollbackMetadata").toString());
          }
          break;
        }
        case HoodieTimeline.SAVEPOINT_ACTION: {
          commitDetails.add(record.get("commitTime"));
          commitDetails.add(record.get("actionType").toString());
          if (!skipMetadata) {
            commitDetails.add(record.get("hoodieSavePointMetadata").toString());
          }
          break;
        }
        default:
          return commitDetails.toArray(new Comparable[commitDetails.size()]);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return commitDetails.toArray(new Comparable[commitDetails.size()]);
  }
}
