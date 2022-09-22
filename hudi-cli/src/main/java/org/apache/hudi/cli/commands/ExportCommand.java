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

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Reader;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.exception.HoodieException;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CLI commands to export various information from a HUDI dataset.
 * <p>
 * "export instants": Export Instants and their metadata from the Timeline to a local
 * directory specified by the parameter --localFolder
 * The instants are exported in the json format.
 */
@ShellComponent
public class ExportCommand {

  @ShellMethod(key = "export instants", value = "Export Instants and their metadata from the Timeline")
  public String exportInstants(
      @ShellOption(value = {"--limit"}, help = "Limit Instants", defaultValue = "-1") final Integer limit,
      @ShellOption(value = {"--actions"}, help = "Comma separated list of Instant actions to export",
              defaultValue = "clean,commit,deltacommit,rollback,savepoint,restore") final String filter,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--localFolder"}, help = "Local Folder to export to") String localFolder)
      throws Exception {

    final String basePath = HoodieCLI.getTableMetaClient().getBasePath();
    final Path archivePath = new Path(basePath + "/.hoodie/.commits_.archive*");
    final Set<String> actionSet = new HashSet<String>(Arrays.asList(filter.split(",")));
    int numExports = limit == -1 ? Integer.MAX_VALUE : limit;
    int numCopied = 0;

    if (!new File(localFolder).isDirectory()) {
      throw new HoodieException(localFolder + " is not a valid local directory");
    }

    // The non archived instants can be listed from the Timeline.
    HoodieTimeline timeline = HoodieCLI.getTableMetaClient().getActiveTimeline().filterCompletedInstants()
        .filter(i -> actionSet.contains(i.getAction()));
    List<HoodieInstant> nonArchivedInstants = timeline.getInstants().collect(Collectors.toList());

    // Archived instants are in the commit archive files
    FileStatus[] statuses = FSUtils.getFs(basePath, HoodieCLI.conf).globStatus(archivePath);
    List<FileStatus> archivedStatuses = Arrays.stream(statuses).sorted((f1, f2) -> (int) (f1.getModificationTime() - f2.getModificationTime())).collect(Collectors.toList());

    if (descending) {
      Collections.reverse(nonArchivedInstants);
      numCopied = copyNonArchivedInstants(nonArchivedInstants, numExports, localFolder);
      if (numCopied < numExports) {
        Collections.reverse(archivedStatuses);
        numCopied += copyArchivedInstants(archivedStatuses, actionSet, numExports - numCopied, localFolder);
      }
    } else {
      numCopied = copyArchivedInstants(archivedStatuses, actionSet, numExports, localFolder);
      if (numCopied < numExports) {
        numCopied += copyNonArchivedInstants(nonArchivedInstants, numExports - numCopied, localFolder);
      }
    }

    return "Exported " + numCopied + " Instants to " + localFolder;
  }

  private int copyArchivedInstants(List<FileStatus> statuses, Set<String> actionSet, int limit, String localFolder) throws Exception {
    int copyCount = 0;
    FileSystem fileSystem = FSUtils.getFs(HoodieCLI.getTableMetaClient().getBasePath(), HoodieCLI.conf);

    for (FileStatus fs : statuses) {
      // read the archived file
      Reader reader = HoodieLogFormat.newReader(fileSystem, new HoodieLogFile(fs.getPath()), HoodieArchivedMetaEntry.getClassSchema());

      // read the avro blocks
      while (reader.hasNext() && copyCount < limit) {
        HoodieAvroDataBlock blk = (HoodieAvroDataBlock) reader.next();
        try (ClosableIterator<HoodieRecord<IndexedRecord>> recordItr = blk.getRecordIterator(HoodieRecordType.AVRO)) {
          while (recordItr.hasNext()) {
            IndexedRecord ir = recordItr.next().getData();
            // Archived instants are saved as arvo encoded HoodieArchivedMetaEntry records. We need to get the
            // metadata record from the entry and convert it to json.
            HoodieArchivedMetaEntry archiveEntryRecord = (HoodieArchivedMetaEntry) SpecificData.get()
                .deepCopy(HoodieArchivedMetaEntry.SCHEMA$, ir);
            final String action = archiveEntryRecord.get("actionType").toString();
            if (!actionSet.contains(action)) {
              continue;
            }

            GenericRecord metadata = null;
            switch (action) {
              case HoodieTimeline.CLEAN_ACTION:
                metadata = archiveEntryRecord.getHoodieCleanMetadata();
                break;
              case HoodieTimeline.COMMIT_ACTION:
              case HoodieTimeline.DELTA_COMMIT_ACTION:
                metadata = archiveEntryRecord.getHoodieCommitMetadata();
                break;
              case HoodieTimeline.ROLLBACK_ACTION:
                metadata = archiveEntryRecord.getHoodieRollbackMetadata();
                break;
              case HoodieTimeline.SAVEPOINT_ACTION:
                metadata = archiveEntryRecord.getHoodieSavePointMetadata();
                break;
              case HoodieTimeline.COMPACTION_ACTION:
                metadata = archiveEntryRecord.getHoodieCompactionMetadata();
                break;
              default:
                throw new HoodieException("Unknown type of action " + action);
            }

            final String instantTime = archiveEntryRecord.get("commitTime").toString();
            final String outPath = localFolder + Path.SEPARATOR + instantTime + "." + action;
            writeToFile(outPath, HoodieAvroUtils.avroToJson(metadata, true));
            if (++copyCount == limit) {
              break;
            }
          }
        }
      }

      reader.close();
    }

    return copyCount;
  }

  private int copyNonArchivedInstants(List<HoodieInstant> instants, int limit, String localFolder) throws Exception {
    int copyCount = 0;

    if (instants.isEmpty()) {
      return copyCount;
    }

    final HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    final HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    for (HoodieInstant instant : instants) {
      String localPath = localFolder + Path.SEPARATOR + instant.getFileName();

      byte[] data = null;
      switch (instant.getAction()) {
        case HoodieTimeline.CLEAN_ACTION: {
          HoodieCleanMetadata metadata = TimelineMetadataUtils.deserializeHoodieCleanMetadata(
              timeline.getInstantDetails(instant).get());
          data = HoodieAvroUtils.avroToJson(metadata, true);
          break;
        }
        case HoodieTimeline.DELTA_COMMIT_ACTION:
        case HoodieTimeline.COMMIT_ACTION:
        case HoodieTimeline.COMPACTION_ACTION: {
          // Already in json format
          data = timeline.getInstantDetails(instant).get();
          break;
        }
        case HoodieTimeline.ROLLBACK_ACTION: {
          HoodieRollbackMetadata metadata = TimelineMetadataUtils.deserializeHoodieRollbackMetadata(
              timeline.getInstantDetails(instant).get());
          data = HoodieAvroUtils.avroToJson(metadata, true);
          break;
        }
        case HoodieTimeline.SAVEPOINT_ACTION: {
          HoodieSavepointMetadata metadata = TimelineMetadataUtils.deserializeHoodieSavepointMetadata(
              timeline.getInstantDetails(instant).get());
          data = HoodieAvroUtils.avroToJson(metadata, true);
          break;
        }
        default: {
          throw new HoodieException("Unknown type of action " + instant.getAction());
        }
      }

      if (data != null) {
        writeToFile(localPath, data);
        copyCount = copyCount + 1;
      }
    }

    return copyCount;
  }

  private void writeToFile(String path, byte[] data) throws Exception {
    FileOutputStream writer = new FileOutputStream(path);
    writer.write(data);
    writer.close();
  }
}
