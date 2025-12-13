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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Reader;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
@Slf4j
public class ExportCommand {

  @ShellMethod(key = "export instants", value = "Export Instants and their metadata from the Timeline")
  public String exportInstants(
      @ShellOption(value = {"--limit"}, help = "Limit Instants", defaultValue = "-1") final Integer limit,
      @ShellOption(value = {"--actions"}, help = "Comma separated list of Instant actions to export",
              defaultValue = "clean,commit,deltacommit,rollback,savepoint,restore") final String filter,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--localFolder"}, help = "Local Folder to export to") String localFolder)
      throws Exception {

    final StoragePath basePath = HoodieCLI.getTableMetaClient().getBasePath();
    final StoragePath archivePath = HoodieCLI.getTableMetaClient().getArchivePath();
    final Set<String> actionSet = new HashSet<String>(Arrays.asList(filter.split(",")));
    int numExports = limit == -1 ? Integer.MAX_VALUE : limit;
    int numCopied = 0;

    if (!new File(localFolder).isDirectory()) {
      throw new HoodieException(localFolder + " is not a valid local directory");
    }

    // The non archived instants can be listed from the Timeline.
    HoodieTimeline timeline = HoodieCLI.getTableMetaClient().getActiveTimeline().filterCompletedInstants()
        .filter(i -> actionSet.contains(i.getAction()));
    List<HoodieInstant> nonArchivedInstants = timeline.getInstants();

    // Archived instants are in the commit archive files
    List<StoragePathInfo> pathInfoList =
        HoodieStorageUtils.getStorage(basePath, HoodieCLI.conf).globEntries(archivePath);
    List<StoragePathInfo> archivedPathInfoList = pathInfoList.stream()
        .sorted(Comparator.comparingLong(StoragePathInfo::getModificationTime))
        .collect(Collectors.toList());

    if (descending) {
      Collections.reverse(nonArchivedInstants);
      numCopied = copyNonArchivedInstants(nonArchivedInstants, numExports, localFolder);
      if (numCopied < numExports) {
        Collections.reverse(archivedPathInfoList);
        numCopied += copyArchivedInstants(archivedPathInfoList, actionSet, numExports - numCopied, localFolder);
      }
    } else {
      numCopied = copyArchivedInstants(archivedPathInfoList, actionSet, numExports, localFolder);
      if (numCopied < numExports) {
        numCopied += copyNonArchivedInstants(nonArchivedInstants, numExports - numCopied, localFolder);
      }
    }

    return "Exported " + numCopied + " Instants to " + localFolder;
  }

  private int copyArchivedInstants(List<StoragePathInfo> pathInfoList,
                                   Set<String> actionSet,
                                   int limit,
                                   String localFolder) throws Exception {
    int copyCount = 0;
    HoodieStorage storage = HoodieStorageUtils.getStorage(
        HoodieCLI.getTableMetaClient().getBasePath(), HoodieCLI.conf);

    for (StoragePathInfo pathInfo : pathInfoList) {
      // read the archived file
      try (Reader reader = HoodieLogFormat.newReader(storage, new HoodieLogFile(pathInfo.getPath()), HoodieSchema.fromAvroSchema(HoodieArchivedMetaEntry.getClassSchema()))) {

        // read the avro blocks
        while (reader.hasNext() && copyCount++ < limit) {
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
              if (metadata == null) {
                log.error("Could not load metadata for action " + action + " at instant time " + instantTime);
                continue;
              }
              final String outPath = localFolder + StoragePath.SEPARATOR + instantTime + "." + action;
              writeToFile(outPath, HoodieAvroUtils.avroToJson(metadata, true));
            }
          }
        }
      }
    }

    return copyCount;
  }

  private int copyNonArchivedInstants(List<HoodieInstant> instants, int limit, String localFolder) throws Exception {
    int copyCount = 0;

    if (instants.isEmpty()) {
      return copyCount;
    }

    final HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    final InstantFileNameGenerator instantFileNameGenerator = metaClient.getInstantFileNameGenerator();
    final HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    for (HoodieInstant instant : instants) {
      String localPath = localFolder + StoragePath.SEPARATOR + instantFileNameGenerator.getFileName(instant);

      byte[] data = null;
      switch (instant.getAction()) {
        case HoodieTimeline.CLEAN_ACTION: {
          HoodieCleanMetadata metadata = timeline.readCleanMetadata(instant);
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
          HoodieRollbackMetadata metadata = timeline.readRollbackMetadata(instant);
          data = HoodieAvroUtils.avroToJson(metadata, true);
          break;
        }
        case HoodieTimeline.SAVEPOINT_ACTION: {
          HoodieSavepointMetadata metadata = timeline.readSavepointMetadata(instant);
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
    try (FileOutputStream writer = new FileOutputStream(path)) {
      writer.write(data);
    } catch (IOException e) {
      throw e;
    }
  }
}
