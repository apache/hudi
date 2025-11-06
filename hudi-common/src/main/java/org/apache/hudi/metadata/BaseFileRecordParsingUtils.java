/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class BaseFileRecordParsingUtils {

  /**
   * Generates RLI Metadata records for base files.
   * If base file is a added to a new file group, all record keys are treated as inserts.
   * If a base file is added to an existing file group, we read previous base file in addition to the latest base file of interest. Find deleted records and generate RLI Metadata records
   * for the same in addition to new insert records.
   *
   * @param basePath             base path of the table.
   * @param writeStat            {@link HoodieWriteStat} of interest.
   * @param writesFileIdEncoding fileID encoding for the table.
   * @param instantTime          instant time of interest.
   * @param storage              instance of {@link HoodieStorage}.
   * @return Iterator of {@link HoodieRecord}s for RLI Metadata partition.
   */
  public static Iterator<HoodieRecord> generateRLIMetadataHoodieRecordsForBaseFile(String basePath,
                                                                                   HoodieWriteStat writeStat,
                                                                                   Integer writesFileIdEncoding,
                                                                                   String instantTime,
                                                                                   HoodieStorage storage,
                                                                                   boolean isPartitionedRLI) {
    String partition = writeStat.getPartitionPath();
    String latestFileName = FSUtils.getFileNameFromPath(writeStat.getPath());
    String fileId = FSUtils.getFileIdFromFileName(latestFileName);

    Set<RecordStatus> recordStatuses = new HashSet<>();
    recordStatuses.add(RecordStatus.INSERT);
    recordStatuses.add(RecordStatus.DELETE);
    // for RLI, we are only interested in INSERTS and DELETES
    Map<RecordStatus, List<String>> recordStatusListMap = getRecordKeyStatuses(basePath, writeStat.getPartitionPath(), latestFileName, writeStat.getPrevBaseFile(), storage,
        recordStatuses);
    List<HoodieRecord> hoodieRecords = new ArrayList<>();
    if (recordStatusListMap.containsKey(RecordStatus.INSERT)) {
      hoodieRecords.addAll(recordStatusListMap.get(RecordStatus.INSERT).stream()
          .map(recordKey -> (HoodieRecord) HoodieMetadataPayload.createRecordIndexUpdate(recordKey, partition, fileId,
              instantTime, writesFileIdEncoding)).collect(toList()));
    }

    if (recordStatusListMap.containsKey(RecordStatus.DELETE)) {
      hoodieRecords.addAll(recordStatusListMap.get(RecordStatus.DELETE).stream()
          .map(recordKey -> HoodieMetadataPayload.createRecordIndexDelete(recordKey, partition, isPartitionedRLI)).collect(toList()));
    }

    return hoodieRecords.iterator();
  }

  /**
   * Fetch list of record keys deleted or updated in file referenced in the {@link HoodieWriteStat} passed.
   *
   * @param basePath  base path of the table.
   * @param writeStat {@link HoodieWriteStat} instance of interest.
   * @param storage   {@link HoodieStorage} instance of interest.
   * @return list of record keys deleted or updated.
   */
  @VisibleForTesting
  public static List<String> getRecordKeysDeletedOrUpdated(String basePath,
                                                           HoodieWriteStat writeStat,
                                                           HoodieStorage storage) {
    String latestFileName = FSUtils.getFileNameFromPath(writeStat.getPath());
    Set<RecordStatus> recordStatuses = new HashSet<>();
    recordStatuses.add(RecordStatus.UPDATE);
    recordStatuses.add(RecordStatus.DELETE);
    // for secondary index, we are interested in UPDATES and DELETES.
    return getRecordKeyStatuses(basePath, writeStat.getPartitionPath(), latestFileName, writeStat.getPrevBaseFile(), storage,
        recordStatuses).values().stream().flatMap((Function<List<String>, Stream<String>>) Collection::stream).collect(toList());
  }

  /**
   * Fetch list of record keys deleted or updated in file referenced in the {@link HoodieWriteStat} passed.
   *
   * @param basePath base path of the table.
   * @param storage  {@link HoodieStorage} instance of interest.
   * @return list of record keys deleted or updated.
   */
  @VisibleForTesting
  public static Map<RecordStatus, List<String>> getRecordKeyStatuses(String basePath,
                                                                     String partition,
                                                                     String latestFileName,
                                                                     String prevFileName,
                                                                     HoodieStorage storage,
                                                                     Set<RecordStatus> recordStatusesOfInterest) {
    Set<String> recordKeysFromLatestBaseFile = getRecordKeysFromBaseFile(storage, basePath, partition, latestFileName);
    if (prevFileName == null) {
      if (recordStatusesOfInterest.contains(RecordStatus.INSERT)) {
        return Collections.singletonMap(RecordStatus.INSERT, new ArrayList<>(recordKeysFromLatestBaseFile));
      } else {
        // if this is a new base file for a new file group, everything is an insert.
        return Collections.emptyMap();
      }
    } else {
      // read from previous base file and find difference to also generate delete records.
      // we will return updates and deletes from this code block
      Set<String> recordKeysFromPreviousBaseFile = getRecordKeysFromBaseFile(storage, basePath, partition, prevFileName);
      Map<RecordStatus, List<String>> toReturn = new HashMap<>(recordStatusesOfInterest.size());
      if (recordStatusesOfInterest.contains(RecordStatus.DELETE)) {
        toReturn.put(RecordStatus.DELETE, recordKeysFromPreviousBaseFile.stream()
            .filter(recordKey -> {
              // deleted record
              return !recordKeysFromLatestBaseFile.contains(recordKey);
            }).collect(toList()));
      }

      List<String> updates = new ArrayList<>();
      List<String> inserts = new ArrayList<>();
      // do one pass and collect list of inserts and updates.
      recordKeysFromLatestBaseFile.stream().forEach(recordKey -> {
        if (recordKeysFromPreviousBaseFile.contains(recordKey)) {
          updates.add(recordKey);
        } else {
          inserts.add(recordKey);
        }
      });
      if (recordStatusesOfInterest.contains(RecordStatus.UPDATE)) {
        toReturn.put(RecordStatus.UPDATE, updates);
      }
      if (recordStatusesOfInterest.contains(RecordStatus.INSERT)) {
        toReturn.put(RecordStatus.INSERT, inserts);
      }
      return toReturn;
    }
  }

  private static Set<String> getRecordKeysFromBaseFile(HoodieStorage storage, String basePath, String partition, String fileName) {
    StoragePath dataFilePath = new StoragePath(basePath, StringUtils.isNullOrEmpty(partition) ? fileName : (partition + StoragePath.SEPARATOR) + fileName);
    FileFormatUtils fileFormatUtils = HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(HoodieFileFormat.PARQUET);
    return fileFormatUtils.readRowKeys(storage, dataFilePath);
  }

  public enum RecordStatus {
    INSERT,
    UPDATE,
    DELETE
  }

}
