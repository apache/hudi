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
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;

public class BaseFileRecordParsingUtils {

  /**
   * Generates RLI Metadata records for base files.
   * If base file is a added to a new file group, all record keys are treated as inserts.
   * If a base file is added to an existing file group, we read previous base file in addition to the latest base file of interest. Find deleted records and generate RLI Metadata records
   * for the same in addition to new insert records.
   * @param basePath base path of the table.
   * @param writeStat {@link HoodieWriteStat} of interest.
   * @param writesFileIdEncoding fileID encoding for the table.
   * @param instantTime instant time of interest.
   * @param storage instance of {@link HoodieStorage}.
   * @return Iterator of {@link HoodieRecord}s for RLI Metadata partition.
   * @throws IOException
   */
  public static Iterator<HoodieRecord> generateRLIMetadataHoodieRecordsForBaseFile(String basePath,
                                                                                   HoodieWriteStat writeStat,
                                                                                   Integer writesFileIdEncoding,
                                                                                   String instantTime,
                                                                                   HoodieStorage storage) throws IOException {
    String partition = writeStat.getPartitionPath();
    String latestFileName = FSUtils.getFileNameFromPath(writeStat.getPath());
    String previousFileName = writeStat.getPrevBaseFile();
    String fileId = FSUtils.getFileId(latestFileName);
    Set<String> recordKeysFromLatestBaseFile = getRecordKeysFromBaseFile(storage, basePath, partition, latestFileName);
    if (previousFileName == null) {
      return recordKeysFromLatestBaseFile.stream().map(recordKey -> (HoodieRecord)HoodieMetadataPayload.createRecordIndexUpdate(recordKey, partition, fileId,
          instantTime, writesFileIdEncoding)).collect(toList()).iterator();
    } else {
      // read from previous base file and find difference to also generate delete records.
      // we will return new inserts and deletes from this code block.
      // this code path will be exercised for these cases: COW merge, MOR compaction and small file handling cases for MOR.
      Set<String> recordKeysFromPreviousBaseFile = getRecordKeysFromBaseFile(storage, basePath, partition, previousFileName);
      List<HoodieRecord> toReturn = recordKeysFromPreviousBaseFile.stream()
          .filter(recordKey -> {
            // deleted record
            return !recordKeysFromLatestBaseFile.contains(recordKey);
          }).map(recordKey -> HoodieMetadataPayload.createRecordIndexDelete(recordKey)).collect(toList());

      toReturn.addAll(recordKeysFromLatestBaseFile.stream()
          .filter(recordKey -> {
            // new inserts
            return !recordKeysFromPreviousBaseFile.contains(recordKey);
          }).map(recordKey ->
              HoodieMetadataPayload.createRecordIndexUpdate(recordKey, partition, fileId,
                  instantTime, writesFileIdEncoding)).collect(toList()));
      return toReturn.iterator();
    }
  }

  /**
   * Fetch list of record keys deleted or updated in file referenced in the {@link HoodieWriteStat} passed.
   * @param basePath base path of the table.
   * @param writeStat {@link HoodieWriteStat} instance of interest.
   * @param storage {@link HoodieStorage} instance of interest.
   * @return list of record keys deleted or updated.
   * @throws IOException
   */
  public static List<String> getRecordKeysDeletedOrUpdated(String basePath,
                                                           HoodieWriteStat writeStat,
                                                           HoodieStorage storage) throws IOException {
    String partition = writeStat.getPartitionPath();
    String latestFileName = FSUtils.getFileNameFromPath(writeStat.getPath());
    String previousFileName = writeStat.getPrevBaseFile();
    Set<String> recordKeysFromLatestBaseFile = getRecordKeysFromBaseFile(storage, basePath, partition, latestFileName);
    if (previousFileName == null) {
      // if this is a new base file for a new file group, everything is an insert.
      return Collections.emptyList();
    } else {
      // read from previous base file and find difference to also generate delete records.
      // we will return updates and deletes from this code block
      Set<String> recordKeysFromPreviousBaseFile = getRecordKeysFromBaseFile(storage, basePath, partition, previousFileName);
      List<String> toReturn = recordKeysFromPreviousBaseFile.stream()
          .filter(recordKey -> {
            // deleted record
            return !recordKeysFromLatestBaseFile.contains(recordKey);
          }).collect(toList());

      toReturn.addAll(recordKeysFromLatestBaseFile.stream()
          .filter(recordKey -> {
            // updates
            return recordKeysFromPreviousBaseFile.contains(recordKey);
          }).collect(toList()));
      return toReturn;
    }
  }

  private static Set<String> getRecordKeysFromBaseFile(HoodieStorage storage, String basePath, String partition, String fileName) throws IOException {
    StoragePath dataFilePath = new StoragePath(basePath, StringUtils.isNullOrEmpty(partition) ? fileName : (partition + Path.SEPARATOR) + fileName);
    FileFormatUtils fileFormatUtils = HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(HoodieFileFormat.PARQUET);
    return fileFormatUtils.readRowKeys(storage, dataFilePath);
  }
}
