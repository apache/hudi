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

package org.apache.hudi.cli.testutils;

import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.apache.hudi.common.testutils.FileCreateUtils.baseFileName;
import static org.apache.hudi.common.util.CollectionUtils.createImmutableList;

public class HoodieTestReplaceCommitMetadataGenerator extends HoodieTestCommitMetadataGenerator {
  public static void createReplaceCommitFileWithMetadata(String basePath, String commitTime, Option<Integer> writes, Option<Integer> updates,
                                                         HoodieTableMetaClient metaclient) throws Exception {

    HoodieReplaceCommitMetadata replaceMetadata = generateReplaceCommitMetadata(basePath, commitTime, UUID.randomUUID().toString(),
        UUID.randomUUID().toString(), writes, updates);
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = getHoodieRequestedReplaceMetadata();

    HoodieTestTable.of(metaclient).addReplaceCommit(commitTime, Option.ofNullable(requestedReplaceMetadata), Option.empty(), replaceMetadata);
  }

  private static HoodieRequestedReplaceMetadata getHoodieRequestedReplaceMetadata() {
    return HoodieRequestedReplaceMetadata.newBuilder()
        .setOperationType(WriteOperationType.INSERT_OVERWRITE.toString())
        .setVersion(1)
        .setExtraMetadata(Collections.emptyMap())
        .build();
  }

  private static HoodieReplaceCommitMetadata generateReplaceCommitMetadata(String basePath, String commitTime, String fileId1, String fileId2, Option<Integer> writes, Option<Integer> updates)
      throws Exception {
    FileCreateUtils.createBaseFile(basePath, DEFAULT_FIRST_PARTITION_PATH, commitTime, fileId1);
    FileCreateUtils.createBaseFile(basePath, DEFAULT_SECOND_PARTITION_PATH, commitTime, fileId2);
    return generateReplaceCommitMetadata(new HashMap<String, List<String>>() {
      {
        put(DEFAULT_FIRST_PARTITION_PATH, createImmutableList(baseFileName(DEFAULT_FIRST_PARTITION_PATH, fileId1)));
        put(DEFAULT_SECOND_PARTITION_PATH, createImmutableList(baseFileName(DEFAULT_SECOND_PARTITION_PATH, fileId2)));
      }
    }, writes, updates);
  }

  private static HoodieReplaceCommitMetadata generateReplaceCommitMetadata(HashMap<String, List<String>> partitionToFilePaths, Option<Integer> writes, Option<Integer> updates) {
    HoodieReplaceCommitMetadata metadata = new HoodieReplaceCommitMetadata();
    partitionToFilePaths.forEach((key, value) -> value.forEach(f -> {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPartitionPath(key);
      writeStat.setPath(DEFAULT_PATH);
      writeStat.setFileId(DEFAULT_FILEID);
      writeStat.setTotalWriteBytes(DEFAULT_TOTAL_WRITE_BYTES);
      writeStat.setPrevCommit(DEFAULT_PRE_COMMIT);
      writeStat.setNumWrites(writes.orElse(DEFAULT_NUM_WRITES));
      writeStat.setNumUpdateWrites(updates.orElse(DEFAULT_NUM_UPDATE_WRITES));
      writeStat.setTotalLogBlocks(DEFAULT_TOTAL_LOG_BLOCKS);
      writeStat.setTotalLogRecords(DEFAULT_TOTAL_LOG_RECORDS);
      metadata.addWriteStat(key, writeStat);
    }));
    metadata.setPartitionToReplaceFileIds(new HashMap<String, List<String>>() {
      {
        //TODO fix
        put(DEFAULT_FIRST_PARTITION_PATH, createImmutableList(baseFileName(DEFAULT_FIRST_PARTITION_PATH, "1")));
      }
    });
    return metadata;
  }
}
