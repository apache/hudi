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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.hudi.common.testutils.FileCreateUtilsLegacy.baseFileName;
import static org.apache.hudi.common.testutils.HoodieTestUtils.COMMIT_METADATA_SER_DE;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.util.CollectionUtils.createImmutableList;

/**
 * Class to be used in tests to keep generating test inserts and updates against a corpus.
 */
public class HoodieTestCommitMetadataGenerator extends HoodieTestDataGenerator {

  // default commit metadata value
  public static final String DEFAULT_PATH = "path";
  public static final String DEFAULT_FILEID = "fileId";
  public static final int DEFAULT_TOTAL_WRITE_BYTES = 50;
  public static final String DEFAULT_PRE_COMMIT = "commit-1";
  public static final int DEFAULT_NUM_WRITES = 10;
  public static final int DEFAULT_NUM_UPDATE_WRITES = 15;
  public static final int DEFAULT_TOTAL_LOG_BLOCKS = 1;
  public static final int DEFAULT_TOTAL_LOG_RECORDS = 10;
  public static final int DEFAULT_OTHER_VALUE = 0;
  public static final String DEFAULT_NULL_VALUE = "null";

  /**
   * Create a commit file with default CommitMetadata.
   */
  public static void createCommitFileWithMetadata(String basePath, String commitTime, StorageConfiguration<?> configuration) throws Exception {
    createCommitFileWithMetadata(basePath, commitTime, configuration, Option.empty(), Option.empty());
  }

  public static void createCommitFileWithMetadata(String basePath, String commitTime, StorageConfiguration<?> configuration,
      Option<Integer> writes, Option<Integer> updates) throws Exception {
    createCommitFileWithMetadata(basePath, commitTime, configuration, writes, updates, Collections.emptyMap());
  }

  public static void createCommitFileWithMetadata(String basePath, String commitTime, StorageConfiguration<?> configuration,
                                                  Option<Integer> writes, Option<Integer> updates, Map<String, String> extraMetadata) throws Exception {
    createCommitFileWithMetadata(basePath, commitTime, configuration, UUID.randomUUID().toString(),
        UUID.randomUUID().toString(), writes, updates, extraMetadata);
  }

  public static void createCommitFileWithMetadata(String basePath, String commitTime, StorageConfiguration<?> configuration,
      String fileId1, String fileId2, Option<Integer> writes, Option<Integer> updates) throws Exception {
    createCommitFileWithMetadata(basePath, commitTime, configuration, fileId1, fileId2, writes, updates, Collections.emptyMap());
  }

  public static void createCommitFileWithMetadata(String basePath, String commitTime, StorageConfiguration<?> configuration,
      String fileId1, String fileId2, Option<Integer> writes, Option<Integer> updates, Map<String, String> extraMetadata) throws Exception {
    List<String> commitFileNames = Arrays.asList(
        INSTANT_FILE_NAME_GENERATOR.makeCommitFileName(commitTime + "_" + InProcessTimeGenerator.createNewInstantTime()),
        INSTANT_FILE_NAME_GENERATOR.makeInflightCommitFileName(commitTime),
        INSTANT_FILE_NAME_GENERATOR.makeRequestedCommitFileName(commitTime));
    for (String name : commitFileNames) {
      HoodieCommitMetadata commitMetadata =
              generateCommitMetadata(basePath, commitTime, fileId1, fileId2, writes, updates, extraMetadata, true);
      createFileWithMetadata(basePath, configuration, name, commitMetadata);
    }
  }

  public static void createCommitFileWithMetadata(String basePath, String commitTime, StorageConfiguration<?> configuration,
                                                  String fileId1, String fileId2, Option<Integer> writes,
                                                  Option<Integer> updates, Map<String, String> extraMetadata,
                                                  boolean setDefaultFileId) throws Exception {
    List<String> commitFileNames = Arrays.asList(
        INSTANT_FILE_NAME_GENERATOR.makeCommitFileName(commitTime + "_" + InProcessTimeGenerator.createNewInstantTime()),
        INSTANT_FILE_NAME_GENERATOR.makeInflightCommitFileName(commitTime),
        INSTANT_FILE_NAME_GENERATOR.makeRequestedCommitFileName(commitTime));
    for (String name : commitFileNames) {
      HoodieCommitMetadata commitMetadata =
          generateCommitMetadata(basePath, commitTime, fileId1, fileId2, writes, updates, extraMetadata, setDefaultFileId);
      createFileWithMetadata(basePath, configuration, name, commitMetadata);
    }
  }

  static <T> void createFileWithMetadata(String basePath, StorageConfiguration<?> configuration, String name, T metadata) throws IOException {
    Path commitFilePath = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + name);
    try (OutputStream os = HadoopFSUtils.getFs(basePath, configuration).create(commitFilePath, true)) {
      COMMIT_METADATA_SER_DE.getInstantWriter(metadata).get().writeToStream(os);
    }
  }

  /**
   * Generate commitMetadata in path.
   */
  public static HoodieCommitMetadata generateCommitMetadata(String basePath, String commitTime) throws Exception {
    return generateCommitMetadata(basePath, commitTime, Option.empty(), Option.empty());
  }

  public static HoodieCommitMetadata generateCommitMetadata(String basePath, String commitTime,
      Option<Integer> writes, Option<Integer> updates) throws Exception {
    return generateCommitMetadata(basePath, commitTime, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
        writes, updates);
  }

  public static HoodieCommitMetadata generateCommitMetadata(String basePath, String commitTime, String fileId1,
      String fileId2, Option<Integer> writes, Option<Integer> updates) throws Exception {
    return generateCommitMetadata(basePath, commitTime, fileId1, fileId2, writes, updates, Collections.emptyMap(), true);
  }

  public static HoodieCommitMetadata generateCommitMetadata(String basePath, String commitTime, String fileId1,
                                                            String fileId2, Option<Integer> writes,
                                                            Option<Integer> updates, Map<String, String> extraMetadata,
                                                            boolean setDefaultFileId) throws Exception {
    FileCreateUtilsLegacy.createBaseFile(basePath, DEFAULT_FIRST_PARTITION_PATH, commitTime, fileId1);
    FileCreateUtilsLegacy.createBaseFile(basePath, DEFAULT_SECOND_PARTITION_PATH, commitTime, fileId2);
    return generateCommitMetadata(new HashMap<String, List<String>>() {
      {
        put(DEFAULT_FIRST_PARTITION_PATH, createImmutableList(baseFileName(DEFAULT_FIRST_PARTITION_PATH, fileId1)));
        put(DEFAULT_SECOND_PARTITION_PATH, createImmutableList(baseFileName(DEFAULT_SECOND_PARTITION_PATH, fileId2)));
      }
    }, writes, updates, extraMetadata, setDefaultFileId);
  }

  private static HoodieCommitMetadata generateCommitMetadata(Map<String, List<String>> partitionToFilePaths,
      Option<Integer> writes, Option<Integer> updates) {
    return generateCommitMetadata(partitionToFilePaths, writes, updates, Collections.emptyMap(), true);
  }

  /**
   * Method to generate commit metadata.
   */
  private static HoodieCommitMetadata generateCommitMetadata(Map<String, List<String>> partitionToFilePaths,
      Option<Integer> writes, Option<Integer> updates, Map<String, String> extraMetadata, boolean setDefaultFileId) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    for (Map.Entry<String, String> entry: extraMetadata.entrySet()) {
      metadata.addMetadata(entry.getKey(), entry.getValue());
    }
    partitionToFilePaths.forEach((key, value) -> value.forEach(f -> {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPartitionPath(key);
      writeStat.setPath(DEFAULT_PATH);
      writeStat.setFileId(setDefaultFileId ? DEFAULT_FILEID : FSUtils.getFileIdFromFileName(f));
      writeStat.setTotalWriteBytes(DEFAULT_TOTAL_WRITE_BYTES);
      writeStat.setPrevCommit(DEFAULT_PRE_COMMIT);
      writeStat.setNumWrites(writes.orElse(DEFAULT_NUM_WRITES));
      writeStat.setNumUpdateWrites(updates.orElse(DEFAULT_NUM_UPDATE_WRITES));
      writeStat.setTotalLogBlocks(DEFAULT_TOTAL_LOG_BLOCKS);
      writeStat.setTotalLogRecords(DEFAULT_TOTAL_LOG_RECORDS);
      metadata.addWriteStat(key, writeStat);
    }));
    return metadata;
  }

}
