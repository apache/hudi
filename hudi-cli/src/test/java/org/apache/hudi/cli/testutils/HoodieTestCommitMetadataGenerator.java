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
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.testutils.HoodieTestDataGenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  public static void createCommitFileWithMetadata(String basePath, String commitTime, Configuration configuration) {
    createCommitFileWithMetadata(basePath, commitTime, configuration, Option.empty(), Option.empty());
  }

  public static void createCommitFileWithMetadata(String basePath, String commitTime, Configuration configuration,
      Option<Integer> writes, Option<Integer> updates) {
    Arrays.asList(HoodieTimeline.makeCommitFileName(commitTime), HoodieTimeline.makeInflightCommitFileName(commitTime),
        HoodieTimeline.makeRequestedCommitFileName(commitTime))
        .forEach(f -> {
          Path commitFile = new Path(
              basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + f);
          FSDataOutputStream os = null;
          try {
            FileSystem fs = FSUtils.getFs(basePath, configuration);
            os = fs.create(commitFile, true);
            // Generate commitMetadata
            HoodieCommitMetadata commitMetadata = generateCommitMetadata(basePath, commitTime, writes, updates);
            // Write empty commit metadata
            os.writeBytes(new String(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
          } catch (IOException ioe) {
            throw new HoodieIOException(ioe.getMessage(), ioe);
          } finally {
            if (null != os) {
              try {
                os.close();
              } catch (IOException e) {
                throw new HoodieIOException(e.getMessage(), e);
              }
            }
          }
        });
  }

  /**
   * Generate commitMetadata in path.
   */
  public static HoodieCommitMetadata generateCommitMetadata(String basePath, String commitTime) throws IOException {
    return generateCommitMetadata(basePath, commitTime, Option.empty(), Option.empty());
  }

  public static HoodieCommitMetadata generateCommitMetadata(String basePath, String commitTime,
      Option<Integer> writes, Option<Integer> updates) throws IOException {
    String file1P0C0 = HoodieTestUtils.createNewDataFile(basePath, DEFAULT_FIRST_PARTITION_PATH, commitTime);
    String file1P1C0 = HoodieTestUtils.createNewDataFile(basePath, DEFAULT_SECOND_PARTITION_PATH, commitTime);
    return generateCommitMetadata(new HashMap<String, List<String>>() {
      {
        put(DEFAULT_FIRST_PARTITION_PATH, CollectionUtils.createImmutableList(file1P0C0));
        put(DEFAULT_SECOND_PARTITION_PATH, CollectionUtils.createImmutableList(file1P1C0));
      }
    }, writes, updates);
  }

  /**
   * Method to generate commit metadata.
   */
  private static HoodieCommitMetadata generateCommitMetadata(Map<String, List<String>> partitionToFilePaths,
      Option<Integer> writes, Option<Integer> updates) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
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
    return metadata;
  }
}
