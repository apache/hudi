/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bucket;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestSQL;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test cases for {@link BucketStreamWriteFunction}.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestBucketStreamWrite {

  private static final Map<String, String> EXPECTED = new HashMap<>();

  static {
    EXPECTED.put("par1", "[id1,par1,id1,Danny,23,1000,par1, id2,par1,id2,Stephen,33,2000,par1]");
    EXPECTED.put("par2", "[id3,par2,id3,Julian,53,3000,par2, id4,par2,id4,Fabian,31,4000,par2]");
    EXPECTED.put("par3", "[id5,par3,id5,Sophia,18,5000,par3, id6,par3,id6,Emma,20,6000,par3]");
    EXPECTED.put("par4", "[id7,par4,id7,Bob,44,7000,par4, id8,par4,id8,Han,56,8000,par4]");
  }

  @TempDir
  File tempFile;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testBucketStreamWriteAfterRollbackFirstFileGroupCreation(boolean isCow) throws Exception {
    // this test is to ensure that the correct fileId can be fetched when recovering from a rollover when a new
    // fileGroup is created for a bucketId
    String tablePath = tempFile.getAbsolutePath();
    doWrite(tablePath, isCow);
    doDeleteCommit(tablePath, isCow);
    doWrite(tablePath, isCow);
    doWrite(tablePath, isCow);

    if (isCow) {
      TestData.checkWrittenData(tempFile, EXPECTED, 4);
    } else {
      HoodieStorage storage = HoodieTestUtils.getStorage(tempFile.getAbsolutePath());
      TestData.checkWrittenDataMOR(storage, tempFile, EXPECTED, 4);
    }
  }

  private static void doDeleteCommit(String tablePath, boolean isCow) throws Exception {
    // create metaClient
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(tablePath);

    // should only contain one instant
    HoodieTimeline activeCompletedTimeline = metaClient.getActiveTimeline().filterCompletedInstants();
    assertEquals(1, activeCompletedTimeline.getInstants().size());

    // rollback path structure: tablePath/.hoodie/.temp/${commitInstant}/${partition}/${fileGroup}_${fileInstant}.parquet.marker.APPEND
    HoodieInstant instant = activeCompletedTimeline.getInstants().get(0);
    String commitInstant = instant.getTimestamp();
    String filename = activeCompletedTimeline.getInstants().get(0).getFileName();

    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
        .fromBytes(metaClient.getActiveTimeline().getInstantDetails(instant).get(),
            HoodieCommitMetadata.class);

    // delete successful commit to simulate an unsuccessful write
    HoodieStorage storage = metaClient.getStorage();
    StoragePath path = new StoragePath(metaClient.getMetaPath(), filename);
    storage.deleteDirectory(path);

    // marker types are different for COW and MOR
    IOType ioType = isCow ? IOType.CREATE : IOType.APPEND;

    commitMetadata.getFileIdAndRelativePaths().forEach((fileId, relativePath) -> {
      // hacky way to reconstruct markers ¯\_(ツ)_/¯
      String[] partitionFileNameSplit = relativePath.split("/");
      String fileInstant = FSUtils.getCommitTime(partitionFileNameSplit[1]);
      String partition = partitionFileNameSplit[0];
      String writeToken = isCow ? getWriteToken(partitionFileNameSplit[1]) : FSUtils.getWriteTokenFromLogPath(new StoragePath(relativePath));
      try {
        FileCreateUtils.createMarkerFile(tablePath, partition, commitInstant, fileInstant, fileId, ioType, writeToken);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private static String getWriteToken(String relativeFilePath) {
    Pattern writeTokenPattern = Pattern.compile("_((\\d+)-(\\d+)-(\\d+))_");
    Matcher matcher = writeTokenPattern.matcher(relativeFilePath);
    if (!matcher.find()) {
      throw new RuntimeException("Invalid relative file path: " + relativeFilePath);
    }
    return matcher.group(1);
  }

  private static void doWrite(String path, boolean isCow) throws InterruptedException, ExecutionException {
    // create hoodie table and perform writes
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), path);

    // use bucket index
    options.put(FlinkOptions.TABLE_TYPE.key(), isCow ? FlinkOptions.TABLE_TYPE_COPY_ON_WRITE : FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    options.put(FlinkOptions.INDEX_TYPE.key(), IndexType.BUCKET.name());
    options.put(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key(), "1");

    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);
    tableEnv.executeSql(TestSQL.INSERT_T1).await();

    // wait for the asynchronous commit to finish
    TimeUnit.SECONDS.sleep(3);
  }
}