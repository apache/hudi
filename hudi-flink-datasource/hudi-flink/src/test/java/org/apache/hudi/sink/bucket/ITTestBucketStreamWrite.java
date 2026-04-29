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

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.HoodieDataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestSQL;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test cases for {@link BucketStreamWriteFunction}.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestBucketStreamWrite {

  private static final Map<String, List<String>> EXPECTED_AS_LIST = new HashMap<>();
  private static final Map<String, String> EXPECTED = new HashMap<>();

  static {
    EXPECTED_AS_LIST.put("par1", Arrays.asList("id1,par1,id1,Danny,23,1000,par1", "id2,par1,id2,Stephen,33,2000,par1"));
    EXPECTED_AS_LIST.put("par2", Arrays.asList("id3,par2,id3,Julian,53,3000,par2", "id4,par2,id4,Fabian,31,4000,par2"));
    EXPECTED_AS_LIST.put("par3", Arrays.asList("id5,par3,id5,Sophia,18,5000,par3", "id6,par3,id6,Emma,20,6000,par3"));
    EXPECTED_AS_LIST.put("par4", Arrays.asList("id7,par4,id7,Bob,44,7000,par4", "id8,par4,id8,Han,56,8000,par4"));

    for (Map.Entry<String, List<String>> entry : EXPECTED_AS_LIST.entrySet()) {
      String value = entry.getValue().stream().map(Object::toString).collect(Collectors.joining(", "));
      EXPECTED.put(entry.getKey(), "[" + value + "]");
    }
  }

  private static final List<String> EXPECTED_ROWS = Arrays.asList(
      "id1,Danny,23,1970-01-01T00:00:01,par1",
      "id2,Stephen,33,1970-01-01T00:00:02,par1",
      "id3,Julian,53,1970-01-01T00:00:03,par2",
      "id4,Fabian,31,1970-01-01T00:00:04,par2",
      "id5,Sophia,18,1970-01-01T00:00:05,par3",
      "id6,Emma,20,1970-01-01T00:00:06,par3",
      "id7,Bob,44,1970-01-01T00:00:07,par4",
      "id8,Han,56,1970-01-01T00:00:08,par4");

  @TempDir
  File tempFile;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testBucketStreamWriteAfterRollbackFirstFileGroupCreation(boolean isCow) throws Exception {
    // this test is to ensure that the correct fileId can be fetched when recovering from a rollover when a new
    // fileGroup is created for a bucketId
    String tablePath = tempFile.getAbsolutePath();
    doWrite(tablePath, isCow, 1);
    doDeleteCommit(tablePath);
    doWrite(tablePath, isCow, 1);
    doWrite(tablePath, isCow, 1);

    if (isCow) {
      TestData.checkWrittenData(tempFile, EXPECTED, 4);
    } else {
      TestData.checkWrittenDataMOR(tempFile, EXPECTED, 4);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testBucketWriteIntoMultipleBuckets(boolean isCow) throws Exception {
    String tablePath = tempFile.getAbsolutePath();
    doWrite(tablePath, isCow, 2);
    if (isCow) {
      TestData.checkWrittenDataCOW(tempFile, EXPECTED_AS_LIST);
    } else {
      TestData.checkWrittenDataMOR(tempFile, EXPECTED, 4);
    }
  }

  @ParameterizedTest
  @CsvSource({"true,true", "false,true", "true,false", "false,false"})
  public void testBucketWriteWithPartitionedRLI(boolean isCow, boolean partitionedTable) throws Exception {
    String tablePath = tempFile.getAbsolutePath();
    Map<String, String> customOptions = new HashMap<>();
    customOptions.put(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
    doWrite(tablePath, isCow, 2, customOptions, partitionedTable);
    checkWrittenData(tablePath, isCow);

    // do another write to trigger lazy bootstrap of rli index backend.
    doWrite(tablePath, isCow, 2, customOptions, partitionedTable);
    checkWrittenData(tablePath, isCow);

    assertRecordIndexMetadataPartitionExists(tablePath);
    String expectedPartitionPath = partitionedTable ? "par1" : "";
    Map<String, HoodieRecordGlobalLocation> locationMap = getRecordKeyIndex(
        tablePath, isCow, Arrays.asList("id1", "id2"), Option.of(expectedPartitionPath));
    assertEquals(2, locationMap.size());
    assertTrue(locationMap.values().stream()
        .allMatch(location -> location.getPartitionPath().equals(expectedPartitionPath)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testBucketWriteRestrictInsert(boolean isCow) {
    String tablePath = tempFile.getAbsolutePath();
    Map<String, String> customOptions = new HashMap<>();
    customOptions.put(FlinkOptions.OPERATION.key(), "insert");
    HoodieNotSupportedException ex =
        Assertions.assertThrows(HoodieNotSupportedException.class, () -> doWrite(tablePath, isCow, 2, customOptions));
    assertLinesMatch(Collections.singletonList("Bucket index supports only upsert operation. Please, use upsert operation or switch to another index type."),
        Collections.singletonList(ex.getMessage()));
  }

  private static void doDeleteCommit(String tablePath) throws Exception {
    // create metaClient
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(tablePath);

    // should only contain one instant
    HoodieTimeline activeCompletedTimeline = metaClient.getActiveTimeline().filterCompletedInstants();
    assertEquals(1, activeCompletedTimeline.getInstants().size());

    // rollback path structure: tablePath/.hoodie/.temp/${commitInstant}/${partition}/${fileGroup}_${fileInstant}.parquet.marker.APPEND
    HoodieInstant instant = activeCompletedTimeline.getInstants().get(0);
    String commitInstant = instant.requestedTime();
    String filename = INSTANT_FILE_NAME_GENERATOR.getFileName(activeCompletedTimeline.getInstants().get(0));

    HoodieCommitMetadata commitMetadata = metaClient.getActiveTimeline().readCommitMetadata(instant);

    // delete successful commit to simulate an unsuccessful write
    HoodieStorage storage = metaClient.getStorage();
    StoragePath path = new StoragePath(metaClient.getTimelinePath(), filename);
    storage.deleteDirectory(path);

    commitMetadata.getFileIdAndRelativePaths().forEach((fileId, relativePath) -> {
      // hacky way to reconstruct markers ¯\_(ツ)_/¯
      String[] partitionFileNameSplit = relativePath.split("/");
      String partition = partitionFileNameSplit[0];
      String fileName = partitionFileNameSplit[1];
      try {
        String markerFileName = FileCreateUtilsLegacy.markerFileName(fileName, IOType.CREATE);
        FileCreateUtilsLegacy.createMarkerFile(tablePath, partition, commitInstant, markerFileName);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private static void assertRecordIndexMetadataPartitionExists(String tablePath) {
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    String mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(tablePath);
    assertTrue(StreamerUtil.tableExists(mdtBasePath, hadoopConf),
        "Metadata table should exist for table with simple bucket RLI stream write");
    assertTrue(StreamerUtil.partitionExists(mdtBasePath, MetadataPartitionType.RECORD_INDEX.getPartitionPath(), hadoopConf),
        "RECORD_INDEX partition should exist for table with simple bucket RLI stream write");
  }

  private static void checkWrittenData(String tablePath, boolean isCow) {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tablePath);
    options.put(FlinkOptions.TABLE_TYPE.key(),
        isCow ? FlinkOptions.TABLE_TYPE_COPY_ON_WRITE : FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    options.put(FlinkOptions.INDEX_TYPE.key(), IndexType.BUCKET.name());
    tableEnv.executeSql(TestConfigurations.getCreateHoodieTableDDL("t1", options, false, "partition"));

    List<String> actual = CollectionUtil.iterableToList(() -> tableEnv.sqlQuery("select * from t1").execute().collect())
        .stream()
        .map(ITTestBucketStreamWrite::formatDataRow)
        .sorted()
        .collect(Collectors.toList());
    assertEquals(EXPECTED_ROWS, actual);
  }

  private static String formatDataRow(Row row) {
    return IntStream.range(0, row.getArity()).mapToObj(i -> row.getField(i).toString()).collect(Collectors.joining(","));
  }

  private static Map<String, HoodieRecordGlobalLocation> getRecordKeyIndex(
      String tablePath, boolean isCow, List<String> keys, Option<String> dataPartition) {
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(tablePath);
    Configuration conf = TestConfigurations.getDefaultConf(metaClient.getBasePath().toString());
    conf.set(FlinkOptions.TABLE_TYPE, isCow ? FlinkOptions.TABLE_TYPE_COPY_ON_WRITE : FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    try (HoodieTableMetadata metadataTable = metaClient.getTableFormat().getMetadataFactory().create(
        HoodieFlinkEngineContext.DEFAULT,
        metaClient.getStorage(),
        StreamerUtil.metadataConfig(conf),
        conf.get(FlinkOptions.PATH))) {
      return HoodieDataUtils.dedupeAndCollectAsMap(
          metadataTable.readRecordIndexLocationsWithKeys(HoodieListData.eager(keys), dataPartition));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void doWrite(String path, boolean isCow, int bucketNum) throws ExecutionException, InterruptedException {
    doWrite(path, isCow, bucketNum, null);
  }

  private static void doWrite(String path, boolean isCow, int bucketNum, Map<String, String> customOptions)
      throws InterruptedException, ExecutionException {
    doWrite(path, isCow, bucketNum, customOptions, true);
  }

  private static void doWrite(
      String path,
      boolean isCow,
      int bucketNum,
      Map<String, String> customOptions,
      boolean partitionedTable)
      throws InterruptedException, ExecutionException {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), path);

    if (null != customOptions) {
      options.putAll(customOptions);
    }

    // use bucket index
    options.put(FlinkOptions.TABLE_TYPE.key(), isCow ? FlinkOptions.TABLE_TYPE_COPY_ON_WRITE : FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    options.put(FlinkOptions.INDEX_TYPE.key(), IndexType.BUCKET.name());
    options.put(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key(), String.valueOf(bucketNum));

    // create hoodie table and perform writes
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL(
        "t1", options, partitionedTable, "partition");
    tableEnv.executeSql(hoodieTableDDL);
    tableEnv.executeSql(TestSQL.INSERT_T1).await();

    // wait for the asynchronous commit to finish
    TimeUnit.SECONDS.sleep(3);
  }
}
