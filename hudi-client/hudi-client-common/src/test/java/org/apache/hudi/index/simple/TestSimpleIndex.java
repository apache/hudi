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

package org.apache.hudi.index.simple;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieWriteableTestTable;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestSimpleIndex extends HoodieCommonTestHarness {
  private static final Schema SCHEMA = getSchemaFromResource(TestSimpleIndex.class, "/exampleSchema.avsc", true);

  @BeforeEach
  void setUp() throws Exception {
    initPath();
    initMetaClient();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testTagLocation(boolean manuallySetPartitions) throws Exception {
    String partition1 = "2016/01/31";
    String partition2 = "2016/01/26";
    String rowKey1 = UUID.randomUUID().toString();
    String rowKey2 = UUID.randomUUID().toString();
    String rowKey3 = UUID.randomUUID().toString();
    String rowKey4 = UUID.randomUUID().toString();
    String recordStr1 = "{\"_row_key\":\"" + rowKey1 + "\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"" + rowKey2 + "\",\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"" + rowKey3 + "\",\"time\":\"2016-01-26T03:16:41.415Z\",\"number\":15}";
    String recordStr4 = "{\"_row_key\":\"" + rowKey4 + "\",\"time\":\"2015-01-31T03:16:41.415Z\",\"number\":32}";
    RawTripTestPayload payload1 = new RawTripTestPayload(recordStr1);
    HoodieRecord record1 = new HoodieAvroRecord(
        new HoodieKey(payload1.getRowKey(), payload1.getPartitionPath()), payload1);
    RawTripTestPayload payload2 = new RawTripTestPayload(recordStr2);
    HoodieRecord record2 = new HoodieAvroRecord(
        new HoodieKey(payload2.getRowKey(), payload2.getPartitionPath()), payload2);
    RawTripTestPayload payload3 = new RawTripTestPayload(recordStr3);
    HoodieRecord record3 = new HoodieAvroRecord(
        new HoodieKey(payload3.getRowKey(), payload3.getPartitionPath()), payload3);
    HoodieRecord record3WithNewPartition = new HoodieAvroRecord(
        new HoodieKey(payload3.getRowKey(), partition1), payload3);
    RawTripTestPayload payload4 = new RawTripTestPayload(recordStr4);
    HoodieAvroRecord record4 = new HoodieAvroRecord(
        new HoodieKey(payload4.getRowKey(), payload4.getPartitionPath()), payload4);
    HoodieData<HoodieRecord<HoodieAvroRecord>> records = HoodieListData.eager(Arrays.asList(record1, record2, record3WithNewPartition, record4));

    HoodieWriteConfig config = makeConfig(manuallySetPartitions);
    Configuration conf = new Configuration(false);

    HoodieEngineContext context = new HoodieLocalEngineContext(metaClient.getStorageConf());
    HoodieTable table = mock(HoodieTable.class, RETURNS_DEEP_STUBS);
    when(table.getConfig()).thenReturn(config);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(table.getStorage()).thenReturn(metaClient.getStorage());
    HoodieSimpleIndex simpleIndex = new HoodieSimpleIndex(config, Option.empty());
    HoodieData<HoodieRecord<HoodieAvroRecord>> taggedRecordRDD = simpleIndex.tagLocation(records, context, table);
    assertFalse(taggedRecordRDD.collectAsList().stream().anyMatch(HoodieRecord::isCurrentLocationKnown));

    HoodieStorage hoodieStorage = new HoodieHadoopStorage(basePath, conf);
    HoodieWriteableTestTable testTable = new HoodieWriteableTestTable(basePath, hoodieStorage, metaClient, SCHEMA, null, null, Option.of(context));

    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String fileId3 = UUID.randomUUID().toString();
    TaskContextSupplier localTaskContextSupplier = new LocalTaskContextSupplier();
    StoragePath filePath1 = testTable.addCommit("001").withInserts(partition1, fileId1, Collections.singletonList(record1), localTaskContextSupplier);
    StoragePath filePath2 = testTable.addCommit("002").withInserts(partition1, fileId2, Collections.singletonList(record2), localTaskContextSupplier);
    testTable.addCommit("003").withInserts(partition2, fileId3, Collections.singletonList(record3), localTaskContextSupplier);

    String timestamp = metaClient.reloadActiveTimeline().lastInstant().get().requestedTime();
    when(table.getBaseFileOnlyView().getLatestBaseFilesBeforeOrOn(partition1, timestamp))
        .thenReturn(Stream.of(new HoodieBaseFile(hoodieStorage.getPathInfo(filePath1)), new HoodieBaseFile(hoodieStorage.getPathInfo(filePath2))));
    when(table.getBaseFileOnlyView().getLatestBaseFilesBeforeOrOn("2015/01/31", timestamp))
        .thenReturn(Stream.empty());

    taggedRecordRDD = simpleIndex.tagLocation(records, context, table);
    Map<String, Option<String>> expectedRecordKeyToFileId = new HashMap<>();
    expectedRecordKeyToFileId.put(rowKey1, Option.of(fileId1));
    expectedRecordKeyToFileId.put(rowKey2, Option.of(fileId2));
    // record3 has a new partition so will not be tagged
    expectedRecordKeyToFileId.put(rowKey3, Option.empty());
    expectedRecordKeyToFileId.put(rowKey4, Option.empty());
    Map<String, Option<String>> actualRecordKeyToFileId = taggedRecordRDD.collectAsList().stream()
        .collect(Collectors.toMap(HoodieRecord::getRecordKey, record -> record.isCurrentLocationKnown() ? Option.of(record.getCurrentLocation().getFileId()) : Option.empty()));
    assertEquals(expectedRecordKeyToFileId, actualRecordKeyToFileId);
  }

  private HoodieWriteConfig makeConfig(boolean manuallySetPartitions) {
    Properties props = new Properties();
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.SIMPLE)
            .withSimpleIndexParallelism(manuallySetPartitions ? 1 : 0)
            .build())
        .build();
  }
}

