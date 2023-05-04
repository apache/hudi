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

package org.apache.hudi.io;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.RawTripTestPayloadKeyGenerator;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.avro.HoodieAvroUtils.addMetadataFields;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.getCommitTimeAtUTC;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieMergedReadHandle extends SparkClientFunctionalTestHarness {

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testReadLatestRecords(HoodieTableType tableType) throws IOException {
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType);
    HoodieWriteConfig writeConfig = getConfigBuilder(true)
        .withProperties(getKeyGenProps())
        .withSchema(RawTripTestPayload.JSON_DATA_SCHEMA_STR)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder()
            .withPayloadClass(RawTripTestPayload.class.getName())
            .withPayloadOrderingField("number").build())
        .withPath(tempDir.toString())
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(writeConfig);
    List<HoodieRecord> insertsAtT0 = getRecordsAtTimestamp(0);
    String commitTime0 = getCommitTimeAtUTC(0);
    client.startCommitWithTime(commitTime0);
    client.upsert(jsc().parallelize(insertsAtT0, 1), commitTime0);

    List<HoodieRecord> updatesAtT1 = getRecordsAtTimestamp(1);
    String commitTime1 = getCommitTimeAtUTC(1);
    client.startCommitWithTime(commitTime1);
    client.upsert(jsc().parallelize(updatesAtT1, 1), commitTime1);

    client.close();

    final String partition = "2016/01/31";
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(writeConfig, context(), metaClient);
    List<Pair<String, String>> partitionPathAndFileIDPairs = table.getHoodieView()
        .getLatestBaseFiles(partition)
        .map(baseFile -> Pair.of(partition, baseFile.getFileId()))
        .collect(Collectors.toList());
    assertEquals(1, partitionPathAndFileIDPairs.size());
    validate(writeConfig, table, partitionPathAndFileIDPairs.get(0), commitTime0, partition, 0);
    validate(writeConfig, table, partitionPathAndFileIDPairs.get(0), commitTime1, partition, 1);
  }

  private void validate(HoodieWriteConfig writeConfig, HoodieTable table, Pair<String, String> partitionPathAndFileIDPair,
      String commitTime, String partition, int timestamp) throws IOException {
    HoodieMergedReadHandle mergedReadHandle = new HoodieMergedReadHandle<>(writeConfig, Option.of(commitTime), table, partitionPathAndFileIDPair);
    List<HoodieRecord> mergedRecords = mergedReadHandle.getMergedRecords();
    assertEquals(1, mergedRecords.size());
    HoodieRecord r = mergedRecords.get(0)
        .wrapIntoHoodieRecordPayloadWithParams(
            addMetadataFields(RawTripTestPayload.JSON_DATA_SCHEMA, writeConfig.allowOperationMetadataField()),
            writeConfig.getProps(), Option.empty(), writeConfig.allowOperationMetadataField(), Option.empty(), false, Option.empty());
    assertEquals("1", r.getRecordKey());
    assertEquals(partition, r.getPartitionPath());
    assertEquals(RawTripTestPayload.class, r.getData().getClass());
    Map<?, ?> data = ((RawTripTestPayload) r.getData()).getJsonDataAsMap();
    assertEquals("1", data.get("_row_key"));
    assertEquals("2016-01-31T00:00:00.000Z", data.get("time"));
    assertEquals(timestamp, data.get("number"));
  }

  private List<HoodieRecord> getRecordsAtTimestamp(int timestamp) throws IOException {
    String recordStr1 = "{\"_row_key\":\"1\",\"time\":\"2016-01-31T00:00:00.000Z\",\"number\":" + timestamp + "}";
    return Collections.singletonList(new RawTripTestPayload(recordStr1).toHoodieRecord());
  }

  private Properties getKeyGenProps() {
    Properties properties = new Properties();
    properties.put("hoodie.datasource.write.keygenerator.class", RawTripTestPayloadKeyGenerator.class.getName());
    properties.put("hoodie.datasource.write.recordkey.field", "_row_key");
    properties.put("hoodie.datasource.write.partitionpath.field", "time");
    properties.put("hoodie.datasource.write.precombine.field", "number");
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "time");
    properties.put(HoodieTableConfig.PRECOMBINE_FIELD.key(), "number");
    return properties;
  }
}
