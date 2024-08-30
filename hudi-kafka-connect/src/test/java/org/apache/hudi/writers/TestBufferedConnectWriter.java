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

package org.apache.hudi.writers;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.connect.writers.BufferedConnectWriter;
import org.apache.hudi.connect.writers.KafkaConnectConfigs;
import org.apache.hudi.schema.SchemaProvider;
import org.apache.hudi.storage.StorageConfiguration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

public class TestBufferedConnectWriter {

  private static final int NUM_RECORDS = 10;
  private static final String COMMIT_TIME = "101";

  private HoodieJavaWriteClient mockHoodieJavaWriteClient;
  private HoodieJavaEngineContext javaEngineContext;
  private KafkaConnectConfigs configs;
  private HoodieWriteConfig writeConfig;
  private SchemaProvider schemaProvider;

  @BeforeEach
  public void setUp() throws Exception {
    mockHoodieJavaWriteClient = mock(HoodieJavaWriteClient.class);
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    javaEngineContext = new HoodieJavaEngineContext(storageConf);
    configs = KafkaConnectConfigs.newBuilder().build();
    schemaProvider = new TestAbstractConnectWriter.TestSchemaProvider();
    writeConfig = HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withPath("/tmp")
        .withSchema(schemaProvider.getSourceSchema().toString())
        .build();
  }

  @Test
  public void testSimpleWriteAndFlush() throws Exception {
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {partitionPath});
    List<HoodieRecord> records = dataGen.generateInserts(COMMIT_TIME, NUM_RECORDS);

    BufferedConnectWriter writer = new BufferedConnectWriter(
        javaEngineContext,
        mockHoodieJavaWriteClient,
        COMMIT_TIME,
        configs,
        writeConfig,
        null,
        schemaProvider);

    for (int i = 0; i < NUM_RECORDS; i++) {
      writer.writeHudiRecord(records.get(i));
    }
    Mockito.verify(mockHoodieJavaWriteClient, times(0))
        .bulkInsertPreppedRecords(anyList(), eq(COMMIT_TIME), eq(Option.empty()));

    writer.flushRecords();
    final ArgumentCaptor<List<HoodieRecord>> actualRecords = ArgumentCaptor.forClass(List.class);
    Mockito.verify(mockHoodieJavaWriteClient, times(1))
        .bulkInsertPreppedRecords(actualRecords.capture(), eq(COMMIT_TIME), eq(Option.empty()));

    actualRecords.getValue().sort(Comparator.comparing(HoodieRecord::getRecordKey));
    records.sort(Comparator.comparing(HoodieRecord::getRecordKey));

    assertEquals(records, actualRecords.getValue());
  }
}
