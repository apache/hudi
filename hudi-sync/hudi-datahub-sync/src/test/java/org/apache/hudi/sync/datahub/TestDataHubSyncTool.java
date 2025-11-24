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

package org.apache.hudi.sync.datahub;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.sync.common.HoodieSyncTool;
import org.apache.hudi.sync.common.util.SyncUtilHelpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.Collections;
import java.util.Properties;

import static org.apache.hudi.common.config.HoodieCommonConfig.META_SYNC_BASE_PATH_KEY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestDataHubSyncTool extends HoodieCommonTestHarness {
  @Test
  void validateInitThroughSyncTool() throws Exception {
    String path = "file:///tmp/path";
    Configuration configuration = new Configuration();
    TypedProperties typedProperties = new TypedProperties();
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieSyncTool syncTool = SyncUtilHelpers.instantiateMetaSyncTool(
        DataHubSyncTool.class.getName(),
        typedProperties,
        configuration,
        new Path(path).getFileSystem(configuration),
        path,
        "PARQUET",
        Option.of(mockMetaClient));
    assertTrue(syncTool instanceof DataHubSyncTool);
    syncTool.close();
  }

  @Test
  void validatePropsConstructor() throws Exception {
    initMetaClient();
    TypedProperties typedProperties = new TypedProperties();
    typedProperties.setProperty(META_SYNC_BASE_PATH_KEY, metaClient.getBasePath().toString());
    assertDoesNotThrow(() -> {
      HoodieSyncTool syncTool = new DataHubSyncTool(typedProperties);
      syncTool.close();
    });
  }

  @Test
  void testSyncHoodieTable_actualLines() {
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockMetaClient.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);
    HoodieTableConfig mockTableConfig = mock(HoodieTableConfig.class);
    when(mockTableConfig.getTableVersion()).thenReturn(HoodieTableVersion.current());
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    HoodieSchema messageType = HoodieSchema.createRecord("record", null, null,
        Collections.singletonList(HoodieSchemaField.of("int_field", HoodieSchema.create(HoodieSchemaType.INT))));

    try (MockedConstruction<DataHubSyncClient> mocked = org.mockito.Mockito.mockConstruction(DataHubSyncClient.class, (mock, context) -> {
      when(mock.getTableName()).thenReturn("test_table");
      when(mock.getStorageSchema()).thenReturn(messageType);
    })) {
      DataHubSyncTool tool = new DataHubSyncTool(new Properties(), null, Option.of(mockMetaClient));
      tool.syncHoodieTable();

      DataHubSyncClient mockClient = mocked.constructed().get(0);
      verify(mockClient).updateTableSchema("test_table", null, null);
      verify(mockClient).updateLastCommitTimeSynced("test_table");
      verify(mockClient).close();
    }
  }
}
