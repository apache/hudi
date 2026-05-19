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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.sync.datahub.config.DataHubSyncConfig;

import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.datahub.DataHubTableProperties.HUDI_BASE_PATH;
import static org.apache.hudi.sync.datahub.DataHubTableProperties.HUDI_TABLE_TYPE;
import static org.apache.hudi.sync.datahub.DataHubTableProperties.HUDI_TABLE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestDataHubTableProperties {
  @Test
  void testGetTableProperties() {
    String path = "file:///tmp/path";
    Map<String, String> expected = new HashMap<>();
    expected.put(HUDI_TABLE_TYPE, "MERGE_ON_READ");
    expected.put(HUDI_TABLE_VERSION, "SIX");
    expected.put(HUDI_BASE_PATH, path);
    expected.put("spark.path", path);
    expected.put("serdeClass", ParquetHiveSerDe.class.getName());
    expected.put("spark.sql.sources.schema.numParts", "1");
    expected.put("spark.sql.sources.provider", "hudi");
    expected.put("spark.hoodie.query.as.ro.table", "false");
    expected.put("spark.sql.sources.schema.part.0", "{\"type\":\"struct\",\"fields\":[{\"name\":\"int_field\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}");
    expected.put("inputFormat", HoodieParquetInputFormat.class.getName());
    expected.put("outputFormat", MapredParquetOutputFormat.class.getName());

    TypedProperties properties = new TypedProperties();
    properties.put(META_SYNC_BASE_PATH.key(), path);
    DataHubSyncConfig config = new DataHubSyncConfig(properties);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    when(metaClient.getTableType().name()).thenReturn(HoodieTableType.MERGE_ON_READ.name());
    when(metaClient.getTableConfig().getTableVersion()).thenReturn(HoodieTableVersion.SIX);
    HoodieSchema schema = HoodieSchema.createRecord("record", null, null, Collections.singletonList(HoodieSchemaField.of("int_field", HoodieSchema.create(HoodieSchemaType.INT))));
    DataHubTableProperties.HoodieTableMetadata tableMetadata = new DataHubTableProperties.HoodieTableMetadata(metaClient, schema);
    Map<String, String> actual = DataHubTableProperties.getTableProperties(config, tableMetadata);

    assertEquals(expected, actual);
  }
}
