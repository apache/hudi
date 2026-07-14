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

package org.apache.hudi.hive.ddl;

import org.apache.hudi.hive.HiveSyncConfig;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Properties;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TestHMSDDLExecutor {

  @Test
  void testCreateTableCanonicalizesLocation() throws Exception {
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "testdb");
    props.setProperty(META_SYNC_TABLE_NAME.key(), "ho_set_data_pymt_pgm");
    props.setProperty(META_SYNC_BASE_PATH.key(), "gs://bucket/db/ho_set_data_pymt_pgm");
    HiveSyncConfig config = new HiveSyncConfig(props);
    // Force a malformed (consecutive/trailing slash) base path *after* construction, bypassing the
    // config-level normalization, so this asserts the executor canonicalizes at the LOCATION
    // boundary itself (strict object stores such as GCS reject "//").
    config.setValue(META_SYNC_BASE_PATH, "gs://bucket/db/ho_set_data_pymt_pgm//");

    IMetaStoreClient client = mock(IMetaStoreClient.class);
    HMSDDLExecutor executor = new HMSDDLExecutor(config, client);

    MessageType schema = MessageTypeParser.parseMessageType("message test { optional binary field (UTF8); }");
    executor.createTable("ho_set_data_pymt_pgm", schema,
        "org.apache.hudi.hadoop.HoodieParquetInputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        new HashMap<>(), new HashMap<>());

    ArgumentCaptor<Table> captor = ArgumentCaptor.forClass(Table.class);
    verify(client).createTable(captor.capture());
    assertEquals("gs://bucket/db/ho_set_data_pymt_pgm", captor.getValue().getSd().getLocation(),
        "table LOCATION must be canonicalized (no consecutive/trailing slashes)");
  }
}
