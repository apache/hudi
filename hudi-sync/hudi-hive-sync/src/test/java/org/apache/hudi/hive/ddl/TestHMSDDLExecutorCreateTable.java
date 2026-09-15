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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.hive.HiveSyncConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TestHMSDDLExecutorCreateTable {

  @Test
  void createTableSetsCreateTimeInSeconds() throws Exception {
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "testdb");
    props.setProperty(META_SYNC_BASE_PATH.key(), "/tmp/test_table");
    HiveSyncConfig config = new HiveSyncConfig(props, new Configuration());

    IMetaStoreClient client = mock(IMetaStoreClient.class);
    HMSDDLExecutor executor = new HMSDDLExecutor(config, client);

    HoodieSchema schema = HoodieSchema.createRecord("test_record", null, null,
        Collections.singletonList(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT))));

    long beforeSec = System.currentTimeMillis() / 1000;
    executor.createTable("test_table", schema, "input.Format", "output.Format", "serde.Class",
        new HashMap<>(), new HashMap<>());
    long afterSec = System.currentTimeMillis() / 1000;

    ArgumentCaptor<Table> captor = ArgumentCaptor.forClass(Table.class);
    verify(client).createTable(captor.capture());
    int createTime = captor.getValue().getCreateTime();

    // createTime must be epoch seconds within the call window.
    assertTrue(createTime >= beforeSec && createTime <= afterSec,
        "createTime should be epoch seconds within [" + beforeSec + ", " + afterSec + "] but was " + createTime);
  }
}
