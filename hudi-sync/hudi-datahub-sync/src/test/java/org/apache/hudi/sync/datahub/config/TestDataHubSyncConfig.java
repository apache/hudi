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

package org.apache.hudi.sync.datahub.config;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;
import datahub.client.rest.RestEmitter;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.Properties;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_DATABASE_NAME;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_DATASET_IDENTIFIER_CLASS;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_EMITTER_SUPPLIER_CLASS;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestDataHubSyncConfig {

  @Test
  void testGetEmitterFromSupplier() {
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SUPPLIER_CLASS.key(), DummySupplier.class.getName());
    DataHubSyncConfig syncConfig = new DataHubSyncConfig(props);
    assertNotNull(syncConfig.getRestEmitter());
  }

  @Test
  void testDatabaseNameAndTableNameWithProps() {
    String db = "db";
    String table = "table";

    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_DATABASE_NAME.key(), db);
    props.setProperty(META_SYNC_DATAHUB_TABLE_NAME.key(), table);

    DataHubSyncConfig syncConfig = new DataHubSyncConfig(props);
    DatasetUrn datasetUrn = syncConfig.getDatasetIdentifier().getDatasetUrn();

    assertEquals(String.format("%s.%s", db, table), datasetUrn.getDatasetNameEntity());

    DataHubSyncConfig.DataHubSyncConfigParams params = new DataHubSyncConfig.DataHubSyncConfigParams();
    params.databaseName = db;
    params.tableName = table;

    assertEquals(db, params.toProps().get(META_SYNC_DATAHUB_DATABASE_NAME.key()));
    assertEquals(table, params.toProps().get(META_SYNC_DATAHUB_TABLE_NAME.key()));
  }

  @Test
  void testDatabaseNameAndTableNameBackwardConfigurationCompatibility() {
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "db");
    props.setProperty(META_SYNC_TABLE_NAME.key(), "table");

    DataHubSyncConfig syncConfig = new DataHubSyncConfig(props);
    DatasetUrn datasetUrn = syncConfig.getDatasetIdentifier().getDatasetUrn();

    assertEquals("db.table", datasetUrn.getDatasetNameEntity());
  }

  @Test
  void testInstantiationWithProps() {
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_DATASET_IDENTIFIER_CLASS.key(), DummyIdentifier.class.getName());
    DataHubSyncConfig syncConfig = new DataHubSyncConfig(props);
    DatasetUrn datasetUrn = syncConfig.getDatasetIdentifier().getDatasetUrn();
    assertEquals("foo", datasetUrn.getPlatformEntity().getPlatformNameEntity());
    assertEquals("project.database.table", datasetUrn.getDatasetNameEntity());
    assertEquals(FabricType.PROD, datasetUrn.getOriginEntity());
  }

  public static class DummySupplier implements DataHubEmitterSupplier {

    @Override
    public RestEmitter get() {
      return RestEmitter.createWithDefaults();
    }
  }

  public static class DummyIdentifier extends HoodieDataHubDatasetIdentifier {

    public DummyIdentifier(Properties props) {
      super(props);
    }

    @Override
    public DatasetUrn getDatasetUrn() {
      try {
        return DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:foo,project.database.table,PROD)");
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
