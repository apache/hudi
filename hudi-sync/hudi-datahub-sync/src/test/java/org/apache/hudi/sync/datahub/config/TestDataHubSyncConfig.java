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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_DATABASE_NAME;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_DATASET_IDENTIFIER_CLASS;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_EMITTER_SUPPLIER_CLASS;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    DatasetUrn datasetUrn = syncConfig.datasetIdentifier.getDatasetUrn();

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
    DatasetUrn datasetUrn = syncConfig.datasetIdentifier.getDatasetUrn();

    assertEquals("db.table", datasetUrn.getDatasetNameEntity());
  }

  @Test
  void testInstantiationWithProps() {
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_DATASET_IDENTIFIER_CLASS.key(), DummyIdentifier.class.getName());
    DataHubSyncConfig syncConfig = new DataHubSyncConfig(props);
    DatasetUrn datasetUrn = syncConfig.datasetIdentifier.getDatasetUrn();
    assertEquals("foo", datasetUrn.getPlatformEntity().getPlatformNameEntity());
    assertEquals("project.database.table", datasetUrn.getDatasetNameEntity());
    assertEquals(FabricType.PROD, datasetUrn.getOriginEntity());
  }

  @Test
  void testGetEmitterWithTlsEnabledSupplier(@TempDir Path tempDir) throws IOException {
    String testServerUrl = "https://datahub.example.com";
    String testToken = "test-token-123";
    Path caCertPath = tempDir.resolve("ca-cert.pem");
    
    String testCertContent = "-----BEGIN CERTIFICATE-----\n"
        + "MIIDazCCAlOgAwIBAgIUFtGMq5vPqvNXGDCH2rJ5n0OhLuQwDQYJKoZIhvcNAQEL\n"
        + "BQAwRTELMAkGA1UEBhMCVVMxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoM\n"
        + "GEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDAeFw0yNDAxMDEwMDAwMDBaFw0yNTAx\n"
        + "MDEwMDAwMDBaMEUxCzAJBgNVBAYTAlVTMRMwEQYDVQQIDApTb21lLVN0YXRlMSEw\n"
        + "HwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQwggEiMA0GCSqGSIb3DQEB\n"
        + "AQUAA4IBDwAwggEKAoIBAQC7bthgxE4i9lRaKov+kFVWy8Bj2w7V1EhGNxUe7j5O\n"
        + "HOLx3hE2B1BcSa2no3W7XgJ9cYQ7jVaDj3bPP7vVZKQ4YCqFH9z2l1kZQKjZQ7gU\n"
        + "FpqRRZHJpYxGKwP7YsBsVz3FJKmUyGP5cFq8mLBIcLa2Y9riYZQz7Wm7VgKzR3eZ\n"
        + "YxDF9N5PbQnQt4bCFed0TlcX5gYFUqiRW5AUe5mECjH7hcWGaAXQBQKNbLiKVYEq\n"
        + "jO4Y3F8xZ7CiMUVLkQMjKwYGNP3iG3msFS9bTiLcHwIx2RbFjxGg+gfvZ8EiYpFE\n"
        + "ZQPvLsbmjqHc5ufhQJ3lfzYF2FjLcVx1VrsVhTQmB9EfAgMBAAGjUzBRMB0GA1Ud\n"
        + "DgQWBBTWV4Dr2cKLsHhP0Hxc+CmZx2DZBzAfBgNVHSMEGDAWgBTWV4Dr2cKLsHhP\n"
        + "0Hxc+CmZx2DZBzAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQCR\n"
        + "jf+kzYIFU+3M0hBpVz7cPYlhSI7gWdSIvY5kYhX4vvRBFGJ8OFo9flUd4vvO+GyV\n"
        + "g+UNmGpgslqYdXuFmC2IftPRgaLpohZWvmR1CYUS3AbcuWUqFKchTHPaJLZTAcNu\n"
        + "xYrqULYzsQxLb1FcXHoqntQe1YvLlkRRPkS9E3Lf0KO6dOlAwJI6qCvhzvtguinh\n"
        + "FcTnSsJl6YQF8gMRdGxGPH5OKnpVr6xVkQ0gzc7P5AdVRDkVAjI8S5tgdMjHJB0L\n"
        + "yN3nwCWMJgJqN5ZkJVz5cYtXdlfJBaDjXRbV+jpOOJRDr6Fy8PQ1EEvhQEoVHQz0\n"
        + "FKevncF0IEv9ZFHQPTLM\n"
        + "-----END CERTIFICATE-----";
    
    Files.write(caCertPath, testCertContent.getBytes());
    
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SUPPLIER_CLASS.key(), TlsEnabledDataHubEmitterSupplier.class.getName());
    props.setProperty(DataHubSyncConfig.META_SYNC_DATAHUB_EMITTER_SERVER.key(), testServerUrl);
    props.setProperty(DataHubSyncConfig.META_SYNC_DATAHUB_EMITTER_TOKEN.key(), testToken);
    props.setProperty(DataHubSyncConfig.META_SYNC_DATAHUB_TLS_CA_CERT_PATH.key(), caCertPath.toString());
    
    DataHubSyncConfig syncConfig = new DataHubSyncConfig(props);
    RestEmitter emitter = syncConfig.getRestEmitter();
    assertNotNull(emitter);
  }

  @Test
  void testGetEmitterWithTlsEnabledSupplierWithoutCert() {
    String testServerUrl = "https://datahub.example.com";
    
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SUPPLIER_CLASS.key(), TlsEnabledDataHubEmitterSupplier.class.getName());
    props.setProperty(DataHubSyncConfig.META_SYNC_DATAHUB_EMITTER_SERVER.key(), testServerUrl);
    
    DataHubSyncConfig syncConfig = new DataHubSyncConfig(props);
    RestEmitter emitter = syncConfig.getRestEmitter();
    assertNotNull(emitter);
  }

  @Test
  void testGetEmitterWithTlsEnabledSupplierMissingServer() {
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SUPPLIER_CLASS.key(), TlsEnabledDataHubEmitterSupplier.class.getName());
    
    DataHubSyncConfig syncConfig = new DataHubSyncConfig(props);
    assertThrows(DataHubEmitterConfigurationException.class, syncConfig::getRestEmitter);
  }

  @Test
  void testGetEmitterWithTlsEnabledSupplierInvalidCertPath() {
    String testServerUrl = "https://datahub.example.com";
    
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SUPPLIER_CLASS.key(), TlsEnabledDataHubEmitterSupplier.class.getName());
    props.setProperty(DataHubSyncConfig.META_SYNC_DATAHUB_EMITTER_SERVER.key(), testServerUrl);
    props.setProperty(DataHubSyncConfig.META_SYNC_DATAHUB_TLS_CA_CERT_PATH.key(), "/nonexistent/path/cert.pem");
    
    DataHubSyncConfig syncConfig = new DataHubSyncConfig(props);
    assertThrows(DataHubEmitterConfigurationException.class, syncConfig::getRestEmitter);
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
