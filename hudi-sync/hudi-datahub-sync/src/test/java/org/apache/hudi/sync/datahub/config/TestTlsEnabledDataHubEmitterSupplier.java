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

import datahub.client.rest.RestEmitter;
import org.apache.hudi.common.config.TypedProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_EMITTER_SERVER;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_EMITTER_TOKEN;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_TLS_CA_CERT_PATH;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestTlsEnabledDataHubEmitterSupplier {

  @TempDir
  Path tempDir;

  @Test
  void testEmitterCreationWithBasicConfig() {
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_EMITTER_TOKEN.key(), "test-token");
    
    TypedProperties typedProps = new TypedProperties(props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    RestEmitter emitter = supplier.get();
    assertNotNull(emitter, "Emitter should be created successfully");
  }

  @Test
  void testEmitterCreationWithCACertificate() throws Exception {
    // Create a dummy CA certificate file for testing
    Path caCertPath = createDummyCertificateFile();
    
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_CA_CERT_PATH.key(), caCertPath.toString());
    
    TypedProperties typedProps = new TypedProperties(props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    RestEmitter emitter = supplier.get();
    assertNotNull(emitter, "Emitter should be created with CA certificate");
  }

  @Test
  void testEmitterCreationFailsWithoutServerUrl() {
    Properties props = new Properties();
    // No server URL provided
    
    TypedProperties typedProps = new TypedProperties(props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    assertThrows(DataHubEmitterConfigurationException.class, supplier::get,
        "Should throw exception when server URL is not provided");
  }

  @Test
  void testEmitterCreationFailsWithInvalidCACertPath() {
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_CA_CERT_PATH.key(), "/non/existent/path/cert.pem");
    
    TypedProperties typedProps = new TypedProperties(props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    // Should throw exception when CA cert path is invalid
    assertThrows(DataHubEmitterConfigurationException.class, supplier::get,
        "Should throw DataHubEmitterConfigurationException when CA certificate file doesn't exist");
  }

  private Path createDummyCertificateFile() throws Exception {
    Path certPath = tempDir.resolve("ca-cert.pem");
    
    // This is a real, valid self-signed certificate that was generated externally
    // Generated with: openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes
    String validCert = "-----BEGIN CERTIFICATE-----\n"
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
    
    Files.write(certPath, validCert.getBytes());
    return certPath;
  }

}