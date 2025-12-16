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
import org.apache.hudi.sync.datahub.HoodieDataHubSyncException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Objects;
import java.util.Properties;

import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_EMITTER_SERVER;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_EMITTER_TOKEN;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_TLS_CA_CERT_PATH;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_TLS_KEYSTORE_PATH;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_TLS_KEYSTORE_PASSWORD;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_TLS_TRUSTSTORE_PATH;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_TLS_TRUSTSTORE_PASSWORD;
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
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
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

    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);

    RestEmitter emitter = supplier.get();
    assertNotNull(emitter, "Emitter should be created with CA certificate");
  }

  @Test
  void testEmitterCreationWithMultipleCACertificates() throws Exception {
    // Load a PEM file with multiple certificates from resources
    Path multiCertPath = copyMultipleCertificateFromResources();

    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_CA_CERT_PATH.key(), multiCertPath.toString());

    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);

    RestEmitter emitter = supplier.get();
    assertNotNull(emitter, "Emitter should be created with multiple CA certificates");
  }

  @Test
  void testEmitterCreationFailsWithoutServerUrl() {
    Properties props = new Properties();
    // No server URL provided
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    assertThrows(HoodieDataHubSyncException.class, supplier::get,
        "Should throw exception when server URL is not provided");
  }

  @Test
  void testEmitterCreationFailsWithInvalidCACertPath() {
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_CA_CERT_PATH.key(), "/non/existent/path/cert.pem");
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    // Should throw exception when CA cert path is invalid
    assertThrows(HoodieDataHubSyncException.class, supplier::get,
        "Should throw HoodieDataHubSyncException when CA certificate file doesn't exist");
  }

  @Test
  void testEmitterCreationWithKeystore() throws Exception {
    Path keystorePath = copyKeystoreFromResources("test-keystore.p12");
    
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_KEYSTORE_PATH.key(), keystorePath.toString());
    props.setProperty(META_SYNC_DATAHUB_TLS_KEYSTORE_PASSWORD.key(), "testpass");
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    RestEmitter emitter = supplier.get();
    assertNotNull(emitter, "Emitter should be created with keystore");
  }

  @Test
  void testEmitterCreationWithTruststore() throws Exception {
    Path truststorePath = copyKeystoreFromResources("test-truststore.p12");
    
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_TRUSTSTORE_PATH.key(), truststorePath.toString());
    props.setProperty(META_SYNC_DATAHUB_TLS_TRUSTSTORE_PASSWORD.key(), "testpass");
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    RestEmitter emitter = supplier.get();
    assertNotNull(emitter, "Emitter should be created with truststore");
  }

  @Test
  void testEmitterCreationWithKeystoreAndTruststore() throws Exception {
    Path keystorePath = copyKeystoreFromResources("test-keystore.p12");
    Path truststorePath = copyKeystoreFromResources("test-truststore.p12");
    
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_KEYSTORE_PATH.key(), keystorePath.toString());
    props.setProperty(META_SYNC_DATAHUB_TLS_KEYSTORE_PASSWORD.key(), "testpass");
    props.setProperty(META_SYNC_DATAHUB_TLS_TRUSTSTORE_PATH.key(), truststorePath.toString());
    props.setProperty(META_SYNC_DATAHUB_TLS_TRUSTSTORE_PASSWORD.key(), "testpass");
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    RestEmitter emitter = supplier.get();
    assertNotNull(emitter, "Emitter should be created with both keystore and truststore");
  }

  @Test
  void testEmitterCreationWithKeystoreWithoutPassword() throws Exception {
    // Create a password-less keystore for testing
    Path keystorePath = createPasswordlessKeystore();
    
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_KEYSTORE_PATH.key(), keystorePath.toString());
    // No password provided - should work with warning
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    RestEmitter emitter = supplier.get();
    assertNotNull(emitter, "Emitter should be created with password-less keystore");
  }

  @Test
  void testEmitterCreationFailsWithInvalidKeystorePath() {
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_KEYSTORE_PATH.key(), "/non/existent/keystore.p12");
    props.setProperty(META_SYNC_DATAHUB_TLS_KEYSTORE_PASSWORD.key(), "testpass");
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    assertThrows(HoodieDataHubSyncException.class, supplier::get,
        "Should throw exception when keystore file doesn't exist");
  }

  private Path copyKeystoreFromResources(String resourceName) throws Exception {
    Path keystorePath = tempDir.resolve(resourceName);
    
    try (InputStream keystoreStream = getClass().getClassLoader().getResourceAsStream(resourceName)) {
      Objects.requireNonNull(keystoreStream, resourceName + " not found in resources");
      Files.copy(keystoreStream, keystorePath);
    }
    
    return keystorePath;
  }

  @Test
  void testEmitterCreationFailsWithKeystoreWrongPassword() throws Exception {
    Path keystorePath = copyKeystoreFromResources("test-keystore.p12");
    
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_KEYSTORE_PATH.key(), keystorePath.toString());
    props.setProperty(META_SYNC_DATAHUB_TLS_KEYSTORE_PASSWORD.key(), "wrongpassword");
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    assertThrows(HoodieDataHubSyncException.class, supplier::get,
        "Should throw exception when keystore password is incorrect");
  }

  @Test
  void testEmitterCreationFailsWithTruststoreWrongPassword() throws Exception {
    Path truststorePath = copyKeystoreFromResources("test-truststore.p12");
    
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_TRUSTSTORE_PATH.key(), truststorePath.toString());
    props.setProperty(META_SYNC_DATAHUB_TLS_TRUSTSTORE_PASSWORD.key(), "wrongpassword");
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    assertThrows(HoodieDataHubSyncException.class, supplier::get,
        "Should throw exception when truststore password is incorrect");
  }

  @Test
  void testEmitterCreationWithTruststoreWithoutPassword() throws Exception {
    // Create a password-less truststore for testing
    Path truststorePath = createPasswordlessTruststore();
    
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_TRUSTSTORE_PATH.key(), truststorePath.toString());
    // No password provided - should work with warning
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    RestEmitter emitter = supplier.get();
    assertNotNull(emitter, "Emitter should be created with password-less truststore");
  }

  @Test
  void testEmitterCreationWithCACertAndTruststore() throws Exception {
    // When both CA cert and truststore are provided, truststore should take precedence
    Path caCertPath = createDummyCertificateFile();
    Path truststorePath = copyKeystoreFromResources("test-truststore.p12");
    
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_CA_CERT_PATH.key(), caCertPath.toString());
    props.setProperty(META_SYNC_DATAHUB_TLS_TRUSTSTORE_PATH.key(), truststorePath.toString());
    props.setProperty(META_SYNC_DATAHUB_TLS_TRUSTSTORE_PASSWORD.key(), "testpass");
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    RestEmitter emitter = supplier.get();
    assertNotNull(emitter, "Emitter should be created with truststore taking precedence over CA cert");
  }

  @Test
  void testEmitterCreationWithEmptyTlsPaths() {
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_CA_CERT_PATH.key(), ""); // Empty string
    props.setProperty(META_SYNC_DATAHUB_TLS_KEYSTORE_PATH.key(), ""); // Empty string
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    // Should work without TLS configuration when paths are empty
    RestEmitter emitter = supplier.get();
    assertNotNull(emitter, "Emitter should be created without TLS when paths are empty");
  }

  @Test
  void testEmitterCreationFailsWithInvalidTruststorePath() {
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_TRUSTSTORE_PATH.key(), "/non/existent/truststore.p12");
    props.setProperty(META_SYNC_DATAHUB_TLS_TRUSTSTORE_PASSWORD.key(), "testpass");
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    assertThrows(HoodieDataHubSyncException.class, supplier::get,
        "Should throw exception when truststore file doesn't exist");
  }

  @Test
  void testEmitterCreationFailsWithInvalidKeystoreFormat() throws Exception {
    // Create an invalid keystore file (just text content)
    Path invalidKeystorePath = tempDir.resolve("invalid-keystore.p12");
    Files.write(invalidKeystorePath, "This is not a valid keystore file".getBytes());
    
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_KEYSTORE_PATH.key(), invalidKeystorePath.toString());
    props.setProperty(META_SYNC_DATAHUB_TLS_KEYSTORE_PASSWORD.key(), "testpass");
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    assertThrows(HoodieDataHubSyncException.class, supplier::get,
        "Should throw exception when keystore file format is invalid");
  }

  @Test
  void testEmitterCreationFailsWithInvalidTruststoreFormat() throws Exception {
    // Create an invalid truststore file (just text content)
    Path invalidTruststorePath = tempDir.resolve("invalid-truststore.p12");
    Files.write(invalidTruststorePath, "This is not a valid truststore file".getBytes());
    
    Properties props = new Properties();
    props.setProperty(META_SYNC_DATAHUB_EMITTER_SERVER.key(), "https://datahub.example.com:8080");
    props.setProperty(META_SYNC_DATAHUB_TLS_TRUSTSTORE_PATH.key(), invalidTruststorePath.toString());
    props.setProperty(META_SYNC_DATAHUB_TLS_TRUSTSTORE_PASSWORD.key(), "testpass");
    
    TypedProperties typedProps = new TypedProperties();
    TypedProperties.putAll(typedProps, props);
    TlsEnabledDataHubEmitterSupplier supplier = new TlsEnabledDataHubEmitterSupplier(typedProps);
    
    assertThrows(HoodieDataHubSyncException.class, supplier::get,
        "Should throw exception when truststore file format is invalid");
  }

  private Path createPasswordlessKeystore() throws Exception {
    Path keystorePath = tempDir.resolve("passwordless-keystore.p12");
    
    // Generate a keystore without password using keytool command
    ProcessBuilder pb = new ProcessBuilder(
        "keytool", "-genkeypair", "-keyalg", "RSA", "-keysize", "2048", 
        "-storetype", "PKCS12", "-keystore", keystorePath.toString(),
        "-validity", "365", "-storepass", "", "-keypass", "",
        "-dname", "CN=test-passwordless,O=Apache Hudi,L=Test,ST=Test,C=US",
        "-alias", "test-passwordless"
    );
    
    Process process = pb.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      // Fallback: create a simple keystore programmatically
      return createSimplePasswordlessKeystore();
    }
    
    return keystorePath;
  }

  private Path createPasswordlessTruststore() throws Exception {
    Path truststorePath = tempDir.resolve("passwordless-truststore.p12");
    
    // Generate a truststore without password using keytool command
    ProcessBuilder pb = new ProcessBuilder(
        "keytool", "-genkeypair", "-keyalg", "RSA", "-keysize", "2048", 
        "-storetype", "PKCS12", "-keystore", truststorePath.toString(),
        "-validity", "365", "-storepass", "", "-keypass", "",
        "-dname", "CN=test-truststore-passwordless,O=Apache Hudi,L=Test,ST=Test,C=US",
        "-alias", "test-truststore-passwordless"
    );
    
    Process process = pb.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      // Fallback: create a simple truststore programmatically
      return createSimplePasswordlessTruststore();
    }
    
    return truststorePath;
  }

  private Path createSimplePasswordlessKeystore() throws Exception {
    Path keystorePath = tempDir.resolve("simple-passwordless-keystore.p12");
    
    // Create a simple keystore without password programmatically
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null); // Initialize empty keystore
    
    try (FileOutputStream fos = new FileOutputStream(keystorePath.toFile())) {
      keyStore.store(fos, new char[0]); // Store with empty password
    }
    
    return keystorePath;
  }

  private Path createSimplePasswordlessTruststore() throws Exception {
    Path truststorePath = tempDir.resolve("simple-passwordless-truststore.p12");
    
    // Create a simple truststore without password programmatically
    KeyStore trustStore = KeyStore.getInstance("PKCS12");
    trustStore.load(null, null); // Initialize empty truststore
    
    try (FileOutputStream fos = new FileOutputStream(truststorePath.toFile())) {
      trustStore.store(fos, new char[0]); // Store with empty password
    }
    
    return truststorePath;
  }

  private Path createDummyCertificateFile() throws Exception {
    Path certPath = tempDir.resolve("ca-cert.pem");

    try (InputStream certStream = getClass().getClassLoader().getResourceAsStream("test-ca-cert.pem")) {
      Objects.requireNonNull(certStream, "test-ca-cert.pem not found in resources");
      Files.copy(certStream, certPath);
    }

    return certPath;
  }

  private Path copyMultipleCertificateFromResources() throws Exception {
    Path multiCertPath = tempDir.resolve("multi-ca-cert.pem");

    try (InputStream certStream = getClass().getClassLoader().getResourceAsStream("multi-ca-cert.pem")) {
      Objects.requireNonNull(certStream, "multi-ca-cert.pem not found in resources");
      Files.copy(certStream, multiCertPath);
    }

    return multiCertPath;
  }

}