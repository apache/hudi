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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.sync.datahub.HoodieDataHubSyncException;

import datahub.client.rest.RestEmitter;
import datahub.shaded.org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import datahub.shaded.org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.SSLContext;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;

/**
 * Custom DataHub emitter supplier that supports TLS configuration with CA certificates.
 * This class reads TLS configuration from Hudi properties and creates a RestEmitter
 * with proper SSL/TLS context for secure communication with DataHub servers.
 */
@Slf4j
public class TlsEnabledDataHubEmitterSupplier implements DataHubEmitterSupplier {

  private final TypedProperties config;

  public TlsEnabledDataHubEmitterSupplier(TypedProperties config) {
    this.config = config;
  }

  @Override
  public RestEmitter get() {
    try {
      String serverUrl = ConfigUtils.getStringWithAltKeys(config, DataHubSyncConfig.META_SYNC_DATAHUB_EMITTER_SERVER, true);
      if (serverUrl == null || serverUrl.isEmpty()) {
        throw new IllegalArgumentException(
            "DataHub server URL must be specified with " + DataHubSyncConfig.META_SYNC_DATAHUB_EMITTER_SERVER.key());
      }

      String token = ConfigUtils.getStringWithAltKeys(config, DataHubSyncConfig.META_SYNC_DATAHUB_EMITTER_TOKEN, true);
      String caCertPath = ConfigUtils.getStringWithAltKeys(config, DataHubSyncConfig.META_SYNC_DATAHUB_TLS_CA_CERT_PATH, true);
      String keystorePath = ConfigUtils.getStringWithAltKeys(config, DataHubSyncConfig.META_SYNC_DATAHUB_TLS_KEYSTORE_PATH, true);
      String keystorePassword = ConfigUtils.getStringWithAltKeys(config, DataHubSyncConfig.META_SYNC_DATAHUB_TLS_KEYSTORE_PASSWORD, true);
      String truststorePath = ConfigUtils.getStringWithAltKeys(config, DataHubSyncConfig.META_SYNC_DATAHUB_TLS_TRUSTSTORE_PATH, true);
      String truststorePassword = ConfigUtils.getStringWithAltKeys(config, DataHubSyncConfig.META_SYNC_DATAHUB_TLS_TRUSTSTORE_PASSWORD, true);

      log.info("Creating DataHub RestEmitter with TLS configuration for server: {}", serverUrl);

      return RestEmitter.create(builder -> {
        builder.server(serverUrl);
        
        if (token != null && !token.isEmpty()) {
          builder.token(token);
        }
        
        // Configure TLS/SSL context if any TLS configuration is provided
        if (hasTlsConfiguration(caCertPath, keystorePath, truststorePath)) {
          log.info("Configuring TLS for DataHub connection");
          SSLContext sslContext = createSSLContext(caCertPath, keystorePath, keystorePassword, truststorePath, truststorePassword);
          
          builder.customizeHttpAsyncClient(httpClientBuilder -> {
            ClientTlsStrategyBuilder tlsStrategyBuilder = ClientTlsStrategyBuilder.create();
            tlsStrategyBuilder.setSslContext(sslContext);

            PoolingAsyncClientConnectionManagerBuilder connectionManagerBuilder =
                PoolingAsyncClientConnectionManagerBuilder.create();
            connectionManagerBuilder.setTlsStrategy(tlsStrategyBuilder.build());

            httpClientBuilder.setConnectionManager(connectionManagerBuilder.build());
          });
          log.info("Successfully configured TLS for DataHub connection");
        }
      });
    } catch (Exception e) {
      throw new HoodieDataHubSyncException("Failed to create TLS-enabled DataHub emitter", e);
    }
  }

  private boolean hasTlsConfiguration(String caCertPath, String keystorePath, String truststorePath) {
    return (caCertPath != null && !caCertPath.isEmpty())
        || (keystorePath != null && !keystorePath.isEmpty())
        || (truststorePath != null && !truststorePath.isEmpty());
  }

  private SSLContext createSSLContext(String caCertPath, String keystorePath, String keystorePassword, 
                                      String truststorePath, String truststorePassword) throws HoodieDataHubSyncException {
    try {
      SSLContextBuilder sslContextBuilder = SSLContexts.custom();

      // Configure client keystore for mutual TLS authentication
      if (keystorePath != null && !keystorePath.isEmpty()) {
        if (!Files.exists(Paths.get(keystorePath))) {
          throw new HoodieDataHubSyncException("Keystore file not found: " + keystorePath);
        }
        if (keystorePassword == null || keystorePassword.isEmpty()) {
          log.warn("No password provided for keystore {}. Using empty password - consider using password-protected keystores for better security.", keystorePath);
        }
        log.info("Loading keystore from: {}", keystorePath);
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        char[] keystorePasswordChars = (keystorePassword != null && !keystorePassword.isEmpty()) 
            ? keystorePassword.toCharArray() : new char[0];
        try (FileInputStream keystoreInputStream = new FileInputStream(keystorePath)) {
          keyStore.load(keystoreInputStream, keystorePasswordChars);
        }
        sslContextBuilder.loadKeyMaterial(keyStore, keystorePasswordChars);
      }

      // Configure truststore or CA certificate for server certificate verification
      if (truststorePath != null && !truststorePath.isEmpty()) {
        if (!Files.exists(Paths.get(truststorePath))) {
          throw new HoodieDataHubSyncException("Truststore file not found: " + truststorePath);
        }
        if (truststorePassword == null || truststorePassword.isEmpty()) {
          log.warn("No password provided for truststore {}. Using empty password - consider using password-protected truststores for better security.", truststorePath);
        }
        log.info("Loading truststore from: {}", truststorePath);
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        char[] truststorePasswordChars = (truststorePassword != null && !truststorePassword.isEmpty()) 
            ? truststorePassword.toCharArray() : new char[0];
        try (FileInputStream trustStoreInputStream = new FileInputStream(truststorePath)) {
          trustStore.load(trustStoreInputStream, truststorePasswordChars);
        }
        sslContextBuilder.loadTrustMaterial(trustStore, null);
      } else if (caCertPath != null && !caCertPath.isEmpty()) {
        if (!Files.exists(Paths.get(caCertPath))) {
          throw new HoodieDataHubSyncException("CA certificate file not found: " + caCertPath);
        }
        log.info("Loading CA certificate from: {}", caCertPath);
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);

        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        try (FileInputStream certInputStream = new FileInputStream(caCertPath)) {
          Collection<? extends Certificate> caCerts = certificateFactory.generateCertificates(certInputStream);
          int certIndex = 0;
          for (Certificate caCert : caCerts) {
            trustStore.setCertificateEntry("ca-cert-" + certIndex, caCert);
            certIndex++;
          }
          log.info("Loaded {} CA certificate(s) from: {}", caCerts.size(), caCertPath);
        }
        sslContextBuilder.loadTrustMaterial(trustStore, null);
      }
      
      return sslContextBuilder.build();
    } catch (Exception e) {
      throw new HoodieDataHubSyncException("Failed to create SSL context with TLS configuration", e);
    }
  }
}