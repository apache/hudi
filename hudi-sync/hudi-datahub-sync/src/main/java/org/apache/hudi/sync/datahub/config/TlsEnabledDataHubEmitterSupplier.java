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
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;

/**
 * Custom DataHub Emitter Supplier that supports TLS configuration with CA certificates.
 * This class reads TLS configuration from Hudi properties and creates a RestEmitter
 * with proper SSL/TLS context.
 */
public class TlsEnabledDataHubEmitterSupplier implements DataHubEmitterSupplier {

  private static final Logger LOG = LoggerFactory.getLogger(TlsEnabledDataHubEmitterSupplier.class);
  
  // Configuration property keys
  public static final String DATAHUB_TLS_CA_CERT_PATH = "hoodie.meta.sync.datahub.tls.ca.cert.path";
  public static final String DATAHUB_EMITTER_SERVER = "hoodie.meta.sync.datahub.emitter.server";
  public static final String DATAHUB_EMITTER_TOKEN = "hoodie.meta.sync.datahub.emitter.token";

  private final TypedProperties config;

  public TlsEnabledDataHubEmitterSupplier(TypedProperties config) {
    this.config = config;
  }

  @Override
  public RestEmitter get() {
    try {
      String serverUrl = config.getString(DATAHUB_EMITTER_SERVER, null);
      if (serverUrl == null || serverUrl.isEmpty()) {
        throw new DataHubEmitterConfigurationException(
            "DataHub server URL must be specified with " + DATAHUB_EMITTER_SERVER);
      }

      String token = config.getString(DATAHUB_EMITTER_TOKEN, null);
      String caCertPath = config.getString(DATAHUB_TLS_CA_CERT_PATH, null);

      LOG.info("Creating DataHub RestEmitter with TLS configuration for server: {}", serverUrl);

      return RestEmitter.create(builder -> {
        builder.server(serverUrl);
        
        if (token != null && !token.isEmpty()) {
          builder.token(token);
        }
        
        // Configure TLS if CA certificate is provided
        if (caCertPath != null && !caCertPath.isEmpty()) {
          LOG.info("Configuring TLS with CA certificate from: {}", caCertPath);
          SSLContext sslContext = createSSLContext(caCertPath);
          
          builder.customizeHttpAsyncClient(httpClientBuilder -> httpClientBuilder.setSSLContext(sslContext));
          LOG.info("Successfully configured TLS for DataHub connection");
        }
      });
    } catch (DataHubEmitterConfigurationException e) {
      throw e;
    } catch (Exception e) {
      throw new DataHubEmitterConfigurationException("Failed to create TLS-enabled DataHub emitter", e);
    }
  }

  private SSLContext createSSLContext(String caCertPath) throws DataHubEmitterConfigurationException {
    if (!Files.exists(Paths.get(caCertPath))) {
      throw new DataHubEmitterConfigurationException("CA certificate file not found: " + caCertPath);
    }
    
    try {
      SSLContextBuilder sslContextBuilder = SSLContexts.custom();

      KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      trustStore.load(null, null);

      CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
      try (FileInputStream certInputStream = new FileInputStream(caCertPath)) {
        Certificate caCert = certificateFactory.generateCertificate(certInputStream);
        trustStore.setCertificateEntry("ca-cert", caCert);
        LOG.info("Successfully loaded CA certificate from: {}", caCertPath);
      }

      sslContextBuilder.loadTrustMaterial(trustStore, null);
      
      return sslContextBuilder.build();
    } catch (Exception e) {
      throw new DataHubEmitterConfigurationException("Failed to create SSL context with CA certificate: " + caCertPath, e);
    }
  }
}