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

package org.apache.hudi.utilities.schema;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.utilities.config.HoodieSchemaProviderConfig;
import org.apache.hudi.utilities.exception.HoodieSchemaFetchException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.spark.api.java.JavaSparkContext;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Base64;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Obtains latest schema from the Confluent/Kafka schema-registry.
 * <p>
 * https://github.com/confluentinc/schema-registry
 */
public class SchemaRegistryProvider extends SchemaProvider {

  /**
   * Configs supported.
   */
  public static class Config {
    @Deprecated
    public static final String SRC_SCHEMA_REGISTRY_URL_PROP =
        HoodieSchemaProviderConfig.SRC_SCHEMA_REGISTRY_URL.key();
    @Deprecated
    public static final String TARGET_SCHEMA_REGISTRY_URL_PROP =
        HoodieSchemaProviderConfig.TARGET_SCHEMA_REGISTRY_URL.key();
    @Deprecated
    public static final String SCHEMA_CONVERTER_PROP =
        HoodieSchemaProviderConfig.SCHEMA_CONVERTER.key();
    public static final String SSL_KEYSTORE_LOCATION_PROP = "schema.registry.ssl.keystore.location";
    public static final String SSL_TRUSTSTORE_LOCATION_PROP = "schema.registry.ssl.truststore.location";
    public static final String SSL_KEYSTORE_PASSWORD_PROP = "schema.registry.ssl.keystore.password";
    public static final String SSL_TRUSTSTORE_PASSWORD_PROP = "schema.registry.ssl.truststore.password";
    public static final String SSL_KEY_PASSWORD_PROP = "schema.registry.ssl.key.password";
  }

  protected Schema cachedSourceSchema;
  protected Schema cachedTargetSchema;

  private final String srcSchemaRegistryUrl;
  private final String targetSchemaRegistryUrl;

  @FunctionalInterface
  public interface SchemaConverter {
    /**
     * Convert original schema string to avro schema string.
     *
     * @param schema original schema string (e.g., JSON)
     * @return avro schema string
     */
    String convert(String schema) throws IOException;
  }

  public Schema parseSchemaFromRegistry(String registryUrl) {
    String schema = fetchSchemaFromRegistry(registryUrl);
    try {
      String schemaConverter = getStringWithAltKeys(config, HoodieSchemaProviderConfig.SCHEMA_CONVERTER, true);
      SchemaConverter converter = !StringUtils.isNullOrEmpty(schemaConverter)
          ? ReflectionUtils.loadClass(schemaConverter)
          : s -> s;
      return new Schema.Parser().parse(converter.convert(schema));
    } catch (Exception e) {
      throw new HoodieSchemaException("Failed to parse schema from registry: " + schema, e);
    }
  }

  /**
   * The method takes the provided url {@code registryUrl} and gets the schema from the schema registry using that url.
   * If the caller provides userInfo credentials in the url (e.g "https://foo:bar@schemaregistry.org") then the credentials
   * are extracted the url using the Matcher and the extracted credentials are set on the request as an Authorization
   * header.
   *
   * @param registryUrl
   * @return the Schema in String form.
   */
  public String fetchSchemaFromRegistry(String registryUrl) {
    try {
      HttpURLConnection connection;
      Matcher matcher = Pattern.compile("://(.*?)@").matcher(registryUrl);
      if (matcher.find()) {
        String creds = matcher.group(1);
        String urlWithoutCreds = registryUrl.replace(creds + "@", "");
        connection = getConnection(urlWithoutCreds);
        setAuthorizationHeader(matcher.group(1), connection);
      } else {
        connection = getConnection(registryUrl);
      }
      ObjectMapper mapper = new ObjectMapper();
      JsonNode node = mapper.readTree(getStream(connection));
      return node.get("schema").asText();
    } catch (Exception e) {
      throw new HoodieSchemaFetchException("Failed to fetch schema from registry", e);
    }
  }

  private SSLSocketFactory sslSocketFactory;

  protected HttpURLConnection getConnection(String url) throws IOException {
    URL registry = new URL(url);
    if (sslSocketFactory != null) {
      // we cannot cast to HttpsURLConnection if url is http so only cast when sslSocketFactory is set
      HttpsURLConnection connection = (HttpsURLConnection) registry.openConnection();
      connection.setSSLSocketFactory(sslSocketFactory);
      return connection;
    }
    return (HttpURLConnection) registry.openConnection();
  }

  protected void setAuthorizationHeader(String creds, HttpURLConnection connection) {
    String encodedAuth = Base64.getEncoder().encodeToString(getUTF8Bytes(creds));
    connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
  }

  protected InputStream getStream(HttpURLConnection connection) throws IOException {
    return connection.getInputStream();
  }

  public SchemaRegistryProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    checkRequiredConfigProperties(props, Collections.singletonList(HoodieSchemaProviderConfig.SRC_SCHEMA_REGISTRY_URL));
    this.srcSchemaRegistryUrl = getStringWithAltKeys(config, HoodieSchemaProviderConfig.SRC_SCHEMA_REGISTRY_URL);
    this.targetSchemaRegistryUrl = getStringWithAltKeys(config, HoodieSchemaProviderConfig.TARGET_SCHEMA_REGISTRY_URL, srcSchemaRegistryUrl);
    if (config.containsKey(Config.SSL_KEYSTORE_LOCATION_PROP)
        || config.containsKey(Config.SSL_TRUSTSTORE_LOCATION_PROP)) {
      setUpSSLStores();
    }
  }

  private void setUpSSLStores() {
    SSLContextBuilder sslContextBuilder = SSLContexts.custom();
    try {
      if (config.containsKey(Config.SSL_TRUSTSTORE_LOCATION_PROP)) {
        sslContextBuilder.loadTrustMaterial(
            new File(config.getString(Config.SSL_TRUSTSTORE_LOCATION_PROP)),
            config.getString(Config.SSL_TRUSTSTORE_PASSWORD_PROP).toCharArray(),
            new TrustSelfSignedStrategy());
      }
      if (config.containsKey(Config.SSL_KEYSTORE_LOCATION_PROP)) {
        sslContextBuilder.loadKeyMaterial(
            new File(config.getString(Config.SSL_KEYSTORE_LOCATION_PROP)),
            config.getString(Config.SSL_KEYSTORE_PASSWORD_PROP).toCharArray(),
            config.getString(Config.SSL_KEY_PASSWORD_PROP).toCharArray()
        );
      }
      sslSocketFactory = sslContextBuilder.build().getSocketFactory();
    } catch (UnrecoverableKeyException | IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException | KeyManagementException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public Schema getSourceSchema() {
    try {
      if (cachedSourceSchema == null) {
        cachedSourceSchema = parseSchemaFromRegistry(this.srcSchemaRegistryUrl);
      }
      return cachedSourceSchema;
    } catch (Exception e) {
      throw new HoodieSchemaFetchException(String.format(
          "Error reading source schema from registry. Please check %s is configured correctly. Truncated URL: %s",
          Config.SRC_SCHEMA_REGISTRY_URL_PROP,
          StringUtils.truncate(srcSchemaRegistryUrl, 10, 10)), e);
    }
  }

  @Override
  public Schema getTargetSchema() {
    try {
      if (cachedTargetSchema == null) {
        cachedTargetSchema = parseSchemaFromRegistry(this.targetSchemaRegistryUrl);
      }
      return cachedTargetSchema;
    } catch (Exception e) {
      throw new HoodieSchemaFetchException(String.format(
          "Error reading target schema from registry. Please check %s is configured correctly. If that is not configured then check %s. Truncated URL: %s",
          Config.SRC_SCHEMA_REGISTRY_URL_PROP,
          Config.TARGET_SCHEMA_REGISTRY_URL_PROP,
          StringUtils.truncate(targetSchemaRegistryUrl, 10, 10)), e);
    }
  }

  // Per SyncOnce call, the cachedschema for the provider is dropped and SourceSchema re-attained
  // Subsequent calls to getSourceSchema within the write batch should be cached.
  @Override
  public void refresh() {
    cachedSourceSchema = null;
    cachedTargetSchema = null;
    getSourceSchema();
    getTargetSchema();
  }
}
