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

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieIOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Obtains latest schema from the Confluent/Kafka schema-registry.
 *
 * https://github.com/confluentinc/schema-registry
 */
public class SchemaRegistryProvider extends SchemaProvider {

  /**
   * Configs supported.
   */
  public static class Config {

    public static final String SRC_SCHEMA_REGISTRY_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.url";
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

  private final Option<SchemaConverter> schemaConverter;
  private final SerializableFunctionUnchecked<String, RestService> restServiceProvider;
  private final SerializableFunctionUnchecked<RestService, SchemaRegistryClient> registryClientProvider;

  public SchemaRegistryProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    checkRequiredConfigProperties(props, Collections.singletonList(HoodieSchemaProviderConfig.SRC_SCHEMA_REGISTRY_URL));
    if (config.containsKey(Config.SSL_KEYSTORE_LOCATION_PROP)
        || config.containsKey(Config.SSL_TRUSTSTORE_LOCATION_PROP)) {
      setUpSSLStores();
    }
    String schemaConverter = getStringWithAltKeys(config, HoodieSchemaProviderConfig.SCHEMA_CONVERTER, true);
    this.schemaConverter = !StringUtils.isNullOrEmpty(schemaConverter)
        ? Option.of((SchemaConverter) ReflectionUtils.loadClass(
        schemaConverter, new Class<?>[] {TypedProperties.class}, config))
        : Option.empty();
    this.restServiceProvider = RestService::new;
    Map<String, Object> schemaRegistryConfigs = new HashMap<>();
    config.forEach((k, v) -> schemaRegistryConfigs.put(k.toString(), v));
    this.registryClientProvider = restService -> new CachedSchemaRegistryClient(restService, 100,
        Arrays.asList(new ProtobufSchemaProvider(), new JsonSchemaProvider(), new AvroSchemaProvider()), schemaRegistryConfigs, null);
  }

  @VisibleForTesting
  SchemaRegistryProvider(TypedProperties props, JavaSparkContext jssc,
                         Option<SchemaConverter> schemaConverter,
                         SerializableFunctionUnchecked<String, RestService> restServiceProvider,
                         SerializableFunctionUnchecked<RestService, SchemaRegistryClient> registryClientProvider) {
    super(props, jssc);
    checkRequiredConfigProperties(props, Collections.singletonList(HoodieSchemaProviderConfig.SRC_SCHEMA_REGISTRY_URL));
    this.schemaConverter = schemaConverter;
    this.restServiceProvider = restServiceProvider;
    this.registryClientProvider = registryClientProvider;
  }

  @FunctionalInterface
  public interface SchemaConverter {
    /**
     * Convert original schema string to avro schema string.
     *
     * @param schema original schema returned from the registry
     * @return avro schema string
     */
    String convert(ParsedSchema schema) throws IOException;
  }

  public Schema parseSchemaFromRegistry(String registryUrl) {
    String schema = fetchSchemaFromRegistry(registryUrl);
    return new Schema.Parser().parse(schema);
  }

  /**
   * The method takes the provided url {@code registryUrl} and gets the schema from the schema registry using that url.
   * If the caller provides userInfo credentials in the url (e.g "https://foo:bar@schemaregistry.org") then the credentials
   * are extracted the url using the Matcher and the extracted credentials are set on the request as an Authorization
   * header.
   * @param registryUrl
   * @return the Schema in String form.
   * @throws IOException
   */
  public String fetchSchemaFromRegistry(String registryUrl) throws IOException {
    URL registry;
    HttpURLConnection connection;
    Matcher matcher = Pattern.compile("://(.*?)@").matcher(registryUrl);
    if (matcher.find()) {
      String creds = matcher.group(1);
      String urlWithoutCreds = registryUrl.replace(creds + "@", "");
      registry = new URL(urlWithoutCreds);
      connection = (HttpURLConnection) registry.openConnection();
      setAuthorizationHeader(matcher.group(1), connection);
    } else {
      registry = new URL(registryUrl);
      connection = (HttpURLConnection) registry.openConnection();
    }
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(getStream(connection));
    return node.get("schema").asText();
  }

  protected void setAuthorizationHeader(String creds, HttpURLConnection connection) {
    String encodedAuth = Base64.getEncoder().encodeToString(creds.getBytes(StandardCharsets.UTF_8));
    connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
  }

  protected InputStream getStream(HttpURLConnection connection) throws IOException {
    return connection.getInputStream();
  }

  public SchemaRegistryProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.SRC_SCHEMA_REGISTRY_URL_PROP));
  }

  private Schema getSchema(String registryUrl) throws IOException {
    return new Schema.Parser().parse(fetchSchemaFromRegistry(registryUrl));
  }

  @Override
  public Schema getSourceSchema() {
    String registryUrl = config.getString(Config.SRC_SCHEMA_REGISTRY_URL_PROP);
    try {
      return getSchema(registryUrl);
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading source schema from registry :" + registryUrl, ioe);
    }
  }

  @Override
  public Schema getTargetSchema() {
    String registryUrl = config.getString(Config.SRC_SCHEMA_REGISTRY_URL_PROP);
    String targetRegistryUrl = config.getString(Config.TARGET_SCHEMA_REGISTRY_URL_PROP, registryUrl);
    try {
      return getSchema(targetRegistryUrl);
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading target schema from registry :" + registryUrl, ioe);
    }
  }
}
