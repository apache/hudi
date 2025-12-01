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

package org.apache.hudi.schema;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.exception.HoodieIOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hudi.common.util.ConfigUtils.OLD_SCHEMAPROVIDER_CONFIG_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.SCHEMAPROVIDER_CONFIG_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Obtains latest schema from the Confluent/Kafka schema-registry.
 * <p>
 * https://github.com/confluentinc/schema-registry
 */
public class SchemaRegistryProvider extends SchemaProvider {

  private final TypedProperties config;

  /**
   * Configs supported.
   */
  public static class Config {
    private static final ConfigProperty<String> SRC_SCHEMA_REGISTRY_URL = ConfigProperty
        .key(SCHEMAPROVIDER_CONFIG_PREFIX + "registry.url")
        .noDefaultValue()
        .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "registry.url")
        .withDocumentation("The schema of the source you are reading from e.g. https://foo:bar@schemaregistry.org");

    private static final ConfigProperty<String> TARGET_SCHEMA_REGISTRY_URL = ConfigProperty
        .key(SCHEMAPROVIDER_CONFIG_PREFIX + "registry.targetUrl")
        .noDefaultValue()
        .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "registry.targetUrl")
        .withDocumentation("The schema of the target you are writing to e.g. https://foo:bar@schemaregistry.org");
  }

  /**
   * The method takes the provided url {@code registryUrl} and gets the schema from the schema registry using that url.
   * If the caller provides userInfo credentials in the url (e.g "https://foo:bar@schemaregistry.org") then the credentials
   * are extracted the url using the Matcher and the extracted credentials are set on the request as an Authorization
   * header.
   *
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
    String encodedAuth = Base64.getEncoder().encodeToString(getUTF8Bytes(creds));
    connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
  }

  protected InputStream getStream(HttpURLConnection connection) throws IOException {
    return connection.getInputStream();
  }

  public SchemaRegistryProvider(TypedProperties props) {
    this.config = props;
    checkRequiredConfigProperties(props, Collections.singletonList(Config.SRC_SCHEMA_REGISTRY_URL));
  }

  private HoodieSchema getSchema(String registryUrl) throws IOException {
    return HoodieSchema.parse(fetchSchemaFromRegistry(registryUrl));
  }

  @Override
  public HoodieSchema getSourceSchema() {
    String registryUrl = getStringWithAltKeys(config, Config.SRC_SCHEMA_REGISTRY_URL);
    try {
      return getSchema(registryUrl);
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading source schema from registry :" + registryUrl, ioe);
    }
  }

  @Override
  public HoodieSchema getTargetSchema() {
    String registryUrl = getStringWithAltKeys(config, Config.SRC_SCHEMA_REGISTRY_URL);
    String targetRegistryUrl = getStringWithAltKeys(
        config, Config.TARGET_SCHEMA_REGISTRY_URL, registryUrl);
    try {
      return getSchema(targetRegistryUrl);
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading target schema from registry :" + registryUrl, ioe);
    }
  }
}
