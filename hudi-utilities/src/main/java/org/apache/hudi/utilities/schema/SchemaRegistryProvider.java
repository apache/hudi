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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
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
import java.util.Collections;
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

    private static final String SRC_SCHEMA_REGISTRY_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.url";
    private static final String TARGET_SCHEMA_REGISTRY_URL_PROP =
        "hoodie.deltastreamer.schemaprovider.registry.targetUrl";
  }

  public String fetchSchemaFromRegistry(String registryUrl) throws IOException {
    URL registry = new URL(registryUrl);
    HttpURLConnection connection = (HttpURLConnection) registry.openConnection();
    Matcher matcher = Pattern.compile("://(.*?)@").matcher(registryUrl);
    if (matcher.find()) {
      setAuthorizationHeader(matcher.group(1), connection);
    }
    ObjectMapper mapper = new ObjectMapper();
    InputStream is = getStream(connection);
    JsonNode node = mapper.readTree(is);
    return node.get("schema").asText();
  }

  public void setAuthorizationHeader(String creds, HttpURLConnection connection) {
    byte[] encodedAuth = Base64.encodeBase64(creds.getBytes(StandardCharsets.UTF_8));
    connection.setRequestProperty("Authorization", "Basic " + new String(encodedAuth));
  }

  public InputStream getStream(HttpURLConnection connection) throws IOException {
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
