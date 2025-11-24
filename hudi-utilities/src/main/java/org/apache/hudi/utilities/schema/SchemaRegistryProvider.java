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
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.utilities.config.HoodieSchemaProviderConfig;
import org.apache.hudi.utilities.exception.HoodieSchemaFetchException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
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
  private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryProvider.class);
  private static final Pattern URL_PATTERN = Pattern.compile("(.*/)subjects/(.*)/versions/(.*)");
  private static final String LATEST = "latest";

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
    this.registryClientProvider = restService -> new CachedSchemaRegistryClient(restService, 100,
        Arrays.asList(new ProtobufSchemaProvider(), new JsonSchemaProvider(), new AvroSchemaProvider()), null, null);
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

  public HoodieSchema parseSchemaFromRegistry(String registryUrl) {
    String schema = fetchSchemaFromRegistry(registryUrl);
    return HoodieSchema.parse(schema);
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
    String schemaType = "";
    try {
      Matcher matcher = Pattern.compile("://(.*?)@").matcher(registryUrl);
      Triple<String, String, String> registryInfo;
      String creds = null;
      if (matcher.find()) {
        creds = matcher.group(1);
        String urlWithoutCreds = registryUrl.replace(creds + "@", "");
        registryInfo = getUrlSubjectAndVersion(urlWithoutCreds);
      } else {
        registryInfo = getUrlSubjectAndVersion(registryUrl);
      }
      String url = registryInfo.getLeft();
      RestService restService = getRestService(url);
      if (creds != null) {
        setAuthorizationHeader(creds, restService);
      }
      String subject = registryInfo.getMiddle();
      String version = registryInfo.getRight();
      SchemaRegistryClient registryClient = registryClientProvider.apply(restService);
      SchemaMetadata schemaMetadata = version.equals(LATEST) ? registryClient.getLatestSchemaMetadata(subject) : registryClient.getSchemaMetadata(subject, Integer.parseInt(version));
      schemaType = schemaMetadata.getSchemaType();
      ParsedSchema parsedSchema = registryClient.parseSchema(schemaMetadata.getSchemaType(), schemaMetadata.getSchema(), schemaMetadata.getReferences())
          .orElseThrow(() -> new HoodieSchemaException("Failed to parse schema from registry"));
      if (schemaConverter.isPresent()) {
        return schemaConverter.get().convert(parsedSchema);
      } else {
        return parsedSchema.canonicalString();
      }
    } catch (IllegalAccessError error) {
      // If we're not processing Protobuf schema, fall back to the legacy method
      if (!ProtobufSchema.TYPE.equalsIgnoreCase(schemaType)) {
        LOG.warn("Falling back to legacy schema retrieval due to IllegalAccessError", error);
        return fetchSchemaUsingLegacyMethod(registryUrl);
      }
      // Otherwise, rethrow the error
      throw error;
    } catch (Exception e) {
      throw new HoodieSchemaFetchException("Failed to fetch schema from registry", e);
    }
  }

  // Legacy method replicating the original HTTP-based schema fetch approach
  String fetchSchemaUsingLegacyMethod(String registryUrl) {
    try {
      HttpURLConnection connection;
      Matcher matcher = Pattern.compile("://(.*?)@").matcher(registryUrl);
      if (matcher.find()) {
        String creds = matcher.group(1);
        String urlWithoutCreds = registryUrl.replace(creds + "@", "");
        connection = getConnection(urlWithoutCreds);
        setAuthorizationHeader(creds, connection);
      } else {
        connection = getConnection(registryUrl);
      }
      ObjectMapper mapper = new ObjectMapper();
      JsonNode node = mapper.readTree(getStream(connection));
      return node.get("schema").asText();
    } catch (Exception e) {
      throw new HoodieSchemaFetchException("Failed to fetch schema from registry (legacy method)", e);
    }
  }

  private HttpURLConnection getConnection(String url) throws IOException {
    URL registry = new URL(url);
    if (sslSocketFactory != null) {
      // we cannot cast to HttpsURLConnection if url is http so only cast when sslSocketFactory is set
      HttpsURLConnection connection = (HttpsURLConnection) registry.openConnection();
      connection.setSSLSocketFactory(sslSocketFactory);
      return connection;
    }
    return (HttpURLConnection) registry.openConnection();
  }

  private void setAuthorizationHeader(String creds, HttpURLConnection connection) {
    String encodedAuth = Base64.getEncoder().encodeToString(getUTF8Bytes(creds));
    connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
  }

  private InputStream getStream(HttpURLConnection connection) throws IOException {
    return connection.getInputStream();
  }

  private Triple<String, String, String> getUrlSubjectAndVersion(String registryUrl) {
    // url may be list of urls
    String[] splitRegistryUrls = registryUrl.split(",");
    String subjectName = null;
    String version = null;
    List<String> urls = new ArrayList<>(splitRegistryUrls.length);
    // url will end with /subjects/{subject}/versions/{version}
    for (String url : splitRegistryUrls) {
      Matcher matcher = URL_PATTERN.matcher(url);
      if (!matcher.matches()) {
        throw new HoodieSchemaFetchException("Failed to extract subject name and version from registry url");
      }
      urls.add(matcher.group(1));
      subjectName = matcher.group(2);
      version = matcher.group(3);
    }
    if (subjectName == null) {
      throw new HoodieSchemaFetchException("Failed to extract subject name from registry url");
    }
    return Triple.of(String.join(",", urls), subjectName, version);
  }

  private SSLSocketFactory sslSocketFactory;

  protected RestService getRestService(String url) {
    RestService restService = restServiceProvider.apply(url);
    if (sslSocketFactory != null) {
      restService.setSslSocketFactory(sslSocketFactory);
      return restService;
    }
    return restService;
  }

  protected void setAuthorizationHeader(String creds, RestService restService) {
    String encodedAuth = Base64.getEncoder().encodeToString(creds.getBytes(StandardCharsets.UTF_8));
    restService.setHttpHeaders(Collections.singletonMap("Authorization", "Basic " + encodedAuth));
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
  public HoodieSchema getSourceSchema() {
    String registryUrl = getStringWithAltKeys(config, HoodieSchemaProviderConfig.SRC_SCHEMA_REGISTRY_URL);
    try {
      return parseSchemaFromRegistry(registryUrl);
    } catch (Exception e) {
      throw new HoodieSchemaFetchException(String.format(
          "Error reading source schema from registry. Please check %s is configured correctly. Truncated URL: %s",
          Config.SRC_SCHEMA_REGISTRY_URL_PROP,
          StringUtils.truncate(registryUrl, 10, 10)), e);
    }
  }

  @Override
  public HoodieSchema getTargetSchema() {
    String registryUrl = getStringWithAltKeys(config, HoodieSchemaProviderConfig.SRC_SCHEMA_REGISTRY_URL);
    String targetRegistryUrl =
        getStringWithAltKeys(config, HoodieSchemaProviderConfig.TARGET_SCHEMA_REGISTRY_URL, registryUrl);
    try {
      return parseSchemaFromRegistry(targetRegistryUrl);
    } catch (Exception e) {
      throw new HoodieSchemaFetchException(String.format(
          "Error reading target schema from registry. Please check %s is configured correctly. If that is not configured then check %s. Truncated URL: %s",
          Config.SRC_SCHEMA_REGISTRY_URL_PROP,
          Config.TARGET_SCHEMA_REGISTRY_URL_PROP,
          StringUtils.truncate(targetRegistryUrl, 10, 10)), e);
    }
  }
}
