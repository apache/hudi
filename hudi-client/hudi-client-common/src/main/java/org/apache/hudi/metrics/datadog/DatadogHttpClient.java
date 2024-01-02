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

package org.apache.hudi.metrics.datadog;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Datadog API HTTP client.
 * <p>
 * Responsible for API endpoint routing, validating API key, and sending requests with metrics payload.
 */
public class DatadogHttpClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(DatadogHttpClient.class);

  private static final String DEFAULT_HOST = "app.us.datadoghq";
  private static final String SERIES_URL_FORMAT = "https://%s.%s/api/v1/series";
  private static final String VALIDATE_URL_FORMAT = "https://%s.%s/api/v1/validate";
  private static final String HEADER_KEY_API_KEY = "DD-API-KEY";

  private final String apiKey;
  private final String seriesUrl;
  private final String validateUrl;
  private final CloseableHttpClient client;

  public DatadogHttpClient(ApiSite apiSite, String apiKey, boolean skipValidation, CloseableHttpClient client, Option<String> host) {
    this.apiKey = apiKey;
    this.seriesUrl = String.format(SERIES_URL_FORMAT, host.orElse(DEFAULT_HOST), apiSite.getDomain());
    this.validateUrl = String.format(VALIDATE_URL_FORMAT, host.orElse(DEFAULT_HOST), apiSite.getDomain());
    this.client = client;
    if (!skipValidation) {
      validateApiKey();
    }
  }

  public DatadogHttpClient(ApiSite apiSite, String apiKey, boolean skipValidation, CloseableHttpClient client) {
    this(apiSite, apiKey, skipValidation, client,  Option.of(DEFAULT_HOST));
  }

  public DatadogHttpClient(ApiSite apiSite, String apiKey, boolean skipValidation, int timeoutSeconds, Option<String> host) {
    this(apiSite, apiKey, skipValidation, HttpClientBuilder.create()
        .setDefaultRequestConfig(RequestConfig.custom()
            .setConnectTimeout(timeoutSeconds * 1000)
            .setConnectionRequestTimeout(timeoutSeconds * 1000)
            .setSocketTimeout(timeoutSeconds * 1000).build())
        .build(), host);
  }

  private void validateApiKey() {
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(apiKey),
        "API key is null or empty.");

    HttpUriRequest request = new HttpGet(validateUrl);
    request.setHeader(HEADER_KEY_API_KEY, apiKey);
    try (CloseableHttpResponse response = client.execute(request)) {
      int statusCode = response.getStatusLine().getStatusCode();
      ValidationUtils.checkState(statusCode == HttpStatus.SC_OK, "API key is invalid.");
    } catch (IOException e) {
      throw new IllegalStateException("Failed to connect to Datadog to validate API key.", e);
    }
  }

  public void send(String payload) {
    HttpPost request = new HttpPost(seriesUrl);
    request.setHeader(HEADER_KEY_API_KEY, apiKey);
    request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
    request.setEntity(new StringEntity(payload, ContentType.APPLICATION_JSON));
    try (CloseableHttpResponse response = client.execute(request)) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode >= 300) {
        LOG.warn(String.format("Failed to send to Datadog. Response was %s", response));
      } else {
        LOG.debug(String.format("Sent metrics data (size: %d) to %s", payload.length(), seriesUrl));
      }
    } catch (IOException e) {
      LOG.warn("Failed to send to Datadog.", e);
    }
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  public enum ApiSite {
    US("com"), EU("eu");

    private final String domain;

    ApiSite(String domain) {
      this.domain = domain;
    }

    public String getDomain() {
      return domain;
    }
  }
}
