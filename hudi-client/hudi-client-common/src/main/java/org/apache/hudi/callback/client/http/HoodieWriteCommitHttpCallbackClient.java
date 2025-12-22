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

package org.apache.hudi.callback.client.http;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.config.HoodieWriteCommitCallbackConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Write commit callback http client.
 */
public class HoodieWriteCommitHttpCallbackClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieWriteCommitHttpCallbackClient.class);

  public static final String HEADER_KEY_API_KEY = "HUDI-CALLBACK-KEY";
  static final String HEADERS_DELIMITER = ";";
  static final String HEADERS_KV_DELIMITER = ":";

  private final String apiKey;
  private final String url;
  private final CloseableHttpClient client;
  private HoodieWriteConfig writeConfig;
  private final Map<String, String> customHeaders;

  public HoodieWriteCommitHttpCallbackClient(HoodieWriteConfig config) {
    this.writeConfig = config;
    this.apiKey = getApiKey();
    this.url = getUrl();
    this.client = getClient();
    this.customHeaders = parseCustomHeaders();
  }

  public HoodieWriteCommitHttpCallbackClient(String apiKey, String url, CloseableHttpClient client, Map<String, String> customHeaders) {
    this.apiKey = apiKey;
    this.url = url;
    this.client = client;
    this.customHeaders = customHeaders != null ? customHeaders : new HashMap<>();
  }

  public void send(String callbackMsg) {
    HttpPost request = new HttpPost(url);
    request.setHeader(HEADER_KEY_API_KEY, apiKey);
    request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
    customHeaders.forEach(request::setHeader);
    request.setEntity(new StringEntity(callbackMsg, ContentType.APPLICATION_JSON));
    try (CloseableHttpResponse response = client.execute(request)) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode >= 300) {
        LOG.warn("Failed to send callback message. Response was {}", response);
      } else {
        LOG.info("Sent Callback data with {} custom headers to {} successfully !", customHeaders.size(), url);
      }
    } catch (IOException e) {
      LOG.warn("Failed to send callback.", e);
    }
  }

  private String getApiKey() {
    return writeConfig.getString(HoodieWriteCommitCallbackConfig.CALLBACK_HTTP_API_KEY_VALUE);
  }

  private String getUrl() {
    return writeConfig.getString(HoodieWriteCommitCallbackConfig.CALLBACK_HTTP_URL);
  }

  private CloseableHttpClient getClient() {
    int timeoutSeconds = getHttpTimeoutSeconds() * 1000;
    return HttpClientBuilder.create()
        .setDefaultRequestConfig(RequestConfig.custom()
            .setConnectTimeout(timeoutSeconds)
            .setConnectionRequestTimeout(timeoutSeconds)
            .setSocketTimeout(timeoutSeconds).build())
        .build();
  }

  private Integer getHttpTimeoutSeconds() {
    return writeConfig.getInt(HoodieWriteCommitCallbackConfig.CALLBACK_HTTP_TIMEOUT_IN_SECONDS);
  }

  private Map<String, String> parseCustomHeaders() {
    Map<String, String> headers = new HashMap<>();
    String headersString = writeConfig.getString(HoodieWriteCommitCallbackConfig.CALLBACK_HTTP_CUSTOM_HEADERS);
    if (!StringUtils.isNullOrEmpty(headersString)) {
      StringTokenizer tokenizer = new StringTokenizer(headersString, HEADERS_DELIMITER);
      while (tokenizer.hasMoreTokens()) {
        String token = tokenizer.nextToken();
        if (!StringUtils.isNullOrEmpty(token)) {
          String[] keyValue = token.split(HEADERS_KV_DELIMITER);
          if (keyValue.length == 2) {
            String trimKey = keyValue[0].trim();
            String trimValue = keyValue[1].trim();
            if (trimKey.length() > 0 && trimValue.length() > 0) {
              headers.put(trimKey, trimValue);
            }
          } else {
            LOG.warn("Unable to parse some custom headers. Supported format is: Header_name1:Header value1;Header_name2:Header value2");
          }
        }
      }
    }
    return headers;
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  @VisibleForTesting
  String getCustomHeaders() {
    return customHeaders.toString();
  }
}
