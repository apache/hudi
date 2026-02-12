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

import org.apache.hudi.config.HoodieWriteCommitCallbackConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link HoodieWriteCommitHttpCallbackClient}.
 */
@ExtendWith(MockitoExtension.class)
class TestCallbackHttpClient {

  public static final String FAKE_API_KEY = "fake_api_key";
  public static final String FAKE_URL = "fake_url";
  public static final String CALLBACK_MSG = "{}";
  public static final String RESPONSE_UNAUTHORIZED = "unauthorized";
  @Mock
  Appender appender;

  @Captor
  ArgumentCaptor<LogEvent> logCaptor;

  @Mock
  CloseableHttpClient httpClient;

  @Mock
  CloseableHttpResponse httpResponse;

  @Mock
  StatusLine statusLine;

  private Level initialLogLevel;

  @BeforeEach
  void prepareAppender() {
    when(appender.getName()).thenReturn("MockAppender-" + UUID.randomUUID());
    when(appender.isStarted()).thenReturn(true);
    when(appender.isStopped()).thenReturn(false);
    Logger logger = (Logger) LogManager.getLogger(HoodieWriteCommitHttpCallbackClient.class);
    initialLogLevel = logger.getLevel();
    logger.setLevel(Level.DEBUG);
    logger.addAppender(appender);
  }

  @AfterEach
  void resetMocks() {
    Logger logger = (Logger) LogManager.getLogger(HoodieWriteCommitHttpCallbackClient.class);
    logger.setLevel(initialLogLevel);
    logger.removeAppender(appender);
    reset(appender, httpClient, httpResponse, statusLine);
  }

  private void mockResponse(int statusCode) {
    when(statusLine.getStatusCode()).thenReturn(statusCode);
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    try {
      when(httpClient.execute(any())).thenReturn(httpResponse);
    } catch (IOException e) {
      fail(e.getMessage(), e);
    }
  }

  @Test
  void sendPayloadShouldLogWhenRequestFailed() throws IOException {
    when(httpClient.execute(any())).thenThrow(IOException.class);

    HoodieWriteCommitHttpCallbackClient hoodieWriteCommitCallBackHttpClient =
        new HoodieWriteCommitHttpCallbackClient(FAKE_API_KEY, FAKE_URL, httpClient, null);
    hoodieWriteCommitCallBackHttpClient.send(CALLBACK_MSG);

    verify(appender).append(logCaptor.capture());
    assertEquals("Failed to send callback.", logCaptor.getValue().getMessage().getFormattedMessage());
    assertEquals(Level.WARN, logCaptor.getValue().getLevel());
  }

  @Test
  void sendPayloadShouldLogUnsuccessfulSending() {
    mockResponse(401);
    when(httpResponse.toString()).thenReturn(RESPONSE_UNAUTHORIZED);

    HoodieWriteCommitHttpCallbackClient hoodieWriteCommitCallBackHttpClient =
        new HoodieWriteCommitHttpCallbackClient(FAKE_API_KEY, FAKE_URL, httpClient, null);
    hoodieWriteCommitCallBackHttpClient.send(CALLBACK_MSG);

    verify(appender).append(logCaptor.capture());
    assertEquals("Failed to send callback message. Response was " + RESPONSE_UNAUTHORIZED, logCaptor.getValue().getMessage().getFormattedMessage());
    assertEquals(Level.ERROR, logCaptor.getValue().getLevel());
  }

  @Test
  void sendPayloadShouldLogSuccessfulSending() {
    mockResponse(202);

    Map<String, String> customHeaders = new HashMap<>();
    customHeaders.put("key1", "val1");
    customHeaders.put("key2", "val2");
    HoodieWriteCommitHttpCallbackClient hoodieWriteCommitCallBackHttpClient =
        new HoodieWriteCommitHttpCallbackClient(FAKE_API_KEY, FAKE_URL, httpClient, customHeaders);
    hoodieWriteCommitCallBackHttpClient.send(CALLBACK_MSG);

    verify(appender).append(logCaptor.capture());
    assertTrue(logCaptor.getValue().getMessage().getFormattedMessage().startsWith("Sent Callback data with 2 custom headers"));
    assertEquals(Level.INFO, logCaptor.getValue().getLevel());
  }

  @Test
  void testParsingCustomHeaders() {
    String customHeaders = "Authorization " + HoodieWriteCommitHttpCallbackClient.HEADERS_KV_DELIMITER + "Basic 12345678";
    HoodieWriteCommitHttpCallbackClient client = makeClient(customHeaders);
    assertEquals("{Authorization=Basic 12345678}", client.getCustomHeaders());
    customHeaders = "Authorization " + HoodieWriteCommitHttpCallbackClient.HEADERS_KV_DELIMITER + "Basic 12345678" + HoodieWriteCommitHttpCallbackClient.HEADERS_DELIMITER
        + " another_header_key " + HoodieWriteCommitHttpCallbackClient.HEADERS_KV_DELIMITER + " another_header_value ";
    client = makeClient(customHeaders);
    assertEquals("{Authorization=Basic 12345678, another_header_key=another_header_value}", client.getCustomHeaders());
    customHeaders = "Authorization" + HoodieWriteCommitHttpCallbackClient.HEADERS_KV_DELIMITER + "Basic 12345678" + HoodieWriteCommitHttpCallbackClient.HEADERS_DELIMITER;
    client = makeClient(customHeaders);
    assertEquals("{Authorization=Basic 12345678}", client.getCustomHeaders());
    customHeaders = "Authorization" + HoodieWriteCommitHttpCallbackClient.HEADERS_KV_DELIMITER + "Basic 12345678"  + HoodieWriteCommitHttpCallbackClient.HEADERS_DELIMITER + "uu";
    client = makeClient(customHeaders);
    assertEquals("{Authorization=Basic 12345678}", client.getCustomHeaders());
    customHeaders = "Authorization" + HoodieWriteCommitHttpCallbackClient.HEADERS_KV_DELIMITER;
    client = makeClient(customHeaders);
    assertEquals("{}", client.getCustomHeaders());
    customHeaders = HoodieWriteCommitHttpCallbackClient.HEADERS_KV_DELIMITER + "Authorization";
    client = makeClient(customHeaders);
    assertEquals("{}", client.getCustomHeaders());
    customHeaders = "Authorization" + HoodieWriteCommitHttpCallbackClient.HEADERS_KV_DELIMITER + "Basic 12345678" + HoodieWriteCommitHttpCallbackClient.HEADERS_KV_DELIMITER
        + "Second header" + HoodieWriteCommitHttpCallbackClient.HEADERS_KV_DELIMITER + "val";
    client = makeClient(customHeaders);
    assertEquals("{}", client.getCustomHeaders());
    customHeaders = null;
    client = makeClient(customHeaders);
    assertEquals("{}", client.getCustomHeaders());
    customHeaders = "";
    client = makeClient(customHeaders);
    assertEquals("{}", client.getCustomHeaders());
    customHeaders = "  ";
    client = makeClient(customHeaders);
    assertEquals("{}", client.getCustomHeaders());
    customHeaders = "  " + HoodieWriteCommitHttpCallbackClient.HEADERS_KV_DELIMITER + " ";
    client = makeClient(customHeaders);
    assertEquals("{}", client.getCustomHeaders());
  }

  private HoodieWriteCommitHttpCallbackClient makeClient(String customHeaders) {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("path")
        .withCallbackConfig(HoodieWriteCommitCallbackConfig.newBuilder()
            .withCallbackHttpApiKey(FAKE_API_KEY)
            .withCallbackHttpUrl(FAKE_URL)
            .withCustomHeaders(customHeaders)
            .build())
        .build();
    return new HoodieWriteCommitHttpCallbackClient(config);
  }
}
