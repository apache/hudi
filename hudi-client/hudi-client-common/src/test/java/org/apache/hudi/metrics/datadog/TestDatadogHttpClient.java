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

import org.apache.hudi.metrics.datadog.DatadogHttpClient.ApiSite;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestDatadogHttpClient {

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
    Logger logger = (Logger) LogManager.getLogger(DatadogHttpClient.class);
    initialLogLevel = logger.getLevel();
    logger.setLevel(Level.DEBUG);
    logger.addAppender(appender);
  }

  @AfterEach
  void resetMocks() {
    Logger logger = (Logger) LogManager.getLogger(DatadogHttpClient.class);
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
  public void validateApiKeyShouldThrowExceptionWhenRequestFailed() throws IOException {
    when(httpClient.execute(any())).thenThrow(IOException.class);

    Throwable t = assertThrows(IllegalStateException.class, () -> {
      new DatadogHttpClient(ApiSite.EU, "foo", false, httpClient);
    });
    assertEquals("Failed to connect to Datadog to validate API key.", t.getMessage());
  }

  @Test
  public void validateApiKeyShouldThrowExceptionWhenResponseNotSuccessful() {
    mockResponse(500);

    Throwable t = assertThrows(IllegalStateException.class, () -> {
      new DatadogHttpClient(ApiSite.EU, "foo", false, httpClient);
    });
    assertEquals("API key is invalid.", t.getMessage());
  }

  @Test
  public void sendPayloadShouldLogWhenRequestFailed() throws IOException {
    when(httpClient.execute(any())).thenThrow(IOException.class);

    DatadogHttpClient ddClient = new DatadogHttpClient(ApiSite.US, "foo", true, httpClient);
    ddClient.send("{}");

    verify(appender).append(logCaptor.capture());
    assertEquals("Failed to send to Datadog.", logCaptor.getValue().getMessage().getFormattedMessage());
    assertEquals(Level.WARN, logCaptor.getValue().getLevel());
  }

  @Test
  public void sendPayloadShouldLogUnsuccessfulSending() {
    mockResponse(401);
    when(httpResponse.toString()).thenReturn("unauthorized");

    DatadogHttpClient ddClient = new DatadogHttpClient(ApiSite.US, "foo", true, httpClient);
    ddClient.send("{}");

    verify(appender).append(logCaptor.capture());
    assertEquals("Failed to send to Datadog. Response was unauthorized", logCaptor.getValue().getMessage().getFormattedMessage());
    assertEquals(Level.WARN, logCaptor.getValue().getLevel());
  }

  @Test
  public void sendPayloadShouldLogSuccessfulSending() {
    mockResponse(202);

    DatadogHttpClient ddClient = new DatadogHttpClient(ApiSite.US, "foo", true, httpClient);
    ddClient.send("{}");
    
    verify(appender).append(logCaptor.capture());
    assertTrue(logCaptor.getValue().getMessage().getFormattedMessage().startsWith("Sent metrics data"));
    assertEquals(Level.DEBUG, logCaptor.getValue().getLevel());
  }

  public static List<Arguments> getApiSiteAndDomain() {
    return Arrays.asList(
        Arguments.of("US", "com"),
        Arguments.of("EU", "eu")
    );
  }

  @ParameterizedTest
  @MethodSource("getApiSiteAndDomain")
  public void testApiSiteReturnCorrectDomain(String apiSite, String domain) {
    assertEquals(domain, ApiSite.valueOf(apiSite).getDomain());
  }
}
