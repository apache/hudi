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
import org.apache.hudi.metrics.datadog.DatadogReporter.MetricType;
import org.apache.hudi.metrics.datadog.DatadogReporter.PayloadBuilder;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestDatadogReporter {

  @Mock
  Appender appender;

  @Captor
  ArgumentCaptor<LogEvent> logCaptor;

  @Mock
  MetricRegistry registry;

  @Mock
  DatadogHttpClient client;

  @AfterEach
  void resetMocks() {
    ((Logger) LogManager.getLogger(DatadogReporter.class)).removeAppender(appender);
    reset(appender, registry, client);
  }

  @Test
  public void stopShouldCloseEnclosedClient() throws IOException {
    new DatadogReporter(registry, client, "foo", Option.empty(), Option.empty(),
        MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.SECONDS).stop();

    verify(client).close();
  }

  @Test
  public void stopShouldLogWhenEnclosedClientFailToClose() throws IOException {
    when(appender.getName()).thenReturn("MockAppender");
    when(appender.isStarted()).thenReturn(true);
    when(appender.isStopped()).thenReturn(false);
    ((Logger) LogManager.getLogger(DatadogReporter.class)).addAppender(appender);
    
    doThrow(IOException.class).when(client).close();

    new DatadogReporter(registry, client, "foo", Option.empty(), Option.empty(),
        MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.SECONDS).stop();

    verify(appender).append(logCaptor.capture());
    assertEquals("Error disconnecting from Datadog.", logCaptor.getValue().getMessage().getFormattedMessage());
    assertEquals(Level.WARN, logCaptor.getValue().getLevel());
  }

  @Test
  public void prefixShouldPrepend() {
    DatadogReporter reporter = new DatadogReporter(
        registry, client, "foo", Option.empty(), Option.empty(),
        MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.SECONDS);
    assertEquals("foo.bar", reporter.prefix("bar"));
  }

  @Test
  public void payloadBuilderShouldBuildExpectedPayloadString() {
    String payload = new PayloadBuilder()
        .withMetricType(MetricType.gauge)
        .addGauge("foo", 0, 0)
        .addGauge("bar", 1, 999)
        .withHost("xhost")
        .withTags(Arrays.asList("tag1", "tag2"))
        .build();
    assertEquals(
        "{\"series\":["
            + "{\"metric\":\"foo\",\"points\":[[0,0]],\"host\":\"xhost\",\"tags\":[\"tag1\",\"tag2\"]},"
            + "{\"metric\":\"bar\",\"points\":[[1,999]],\"host\":\"xhost\",\"tags\":[\"tag1\",\"tag2\"]}]}",
        payload);
  }
}
