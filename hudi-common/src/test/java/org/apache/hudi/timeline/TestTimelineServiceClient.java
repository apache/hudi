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

package org.apache.hudi.timeline;

import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.Option;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.http.NoHttpResponseException;
import org.apache.http.ParseException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link public class TestTimelineServiceClient}.
 */
class TestTimelineServiceClient {

  private static final String TEST_ENDPOINT = "/test-endpoint";
  private static final int DEFAULT_READ_TIMEOUT_SECS = 5;
  private static final boolean DEFAULT_HTTP_RESPONSE = true;

  private Server server;
  private int serverPort;

  @BeforeEach
  public void setUp() throws Exception {
    // Create a Jetty server
    server = new Server(0);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    server.setHandler(context);
    context.addServlet(new ServletHolder(new TestServlet()), TEST_ENDPOINT);

    // Start the server
    server.start();
    serverPort = server.getURI().getPort();
  }

  @AfterEach
  public void tearDown() throws Exception {
    // Stop the server after each test
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testSuccessfulGetRequest() throws IOException {
    FileSystemViewStorageConfig.Builder builder = FileSystemViewStorageConfig.newBuilder().withRemoteServerHost("localhost")
        .withRemoteServerPort(serverPort)
        .withRemoteTimelineClientTimeoutSecs(DEFAULT_READ_TIMEOUT_SECS);
    MockTimelineServiceNetworkClient client = new MockTimelineServiceNetworkClient(builder.build());
    TimelineServiceClientBase.Request request =
        TimelineServiceClientBase.Request.newBuilder(TimelineServiceClientBase.RequestMethod.GET, TEST_ENDPOINT).build();
    TimelineServiceClientBase.Response response = client.makeRequest(request);
    assertEquals(DEFAULT_HTTP_RESPONSE, response.getDecodedContent(new TypeReference<Boolean>() {}));
  }

  @Test
  public void testSuccessfulPostRequest() throws IOException {
    FileSystemViewStorageConfig.Builder builder = FileSystemViewStorageConfig.newBuilder().withRemoteServerHost("localhost")
        .withRemoteServerPort(serverPort)
        .withRemoteTimelineClientTimeoutSecs(DEFAULT_READ_TIMEOUT_SECS);
    MockTimelineServiceNetworkClient client = new MockTimelineServiceNetworkClient(builder.build());
    TimelineServiceClientBase.Request request = TimelineServiceClientBase.Request.newBuilder(TimelineServiceClientBase.RequestMethod.POST, TEST_ENDPOINT)
        .addQueryParam("key1", "val1")
        .addQueryParams(Collections.singletonMap("key2", "val2"))
        .build();
    TimelineServiceClientBase.Response response = client.makeRequest(request);
    assertEquals(DEFAULT_HTTP_RESPONSE, response.getDecodedContent(new TypeReference<Boolean>() {}));
  }

  private static List<Arguments> testScenariosForFailures() {
    return asList(
        // Ensure that the retries can handle both IOExceptions and RuntimeExceptions.
        Arguments.of(5, 2, InducedFailuresInfo.ExceptionType.NO_HTTP_RESPONSE_EXCEPTION, true),
        Arguments.of(5, 2, InducedFailuresInfo.ExceptionType.PARSE_EXCEPTION, true),
        Arguments.of(2, 5, InducedFailuresInfo.ExceptionType.NO_HTTP_RESPONSE_EXCEPTION, false),
        Arguments.of(2, 5, InducedFailuresInfo.ExceptionType.PARSE_EXCEPTION, false)
        );
  }

  @ParameterizedTest
  @MethodSource("testScenariosForFailures")
  public void testRetriesForExceptions(int numberOfRetries,
                                                    int numberOfInducedFailures,
                                                    InducedFailuresInfo.ExceptionType exceptionType,
                                                    boolean shouldSucceed) throws IOException {
    FileSystemViewStorageConfig.Builder builder = FileSystemViewStorageConfig.newBuilder().withRemoteServerHost("localhost")
        .withRemoteServerPort(serverPort)
        .withRemoteTimelineClientTimeoutSecs(DEFAULT_READ_TIMEOUT_SECS)
        .withRemoteTimelineClientRetry(true)
        .withRemoteTimelineClientMaxRetryIntervalMs(2000L)
        .withRemoteTimelineClientMaxRetryNumbers(numberOfRetries);

    InducedFailuresInfo inducedFailuresInfo = new InducedFailuresInfo(exceptionType, numberOfInducedFailures);
    MockTimelineServiceNetworkClient client = new MockTimelineServiceNetworkClient(builder.build(), Option.of(inducedFailuresInfo));

    TimelineServiceClientBase.Request request = TimelineServiceClientBase.Request.newBuilder(TimelineServiceClientBase.RequestMethod.GET, TEST_ENDPOINT).build();
    if (shouldSucceed) {
      TimelineServiceClientBase.Response response = client.makeRequest(request);
      assertEquals(DEFAULT_HTTP_RESPONSE, response.getDecodedContent(new TypeReference<Boolean>() {}));
    } else {
      Class<? extends Exception> expectedException = exceptionType.getExceptionType();
      assertThrows(expectedException, () -> client.makeRequest(request), "Should throw an Exception.'");
    }
  }

  private static class MockTimelineServiceNetworkClient extends TimelineServiceClient {

    private final Option<InducedFailuresInfo> inducedFailuresInfo;
    private int currentInducedFailures;

    public MockTimelineServiceNetworkClient(FileSystemViewStorageConfig config) {
      this(config, Option.empty());
    }

    public MockTimelineServiceNetworkClient(FileSystemViewStorageConfig config, Option<InducedFailuresInfo> inducedFailuresInfo) {
      super(config);
      this.inducedFailuresInfo = inducedFailuresInfo;
      currentInducedFailures = 0;
    }

    @Override
    protected Response executeRequest(Request request) throws IOException {
      if (inducedFailuresInfo.isPresent() && ++currentInducedFailures <= inducedFailuresInfo.get().maxInducedFailures) {
        switch (inducedFailuresInfo.get().exceptionType) {
          case PARSE_EXCEPTION:
            throw new ParseException("Parse Exception");
          case NO_HTTP_RESPONSE_EXCEPTION:
          default:
            throw new NoHttpResponseException("No HTTP Response Exception");
        }
      }
      return super.executeRequest(request);
    }
  }

  @AllArgsConstructor
  private static class InducedFailuresInfo {
    private final ExceptionType exceptionType;
    private final int maxInducedFailures;

    @Getter
    @AllArgsConstructor(access = AccessLevel.PACKAGE)
    public enum ExceptionType {
      NO_HTTP_RESPONSE_EXCEPTION(NoHttpResponseException.class),
      PARSE_EXCEPTION(ParseException.class);

      private final Class<? extends Exception> exceptionType;
    }
  }

  // A simple servlet to handle HTTP requests for test purposes.
  private static class TestServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      sendResponse(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      sendResponse(req, resp);
    }

    private void sendResponse(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      // Set the content type
      resp.setContentType("text/plain");
      resp.setStatus(HttpServletResponse.SC_OK);
      ObjectMapper objectMapper = new ObjectMapper();
      resp.getWriter().println(objectMapper.writeValueAsString(DEFAULT_HTTP_RESPONSE));
    }
  }
}
