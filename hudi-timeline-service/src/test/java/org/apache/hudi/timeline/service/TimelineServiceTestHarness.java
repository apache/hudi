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

package org.apache.hudi.timeline.service;

import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Map;

/**
 * Provides a wrapped {@link TimelineService} to emulate HTTP request
 * failures for testing. When number of failures N is configured above 0,
 * it proxies all HTTP requests to the wrapper timeline service,
 * and fails the first N HTTP requests.
 */
public class TimelineServiceTestHarness extends TimelineService {

  private static final String PROXY_ALL_URLS = "/*";
  @Setter
  private int numberOfSimulatedConnectionFailures;
  private Option<Server> server;
  private int serverPort;

  public TimelineServiceTestHarness(Configuration hadoopConf,
                                    Config timelineServerConf,
                                    FileSystemViewManager globalFileSystemViewManager) throws IOException {
    super(
        new HadoopStorageConfiguration(hadoopConf),
        timelineServerConf,
        globalFileSystemViewManager);
    server = Option.empty();
    serverPort = 0;
  }

  @Override
  public int startService() throws IOException {
    if (numberOfSimulatedConnectionFailures > 0) {
      try {
        int timelineServicePort = super.startService();
        server = Option.of(new Server(serverPort));
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.addServlet(new ServletHolder(new FailureInducingHttpServlet(timelineServicePort, numberOfSimulatedConnectionFailures)), PROXY_ALL_URLS);
        server.get().setHandler(context);
        server.get().start();
        serverPort = server.get().getURI().getPort();
        // Proxy requests so we can emulate failure.
        return serverPort;
      } catch (Exception exception) {
        throw new IOException(exception);
      }
    }
    // Act as a pass through in the case that failure emulation is not required.
    return super.startService();
  }

  @Override
  public int getServerPort() {
    if (serverPort > 0) {
      return serverPort;
    }
    return super.getServerPort();
  }

  @Override
  public void close() {
    super.close();
    server.ifPresent(server -> {
      try {
        server.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private int numberOfSimulatedConnectionFailures = 0;

    public Builder withNumberOfSimulatedConnectionFailures(int numberOfSimulatedConnectionFailures) {
      this.numberOfSimulatedConnectionFailures = numberOfSimulatedConnectionFailures;
      return this;
    }

    public TimelineServiceTestHarness build(Configuration hadoopConf,
                                            Config timelineServerConf,
                                            FileSystemViewManager globalFileSystemViewManager) throws IOException {
      TimelineServiceTestHarness timelineServiceTestHarness = new TimelineServiceTestHarness(hadoopConf, timelineServerConf, globalFileSystemViewManager);
      timelineServiceTestHarness.setNumberOfSimulatedConnectionFailures(numberOfSimulatedConnectionFailures);
      return timelineServiceTestHarness;
    }
  }

  private static class FailureInducingHttpServlet extends HttpServlet {

    private final int timelineServerPort;
    private final int maxSimulatedConnectionFailures;
    private int currentNumConnectionSimulatedFailures;

    public FailureInducingHttpServlet(int timelineServerPort,
                                      int maxSimulatedConnectionFailures) {
      this.timelineServerPort = timelineServerPort;
      this.maxSimulatedConnectionFailures = maxSimulatedConnectionFailures;
      currentNumConnectionSimulatedFailures = 0;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      // Emulate HTTP request failures for the first maxSimulatedConnectionFailures requests.
      if (++currentNumConnectionSimulatedFailures <= maxSimulatedConnectionFailures) {
        throw new NoHttpResponseException("Simulated connection failure");
      }

      // After maxSimulatedConnectionFailures requests, proxy the requests to the actual Timeline service.
      URIBuilder builder =
          new URIBuilder().setHost("localhost")
              .setPort(timelineServerPort)
              .setPath(req.getPathInfo())
              .setScheme("http");

      Map<String, String[]> parameterMap = req.getParameterMap();
      for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
        String paramName = entry.getKey();
        String[] paramValues = entry.getValue();
        // Store each value in the Result object
        for (String value : paramValues) {
          builder.addParameter(paramName, value);
        }
      }

      String url = builder.toString();
      org.apache.http.client.fluent.Response response;
      switch (req.getMethod()) {
        case "GET":
          response = org.apache.http.client.fluent.Request.Get(url).connectTimeout(10000).socketTimeout(10000).execute();
          break;
        case "POST":
        default:
          response = org.apache.http.client.fluent.Request.Post(url).connectTimeout(10000).socketTimeout(10000).execute();
      }

      HttpResponse httpResponse = response.returnResponse();
      resp.setContentType(httpResponse.getEntity().getContentType().getValue());
      resp.setStatus(httpResponse.getStatusLine().getStatusCode());
      resp.getWriter().println(EntityUtils.toString(httpResponse.getEntity()));
    }
  }
}
