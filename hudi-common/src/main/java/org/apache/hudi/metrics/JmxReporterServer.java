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

package org.apache.hudi.metrics;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import lombok.Getter;

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import java.io.IOException;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Objects;

/**
 * A reporter which publishes metric values to a JMX server.
 */
public class JmxReporterServer {

  /**
   * Returns a new {@link JmxReporterServer.Builder} for {@link JmxReporterServer}.
   *
   * @param registry the registry to report
   * @return a {@link JmxReporterServer.Builder} instance for a {@link JmxReporterServer}
   */
  public static JmxReporterServer.Builder forRegistry(MetricRegistry registry) {
    return new JmxReporterServer.Builder(registry);
  }

  /**
   * A builder for {@link JmxReporterServer} instances.
   */
  public static class Builder {

    private final MetricRegistry registry;
    private MBeanServer mBeanServer;
    private String host;
    private int port;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
    }

    public JmxReporterServer.Builder host(String host) {
      this.host = host;
      return this;
    }

    public JmxReporterServer.Builder port(int port) {
      this.port = port;
      return this;
    }

    public JmxReporterServer.Builder registerWith(MBeanServer mBeanServer) {
      this.mBeanServer = mBeanServer;
      return this;
    }

    public JmxReporterServer build() {
      Objects.requireNonNull(registry, "registry cannot be null!");
      Objects.requireNonNull(mBeanServer, "mBeanServer cannot be null!");
      ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(host), "host cannot be null or empty!");
      return new JmxReporterServer(registry, host, port, mBeanServer);
    }
  }

  private JMXConnectorServer connector;
  private Registry rmiRegistry;
  @Getter
  private JmxReporter reporter;

  protected JmxReporterServer(MetricRegistry registry, String host, int port,
      MBeanServer mBeanServer) {
    String serviceUrl =
        "service:jmx:rmi://localhost:" + port + "/jndi/rmi://" + host + ":" + port + "/jmxrmi";
    try {
      JMXServiceURL url = new JMXServiceURL(serviceUrl);
      connector = JMXConnectorServerFactory
          .newJMXConnectorServer(url, null, mBeanServer);
      rmiRegistry = LocateRegistry.createRegistry(port);
      reporter = JmxReporter.forRegistry(registry).registerWith(mBeanServer).build();
    } catch (Exception e) {
      throw new HoodieException("Jmx service url created " + serviceUrl, e);
    }
  }

  public void start() {
    ValidationUtils.checkArgument(reporter != null && connector != null,
        "reporter or connector cannot be null!");
    try {
      connector.start();
      reporter.start();
    } catch (Exception e) {
      throw new HoodieException("connector or reporter start failed", e);
    }
  }

  public void stop() throws IOException {
    stopConnector();
    stopReport();
    stopRmiRegistry();
  }

  private void stopRmiRegistry() {
    if (rmiRegistry != null) {
      try {
        UnicastRemoteObject.unexportObject(rmiRegistry, true);
      } catch (NoSuchObjectException e) {
        throw new HoodieException("Could not un-export our RMI registry", e);
      } finally {
        rmiRegistry = null;
      }
    }
  }

  private void stopConnector() throws IOException {
    if (connector != null) {
      try {
        connector.stop();
      } finally {
        connector = null;
      }
    }
  }

  private void stopReport() {
    if (reporter != null) {
      try {
        reporter.stop();
      } finally {
        reporter = null;
      }
    }
  }
}
