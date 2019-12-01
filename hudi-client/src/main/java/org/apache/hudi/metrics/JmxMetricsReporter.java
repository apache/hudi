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

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.common.base.Preconditions;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Iterator;
import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.rmi.registry.LocateRegistry;
import org.apache.hudi.client.utils.NetUtils;

/**
 * Implementation of Jmx reporter, which used to report jmx metric.
 */
public class JmxMetricsReporter extends MetricsReporter {
  private static final Logger LOG = LogManager.getLogger(JmxMetricsReporter.class);
  private final JMXConnectorServer connector;
  private final JmxReporter reporter;
  private String host;

  public JmxMetricsReporter(HoodieWriteConfig config, MetricRegistry registry) {
    try {
      // Check the host and port here
      this.host = config.getJmxHost();
      String portsConfig = config.getJmxPorts();
      if (host == null || portsConfig == null) {
        throw new RuntimeException(
            String.format("Jmx cannot be initialized with host[%s] and port[%s].",
                host, portsConfig));
      }

      Iterator<Integer> ports = NetUtils.getPortRangeFromString(portsConfig);
      JMXConnectorServer successConnectorServer = null;
      JmxReporter successReporter = null;
      while (ports.hasNext() && successConnectorServer == null) {
        int port = ports.next();
        LocateRegistry.createRegistry(port);
        String serviceUrl =
            "service:jmx:rmi://" + host + ":" + port + "/jndi/rmi://" + host + ":" + port
                + "/jmxrmi";
        JMXServiceURL url = new JMXServiceURL(serviceUrl);
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        successConnectorServer = JMXConnectorServerFactory
            .newJMXConnectorServer(url, null, mBeanServer);
        successReporter = JmxReporter.forRegistry(registry).registerWith(mBeanServer).build();
      }
      if (successConnectorServer == null || successReporter == null) {
        throw new RuntimeException(
            "Could not start JMX server on any configured port. Ports: " + portsConfig);
      }
      this.connector = successConnectorServer;
      this.reporter = successReporter;
      LOG.info("Configured JMXReporter with {port:" + portsConfig + "}");
    } catch (Exception e) {
      String msg = "Jmx initialize failed: ";
      LOG.error(msg, e);
      throw new HoodieException(msg, e);
    }
  }

  @Override
  public void start() {
    try {
      Preconditions.checkNotNull(connector, "Cannot start as the jmxReporter is null.");
      connector.start();
      reporter.start();
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  @Override
  public void report() {
  }

  @Override
  public Closeable getReporter() {
    return reporter;
  }
}
