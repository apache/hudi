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

import com.google.common.base.Preconditions;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.rmi.registry.LocateRegistry;

/**
 * Implementation of Jmx reporter, which used to report jmx metric.
 */
public class JmxMetricsReporter extends MetricsReporter {

  private static Logger logger = LogManager.getLogger(JmxMetricsReporter.class);
  private final JMXConnectorServer connector;
  private String host;
  private int port;

  public JmxMetricsReporter(HoodieWriteConfig config) {
    try {
      // Check the host and port here
      this.host = config.getJmxHost();
      this.port = config.getJmxPort();
      if (host == null || port == 0) {
        throw new RuntimeException(
            String.format("Jmx cannot be initialized with host[%s] and port[%s].",
                host, port));
      }
      LocateRegistry.createRegistry(port);
      String serviceUrl =
          "service:jmx:rmi://" + host + ":" + port + "/jndi/rmi://" + host + ":" + port + "/jmxrmi";
      JMXServiceURL url = new JMXServiceURL(serviceUrl);
      this.connector = JMXConnectorServerFactory
          .newJMXConnectorServer(url, null, ManagementFactory.getPlatformMBeanServer());
    } catch (Exception e) {
      String msg = "Jmx initialize failed: ";
      logger.error(msg, e);
      throw new HoodieException(msg, e);
    }
  }

  @Override
  public void start() {
    try {
      Preconditions.checkNotNull(connector, "Cannot start as the jmxReporter is null.");
      connector.start();
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  @Override
  public void report() {
  }

  @Override
  public Closeable getReporter() {
    return null;
  }
}
