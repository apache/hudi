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
import org.apache.hudi.config.metrics.HoodieMetricsJmxConfig;
import org.apache.hudi.exception.HoodieException;

import com.codahale.metrics.MetricRegistry;
import org.apache.log4j.LogManager;

import javax.management.MBeanServer;

import java.lang.management.ManagementFactory;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Implementation of Jmx reporter, which used to report jmx metric.
 */
public class JmxMetricsReporter extends MetricsReporter {

  private static final org.apache.log4j.Logger LOG = LogManager.getLogger(JmxMetricsReporter.class);

  private final MetricRegistry registry;
  private JmxReporterServer jmxReporterServer;

  public JmxMetricsReporter(HoodieWriteConfig config, MetricRegistry registry) {
    try {
      this.registry = registry;
      // Check the host and port here
      String host = config.getString(HoodieMetricsJmxConfig.JMX_HOST_NAME);
      String portsConfig = config.getString(HoodieMetricsJmxConfig.JMX_PORT_NUM);
      if (host == null || portsConfig == null) {
        throw new HoodieException(
            String.format("Jmx cannot be initialized with host[%s] and port[%s].",
                host, portsConfig));
      }
      int[] ports = getPortRangeFromString(portsConfig);
      boolean successfullyStartedServer = false;
      for (int port : ports) {
        jmxReporterServer = createJmxReport(host, port);
        LOG.info("Started JMX server on port " + port + ".");
        successfullyStartedServer = true;
        break;
      }
      if (!successfullyStartedServer) {
        throw new HoodieException(
            "Could not start JMX server on any configured port. Ports: " + portsConfig);
      }
      LOG.info("Configured JMXReporter with {port:" + portsConfig + "}");
    } catch (Exception e) {
      String msg = "Jmx initialize failed: ";
      LOG.error(msg, e);
      throw new HoodieException(msg, e);
    }
  }

  @Override
  public void start() {
    if (jmxReporterServer != null) {
      jmxReporterServer.start();
    } else {
      LOG.error("Cannot start as the jmxReporter is null.");
    }
  }

  @Override
  public void report() {
  }

  @Override
  public void stop() {
    Objects.requireNonNull(jmxReporterServer, "jmxReporterServer is not running.");
    try {
      jmxReporterServer.stop();
    } catch (Exception e) {
      throw new HoodieException("Stop jmxReporterServer fail", e);
    }
  }

  private JmxReporterServer createJmxReport(String host, int port) {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    return JmxReporterServer.forRegistry(registry)
        .host(host)
        .port(port)
        .registerWith(mBeanServer)
        .build();
  }

  private int[] getPortRangeFromString(String portsConfig) {
    String range = portsConfig.trim();
    int dashIdx = range.indexOf('-');
    final int start;
    final int end;
    if (dashIdx == -1) {
      start = Integer.parseInt(range);
      end = Integer.parseInt(range);
    } else {
      start = Integer.parseInt(range.substring(0, dashIdx));
      end = Integer.parseInt(range.substring(dashIdx + 1));
    }
    return IntStream.rangeClosed(start, end)
        .filter(port -> (0 <= port && port <= 65535))
        .toArray();
  }
}
