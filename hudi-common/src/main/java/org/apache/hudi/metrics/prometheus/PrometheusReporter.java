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

package org.apache.hudi.metrics.prometheus;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.MetricsReporter;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.dropwizard.samplebuilder.DefaultSampleBuilder;
import io.prometheus.client.dropwizard.samplebuilder.SampleBuilder;
import io.prometheus.client.exporter.HTTPServer;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Implementation of Prometheus reporter, which connects to the Http server, and get metrics
 * from that server.
 */
public class PrometheusReporter extends MetricsReporter {
  private static final Pattern LABEL_PATTERN = Pattern.compile("\\s*,\\s*");

  private static final Logger LOG = LoggerFactory.getLogger(PrometheusReporter.class);
  private static final Map<Integer, PrometheusServerState> PORT_TO_SERVER_STATE = new ConcurrentHashMap<>();

  @Getter
  private static class PrometheusServerState {
    private final HTTPServer httpServer;
    private final CollectorRegistry collectorRegistry;
    private final AtomicInteger referenceCount;
    private final Set<DropwizardExports> exports;

    public PrometheusServerState(HTTPServer httpServer, CollectorRegistry collectorRegistry) {
      this.httpServer = httpServer;
      this.collectorRegistry = collectorRegistry;
      this.referenceCount = new AtomicInteger(0);
      this.exports = ConcurrentHashMap.newKeySet();
    }

  }

  private final DropwizardExports metricExports;
  private final CollectorRegistry collectorRegistry;
  private final int serverPort;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private volatile boolean unregistered = false;

  public PrometheusReporter(HoodieMetricsConfig metricsConfig, MetricRegistry registry) {
    this.serverPort = metricsConfig.getPrometheusPort();
    List<String> labelNames = new ArrayList<>();
    List<String> labelValues = new ArrayList<>();
    if (StringUtils.nonEmpty(metricsConfig.getPushGatewayLabels())) {
      LABEL_PATTERN.splitAsStream(metricsConfig.getPushGatewayLabels().trim()).map(s -> s.split(":", 2))
          .forEach(parts -> {
            labelNames.add(parts[0]);
            labelValues.add(parts[1]);
          });
    }
    metricExports = new DropwizardExports(registry, new LabeledSampleBuilder(labelNames, labelValues));
    
    PrometheusServerState serverState = getAndRegisterServerState(serverPort, metricExports);
    this.collectorRegistry = serverState.getCollectorRegistry();
    
    LOG.debug("Registered PrometheusReporter for port {}, reference count: {}", 
             serverPort, serverState.getReferenceCount().get());
  }

  private static synchronized PrometheusServerState getAndRegisterServerState(int serverPort, DropwizardExports metricExports) {
    PrometheusServerState serverState = PORT_TO_SERVER_STATE.get(serverPort);
    if (serverState == null) {
      try {
        CollectorRegistry collectorRegistry = new CollectorRegistry();
        HTTPServer server = new HTTPServer(new InetSocketAddress(serverPort), collectorRegistry, true);
        serverState = new PrometheusServerState(server, collectorRegistry);
        PORT_TO_SERVER_STATE.put(serverPort, serverState);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          try {
            server.close();
          } catch (Exception e) {
            LOG.debug("Error closing Prometheus HTTP server during shutdown: {}", e.getMessage());
          }
        }));
      } catch (Exception e) {
        String msg = "Could not start PrometheusReporter HTTP server on port " + serverPort;
        LOG.error(msg, e);
        throw new HoodieException(msg, e);
      }
    }
    
    metricExports.register(serverState.getCollectorRegistry());
    serverState.getExports().add(metricExports);
    serverState.getReferenceCount().incrementAndGet();
    
    return serverState;
  }

  @Override
  public void start() {
  }

  @Override
  public void report() {
  }

  @Override
  public void stop() {
    if (!stopped.getAndSet(true)) {
      try {
        synchronized (PrometheusReporter.class) {
          LOG.debug("PrometheusReporter.stop() called for port {}", serverPort);
          PrometheusServerState serverState = PORT_TO_SERVER_STATE.get(serverPort);
          if (serverState == null) {
            LOG.warn("No server state found for port {} during stop()", serverPort);
            return;
          }
          
          unregisterMetricExports();
          removeFromExportsTracking(serverState);
          
          int newReferenceCount = decrementReferenceCount(serverState);
          if (newReferenceCount <= 0) {
            cleanupServer(serverPort);
          } else {
            LOG.debug("Prometheus server on port {} still has {} references, keeping server alive", 
                     serverPort, newReferenceCount);
          }
        }
      } catch (Exception e) {
        LOG.error("Error in PrometheusReporter.stop() for port {}", serverPort, e);
      }
    }
  }

  private void unregisterMetricExports() {
    if (!unregistered) {
      try {
        collectorRegistry.unregister(metricExports);
        unregistered = true;
      } catch (Exception e) {
        LOG.debug("Error unregistering metric exports for port {}: {}", serverPort, e.getMessage());
      }
    }
  }

  private void removeFromExportsTracking(PrometheusServerState serverState) {
    serverState.getExports().remove(metricExports);
  }

  private int decrementReferenceCount(PrometheusServerState serverState) {
    int newCount = serverState.getReferenceCount().decrementAndGet();
    LOG.debug("Unregistered PrometheusReporter for port {}, reference count: {}", 
             serverPort, newCount);
    return newCount;
  }

  private static synchronized void cleanupServer(int serverPort) {
    LOG.info("No more references to Prometheus server on port {}, stopping server", serverPort);
    PrometheusServerState serverState = PORT_TO_SERVER_STATE.remove(serverPort);
    if (serverState != null) {
      try {
        serverState.getHttpServer().close();
      } catch (Exception e) {
        LOG.debug("Error closing Prometheus HTTP server on port {}: {}", serverPort, e.getMessage());
      }
    }
  }

  public static boolean isServerRunning(int port) {
    return PORT_TO_SERVER_STATE.containsKey(port);
  }

  public static int getReferenceCount(int port) {
    PrometheusServerState serverState = PORT_TO_SERVER_STATE.get(port);
    return serverState != null ? serverState.getReferenceCount().get() : 0;
  }

  public static int getActiveExportsCount(int port) {
    PrometheusServerState serverState = PORT_TO_SERVER_STATE.get(port);
    return serverState != null ? serverState.getExports().size() : 0;
  }

  private static class LabeledSampleBuilder implements SampleBuilder {
    private final DefaultSampleBuilder defaultMetricSampleBuilder = new DefaultSampleBuilder();
    private final List<String> labelNames;
    private final List<String> labelValues;

    public LabeledSampleBuilder(List<String> labelNames, List<String> labelValues) {
      this.labelNames = labelNames;
      this.labelValues = labelValues;
    }

    @Override
    public Collector.MetricFamilySamples.Sample createSample(String dropwizardName, String nameSuffix, List<String> additionalLabelNames, List<String> additionalLabelValues, double value) {
      return defaultMetricSampleBuilder.createSample(
          dropwizardName,
          nameSuffix,
          labelNames,
          labelValues,
          value);
    }
  }
}
