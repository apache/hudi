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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Implementation of Prometheus reporter, which connects to the Http server, and get metrics
 * from that server.
 */
public class PrometheusReporter extends MetricsReporter {
  private static final Pattern LABEL_PATTERN = Pattern.compile("\\s*,\\s*");

  private static final Logger LOG = LoggerFactory.getLogger(PrometheusReporter.class);
  private static final Map<Integer, CollectorRegistry> PORT_TO_COLLECTOR_REGISTRY = new HashMap<>();
  private static final Map<Integer, HTTPServer> PORT_TO_SERVER = new HashMap<>();
  
  private static final Map<Integer, AtomicInteger> PORT_TO_REFERENCE_COUNT = new HashMap<>();
  private static final Map<Integer, Set<DropwizardExports>> PORT_TO_EXPORTS = new HashMap<>();

  private final DropwizardExports metricExports;
  private final CollectorRegistry collectorRegistry;
  private final int serverPort;
  private volatile boolean stopped = false;
  private volatile boolean unregistered = false;

  public PrometheusReporter(HoodieMetricsConfig metricsConfig, MetricRegistry registry) {
    this.serverPort = metricsConfig.getPrometheusPort();
    if (!PORT_TO_SERVER.containsKey(serverPort) || !PORT_TO_COLLECTOR_REGISTRY.containsKey(serverPort)) {
      startHttpServer(serverPort);
    }
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
    this.collectorRegistry = PORT_TO_COLLECTOR_REGISTRY.get(serverPort);
    
    synchronized (PrometheusReporter.class) {
      metricExports.register(collectorRegistry);
      PORT_TO_EXPORTS.computeIfAbsent(serverPort, k -> ConcurrentHashMap.newKeySet()).add(metricExports);
      PORT_TO_REFERENCE_COUNT.computeIfAbsent(serverPort, k -> new AtomicInteger(0)).incrementAndGet();
      
      LOG.debug("Registered PrometheusReporter for port {}, reference count: {}", 
               serverPort, PORT_TO_REFERENCE_COUNT.get(serverPort).get());
    }
  }

  private static void startHttpServer(int serverPort) {
    synchronized (PrometheusReporter.class) {
      if (!PORT_TO_COLLECTOR_REGISTRY.containsKey(serverPort)) {
        PORT_TO_COLLECTOR_REGISTRY.put(serverPort, new CollectorRegistry());
      }
      if (!PORT_TO_SERVER.containsKey(serverPort)) {
        try {
          HTTPServer server = new HTTPServer(new InetSocketAddress(serverPort), PORT_TO_COLLECTOR_REGISTRY.get(serverPort), true);
          PORT_TO_SERVER.put(serverPort, server);
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
    }
  }

  @Override
  public void start() {
  }

  @Override
  public void report() {
  }

  @Override
  public void stop() {
    try {
      synchronized (PrometheusReporter.class) {
        if (isAlreadyStopped()) {
          return;
        }
        
        markAsStopped();
        unregisterMetricExports();
        removeFromExportsTracking();
        
        int newReferenceCount = decrementReferenceCount();
        if (newReferenceCount <= 0) {
          cleanupServerResources();
        } else {
          logServerKeptAlive(newReferenceCount);
        }
      }
    } catch (Exception e) {
      LOG.error("Error in PrometheusReporter.stop() for port {}", serverPort, e);
    }
  }

  private boolean isAlreadyStopped() {
    if (stopped) {
      LOG.debug("PrometheusReporter.stop() called for port {} but already stopped", serverPort);
      return true;
    }
    return false;
  }

  private void markAsStopped() {
    stopped = true;
    LOG.debug("PrometheusReporter.stop() called for port {}", serverPort);
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

  private void removeFromExportsTracking() {
    Set<DropwizardExports> exports = PORT_TO_EXPORTS.get(serverPort);
    if (exports != null) {
      exports.remove(metricExports);
    }
  }

  private int decrementReferenceCount() {
    AtomicInteger refCount = PORT_TO_REFERENCE_COUNT.get(serverPort);
    if (refCount != null) {
      int newCount = refCount.decrementAndGet();
      LOG.debug("Unregistered PrometheusReporter for port {}, reference count: {}", 
               serverPort, newCount);
      return newCount;
    } else {
      LOG.warn("No reference count found for port {} during stop()", serverPort);
      return 0;
    }
  }

  private void cleanupServerResources() {
    LOG.info("No more references to Prometheus server on port {}, stopping server", serverPort);
    closeHttpServer();
    removeAllPortResources();
  }

  private void closeHttpServer() {
    HTTPServer httpServer = PORT_TO_SERVER.remove(serverPort);
    if (httpServer != null) {
      try {
        httpServer.close();
      } catch (Exception e) {
        LOG.debug("Error closing Prometheus HTTP server on port {}: {}", serverPort, e.getMessage());
      }
    }
  }

  private void removeAllPortResources() {
    PORT_TO_COLLECTOR_REGISTRY.remove(serverPort);
    PORT_TO_REFERENCE_COUNT.remove(serverPort);
    PORT_TO_EXPORTS.remove(serverPort);
  }

  private void logServerKeptAlive(int referenceCount) {
    LOG.debug("Prometheus server on port {} still has {} references, keeping server alive", 
             serverPort, referenceCount);
  }

  public static boolean isServerRunning(int port) {
    synchronized (PrometheusReporter.class) {
      return PORT_TO_SERVER.containsKey(port);
    }
  }

  public static int getReferenceCount(int port) {
    synchronized (PrometheusReporter.class) {
      AtomicInteger refCount = PORT_TO_REFERENCE_COUNT.get(port);
      return refCount != null ? refCount.get() : 0;
    }
  }

  public static int getActiveExportsCount(int port) {
    synchronized (PrometheusReporter.class) {
      Set<DropwizardExports> exports = PORT_TO_EXPORTS.get(port);
      return exports != null ? exports.size() : 0;
    }
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
