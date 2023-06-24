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

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metrics.MetricUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class PushGatewayReporter extends ScheduledReporter {

  private static final Logger LOG = LoggerFactory.getLogger(PushGatewayReporter.class);
  // Ensures that we maintain a single PushGw client (single connection pool) per Push Gw Server instance.
  private static final Map<String, PushGateway> PUSH_GATEWAY_PER_HOSTNAME = new ConcurrentHashMap<>();

  private final PushGateway pushGatewayClient;
  private final DropwizardExports metricExports;
  private final CollectorRegistry collectorRegistry;
  private final String jobName;
  private final Map<String, String> labels;
  private final boolean deleteShutdown;
  private final HashMap<String, io.prometheus.client.Gauge> gaugeHashMap;
  private final MetricRegistry registry;

  protected PushGatewayReporter(MetricRegistry registry,
                                MetricFilter filter,
                                TimeUnit rateUnit,
                                TimeUnit durationUnit,
                                String jobName,
                                Map<String, String> labels,
                                String serverHost,
                                int serverPort,
                                boolean deleteShutdown) {
    super(registry, "hudi-push-gateway-reporter", filter, rateUnit, durationUnit);
    this.jobName = jobName;
    this.labels = labels;
    this.deleteShutdown = deleteShutdown;
    this.registry = registry;
    collectorRegistry = new CollectorRegistry();
    metricExports = new DropwizardExports(registry);
    pushGatewayClient = createPushGatewayClient(serverHost, serverPort);
    metricExports.register(collectorRegistry);
    gaugeHashMap = new HashMap<>();

  }

  private synchronized PushGateway createPushGatewayClient(String serverHost, int serverPort) {
    final String serverUrl = String.format("%s:%s", serverHost, serverPort);
    if (PUSH_GATEWAY_PER_HOSTNAME.containsKey(serverUrl)) {
      return PUSH_GATEWAY_PER_HOSTNAME.get(serverUrl);
    }

    PushGateway pushGateway;
    if (serverPort == 443) {
      try {
        pushGateway = new PushGateway(new URL("https://" + serverUrl));
      } catch (MalformedURLException e) {
        e.printStackTrace();
        throw new IllegalArgumentException("Malformed pushgateway host: " + serverHost);
      }
    } else {
      pushGateway = new PushGateway(serverUrl);
    }
    PUSH_GATEWAY_PER_HOSTNAME.put(serverUrl, pushGateway);
    return pushGateway;
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges,
                     SortedMap<String, Counter> counters,
                     SortedMap<String, Histogram> histograms,
                     SortedMap<String, Meter> meters,
                     SortedMap<String, Timer> timers) {
    try {
      handleLabeledMetrics();
      pushGatewayClient.pushAdd(collectorRegistry, jobName, labels);
    } catch (IOException e) {
      LOG.warn("Can't push monitoring information to pushGateway", e);
    }
  }

  @Override
  public void start(long period, TimeUnit unit) {
    super.start(period, unit);
  }

  @Override
  public void stop() {
    super.stop();
    try {
      if (deleteShutdown) {
        collectorRegistry.unregister(metricExports);
        pushGatewayClient.delete(jobName, labels);
        for (String key : gaugeHashMap.keySet()) {
          Pair<String, Map<String, String>> mapPair = MetricUtils.getLabelsAndMetricMap(key);
          pushGatewayClient.delete(mapPair.getKey(), mapPair.getValue());
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to delete metrics from pushGateway with jobName {" + jobName + "}", e);
    }
  }

  private void handleLabeledMetrics() {
    registry.getGauges().entrySet().forEach(gaugeEntry -> {
      String key = gaugeEntry.getKey();
      Pair<String, Map<String,String>> stringMapPair = MetricUtils.getLabelsAndMetricMap(key);
      if (stringMapPair.getValue().size() > 0) {
        List<String> labelNames = new ArrayList<>();
        List<String> labelValues = new ArrayList<>();
        for (Map.Entry et : stringMapPair.getValue().entrySet()) {
          labelNames.add((String) et.getKey());
          labelValues.add((String) et.getValue());
        }
        if (!gaugeHashMap.containsKey(key)) {
          gaugeHashMap.put(key, io.prometheus.client.Gauge.build().help("labeled metricName:" + stringMapPair.getKey())
              .name(stringMapPair.getKey())
              .labelNames(labelNames.toArray(new String[0])).register(collectorRegistry));
        }
        gaugeHashMap.get(key)
            .labels(labelValues.toArray(new String[0]))
            .set((Long) gaugeEntry.getValue().getValue());
      }
    });
  }
}
