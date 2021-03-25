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
import org.apache.hudi.common.util.ValidationUtils;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A reporter which publishes metric values to Datadog API.
 * <p>
 * Responsible for collecting and composing metrics payload.
 * <p>
 * Internally use {@link DatadogHttpClient} to interact with Datadog APIs.
 */
public class DatadogReporter extends ScheduledReporter {

  private static final Logger LOG = LogManager.getLogger(DatadogReporter.class);

  private final DatadogHttpClient client;
  private final String prefix;
  private final Option<String> host;
  private final Option<List<String>> tags;
  private final Clock clock;

  protected DatadogReporter(
      MetricRegistry registry,
      DatadogHttpClient client,
      String prefix,
      Option<String> host,
      Option<List<String>> tags,
      MetricFilter filter,
      TimeUnit rateUnit,
      TimeUnit durationUnit) {
    super(registry, "hudi-datadog-reporter", filter, rateUnit, durationUnit);
    this.client = client;
    this.prefix = prefix;
    this.host = host;
    this.tags = tags;
    this.clock = Clock.defaultClock();
  }

  @Override
  public void report(
      SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
    final long now = clock.getTime() / 1000;
    final PayloadBuilder builder = new PayloadBuilder();

    builder.withMetricType(MetricType.gauge);
    gauges.forEach((metricName, metric) -> {
      builder.addGauge(prefix(metricName), now, (long) metric.getValue());
    });

    host.ifPresent(builder::withHost);
    tags.ifPresent(builder::withTags);

    client.send(builder.build());
  }

  protected String prefix(String... components) {
    return MetricRegistry.name(prefix, components);
  }

  @Override
  public void stop() {
    try {
      super.stop();
    } finally {
      try {
        client.close();
      } catch (IOException e) {
        LOG.warn("Error disconnecting from Datadog.", e);
      }
    }
  }

  /**
   * Build payload that contains metrics data.
   * <p>
   * Refer to Datadog API reference https://docs.datadoghq.com/api/?lang=bash#post-timeseries-points
   */
  static class PayloadBuilder {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ObjectNode payload;
    private final ArrayNode series;
    private MetricType type;

    PayloadBuilder() {
      payload = MAPPER.createObjectNode();
      series = payload.putArray("series");
    }

    PayloadBuilder withMetricType(MetricType type) {
      this.type = type;
      return this;
    }

    PayloadBuilder addGauge(String metric, long timestamp, long gaugeValue) {
      ValidationUtils.checkState(type == MetricType.gauge);
      ObjectNode seriesItem = MAPPER.createObjectNode().put("metric", metric);
      seriesItem.putArray("points").addArray().add(timestamp).add(gaugeValue);
      series.add(seriesItem);
      return this;
    }

    PayloadBuilder withHost(String host) {
      series.forEach(seriesItem -> ((ObjectNode) seriesItem).put("host", host));
      return this;
    }

    PayloadBuilder withTags(List<String> tags) {
      series.forEach(seriesItem -> {
        ((ObjectNode) seriesItem)
            .putArray("tags")
            .addAll(tags.stream().map(TextNode::new).collect(Collectors.toList()));
      });
      return this;
    }

    String build() {
      return payload.toString();
    }
  }

  enum MetricType {
    gauge;
  }
}
