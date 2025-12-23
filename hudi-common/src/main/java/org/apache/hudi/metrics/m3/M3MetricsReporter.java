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

package org.apache.hudi.metrics.m3;

import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.metrics.MetricsReporter;

import com.codahale.metrics.MetricRegistry;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.m3.M3Reporter;
import com.uber.m3.util.Duration;
import com.uber.m3.util.ImmutableMap;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of M3 Metrics reporter, which can report metrics to a https://m3db.io/ service
 */
@Slf4j
public class M3MetricsReporter extends MetricsReporter {

  private final HoodieMetricsConfig metricsConfig;
  private final MetricRegistry registry;
  private final ImmutableMap<String, String> tags;

  public M3MetricsReporter(HoodieMetricsConfig metricsConfig, MetricRegistry registry) {
    this.metricsConfig = metricsConfig;
    this.registry = registry;

    ImmutableMap.Builder tagBuilder = new ImmutableMap.Builder<>();
    tagBuilder.putAll(parseOptionalTags(metricsConfig.getM3Tags()));
    tagBuilder.put("service", metricsConfig.getM3Service());
    tagBuilder.put("env", metricsConfig.getM3Env());
    this.tags = tagBuilder.build();
    log.info("Building M3 Reporter with M3 tags mapping: {}", tags);
  }

  private static Map parseOptionalTags(String tagValueString) {
    Map parsedTags = new HashMap();
    if (!tagValueString.isEmpty()) {
      Arrays.stream(tagValueString.split(",")).forEach((tagValuePair) -> {
        String[] parsedTagValuePair = Arrays.stream(tagValuePair.split("="))
            .map((tagOrValue) -> tagOrValue.trim()).filter((tagOrValue) -> !tagOrValue.isEmpty())
            .toArray(String[]::new);
        if (parsedTagValuePair.length != 2) {
          throw new RuntimeException(String.format(
              "M3 Reporter tags cannot be initialized with tags [%s] due to not being in format `tag=value, . . .`.",
              tagValuePair));
        }
        parsedTags.put(parsedTagValuePair[0], parsedTagValuePair[1]);
      });
    }
    return parsedTags;
  }

  @Override
  public void start() {
  }

  @Override
  public void report() {
    /*
      Although com.uber.m3.tally.Scope supports automatically submitting metrics in an interval
      via a background task, it does not seem to support
      - an API for explicitly flushing/emitting all metrics
      - Taking in an external com.codahale.metrics.MetricRegistry metrics registry and automatically
      adding any new counters/gauges whenever they are added to the registry
      Due to this, this implementation emits metrics by creating a Scope, adding all metrics from
      the HUDI metircs registry as counters/gauges to the scope, and then closing the Scope. Since
      closing this Scope will implicitly flush all M3 metrics, the reporting intervals
      are configured to be Integer.MAX_VALUE.
     */
    synchronized (this) {
      try (Scope scope = new RootScopeBuilder()
          .reporter(new M3Reporter.Builder(
              new InetSocketAddress(metricsConfig.getM3ServerHost(), metricsConfig.getM3ServerPort()))
              .includeHost(true).commonTags(tags)
              .build())
          .reportEvery(Duration.ofSeconds(Integer.MAX_VALUE))
          .tagged(tags)) {

        M3ScopeReporterAdaptor scopeReporter = new M3ScopeReporterAdaptor(registry, scope);
        scopeReporter.start(Integer.MAX_VALUE, TimeUnit.SECONDS);
        scopeReporter.report();
        scopeReporter.stop();
      } catch (Exception e) {
        log.error("Error reporting metrics to M3:", e);
      }
    }
  }

  @Override
  public void stop() {
  }
}






