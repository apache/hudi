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

package org.apache.hudi.aws.metrics.cloudwatch;

import org.apache.hudi.aws.credentials.HoodieAWSCredentialsProviderFactory;
import org.apache.hudi.common.util.Option;

import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A reporter for publishing metrics to Amazon CloudWatch. It is responsible for collecting, converting DropWizard
 * metrics to CloudWatch metrics and composing metrics payload.
 */
public class CloudWatchReporter extends ScheduledReporter {

  static final String DIMENSION_TABLE_NAME_KEY = "Table";
  static final String DIMENSION_METRIC_TYPE_KEY = "Metric Type";
  static final String DIMENSION_GAUGE_TYPE_VALUE = "gauge";
  static final String DIMENSION_COUNT_TYPE_VALUE = "count";

  private static final Logger LOG = LoggerFactory.getLogger(CloudWatchReporter.class);

  private final CloudWatchAsyncClient cloudWatchClientAsync;
  private final Clock clock;
  private final String prefix;
  private final String namespace;
  private final int maxDatumsPerRequest;

  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  public static class Builder {
    private final MetricRegistry registry;
    private Clock clock;
    private String prefix;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;
    private String namespace;
    private int maxDatumsPerRequest;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.clock = Clock.defaultClock();
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
      this.maxDatumsPerRequest = 20;
    }

    public Builder withClock(Clock clock) {
      this.clock = clock;
      return this;
    }

    public Builder prefixedWith(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    public Builder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder maxDatumsPerRequest(int maxDatumsPerRequest) {
      this.maxDatumsPerRequest = maxDatumsPerRequest;
      return this;
    }

    public CloudWatchReporter build(Properties props) {
      return new CloudWatchReporter(registry,
          getAmazonCloudWatchClient(props),
          clock,
          prefix,
          namespace,
          maxDatumsPerRequest,
          filter,
          rateUnit,
          durationUnit);
    }

    CloudWatchReporter build(CloudWatchAsyncClient amazonCloudWatchAsync) {
      return new CloudWatchReporter(registry,
          amazonCloudWatchAsync,
          clock,
          prefix,
          namespace,
          maxDatumsPerRequest,
          filter,
          rateUnit,
          durationUnit);
    }
  }

  protected CloudWatchReporter(MetricRegistry registry,
                               CloudWatchAsyncClient cloudWatchClientAsync,
                               Clock clock,
                               String prefix,
                               String namespace,
                               int maxDatumsPerRequest,
                               MetricFilter filter,
                               TimeUnit rateUnit,
                               TimeUnit durationUnit) {
    super(registry, "hudi-cloudWatch-reporter", filter, rateUnit, durationUnit);
    this.cloudWatchClientAsync = cloudWatchClientAsync;
    this.clock = clock;
    this.prefix = prefix;
    this.namespace = namespace;
    this.maxDatumsPerRequest = maxDatumsPerRequest;
  }

  private static CloudWatchAsyncClient getAmazonCloudWatchClient(Properties props) {
    return CloudWatchAsyncClient.builder()
        .credentialsProvider(HoodieAWSCredentialsProviderFactory.getAwsCredentialsProvider(props))
        .build();
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges,
                     SortedMap<String, Counter> counters,
                     SortedMap<String, Histogram> histograms,
                     SortedMap<String, Meter> meters,
                     SortedMap<String, Timer> timers) {
    LOG.info("Reporting Metrics to CloudWatch.");

    final long timestampMilliSec = clock.getTime();
    List<MetricDatum> metricsData = new ArrayList<>();

    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
      processGauge(entry.getKey(), entry.getValue(), timestampMilliSec, metricsData);
    }

    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      processCounter(entry.getKey(), entry.getValue(), timestampMilliSec, metricsData);
    }

    for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
      processCounter(entry.getKey(), entry.getValue(), timestampMilliSec, metricsData);
      //TODO: Publish other Histogram metrics to cloud watch
    }

    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      processCounter(entry.getKey(), entry.getValue(), timestampMilliSec, metricsData);
      //TODO: Publish other Meter metrics to cloud watch
    }

    for (Map.Entry<String, Timer> entry : timers.entrySet()) {
      processCounter(entry.getKey(), entry.getValue(), timestampMilliSec, metricsData);
      //TODO: Publish other Timer metrics to cloud watch
    }

    report(metricsData);
  }

  private void report(List<MetricDatum> metricsData) {
    List<Future<PutMetricDataResponse>> cloudWatchFutures = new ArrayList<>(metricsData.size());
    List<List<MetricDatum>> partitions = new ArrayList<>();

    for (int i = 0; i < metricsData.size(); i += maxDatumsPerRequest) {
      int end = Math.min(metricsData.size(), i + maxDatumsPerRequest);
      partitions.add(metricsData.subList(i, end));
    }

    for (List<MetricDatum> partition : partitions) {
      PutMetricDataRequest request = PutMetricDataRequest.builder()
          .namespace(namespace)
          .metricData(partition)
          .build();

      cloudWatchFutures.add(cloudWatchClientAsync.putMetricData(request));
    }

    for (final Future<PutMetricDataResponse> cloudWatchFuture : cloudWatchFutures) {
      try {
        cloudWatchFuture.get(30, TimeUnit.SECONDS);
      } catch (final Exception ex) {
        LOG.error("Error reporting metrics to CloudWatch. The data in this CloudWatch request "
            + "may have been discarded, and not made it to CloudWatch.", ex);
        if (ex instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private void processGauge(final String metricName,
                            final Gauge<?> gauge,
                            final long timestampMilliSec,
                            final List<MetricDatum> metricData) {
    Option.ofNullable(gauge.getValue())
        .toJavaOptional()
        .filter(value -> value instanceof Number)
        .map(value -> (Number) value)
        .ifPresent(value -> stageMetricDatum(metricName,
            value.doubleValue(),
            DIMENSION_GAUGE_TYPE_VALUE,
            StandardUnit.NONE,
            timestampMilliSec,
            metricData));
  }

  private void processCounter(final String metricName,
                              final Counting counter,
                              final long timestampMilliSec,
                              final List<MetricDatum> metricData) {
    stageMetricDatum(metricName,
        counter.getCount(),
        DIMENSION_COUNT_TYPE_VALUE,
        StandardUnit.COUNT,
        timestampMilliSec,
        metricData);
  }

  private void stageMetricDatum(String metricName,
                                double metricValue,
                                String metricType,
                                StandardUnit standardUnit,
                                long timestampMilliSec,
                                List<MetricDatum> metricData) {
    String[] metricNameParts = metricName.split("\\.", 2);
    String tableName = metricNameParts[0];


    metricData.add(MetricDatum.builder()
        .timestamp(Instant.ofEpochMilli(timestampMilliSec))
        .metricName(prefix(metricNameParts[1]))
        .value(metricValue)
        .dimensions(getDimensions(tableName, metricType))
        .unit(standardUnit)
        .build());
  }

  private List<Dimension> getDimensions(String tableName, String metricType) {
    List<Dimension> dimensions = new ArrayList<>();
    dimensions.add(Dimension.builder()
        .name(DIMENSION_TABLE_NAME_KEY)
        .value(tableName)
        .build());
    dimensions.add(Dimension.builder()
        .name(DIMENSION_METRIC_TYPE_KEY)
        .value(metricType)
        .build());
    return dimensions;
  }

  private String prefix(String... components) {
    return MetricRegistry.name(prefix, components);
  }

  @Override
  public void stop() {
    try {
      super.stop();
    } finally {
      try {
        cloudWatchClientAsync.close();
      } catch (Exception ex) {
        LOG.warn("Exception while shutting down CloudWatch client.", ex);
      }
    }
  }
}
