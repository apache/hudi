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

package org.apache.hudi.aws.cloudwatch;

import org.apache.hudi.aws.credentials.HoodieAWSCredentialsProviderFactory;
import org.apache.hudi.common.util.Option;

import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
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

  private static final Logger LOG = LogManager.getLogger(CloudWatchReporter.class);

  private final AmazonCloudWatchAsync cloudWatchClientAsync;
  private final Clock clock;
  private final String prefix;
  private final String namespace;
  private final int maxDatumsPerRequest;

  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  public static class Builder {
    private MetricRegistry registry;
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

    CloudWatchReporter build(AmazonCloudWatchAsync amazonCloudWatchAsync) {
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
                               AmazonCloudWatchAsync cloudWatchClientAsync,
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

  private static AmazonCloudWatchAsync getAmazonCloudWatchClient(Properties props) {
    return AmazonCloudWatchAsyncClientBuilder.standard()
        .withCredentials(HoodieAWSCredentialsProviderFactory.getAwsCredentialsProvider(props))
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
    List<Future<PutMetricDataResult>> cloudWatchFutures = new ArrayList<>(metricsData.size());
    List<List<MetricDatum>> partitions = new ArrayList<>();

    for (int i = 0; i < metricsData.size(); i += maxDatumsPerRequest) {
      int end = Math.min(metricsData.size(), i + maxDatumsPerRequest);
      partitions.add(metricsData.subList(i, end));
    }

    for (List<MetricDatum> partition : partitions) {
      PutMetricDataRequest request = new PutMetricDataRequest()
          .withNamespace(namespace)
          .withMetricData(partition);

      cloudWatchFutures.add(cloudWatchClientAsync.putMetricDataAsync(request));
    }

    for (final Future<PutMetricDataResult> cloudWatchFuture : cloudWatchFutures) {
      try {
        cloudWatchFuture.get(30, TimeUnit.SECONDS);
      } catch (final Exception ex) {
        LOG.error("Error reporting metrics to CloudWatch. The data in this CloudWatch request "
            + "may have been discarded, and not made it to CloudWatch.", ex);
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
            StandardUnit.None,
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
        StandardUnit.Count,
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


    metricData.add(new MetricDatum()
        .withTimestamp(new Date(timestampMilliSec))
        .withMetricName(prefix(metricNameParts[1]))
        .withValue(metricValue)
        .withDimensions(getDimensions(tableName, metricType))
        .withUnit(standardUnit));
  }

  private List<Dimension> getDimensions(String tableName, String metricType) {
    List<Dimension> dimensions = new ArrayList<>();
    dimensions.add(new Dimension()
        .withName(DIMENSION_TABLE_NAME_KEY)
        .withValue(tableName));
    dimensions.add(new Dimension()
        .withName(DIMENSION_METRIC_TYPE_KEY)
        .withValue(metricType));
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
        cloudWatchClientAsync.shutdown();
      } catch (Exception ex) {
        LOG.warn("Exception while shutting down CloudWatch client.", ex);
      }
    }
  }
}
