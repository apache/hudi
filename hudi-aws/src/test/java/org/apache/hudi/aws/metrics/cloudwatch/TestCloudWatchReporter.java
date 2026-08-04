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

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.aws.metrics.cloudwatch.CloudWatchReporter.DIMENSION_COUNT_TYPE_VALUE;
import static org.apache.hudi.aws.metrics.cloudwatch.CloudWatchReporter.DIMENSION_GAUGE_TYPE_VALUE;
import static org.apache.hudi.aws.metrics.cloudwatch.CloudWatchReporter.DIMENSION_METRIC_TYPE_KEY;
import static org.apache.hudi.aws.metrics.cloudwatch.CloudWatchReporter.DIMENSION_TABLE_NAME_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class TestCloudWatchReporter {

  private static final String NAMESPACE = "Hudi Test";
  private static final String PREFIX = "testPrefix";
  private static final String TABLE_NAME = "testTable";
  private static final int MAX_DATUMS_PER_REQUEST = 2;

  @Mock
  MetricRegistry metricRegistry;

  @Mock(lenient = true)
  CloudWatchAsyncClient cloudWatchAsync;

  @Mock
  CompletableFuture<PutMetricDataResponse> cloudWatchFuture;

  @Captor
  ArgumentCaptor<PutMetricDataRequest> putMetricDataRequestCaptor;

  CloudWatchReporter reporter;

  @BeforeEach
  public void setup() {
    reporter = CloudWatchReporter.forRegistry(metricRegistry)
        .namespace(NAMESPACE)
        .prefixedWith(PREFIX)
        .maxDatumsPerRequest(MAX_DATUMS_PER_REQUEST)
        .withClock(Clock.defaultClock())
        .filter(MetricFilter.ALL)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build(cloudWatchAsync);

    Mockito.when(cloudWatchAsync.putMetricData((PutMetricDataRequest) ArgumentMatchers.any())).thenReturn(cloudWatchFuture);
  }

  @Test
  public void testReporter() {
    SortedMap<String, Gauge> gauges = new TreeMap<>();
    Gauge<Long> gauge1 = () -> 100L;
    Gauge<Double> gauge2 = () -> 100.1;
    gauges.put(TABLE_NAME + ".gauge1", gauge1);
    gauges.put(TABLE_NAME + ".gauge2", gauge2);

    SortedMap<String, Counter> counters = new TreeMap<>();
    Counter counter1 = new Counter();
    counter1.inc(200);
    counters.put(TABLE_NAME + ".counter1", counter1);

    SortedMap<String, Histogram> histograms = new TreeMap<>();
    Histogram histogram1 = new Histogram(new ExponentiallyDecayingReservoir());
    histogram1.update(300);
    histograms.put(TABLE_NAME + ".histogram1", histogram1);

    SortedMap<String, Meter> meters = new TreeMap<>();
    Meter meter1 = new Meter();
    meter1.mark(400);
    meters.put(TABLE_NAME + ".meter1", meter1);

    SortedMap<String, Timer> timers = new TreeMap<>();
    Timer timer1 = new Timer();
    timer1.update(100, TimeUnit.SECONDS);
    timers.put(TABLE_NAME + ".timer1", timer1);

    Mockito.when(metricRegistry.getGauges(MetricFilter.ALL)).thenReturn(gauges);
    Mockito.when(metricRegistry.getCounters(MetricFilter.ALL)).thenReturn(counters);
    Mockito.when(metricRegistry.getHistograms(MetricFilter.ALL)).thenReturn(histograms);
    Mockito.when(metricRegistry.getMeters(MetricFilter.ALL)).thenReturn(meters);
    Mockito.when(metricRegistry.getTimers(MetricFilter.ALL)).thenReturn(timers);

    reporter.report();

    // Since there are 6 metrics in total, and max datums per request is 2 we would expect 3 calls to CloudWatch
    // with 2 datums in each
    Mockito.verify(cloudWatchAsync, Mockito.times(3)).putMetricData(putMetricDataRequestCaptor.capture());
    Assertions.assertEquals(NAMESPACE, putMetricDataRequestCaptor.getValue().namespace());

    List<PutMetricDataRequest> putMetricDataRequests = putMetricDataRequestCaptor.getAllValues();
    putMetricDataRequests.forEach(request -> assertEquals(2, request.metricData().size()));

    List<MetricDatum> metricDataBatch1 = putMetricDataRequests.get(0).metricData();
    assertEquals(PREFIX + ".gauge1", metricDataBatch1.get(0).metricName());
    assertEquals(Double.valueOf(gauge1.getValue()), metricDataBatch1.get(0).value());
    assertDimensions(metricDataBatch1.get(0).dimensions(), DIMENSION_GAUGE_TYPE_VALUE);

    assertEquals(PREFIX + ".gauge2", metricDataBatch1.get(1).metricName());
    assertEquals(gauge2.getValue(), metricDataBatch1.get(1).value());
    assertDimensions(metricDataBatch1.get(1).dimensions(), DIMENSION_GAUGE_TYPE_VALUE);

    List<MetricDatum> metricDataBatch2 = putMetricDataRequests.get(1).metricData();
    assertEquals(PREFIX + ".counter1", metricDataBatch2.get(0).metricName());
    assertEquals(counter1.getCount(), metricDataBatch2.get(0).value().longValue());
    assertDimensions(metricDataBatch2.get(0).dimensions(), DIMENSION_COUNT_TYPE_VALUE);

    assertEquals(PREFIX + ".histogram1", metricDataBatch2.get(1).metricName());
    assertEquals(histogram1.getCount(), metricDataBatch2.get(1).value().longValue());
    assertDimensions(metricDataBatch2.get(1).dimensions(), DIMENSION_COUNT_TYPE_VALUE);

    List<MetricDatum> metricDataBatch3 = putMetricDataRequests.get(2).metricData();
    assertEquals(PREFIX + ".meter1", metricDataBatch3.get(0).metricName());
    assertEquals(meter1.getCount(), metricDataBatch3.get(0).value().longValue());
    assertDimensions(metricDataBatch3.get(0).dimensions(), DIMENSION_COUNT_TYPE_VALUE);

    assertEquals(PREFIX + ".timer1", metricDataBatch3.get(1).metricName());
    assertEquals(timer1.getCount(), metricDataBatch3.get(1).value().longValue());
    assertDimensions(metricDataBatch3.get(1).dimensions(), DIMENSION_COUNT_TYPE_VALUE);

    reporter.stop();
    Mockito.verify(cloudWatchAsync).close();
  }

  /**
   * A metric name with no dot has no table name to report under, and such names do reach the reporter:
   * {@code HoodieMetadataMetrics#setMetric} registers gauges without the metrics-name prefix, so
   * {@code BaseTableMetadata#getBloomFilters} contributes a bare
   * {@code lookup_meta_index_bloom_filters_file_count} on the normal bloom-index read path. This used to
   * throw, and {@link com.codahale.metrics.ScheduledReporter} suppresses whatever {@code report()} throws,
   * so no metrics reached CloudWatch at all - which is what #12182 and #13051 report. The unmappable metric
   * is now skipped and the rest of the batch is still published. See HUDI issue #19507 for the producer side.
   */
  @Test
  public void testReportSkipsMetricsWithoutTableNameAndPublishesTheRest() {
    SortedMap<String, Gauge> gauges = new TreeMap<>();
    Gauge<Long> unmappable = () -> 7L;
    Gauge<Double> wellFormed = () -> 100.1;
    gauges.put("lookup_meta_index_bloom_filters_file_count", unmappable);
    gauges.put(TABLE_NAME + ".gauge2", wellFormed);

    Mockito.when(metricRegistry.getGauges(MetricFilter.ALL)).thenReturn(gauges);

    reporter.report();

    Mockito.verify(cloudWatchAsync, Mockito.times(1)).putMetricData(putMetricDataRequestCaptor.capture());
    List<MetricDatum> metricData = putMetricDataRequestCaptor.getValue().metricData();
    assertEquals(1, metricData.size(),
        "The unmappable metric should be skipped and the well-formed one still published");
    assertEquals(PREFIX + ".gauge2", metricData.get(0).metricName());
    assertEquals(wellFormed.getValue(), metricData.get(0).value());
    assertDimensions(metricData.get(0).dimensions(), DIMENSION_GAUGE_TYPE_VALUE);
  }

  /**
   * An empty first segment is reachable: {@code hoodie.metrics.reporter.metricsname.prefix} defaults to
   * {@code ""} and {@code Metrics#registerGauges} still joins it with a dot, giving {@code ".foo"}. That
   * splits into two parts and so passed the length check, then asked CloudWatch for an empty {@code Table}
   * dimension value, which it rejects for the whole PutMetricData request - losing the batch again.
   */
  @Test
  public void testReportSkipsMetricsWithAnEmptyTableName() {
    SortedMap<String, Gauge> gauges = new TreeMap<>();
    gauges.put(".gauge1", (Gauge<Long>) () -> 7L);
    gauges.put(TABLE_NAME + ".gauge2", (Gauge<Long>) () -> 100L);

    Mockito.when(metricRegistry.getGauges(MetricFilter.ALL)).thenReturn(gauges);

    reporter.report();

    Mockito.verify(cloudWatchAsync, Mockito.times(1)).putMetricData(putMetricDataRequestCaptor.capture());
    List<MetricDatum> metricData = putMetricDataRequestCaptor.getValue().metricData();
    assertEquals(1, metricData.size(), "a metric whose table name is empty should be skipped");
    assertEquals(PREFIX + ".gauge2", metricData.get(0).metricName());
  }

  /**
   * An interval in which every metric is unmappable leaves nothing staged. CloudWatch rejects an empty
   * PutMetricData request, so the reporter must not send one.
   */
  @Test
  public void testReportSendsNothingWhenEveryMetricIsUnmappable() {
    SortedMap<String, Gauge> gauges = new TreeMap<>();
    gauges.put("lookup_meta_index_bloom_filters_file_count", (Gauge<Long>) () -> 7L);
    gauges.put("bootstrap_error", (Gauge<Long>) () -> 1L);

    Mockito.when(metricRegistry.getGauges(MetricFilter.ALL)).thenReturn(gauges);

    reporter.report();

    Mockito.verify(cloudWatchAsync, Mockito.never()).putMetricData(ArgumentMatchers.any(PutMetricDataRequest.class));
  }

  /**
   * The unmappable-name set exists so a persistent offender is logged once rather than every reporting
   * interval. Without this, deleting the set and logging unconditionally would pass the suite.
   */
  @Test
  public void testUnmappableMetricIsLoggedOncePerName() {
    SortedMap<String, Gauge> gauges = new TreeMap<>();
    gauges.put("lookup_meta_index_bloom_filters_file_count", (Gauge<Long>) () -> 7L);
    Mockito.when(metricRegistry.getGauges(MetricFilter.ALL)).thenReturn(gauges);

    CapturingAppender appender = CapturingAppender.attachTo(CloudWatchReporter.class);
    try {
      reporter.report();
      reporter.report();
    } finally {
      appender.detach();
    }

    assertEquals(1, appender.warningsContaining("lookup_meta_index_bloom_filters_file_count"),
        "a persistent unmappable name should be warned about once, not once per interval");
  }

  /** Captures WARN events from a single logger, so "logged once" can be asserted. */
  private static final class CapturingAppender extends AbstractAppender {
    private final List<String> warnings = Collections.synchronizedList(new ArrayList<>());
    private final LoggerConfig loggerConfig;
    private final Level previousLevel;

    private CapturingAppender(LoggerConfig loggerConfig) {
      super("CapturingAppender", null, null, true, null);
      this.loggerConfig = loggerConfig;
      this.previousLevel = loggerConfig.getLevel();
    }

    static CapturingAppender attachTo(Class<?> loggerFor) {
      LoggerContext context = (LoggerContext) LogManager.getContext(false);
      LoggerConfig loggerConfig = context.getConfiguration().getLoggerConfig(loggerFor.getName());
      CapturingAppender appender = new CapturingAppender(loggerConfig);
      appender.start();
      loggerConfig.addAppender(appender, Level.WARN, null);
      loggerConfig.setLevel(Level.WARN);
      context.updateLoggers();
      return appender;
    }

    void detach() {
      loggerConfig.removeAppender(getName());
      loggerConfig.setLevel(previousLevel);
      ((LoggerContext) LogManager.getContext(false)).updateLoggers();
      stop();
    }

    long warningsContaining(String needle) {
      synchronized (warnings) {
        return warnings.stream().filter(m -> m.contains(needle)).count();
      }
    }

    @Override
    public void append(LogEvent event) {
      if (event.getLevel().isMoreSpecificThan(Level.WARN)) {
        warnings.add(event.getMessage().getFormattedMessage());
      }
    }
  }

  private void assertDimensions(List<Dimension> actualDimensions, String metricTypeDimensionVal) {
    assertEquals(2, actualDimensions.size());

    Dimension expectedTableNameDimension = Dimension.builder()
        .name(DIMENSION_TABLE_NAME_KEY)
        .value(TABLE_NAME)
        .build();
    Dimension expectedMetricTypeDimension = Dimension.builder()
        .name(DIMENSION_METRIC_TYPE_KEY)
        .value(metricTypeDimensionVal)
        .build();

    assertEquals(expectedTableNameDimension, actualDimensions.get(0));
    assertEquals(expectedMetricTypeDimension, actualDimensions.get(1));
  }
}