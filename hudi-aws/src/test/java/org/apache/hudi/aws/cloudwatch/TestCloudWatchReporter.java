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

import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
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

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.aws.cloudwatch.CloudWatchReporter.DIMENSION_COUNT_TYPE_VALUE;
import static org.apache.hudi.aws.cloudwatch.CloudWatchReporter.DIMENSION_GAUGE_TYPE_VALUE;
import static org.apache.hudi.aws.cloudwatch.CloudWatchReporter.DIMENSION_METRIC_TYPE_KEY;
import static org.apache.hudi.aws.cloudwatch.CloudWatchReporter.DIMENSION_TABLE_NAME_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class TestCloudWatchReporter {

  private static final String NAMESPACE = "Hudi Test";
  private static final String PREFIX = "testPrefix";
  private static final String TABLE_NAME = "testTable";
  private static final int MAX_DATUMS_PER_REQUEST = 2;

  @Mock
  MetricRegistry metricRegistry;

  @Mock
  AmazonCloudWatchAsync cloudWatchAsync;

  @Mock
  CompletableFuture<PutMetricDataResult> cloudWatchFuture;

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

    Mockito.when(cloudWatchAsync.putMetricDataAsync(ArgumentMatchers.any())).thenReturn(cloudWatchFuture);
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
    Mockito.verify(cloudWatchAsync, Mockito.times(3)).putMetricDataAsync(putMetricDataRequestCaptor.capture());
    Assertions.assertEquals(NAMESPACE, putMetricDataRequestCaptor.getValue().getNamespace());

    List<PutMetricDataRequest> putMetricDataRequests = putMetricDataRequestCaptor.getAllValues();
    putMetricDataRequests.forEach(request -> assertEquals(2, request.getMetricData().size()));

    List<MetricDatum> metricDataBatch1 = putMetricDataRequests.get(0).getMetricData();
    assertEquals(PREFIX + ".gauge1", metricDataBatch1.get(0).getMetricName());
    assertEquals(Double.valueOf(gauge1.getValue()), metricDataBatch1.get(0).getValue());
    assertDimensions(metricDataBatch1.get(0).getDimensions(), DIMENSION_GAUGE_TYPE_VALUE);

    assertEquals(PREFIX + ".gauge2", metricDataBatch1.get(1).getMetricName());
    assertEquals(gauge2.getValue(), metricDataBatch1.get(1).getValue());
    assertDimensions(metricDataBatch1.get(1).getDimensions(), DIMENSION_GAUGE_TYPE_VALUE);

    List<MetricDatum> metricDataBatch2 = putMetricDataRequests.get(1).getMetricData();
    assertEquals(PREFIX + ".counter1", metricDataBatch2.get(0).getMetricName());
    assertEquals(counter1.getCount(), metricDataBatch2.get(0).getValue().longValue());
    assertDimensions(metricDataBatch2.get(0).getDimensions(), DIMENSION_COUNT_TYPE_VALUE);

    assertEquals(PREFIX + ".histogram1", metricDataBatch2.get(1).getMetricName());
    assertEquals(histogram1.getCount(), metricDataBatch2.get(1).getValue().longValue());
    assertDimensions(metricDataBatch2.get(1).getDimensions(), DIMENSION_COUNT_TYPE_VALUE);

    List<MetricDatum> metricDataBatch3 = putMetricDataRequests.get(2).getMetricData();
    assertEquals(PREFIX + ".meter1", metricDataBatch3.get(0).getMetricName());
    assertEquals(meter1.getCount(), metricDataBatch3.get(0).getValue().longValue());
    assertDimensions(metricDataBatch3.get(0).getDimensions(), DIMENSION_COUNT_TYPE_VALUE);

    assertEquals(PREFIX + ".timer1", metricDataBatch3.get(1).getMetricName());
    assertEquals(timer1.getCount(), metricDataBatch3.get(1).getValue().longValue());
    assertDimensions(metricDataBatch3.get(1).getDimensions(), DIMENSION_COUNT_TYPE_VALUE);

    reporter.stop();
    Mockito.verify(cloudWatchAsync).shutdown();
  }

  private void assertDimensions(List<Dimension> actualDimensions, String metricTypeDimensionVal) {
    assertEquals(2, actualDimensions.size());

    Dimension expectedTableNameDimension = new Dimension()
        .withName(DIMENSION_TABLE_NAME_KEY)
        .withValue(TABLE_NAME);
    Dimension expectedMetricTypeDimension = new Dimension()
        .withName(DIMENSION_METRIC_TYPE_KEY)
        .withValue(metricTypeDimensionVal);

    assertEquals(expectedTableNameDimension, actualDimensions.get(0));
    assertEquals(expectedMetricTypeDimension, actualDimensions.get(1));
  }
}