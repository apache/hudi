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

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.metrics.MetricsReporterFactory;
import org.apache.hudi.metrics.MetricsReporterType;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestCloudWatchMetricsReporter {

  @Mock
  private HoodieWriteConfig writeConfig;

  @Mock
  private HoodieMetricsConfig metricsConfig;

  @Mock
  private MetricRegistry registry;

  @Mock
  private CloudWatchReporter reporter;

  @Test
  public void testReporter() {
    when(metricsConfig.getCloudWatchReportPeriodSeconds()).thenReturn(30);
    CloudWatchMetricsReporter metricsReporter = new CloudWatchMetricsReporter(metricsConfig, registry, reporter);

    metricsReporter.start();
    verify(reporter, times(1)).start(30, TimeUnit.SECONDS);

    metricsReporter.report();
    verify(reporter, times(1)).report();

    metricsReporter.stop();
    verify(reporter, times(1)).stop();
  }

  @Test
  public void testReporterUsingMetricsConfig() {
    when(writeConfig.getMetricsConfig()).thenReturn(metricsConfig);
    when(metricsConfig.getCloudWatchReportPeriodSeconds()).thenReturn(30);
    CloudWatchMetricsReporter metricsReporter = new CloudWatchMetricsReporter(writeConfig, registry, reporter);

    metricsReporter.start();
    verify(reporter, times(1)).start(30, TimeUnit.SECONDS);

    metricsReporter.report();
    verify(reporter, times(1)).report();

    metricsReporter.stop();
    verify(reporter, times(1)).stop();
  }

  @Test
  public void testReporterViaReporterFactory() {
    try {
      when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.CLOUDWATCH);
      // MetricsReporterFactory uses reflection to create CloudWatchMetricsReporter
      // This test verifies that reflection is working well and is able to invoke the CloudWatchMetricsReporter constructor
      MetricsReporterFactory.createReporter(metricsConfig, registry).get();
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof InvocationTargetException);
      assertTrue(Arrays.stream(((InvocationTargetException) e.getCause()).getTargetException().getStackTrace()).anyMatch(
          ste -> ste.toString().contains("org.apache.hudi.aws.metrics.cloudwatch.CloudWatchReporter.getAmazonCloudWatchClient")));
    }
  }
}
