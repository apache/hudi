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

package org.apache.hudi.metrics.cloudwatch;

import org.apache.hudi.aws.cloudwatch.CloudWatchReporter;
import org.apache.hudi.config.HoodieWriteConfig;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestCloudWatchMetricsReporter {

  @Mock
  private HoodieWriteConfig config;

  @Mock
  private MetricRegistry registry;

  @Mock
  private CloudWatchReporter reporter;

  @Test
  public void testReporter() {
    when(config.getCloudWatchReportPeriodSeconds()).thenReturn(30);
    CloudWatchMetricsReporter metricsReporter = new CloudWatchMetricsReporter(config, registry, reporter);

    metricsReporter.start();
    verify(reporter, times(1)).start(30, TimeUnit.SECONDS);

    metricsReporter.report();
    verify(reporter, times(1)).report();

    metricsReporter.stop();
    verify(reporter, times(1)).stop();
  }
}
