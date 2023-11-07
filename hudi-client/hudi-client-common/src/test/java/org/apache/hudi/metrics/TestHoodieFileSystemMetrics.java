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

package org.apache.hudi.metrics;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for the Jmx metrics report.
 */
@ExtendWith(MockitoExtension.class)
public class TestHoodieFileSystemMetrics extends HoodieCommonTestHarness {

  @Mock
  HoodieWriteConfig config;
  HoodieMetrics hoodieMetrics;
  Metrics metrics;
  private final HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
  private final HoodieWrapperFileSystem fileSystem = Mockito.mock(HoodieWrapperFileSystem.class);
  private final HoodieEngineContext context = Mockito.mock(HoodieEngineContext.class);
  private final HoodieTable table = Mockito.mock(HoodieTable.class);

  @BeforeEach
  void setup() throws IOException {
    Mockito.when(config.isMetricsOn()).thenReturn(true);
    Mockito.when(config.getTableName()).thenReturn("foo");
    Mockito.when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.FILESYSTEM);
    Mockito.when(config.getBasePath()).thenReturn("/tmp/dir/foo");
    Mockito.when(table.getMetaClient()).thenReturn(metaClient);
    Mockito.when(metaClient.getFs()).thenReturn(fileSystem);
    Mockito.when(table.getContext()).thenReturn(context);
    Mockito.when(config.getMetricReporterMetricsNamePrefix()).thenReturn("test");
    hoodieMetrics = new HoodieMetrics(config);
    metrics = hoodieMetrics.getMetrics();
    initMetaClient();
  }

  @AfterEach
  void shutdownMetrics() {
    metrics.shutdown();
  }

  @Test
  public void testRegisterGauge() {
    metrics.registerGauge("file_metrics1", 123L);
    assertEquals("123", metrics.getRegistry().getGauges()
        .get("file_metrics1").getValue().toString());
  }

  @Test
  public void testFileExists() {
    metrics.registerGauge("file_metrics1", 123L);
    Metrics.shutdownAllMetrics();
    String filePath = config.getBasePath() + "/.hoodie/metrics/foo_metrics.json";
    File file = new File(filePath);
    Assertions.assertTrue(file.exists());
  }
}
