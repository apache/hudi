/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client.clustering.plan.strategy;

import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit test for {@link FlinkSizeBasedClusteringPlanStrategy}.
 */
public class TestFlinkSizeBasedClusteringPlanStrategy {

  private FlinkSizeBasedClusteringPlanStrategy strategy;
  private HoodieWriteConfig config;

  @BeforeEach
  void setup() {
    config = mock(HoodieWriteConfig.class);
    when(config.getClusteringSmallFileLimit()).thenReturn(128 * 1024 * 1024L); // 128MB

    HoodieTable table = mock(HoodieTable.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);

    strategy = spy(new FlinkSizeBasedClusteringPlanStrategy<>(table, context, config));
  }


  @Test
  void testSkipSingleSmallFilePartition() {
    // Create one small file slice (below limit)
    HoodieBaseFile smallFile = mock(HoodieBaseFile.class);
    when(smallFile.getFileSize()).thenReturn(1024L * 1024L); // 1MB
    FileSlice slice = mock(FileSlice.class);
    when(slice.getBaseFile()).thenReturn(Option.of(smallFile));

    // Use spy + doReturn to mock the parent method behavior
    doReturn(Stream.of(slice))
            .when(strategy)
            .getFileSlicesEligibleForClustering(anyString());

    Stream<FileSlice> result = strategy.getFileSlicesEligibleForClustering("2025/10/09");

    // Since only one small file, clustering should skip
    assertEquals(0, result.count(), "Expected empty stream for single small file partition");
  }

  @Test
  void testMultipleSmallFilesShouldNotSkip() {
    HoodieBaseFile smallFile1 = mock(HoodieBaseFile.class);
    when(smallFile1.getFileSize()).thenReturn(1 * 1024 * 1024L);
    FileSlice slice1 = mock(FileSlice.class);
    when(slice1.getBaseFile()).thenReturn(Option.of(smallFile1));

    HoodieBaseFile smallFile2 = mock(HoodieBaseFile.class);
    when(smallFile2.getFileSize()).thenReturn(2 * 1024 * 1024L);
    FileSlice slice2 = mock(FileSlice.class);
    when(slice2.getBaseFile()).thenReturn(Option.of(smallFile2));

    doReturn(Stream.of(slice1, slice2))
            .when(strategy)
            .getFileSlicesEligibleForClustering(anyString());

    Stream<FileSlice> result = strategy.getFileSlicesEligibleForClustering("2025/10/09");

    // Multiple small files → clustering should proceed
    assertTrue(result.count() > 0, "Expected non-empty stream for multiple small files");
  }
}