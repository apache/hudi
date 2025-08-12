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

package org.apache.hudi.table.action.cluster.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class TestPartitionAwareClusteringPlanStrategy {

  @Mock
  HoodieTable table;
  @Mock
  HoodieEngineContext context;
  @Mock
  HoodieStorage storage;
  
  HoodieWriteConfig hoodieWriteConfig;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    Properties props = new Properties();
    props.setProperty("hoodie.clustering.plan.strategy.partition.regex.pattern", "2021072.*");
    this.hoodieWriteConfig = HoodieWriteConfig
        .newBuilder()
        .withPath("dummy_Table_Path")
        .withClusteringConfig(HoodieClusteringConfig
            .newBuilder()
            .fromProperties(props)
            .build())
        .build();
  }

  @Test
  public void testFilterPartitionPaths() {
    PartitionAwareClusteringPlanStrategy strategyTestRegexPattern = new DummyPartitionAwareClusteringPlanStrategy(table, context, hoodieWriteConfig);

    ArrayList<String> fakeTimeBasedPartitionsPath = new ArrayList<>();
    fakeTimeBasedPartitionsPath.add("20210718");
    fakeTimeBasedPartitionsPath.add("20210715");
    fakeTimeBasedPartitionsPath.add("20210723");
    fakeTimeBasedPartitionsPath.add("20210716");
    fakeTimeBasedPartitionsPath.add("20210719");
    fakeTimeBasedPartitionsPath.add("20210721");

    List list = strategyTestRegexPattern.getRegexPatternMatchedPartitions(hoodieWriteConfig, fakeTimeBasedPartitionsPath);
    assertEquals(2, list.size());
    assertTrue(list.contains("20210721"));
    assertTrue(list.contains("20210723"));
  }

  @Test
  public void testSchemaEvolutionDisabled_GroupsFilesBySchema() {
    // Given: Schema evolution is disabled
    HoodieWriteConfig configWithEvolutionDisabled = HoodieWriteConfig
        .newBuilder()
        .withPath("dummy_Table_Path")
        .build();
    configWithEvolutionDisabled.setValue(HoodieClusteringConfig.FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE, "false");
    
    when(table.getStorage()).thenReturn(storage);
    
    TestablePartitionAwareClusteringPlanStrategy strategy = 
        new TestablePartitionAwareClusteringPlanStrategy(table, context, configWithEvolutionDisabled);
    
    // Create file slices with different schemas
    FileSlice slice1 = createFileSlice("file1", 100L, 1001); // schema hash 1001
    FileSlice slice2 = createFileSlice("file2", 200L, 1001); // same schema hash
    FileSlice slice3 = createFileSlice("file3", 150L, 1002); // different schema hash
    FileSlice slice4 = createFileSlice("file4", 180L, 1002); // same as slice3
    
    List<FileSlice> fileSlices = Arrays.asList(slice1, slice2, slice3, slice4);
    
    try (MockedStatic<ParquetUtils> parquetUtilsMock = mockStatic(ParquetUtils.class)) {
      parquetUtilsMock.when(() -> ParquetUtils.readSchemaHash(eq(storage), any(StoragePath.class)))
          .thenAnswer(invocation -> {
            StoragePath path = invocation.getArgument(1);
            String pathStr = path.toString();
            if (pathStr.contains("file1") || pathStr.contains("file2")) {
              return 1001;
            } else {
              return 1002;
            }
          });
      
      // When: Building clustering groups
      Pair<Stream<HoodieClusteringGroup>, Boolean> result = 
          strategy.testBuildClusteringGroupsForPartition("test_partition", fileSlices);
      
      List<HoodieClusteringGroup> groups = result.getLeft().collect(java.util.stream.Collectors.toList());
      
      // Then: Should create groups based on schema and size order
      // Files are processed by size desc: file2(200), file4(180), file3(150), file1(100)
      // Group 1: file2 (schema 1001)
      // Group 2: file4, file3 (schema 1002) 
      // Group 3: file1 (schema 1001) - can't join group 1 as algorithm doesn't look back
      assertEquals(3, groups.size(), "Should create 3 groups based on size order and schema");
      
      // Verify first group has file2 (schema 1001)
      assertEquals(1, groups.get(0).getSlices().size());
      
      // Verify second group has file4 and file3 (schema 1002)
      assertEquals(2, groups.get(1).getSlices().size());
      
      // Verify third group has file1 (schema 1001) 
      assertEquals(1, groups.get(2).getSlices().size());
    }
  }

  @Test
  public void testSchemaEvolutionEnabled_DoesNotGroupBySchema() {
    // Given: Schema evolution is enabled (default)
    HoodieWriteConfig configWithEvolutionEnabled = HoodieWriteConfig
        .newBuilder()
        .withPath("dummy_Table_Path")
        .build();
    configWithEvolutionEnabled.setValue(HoodieClusteringConfig.FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE, "true");
    
    when(table.getStorage()).thenReturn(storage);
    
    TestablePartitionAwareClusteringPlanStrategy strategy = 
        new TestablePartitionAwareClusteringPlanStrategy(table, context, configWithEvolutionEnabled);
    
    // Create file slices with different schemas
    FileSlice slice1 = createFileSlice("file1", 100L, 1001);
    FileSlice slice2 = createFileSlice("file2", 200L, 1001);
    FileSlice slice3 = createFileSlice("file3", 150L, 1002); // different schema
    FileSlice slice4 = createFileSlice("file4", 180L, 1002);
    
    List<FileSlice> fileSlices = Arrays.asList(slice1, slice2, slice3, slice4);
    
    // When: Building clustering groups (schema grouping should be disabled)
    Pair<Stream<HoodieClusteringGroup>, Boolean> result = 
        strategy.testBuildClusteringGroupsForPartition("test_partition", fileSlices);
    
    List<HoodieClusteringGroup> groups = result.getLeft().collect(java.util.stream.Collectors.toList());
    
    // Then: Should create one group with all files (since total size is small)
    assertEquals(1, groups.size(), "Should group all files together when schema evolution is enabled");
    assertEquals(4, groups.get(0).getSlices().size(), "All files should be in the same group");
  }

  @Test
  public void testSchemaEvolutionDisabled_LargeGroup_SplitsBySize() {
    // Given: Schema evolution is disabled but group size exceeds limit
    HoodieWriteConfig configWithEvolutionDisabled = HoodieWriteConfig
        .newBuilder()
        .withPath("dummy_Table_Path")
        .build();
    configWithEvolutionDisabled.setValue(HoodieClusteringConfig.FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE, "false");
    configWithEvolutionDisabled.setValue(HoodieClusteringConfig.PLAN_STRATEGY_MAX_BYTES_PER_OUTPUT_FILEGROUP, "300"); // Small limit
    
    when(table.getStorage()).thenReturn(storage);
    
    TestablePartitionAwareClusteringPlanStrategy strategy = 
        new TestablePartitionAwareClusteringPlanStrategy(table, context, configWithEvolutionDisabled);
    
    // Create file slices with same schema but large total size
    FileSlice slice1 = createFileSlice("file1", 200L, 1001); // schema hash 1001
    FileSlice slice2 = createFileSlice("file2", 200L, 1001); // same schema, but total > 300
    
    List<FileSlice> fileSlices = Arrays.asList(slice1, slice2);
    
    try (MockedStatic<ParquetUtils> parquetUtilsMock = mockStatic(ParquetUtils.class)) {
      parquetUtilsMock.when(() -> ParquetUtils.readSchemaHash(eq(storage), any(StoragePath.class)))
          .thenReturn(1001);
      
      // When: Building clustering groups
      Pair<Stream<HoodieClusteringGroup>, Boolean> result = 
          strategy.testBuildClusteringGroupsForPartition("test_partition", fileSlices);
      
      List<HoodieClusteringGroup> groups = result.getLeft().collect(java.util.stream.Collectors.toList());
      
      // Then: Should create separate groups due to size limit, even with same schema
      assertEquals(2, groups.size(), "Should split by size even with same schema");
    }
  }

  private FileSlice createFileSlice(String fileName, long fileSize, int schemaHash) {
    FileSlice slice = mock(FileSlice.class);
    HoodieBaseFile baseFile = mock(HoodieBaseFile.class);
    
    when(slice.getBaseFile()).thenReturn(Option.of(baseFile));
    when(slice.getPartitionPath()).thenReturn("test_partition");
    when(slice.getFileId()).thenReturn(fileName + "_id");
    when(slice.getLogFiles()).thenAnswer(invocation -> Stream.empty());
    when(baseFile.getFileSize()).thenReturn(fileSize);
    when(baseFile.getPath()).thenReturn("/test/path/" + fileName + ".parquet");
    when(baseFile.getBootstrapBaseFile()).thenReturn(Option.empty());
    
    return slice;
  }

  class DummyPartitionAwareClusteringPlanStrategy extends PartitionAwareClusteringPlanStrategy {

    public DummyPartitionAwareClusteringPlanStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
      super(table, engineContext, writeConfig);
    }

    @Override
    protected Map<String, String> getStrategyParams() {
      return null;
    }
  }

  class TestablePartitionAwareClusteringPlanStrategy extends PartitionAwareClusteringPlanStrategy {

    public TestablePartitionAwareClusteringPlanStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
      super(table, engineContext, writeConfig);
    }

    @Override
    protected Map<String, String> getStrategyParams() {
      return null;
    }

    // Expose protected method for testing  
    public Pair<Stream<HoodieClusteringGroup>, Boolean> testBuildClusteringGroupsForPartition(String partitionPath, List<FileSlice> fileSlices) {
      return super.buildClusteringGroupsForPartition(partitionPath, fileSlices);
    }
  }
}
