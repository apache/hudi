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
    Properties props = new Properties();
    props.setProperty("hoodie.file.stitching.binary.copy.schema.evolution.enable", "false");
    HoodieWriteConfig configWithEvolutionDisabled = HoodieWriteConfig
        .newBuilder()
        .withPath("dummy_Table_Path")
        .withProps(props)
        .build();
    
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
            if (path.toString().contains("file1") || path.toString().contains("file2")) {
              return 1001;
            } else {
              return 1002;
            }
          });
      
      // When: Building clustering groups
      Pair<Stream<HoodieClusteringGroup>, Boolean> result = 
          strategy.testBuildClusteringGroupsForPartition("test_partition", fileSlices);
      
      List<HoodieClusteringGroup> groups = result.getLeft().collect(java.util.stream.Collectors.toList());
      
      // Then: Should create separate groups for different schemas
      assertEquals(2, groups.size(), "Should create separate groups for different schema hashes");
      
      // Verify first group has files with schema hash 1001 (file2, file1 - sorted by size desc)
      assertEquals(2, groups.get(0).getSlices().size());
      
      // Verify second group has files with schema hash 1002 (file4, file3 - sorted by size desc)  
      assertEquals(2, groups.get(1).getSlices().size());
    }
  }

  @Test
  public void testSchemaEvolutionEnabled_DoesNotGroupBySchema() {
    // Given: Schema evolution is enabled (default)
    Properties props = new Properties();
    props.setProperty("hoodie.file.stitching.binary.copy.schema.evolution.enable", "true");
    HoodieWriteConfig configWithEvolutionEnabled = HoodieWriteConfig
        .newBuilder()
        .withPath("dummy_Table_Path")
        .withProps(props)
        .build();
    
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
    Properties props = new Properties();
    props.setProperty("hoodie.file.stitching.binary.copy.schema.evolution.enable", "false");
    props.setProperty("hoodie.clustering.plan.strategy.max.bytes.per.group", "300"); // Small limit
    HoodieWriteConfig configWithEvolutionDisabled = HoodieWriteConfig
        .newBuilder()
        .withPath("dummy_Table_Path")
        .withProps(props)
        .build();
    
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
    when(baseFile.getFileSize()).thenReturn(fileSize);
    when(baseFile.getPath()).thenReturn("/test/path/" + fileName + ".parquet");
    
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
