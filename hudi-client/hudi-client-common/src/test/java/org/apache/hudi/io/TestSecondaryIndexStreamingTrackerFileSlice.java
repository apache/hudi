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

package org.apache.hudi.io;

import org.apache.hudi.client.SecondaryIndexStats;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class TestSecondaryIndexStreamingTrackerFileSlice {

  private HoodieTable hoodieTable;
  private HoodieWriteConfig config;
  private WriteStatus writeStatus;
  private Schema schema;
  private HoodieTableMetaClient metaClient;
  private HoodieEngineContext engineContext;
  private HoodieReaderContext readerContext;

  @BeforeEach
  public void setUp() {
    hoodieTable = mock(HoodieTable.class);
    config = mock(HoodieWriteConfig.class);
    writeStatus = new WriteStatus(true, 0.0d);
    schema = mock(Schema.class);
    metaClient = mock(HoodieTableMetaClient.class);
    engineContext = mock(HoodieEngineContext.class);
    readerContext = mock(HoodieReaderContext.class);
    
    ReaderContextFactory readerContextFactory = mock(ReaderContextFactory.class);
    when(readerContextFactory.getContext()).thenReturn(readerContext);
    when(hoodieTable.getContext()).thenReturn(engineContext);
    when(engineContext.getReaderContextFactory(any())).thenReturn(readerContextFactory);
    when(hoodieTable.getMetaClient()).thenReturn(metaClient);
    when(config.getBasePath()).thenReturn("/tmp/test");
    when(config.getProps()).thenReturn(new TypedProperties());
  }

  /**
   * Given: No existing file slice and new log files to be added
   * When: trackSecondaryIndexStats is called with empty fileSliceOpt
   * Then: All records from new log files should be treated as inserts with isDeleted=false
   */
  @Test
  public void testTrackSecondaryIndexStats_EmptyFileSlice() {
    String partitionPath = "2023/01/01";
    String fileId = "file-001";
    List<String> newLogFiles = Arrays.asList(
        partitionPath + "/.file-001_20231201120000.log.1",
        partitionPath + "/.file-001_20231201120000.log.2");
    String instantTime = "20231201120000";
    
    HoodieIndexDefinition indexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
    
    try (MockedStatic<SecondaryIndexRecordGenerationUtils> mockedUtils = 
            mockStatic(SecondaryIndexRecordGenerationUtils.class)) {
      
      // Mock empty previous file slice
      Map<String, String> emptyMap = Collections.emptyMap();
      
      // Mock current file slice with new records
      Map<String, String> currentMap = new HashMap<>();
      currentMap.put("key1", "100.5");
      currentMap.put("key2", "200.75");
      
      mockedUtils.when(() -> SecondaryIndexRecordGenerationUtils.getRecordKeyToSecondaryKey(
          eq(metaClient), eq(readerContext), any(FileSlice.class), eq(schema),
          eq(indexDef), eq(instantTime), any(TypedProperties.class), eq(true)))
          .thenReturn(currentMap);
      
      // Test with empty file slice
      SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
          partitionPath, fileId, Option.empty(), newLogFiles, writeStatus,
          hoodieTable, Collections.singletonList(indexDef), config, instantTime, schema);
      
      // All records should be new inserts
      assertEquals(2, writeStatus.getIndexStats().getSecondaryIndexStats()
          .get(indexDef.getIndexName()).size());
      
      writeStatus.getIndexStats().getSecondaryIndexStats()
          .get(indexDef.getIndexName()).forEach(stats -> {
            assertTrue(currentMap.containsKey(stats.getRecordKey()));
            assertEquals(currentMap.get(stats.getRecordKey()), stats.getSecondaryKeyValue());
            assertEquals(false, stats.isDeleted());
          });
    }
  }

  /**
   * Given: An existing file slice with records and new log files with updates/inserts/deletes
   * When: trackSecondaryIndexStats is called with both existing and new data
   * Then: Should correctly identify unchanged records (no update), updated records (delete old + insert new), 
   *       new records (insert), and deleted records (delete)
   */
  @Test
  public void testTrackSecondaryIndexStats_WithExistingFileSlice() {
    String partitionPath = "2023/01/01";
    String fileId = "file-002";
    List<String> newLogFiles = Arrays.asList(
        partitionPath + "/.file-002_20231201130000.log.1");
    String instantTime = "20231201130000";
    
    FileSlice existingFileSlice = new FileSlice(
        new HoodieFileGroupId(partitionPath, fileId), instantTime);
    
    HoodieIndexDefinition indexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
    
    try (MockedStatic<SecondaryIndexRecordGenerationUtils> mockedUtils = 
            mockStatic(SecondaryIndexRecordGenerationUtils.class)) {
      
      // Mock previous file slice
      Map<String, String> previousMap = new HashMap<>();
      previousMap.put("key1", "100.5");
      previousMap.put("key2", "200.75");
      previousMap.put("key3", "300.0"); // Will be deleted
      
      // Mock current file slice
      Map<String, String> currentMap = new HashMap<>();
      currentMap.put("key1", "100.5"); // Same value
      currentMap.put("key2", "250.0"); // Updated value
      currentMap.put("key4", "400.5"); // New record
      
      mockedUtils.when(() -> SecondaryIndexRecordGenerationUtils.getRecordKeyToSecondaryKey(
          eq(metaClient), eq(readerContext), eq(existingFileSlice), eq(schema),
          eq(indexDef), eq(instantTime), any(TypedProperties.class), eq(false)))
          .thenReturn(previousMap);
      
      mockedUtils.when(() -> SecondaryIndexRecordGenerationUtils.getRecordKeyToSecondaryKey(
          eq(metaClient), eq(readerContext), any(FileSlice.class), eq(schema),
          eq(indexDef), eq(instantTime), any(TypedProperties.class), eq(true)))
          .thenReturn(currentMap);
      
      // Test with existing file slice
      SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
          partitionPath, fileId, Option.of(existingFileSlice), newLogFiles, writeStatus,
          hoodieTable, Collections.singletonList(indexDef), config, instantTime, schema);
      
      // Should have: 1 update (delete + insert for key2), 1 insert (key4), 1 delete (key3)
      List<SecondaryIndexStats> stats = writeStatus.getIndexStats().getSecondaryIndexStats()
          .get(indexDef.getIndexName());
      assertEquals(4, stats.size()); // 2 for update + 1 insert + 1 delete
      
      // Verify each operation
      long deleteCount = stats.stream()
          .filter(s -> s.isDeleted())
          .count();
      assertEquals(2, deleteCount); // key2 old value + key3
      
      long insertCount = stats.stream()
          .filter(s -> !s.isDeleted())
          .count();
      assertEquals(2, insertCount); // key2 new value + key4
      
      // Validate detailed stats content
      // Check key2 update (delete old value + insert new value)
      assertTrue(stats.stream().anyMatch(s ->
          s.getRecordKey().equals("key2")
          && s.getSecondaryKeyValue().equals("200.75")
          && s.isDeleted()), "Should have delete entry for key2's old value");
      
      assertTrue(stats.stream().anyMatch(s -> 
          s.getRecordKey().equals("key2")
          && s.getSecondaryKeyValue().equals("250.0")
          && !s.isDeleted()), "Should have insert entry for key2's new value");
      
      // Check key3 deletion
      assertTrue(stats.stream().anyMatch(s -> 
          s.getRecordKey().equals("key3")
          && s.getSecondaryKeyValue().equals("300.0")
          && s.isDeleted()), "Should have delete entry for key3");
      
      // Check key4 insertion
      assertTrue(stats.stream().anyMatch(s -> 
          s.getRecordKey().equals("key4")
          && s.getSecondaryKeyValue().equals("400.5")
          && !s.isDeleted()), "Should have insert entry for key4");
      
      // Verify key1 is not in stats (no change)
      assertFalse(stats.stream().anyMatch(s -> 
          s.getRecordKey().equals("key1")), "key1 should not have any stats entries as its value didn't change");
    }
  }

  /**
   * Given: Multiple secondary index definitions (fare_index and name_index)
   * When: trackSecondaryIndexStats is called with multiple indexes
   * Then: Each index should be processed independently and have its own stats
   */
  @Test
  public void testTrackSecondaryIndexStats_MultipleIndexes() {
    String partitionPath = "2023/01/01";
    String fileId = "file-003";
    List<String> newLogFiles = Collections.emptyList();
    String instantTime = "20231201140000";
    
    HoodieIndexDefinition fareIndex = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
        
    HoodieIndexDefinition nameIndex = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_name_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("name"))
        .build();
    
    try (MockedStatic<SecondaryIndexRecordGenerationUtils> mockedUtils = 
            mockStatic(SecondaryIndexRecordGenerationUtils.class)) {
      
      // Mock for fare index
      Map<String, String> fareMap = new HashMap<>();
      fareMap.put("key1", "100.5");
      
      // Mock for name index
      Map<String, String> nameMap = new HashMap<>();
      nameMap.put("key1", "John Doe");
      
      mockedUtils.when(() -> SecondaryIndexRecordGenerationUtils.getRecordKeyToSecondaryKey(
          eq(metaClient), eq(readerContext), any(FileSlice.class), eq(schema),
          eq(fareIndex), eq(instantTime), any(TypedProperties.class), anyBoolean()))
          .thenReturn(fareMap);
          
      mockedUtils.when(() -> SecondaryIndexRecordGenerationUtils.getRecordKeyToSecondaryKey(
          eq(metaClient), eq(readerContext), any(FileSlice.class), eq(schema),
          eq(nameIndex), eq(instantTime), any(TypedProperties.class), anyBoolean()))
          .thenReturn(nameMap);
      
      // Test with multiple indexes
      SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
          partitionPath, fileId, Option.empty(), newLogFiles, writeStatus,
          hoodieTable, Arrays.asList(fareIndex, nameIndex), config, instantTime, schema);
      
      // Should have entries for both indexes
      assertEquals(2, writeStatus.getIndexStats().getSecondaryIndexStats().size());
      assertTrue(writeStatus.getIndexStats().getSecondaryIndexStats()
          .containsKey(fareIndex.getIndexName()));
      assertTrue(writeStatus.getIndexStats().getSecondaryIndexStats()
          .containsKey(nameIndex.getIndexName()));
    }
  }

  /**
   * Given: An IOException occurs when reading from the file slice
   * When: trackSecondaryIndexStats tries to process the file slice
   * Then: Should throw HoodieIOException wrapping the original IOException
   */
  @Test
  public void testTrackSecondaryIndexStats_IOExceptionHandling() {
    String partitionPath = "2023/01/01";
    String fileId = "file-004";
    List<String> newLogFiles = Arrays.asList(
        partitionPath + "/.file-004_20231201150000.log.1");
    String instantTime = "20231201150000";
    
    FileSlice existingFileSlice = new FileSlice(
        new HoodieFileGroupId(partitionPath, fileId), instantTime);
    
    HoodieIndexDefinition indexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
    
    try (MockedStatic<SecondaryIndexRecordGenerationUtils> mockedUtils = 
            mockStatic(SecondaryIndexRecordGenerationUtils.class)) {
      
      // Mock IOException when reading previous file slice
      mockedUtils.when(() -> SecondaryIndexRecordGenerationUtils.getRecordKeyToSecondaryKey(
          eq(metaClient), eq(readerContext), eq(existingFileSlice), eq(schema),
          eq(indexDef), eq(instantTime), any(TypedProperties.class), eq(false)))
          .thenThrow(new IOException("Failed to read file slice"));
      
      // Test IOException handling
      assertThrows(HoodieIOException.class, () -> 
          SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
              partitionPath, fileId, Option.of(existingFileSlice), newLogFiles, writeStatus,
              hoodieTable, Collections.singletonList(indexDef), config, instantTime, schema));
    }
  }

  /**
   * Given: An empty list of secondary index definitions
   * When: trackSecondaryIndexStats is called
   * Then: No secondary index updates should be recorded
   */
  @Test
  public void testTrackSecondaryIndexStats_EmptyIndexDefinitions() {
    String partitionPath = "2023/01/01";
    String fileId = "file-005";
    List<String> newLogFiles = Arrays.asList(
        partitionPath + "/.file-005_20231201160000.log.1");
    String instantTime = "20231201160000";
    
    // Test with empty index definitions
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        partitionPath, fileId, Option.empty(), newLogFiles, writeStatus,
        hoodieTable, Collections.emptyList(), config, instantTime, schema);
    
    // No secondary index updates should be recorded
    assertEquals(0, writeStatus.getIndexStats().getSecondaryIndexStats().size());
  }

  /**
   * Given: Records with null secondary key values
   * When: trackSecondaryIndexStats processes these records
   * Then: Null values should be handled correctly and stored as null in secondary index stats
   */
  @Test
  public void testTrackSecondaryIndexStats_NullSecondaryKeyValues() {
    String partitionPath = "2023/01/01";
    String fileId = "file-006";
    List<String> newLogFiles = Arrays.asList(
        partitionPath + "/.file-006_20231201170000.log.1");
    String instantTime = "20231201170000";
    
    HoodieIndexDefinition indexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
    
    try (MockedStatic<SecondaryIndexRecordGenerationUtils> mockedUtils = 
            mockStatic(SecondaryIndexRecordGenerationUtils.class)) {
      
      // Mock with null secondary key values
      Map<String, String> mapWithNulls = new HashMap<>();
      mapWithNulls.put("key1", null);
      mapWithNulls.put("key2", "200.0");
      
      mockedUtils.when(() -> SecondaryIndexRecordGenerationUtils.getRecordKeyToSecondaryKey(
          eq(metaClient), eq(readerContext), any(FileSlice.class), eq(schema),
          eq(indexDef), eq(instantTime), any(TypedProperties.class), anyBoolean()))
          .thenReturn(mapWithNulls);
      
      // Test with null values
      SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
          partitionPath, fileId, Option.empty(), newLogFiles, writeStatus,
          hoodieTable, Collections.singletonList(indexDef), config, instantTime, schema);
      
      // Should handle null values correctly
      assertEquals(2, writeStatus.getIndexStats().getSecondaryIndexStats()
          .get(indexDef.getIndexName()).size());
      
      // Verify null value is handled
      boolean hasNullValue = writeStatus.getIndexStats().getSecondaryIndexStats()
          .get(indexDef.getIndexName()).stream()
          .anyMatch(stats -> ((org.apache.hudi.client.SecondaryIndexStats) stats)
              .getSecondaryKeyValue() == null);
      assertTrue(hasNullValue);
    }
  }

  /**
   * Given: Previous and current file slices have the same secondary key values for all records
   * When: trackSecondaryIndexStats compares the two slices
   * Then: No secondary index updates should be recorded (optimization for unchanged values)
   */
  @Test
  public void testTrackSecondaryIndexStats_SameSecondaryKeyNoUpdate() {
    String partitionPath = "2023/01/01";
    String fileId = "file-007";
    List<String> newLogFiles = Arrays.asList(
        partitionPath + "/.file-007_20231201180000.log.1");
    String instantTime = "20231201180000";
    
    FileSlice existingFileSlice = new FileSlice(
        new HoodieFileGroupId(partitionPath, fileId), instantTime);
    
    HoodieIndexDefinition indexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
    
    try (MockedStatic<SecondaryIndexRecordGenerationUtils> mockedUtils = 
            mockStatic(SecondaryIndexRecordGenerationUtils.class)) {
      
      // Mock same values in both previous and current
      Map<String, String> sameMap = new HashMap<>();
      sameMap.put("key1", "100.5");
      sameMap.put("key2", "200.75");
      
      mockedUtils.when(() -> SecondaryIndexRecordGenerationUtils.getRecordKeyToSecondaryKey(
          eq(metaClient), eq(readerContext), any(FileSlice.class), eq(schema),
          eq(indexDef), eq(instantTime), any(TypedProperties.class), anyBoolean()))
          .thenReturn(sameMap);
      
      // Test with same values
      SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
          partitionPath, fileId, Option.of(existingFileSlice), newLogFiles, writeStatus,
          hoodieTable, Collections.singletonList(indexDef), config, instantTime, schema);
      
      // No updates should be recorded when values are the same
      assertEquals(0, writeStatus.getIndexStats().getSecondaryIndexStats().size());
    }
  }
}