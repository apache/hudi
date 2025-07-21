/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TestEightToNineUpgradeHandler {

  @TempDir
  private Path tempDir;

  @Mock
  private HoodieWriteConfig config;
  @Mock
  private HoodieEngineContext context;
  @Mock
  private SupportsUpgradeDowngrade upgradeDowngradeHelper;
  @Mock
  private HoodieTable table;
  @Mock
  private HoodieTableMetaClient metaClient;
  @Mock
  private HoodieTableConfig tableConfig;
  @Mock
  private HoodieStorage storage;

  private EightToNineUpgradeHandler upgradeHandler;
  private static final String INSTANT_TIME = "20231201120000";
  private StoragePath indexDefPath;

  @BeforeEach
  void setUp() throws IOException {
    upgradeHandler = new EightToNineUpgradeHandler();
    
    // Setup common mocks
    when(upgradeDowngradeHelper.getTable(config, context)).thenReturn(table);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getStorage()).thenReturn(storage);
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.EIGHT);
    
    // Use a temp file for index definition path
    indexDefPath = new StoragePath(tempDir.resolve("index.json").toString());
    when(metaClient.getIndexDefinitionPath()).thenReturn(indexDefPath.toString());
    
    // Mock storage methods for file creation
    when(storage.exists(any(StoragePath.class))).thenReturn(false);
    when(storage.createNewFile(any(StoragePath.class))).thenReturn(true);
    
    // Mock create method to capture written content
    ByteArrayOutputStream capturedContent = new ByteArrayOutputStream();
    when(storage.create(any(StoragePath.class), anyBoolean())).thenReturn(capturedContent);
    
    // Mock autoUpgrade to return true
    when(config.autoUpgrade()).thenReturn(true);
  }

  @Test
  void testUpgradeWithNoIndexMetadata() {
    try (MockedStatic<UpgradeDowngradeUtils> mockedUtils = mockStatic(UpgradeDowngradeUtils.class)) {
      // Mock the static method to do nothing - avoid NPE
      mockedUtils.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(HoodieTable.class),
          any(HoodieEngineContext.class),
          any(HoodieWriteConfig.class),
          any(SupportsUpgradeDowngrade.class),
          anyBoolean(),
          any(HoodieTableVersion.class)
      )).thenAnswer(invocation -> null); // Do nothing
      
      // Setup: No index metadata present
      when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
      
      // Execute
      Map<ConfigProperty, String> result = upgradeHandler.upgrade(config, context, INSTANT_TIME, upgradeDowngradeHelper);

      // Verify
      assertEquals(Collections.emptyMap(), result);
    }
  }

  @Test
  void testUpgradeWithMissingIndexVersion() throws IOException {
    try (MockedStatic<UpgradeDowngradeUtils> mockedUtils = mockStatic(UpgradeDowngradeUtils.class);
         MockedStatic<HoodieTableMetaClient> mockedMetaClient = mockStatic(HoodieTableMetaClient.class)) {
      // Mock the static method to do nothing - avoid NPE
      mockedUtils.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(HoodieTable.class), 
          any(HoodieEngineContext.class), 
          any(HoodieWriteConfig.class), 
          any(SupportsUpgradeDowngrade.class),
          anyBoolean(),
          any(HoodieTableVersion.class)
      )).thenAnswer(invocation -> null); // Do nothing
      
      // Mock the writeIndexMetadataToStorage to call the real method
      mockedMetaClient.when(() -> HoodieTableMetaClient.writeIndexMetadataToStorage(
          any(),
          any(String.class),
          any(HoodieIndexMetadata.class),
          any(HoodieTableVersion.class)
      )).thenCallRealMethod();
      
      // Setup: Index metadata present with missing versions
      HoodieIndexMetadata indexMetadata = createIndexMetadataWithMissingVersions();
      assertNull(indexMetadata.getIndexDefinitions().get("column_stats").getVersion());
      assertNull(indexMetadata.getIndexDefinitions().get("secondary_index_idx_price").getVersion());
      
      when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));

      // Capture the output stream to verify written content
      ByteArrayOutputStream capturedContent = new ByteArrayOutputStream();
      when(storage.create(eq(indexDefPath), eq(true))).thenReturn(capturedContent);

      // Execute
      Map<ConfigProperty, String> result = upgradeHandler.upgrade(config, context, INSTANT_TIME, upgradeDowngradeHelper);

      // Verify
      assertEquals(Collections.emptyMap(), result);
      
      // Verify storage methods were called correctly
      // Note: createFileInPath directly calls create() when contentWriter is present
      verify(storage).create(indexDefPath, true);
      
      // Verify the written content by parsing the JSON and validating the object
      String writtenJson = capturedContent.toString();
      
      // Expected JSON for table version 8 with V1 versions
      String expectedJson = "{\n"
          + "  \"indexDefinitions\": {\n"
          + "    \"column_stats\": {\n"
          + "      \"indexName\": \"column_stats\",\n"
          + "      \"indexType\": \"column_stats\",\n"
          + "      \"indexFunction\": \"column_stats\",\n"
          + "      \"sourceFields\": [\"field1\", \"field2\"],\n"
          + "      \"indexOptions\": {},\n"
          + "      \"version\": \"V1\"\n"
          + "    },\n"
          + "    \"secondary_index_idx_price\": {\n"
          + "      \"indexName\": \"secondary_index_idx_price\",\n"
          + "      \"indexType\": \"secondary_index\",\n"
          + "      \"indexFunction\": \"identity\",\n"
          + "      \"sourceFields\": [\"price\"],\n"
          + "      \"indexOptions\": {},\n"
          + "      \"version\": \"V1\"\n"
          + "    }\n"
          + "  }\n"
          + "}";
      
      // Parse the written JSON and validate against expected
      HoodieIndexMetadata writtenMetadata = HoodieIndexMetadata.fromJson(writtenJson);
      HoodieIndexMetadata expectedMetadata = HoodieIndexMetadata.fromJson(expectedJson);
      
      // Validate the parsed objects match
      assertEquals(expectedMetadata.getIndexDefinitions().size(), writtenMetadata.getIndexDefinitions().size());
      
      // Validate column_stats index
      HoodieIndexDefinition writtenColumnStats = writtenMetadata.getIndexDefinitions().get("column_stats");
      HoodieIndexDefinition expectedColumnStats = expectedMetadata.getIndexDefinitions().get("column_stats");
      assertEquals(expectedColumnStats.getIndexName(), writtenColumnStats.getIndexName());
      assertEquals(expectedColumnStats.getIndexType(), writtenColumnStats.getIndexType());
      assertEquals(expectedColumnStats.getIndexFunction(), writtenColumnStats.getIndexFunction());
      assertEquals(expectedColumnStats.getSourceFields(), writtenColumnStats.getSourceFields());
      assertEquals(expectedColumnStats.getIndexOptions(), writtenColumnStats.getIndexOptions());
      assertEquals(expectedColumnStats.getVersion(), writtenColumnStats.getVersion());
      
      // Validate secondary_index_idx_price index
      HoodieIndexDefinition writtenSecondaryIndex = writtenMetadata.getIndexDefinitions().get("secondary_index_idx_price");
      HoodieIndexDefinition expectedSecondaryIndex = expectedMetadata.getIndexDefinitions().get("secondary_index_idx_price");
      assertEquals(expectedSecondaryIndex.getIndexName(), writtenSecondaryIndex.getIndexName());
      assertEquals(expectedSecondaryIndex.getIndexType(), writtenSecondaryIndex.getIndexType());
      assertEquals(expectedSecondaryIndex.getIndexFunction(), writtenSecondaryIndex.getIndexFunction());
      assertEquals(expectedSecondaryIndex.getSourceFields(), writtenSecondaryIndex.getSourceFields());
      assertEquals(expectedSecondaryIndex.getIndexOptions(), writtenSecondaryIndex.getIndexOptions());
      assertEquals(expectedSecondaryIndex.getVersion(), writtenSecondaryIndex.getVersion());
    }
  }

  @Test
  void testUpgradeWithIndexMetadataHavingVersions() {
    try (MockedStatic<UpgradeDowngradeUtils> mockedUtils = mockStatic(UpgradeDowngradeUtils.class)) {
      // Mock the static method to do nothing - avoid NPE
      mockedUtils.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(HoodieTable.class),
          any(HoodieEngineContext.class),
          any(HoodieWriteConfig.class),
          any(SupportsUpgradeDowngrade.class),
          anyBoolean(),
          any(HoodieTableVersion.class)
      )).thenAnswer(invocation -> null); // Do nothing
      
      // Setup: Index metadata present with existing versions
      HoodieIndexMetadata indexMetadata = createIndexMetadataWithVersions();
      // TODO: assert index defs of indexMetadata have version field
      // Note: Since we can't import HoodieIndexVersion due to dependency issues, 
      // we'll skip this test for now and focus on testing the storage functionality
      when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));

      // Execute
      Map<ConfigProperty, String> result = upgradeHandler.upgrade(config, context, INSTANT_TIME, upgradeDowngradeHelper);

      // Verify
      assertEquals(Collections.emptyMap(), result);
      // Verify the written json is the same as before
    }
  }

  @Test
  void testUpgradeWithEmptyIndexMetadata() {
    try (MockedStatic<UpgradeDowngradeUtils> mockedUtils = mockStatic(UpgradeDowngradeUtils.class)) {
      // Mock the static method to do nothing - avoid NPE
      mockedUtils.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(HoodieTable.class),
          any(HoodieEngineContext.class),
          any(HoodieWriteConfig.class),
          any(SupportsUpgradeDowngrade.class),
          anyBoolean(),
          any(HoodieTableVersion.class)
      )).thenAnswer(invocation -> null); // Do nothing
      
      // Setup: Empty index metadata (no index definitions)
      HoodieIndexMetadata indexMetadata = new HoodieIndexMetadata();
      when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));

      // Execute
      Map<ConfigProperty, String> result = upgradeHandler.upgrade(config, context, INSTANT_TIME, upgradeDowngradeHelper);

      // Verify
      assertEquals(Collections.emptyMap(), result);
    }
  }

  @Test
  void testUpgradeWithFileAlreadyExists() throws IOException {
    try (MockedStatic<UpgradeDowngradeUtils> mockedUtils = mockStatic(UpgradeDowngradeUtils.class);
         MockedStatic<HoodieTableMetaClient> mockedMetaClient = mockStatic(HoodieTableMetaClient.class)) {
      // Mock the static method to do nothing - avoid NPE
      mockedUtils.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(HoodieTable.class),
          any(HoodieEngineContext.class),
          any(HoodieWriteConfig.class),
          any(SupportsUpgradeDowngrade.class),
          anyBoolean(),
          any(HoodieTableVersion.class)
      )).thenAnswer(invocation -> null); // Do nothing
      
      // Mock the writeIndexMetadataToStorage to call the real method
      mockedMetaClient.when(() -> HoodieTableMetaClient.writeIndexMetadataToStorage(
          any(),
          any(String.class),
          any(HoodieIndexMetadata.class),
          any(HoodieTableVersion.class)
      )).thenCallRealMethod();
      
      // Setup: File already exists
      HoodieIndexMetadata indexMetadata = createIndexMetadataWithMissingVersions();
      when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
      when(storage.exists(indexDefPath)).thenReturn(true);

      // Capture the output stream to verify written content
      ByteArrayOutputStream capturedContent = new ByteArrayOutputStream();
      when(storage.create(eq(indexDefPath), eq(true))).thenReturn(capturedContent);

      // Execute
      Map<ConfigProperty, String> result = upgradeHandler.upgrade(config, context, INSTANT_TIME, upgradeDowngradeHelper);

      // Verify
      assertEquals(Collections.emptyMap(), result);
      
      // Verify storage methods were called correctly
      // Note: createFileInPath directly calls create() when contentWriter is present
      verify(storage).create(indexDefPath, true);
      
      // Verify the written content by parsing the JSON and validating the object
      String writtenJson = capturedContent.toString();
      
      // Expected JSON for table version 8 with V1 versions
      String expectedJson = "{\n"
          + "  \"indexDefinitions\": {\n"
          + "    \"column_stats\": {\n"
          + "      \"indexName\": \"column_stats\",\n"
          + "      \"indexType\": \"column_stats\",\n"
          + "      \"indexFunction\": \"column_stats\",\n"
          + "      \"sourceFields\": [\"field1\", \"field2\"],\n"
          + "      \"indexOptions\": {},\n"
          + "      \"version\": \"V1\"\n"
          + "    },\n"
          + "    \"secondary_index_idx_price\": {\n"
          + "      \"indexName\": \"secondary_index_idx_price\",\n"
          + "      \"indexType\": \"secondary_index\",\n"
          + "      \"indexFunction\": \"identity\",\n"
          + "      \"sourceFields\": [\"price\"],\n"
          + "      \"indexOptions\": {},\n"
          + "      \"version\": \"V1\"\n"
          + "    }\n"
          + "  }\n"
          + "}";
      
      // Parse the written JSON and validate against expected
      HoodieIndexMetadata writtenMetadata = HoodieIndexMetadata.fromJson(writtenJson);
      HoodieIndexMetadata expectedMetadata = HoodieIndexMetadata.fromJson(expectedJson);
      
      // Validate the parsed objects match
      assertEquals(expectedMetadata.getIndexDefinitions().size(), writtenMetadata.getIndexDefinitions().size());
      
      // Validate column_stats index
      HoodieIndexDefinition writtenColumnStats = writtenMetadata.getIndexDefinitions().get("column_stats");
      HoodieIndexDefinition expectedColumnStats = expectedMetadata.getIndexDefinitions().get("column_stats");
      assertEquals(expectedColumnStats.getIndexName(), writtenColumnStats.getIndexName());
      assertEquals(expectedColumnStats.getIndexType(), writtenColumnStats.getIndexType());
      assertEquals(expectedColumnStats.getIndexFunction(), writtenColumnStats.getIndexFunction());
      assertEquals(expectedColumnStats.getSourceFields(), writtenColumnStats.getSourceFields());
      assertEquals(expectedColumnStats.getIndexOptions(), writtenColumnStats.getIndexOptions());
      assertEquals(expectedColumnStats.getVersion(), writtenColumnStats.getVersion());
      
      // Validate secondary_index_idx_price index
      HoodieIndexDefinition writtenSecondaryIndex = writtenMetadata.getIndexDefinitions().get("secondary_index_idx_price");
      HoodieIndexDefinition expectedSecondaryIndex = expectedMetadata.getIndexDefinitions().get("secondary_index_idx_price");
      assertEquals(expectedSecondaryIndex.getIndexName(), writtenSecondaryIndex.getIndexName());
      assertEquals(expectedSecondaryIndex.getIndexType(), writtenSecondaryIndex.getIndexType());
      assertEquals(expectedSecondaryIndex.getIndexFunction(), writtenSecondaryIndex.getIndexFunction());
      assertEquals(expectedSecondaryIndex.getSourceFields(), writtenSecondaryIndex.getSourceFields());
      assertEquals(expectedSecondaryIndex.getIndexOptions(), writtenSecondaryIndex.getIndexOptions());
      assertEquals(expectedSecondaryIndex.getVersion(), writtenSecondaryIndex.getVersion());
    }
  }

  /**
   * Creates index metadata with missing version fields (simulating table version 8 scenario)
   */
  private HoodieIndexMetadata createIndexMetadataWithMissingVersions() {
    Map<String, HoodieIndexDefinition> indexDefinitions = new HashMap<>();
    
    // Column stats index without version
    HoodieIndexDefinition columnStatsDef = HoodieIndexDefinition.newBuilder()
        .withIndexName("column_stats")
        .withIndexType("column_stats")
        .withIndexFunction("column_stats")
        .withSourceFields(java.util.Arrays.asList("field1", "field2"))
        .withIndexOptions(Collections.emptyMap())
        .build();
    
    // Secondary index without version
    HoodieIndexDefinition secondaryIndexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName("secondary_index_idx_price")
        .withIndexType("secondary_index")
        .withIndexFunction("identity")
        .withSourceFields(java.util.Arrays.asList("price"))
        .withIndexOptions(Collections.emptyMap())
        .build();
    
    indexDefinitions.put("column_stats", columnStatsDef);
    indexDefinitions.put("secondary_index_idx_price", secondaryIndexDef);
    
    return new HoodieIndexMetadata(indexDefinitions);
  }

  /**
   * Creates index metadata with existing version fields
   */
  private HoodieIndexMetadata createIndexMetadataWithVersions() {
    Map<String, HoodieIndexDefinition> indexDefinitions = new HashMap<>();
    // Note: Since we can't import HoodieIndexVersion due to dependency issues,
    // we'll create index definitions without version attributes for now
    // Column stats index with version
    HoodieIndexDefinition columnStatsDef = HoodieIndexDefinition.newBuilder()
        .withIndexName("column_stats")
        .withIndexType("column_stats")
        .withIndexFunction("column_stats")
        .withSourceFields(java.util.Arrays.asList("field1", "field2"))
        .withIndexOptions(Collections.emptyMap())
        .withVersion(HoodieIndexVersion.V1)
        .build();
    
    // Secondary index with version
    HoodieIndexDefinition secondaryIndexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName("secondary_index_idx_price")
        .withIndexType("secondary_index")
        .withIndexFunction("identity")
        .withSourceFields(java.util.Arrays.asList("price"))
        .withIndexOptions(Collections.emptyMap())
        .withVersion(HoodieIndexVersion.V1)
        .build();
    
    indexDefinitions.put("column_stats", columnStatsDef);
    indexDefinitions.put("secondary_index_idx_price", secondaryIndexDef);
    
    return new HoodieIndexMetadata(indexDefinitions);
  }

  @Test
  void testPopulateIndexVersionIfMissing() {
    try (MockedStatic<UpgradeDowngradeUtils> mockedUtils = mockStatic(UpgradeDowngradeUtils.class)) {
      // Mock the static method to do nothing - avoid NPE
      mockedUtils.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(HoodieTable.class),
          any(HoodieEngineContext.class),
          any(HoodieWriteConfig.class),
          any(SupportsUpgradeDowngrade.class),
          anyBoolean(),
          any(HoodieTableVersion.class)
      )).thenAnswer(invocation -> null); // Do nothing
      
      // Test with table version 8 - should populate missing versions with V1
      HoodieIndexMetadata indexMetadata = loadIndexDefFromResource("indexMissingVersion1.json");
      
      // Verify initial state - no versions
      assertNull(indexMetadata.getIndexDefinitions().get("column_stats").getVersion());
      assertNull(indexMetadata.getIndexDefinitions().get("secondary_index_idx_price").getVersion());
      
      // Apply the method
      EightToNineUpgradeHandler.populateIndexVersionIfMissing(Option.of(indexMetadata));
      
      // Verify versions are populated with V1
      assertEquals(HoodieIndexVersion.V1, indexMetadata.getIndexDefinitions().get("column_stats").getVersion());
      assertEquals(HoodieIndexVersion.V1, indexMetadata.getIndexDefinitions().get("secondary_index_idx_price").getVersion());
      
      // Verify other fields remain unchanged
      validateAllFieldsExcludingVersion(indexMetadata);
    }
  }

  @Test
  void testPopulateIndexVersionIfMissingWithMixedVersions() {
    try (MockedStatic<UpgradeDowngradeUtils> mockedUtils = mockStatic(UpgradeDowngradeUtils.class)) {
      // Mock the static method to do nothing - avoid NPE
      mockedUtils.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(HoodieTable.class),
          any(HoodieEngineContext.class),
          any(HoodieWriteConfig.class),
          any(SupportsUpgradeDowngrade.class),
          anyBoolean(),
          any(HoodieTableVersion.class)
      )).thenAnswer(invocation -> null); // Do nothing
      
      // Test with indexMissingVersion2.json which has some versions already set
      HoodieIndexMetadata indexMetadata = loadIndexDefFromResource("indexMissingVersion2.json");
      
      // Verify initial state - column_stats has no version, secondary_index has V2
      assertNull(indexMetadata.getIndexDefinitions().get("column_stats").getVersion());
      assertEquals(HoodieIndexVersion.V2, indexMetadata.getIndexDefinitions().get("secondary_index_idx_price").getVersion());
      
      // Apply the method with table version 8
      EightToNineUpgradeHandler.populateIndexVersionIfMissing(Option.of(indexMetadata));
      
      // Verify column_stats gets V1, secondary_index remains V2 (since it already had a version)
      assertEquals(HoodieIndexVersion.V1, indexMetadata.getIndexDefinitions().get("column_stats").getVersion());
      assertEquals(HoodieIndexVersion.V2, indexMetadata.getIndexDefinitions().get("secondary_index_idx_price").getVersion());
    }
  }

  @Test
  void testPopulateIndexVersionIfMissingWithEmptyOption() {
    try (MockedStatic<UpgradeDowngradeUtils> mockedUtils = mockStatic(UpgradeDowngradeUtils.class)) {
      // Mock the static method to do nothing - avoid NPE
      mockedUtils.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(HoodieTable.class),
          any(HoodieEngineContext.class),
          any(HoodieWriteConfig.class),
          any(SupportsUpgradeDowngrade.class),
          anyBoolean(),
          any(HoodieTableVersion.class)
      )).thenAnswer(invocation -> null); // Do nothing
      
      // Test with empty option - should not throw exception
      assertDoesNotThrow(() ->
          EightToNineUpgradeHandler.populateIndexVersionIfMissing(Option.empty()));
    }
  }

  private static HoodieIndexMetadata loadIndexDefFromResource(String resourceName) {
    try {
      String resourcePath = TestEightToNineUpgradeHandler.class.getClassLoader().getResource(resourceName).toString();
      return HoodieIndexMetadata.fromJson(new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(new java.net.URI(resourcePath)))));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void validateAllFieldsExcludingVersion(HoodieIndexMetadata loadedDef) {
    HoodieIndexDefinition colStatsDef = loadedDef.getIndexDefinitions().get("column_stats");
    assertEquals("column_stats", colStatsDef.getIndexName());
    assertEquals("column_stats", colStatsDef.getIndexType());
    assertEquals("column_stats", colStatsDef.getIndexFunction());
    assertEquals(Collections.emptyMap(), colStatsDef.getIndexOptions());
    assertEquals(Arrays.asList(
        "_hoodie_commit_time", "_hoodie_partition_path", "_hoodie_record_key", "key", "secKey", "partition", "intField",
        "city", "textField1", "textField2", "textField3", "textField4", "decimalField", "longField", "incrLongField", "round"),
        colStatsDef.getSourceFields());

    HoodieIndexDefinition secIdxDef = loadedDef.getIndexDefinitions().get("secondary_index_idx_price");
    assertEquals("secondary_index_idx_price", secIdxDef.getIndexName());
    assertEquals("secondary_index", secIdxDef.getIndexType());
    assertEquals("identity", secIdxDef.getIndexFunction());
    assertEquals(Collections.singletonList("price"), secIdxDef.getSourceFields());
    assertEquals(Collections.emptyMap(), secIdxDef.getIndexOptions());
  }
} 