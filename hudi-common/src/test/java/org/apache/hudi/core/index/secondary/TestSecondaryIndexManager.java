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

package org.apache.hudi.core.index.secondary;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieSecondaryIndexException;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link SecondaryIndexManager}.
 *
 * <p>{@link HoodieTableMetaClient} is mocked since {@code hudi-common} does not have access to a
 * concrete, filesystem-backed {@link org.apache.hudi.storage.HoodieStorage} implementation at test
 * time. The static {@link HoodieTableConfig#update} / {@link HoodieTableConfig#delete} calls are
 * intercepted and applied directly to an in-memory {@link HoodieTableConfig}, faithfully mirroring
 * what the real implementation persists to disk.
 */
public class TestSecondaryIndexManager {

  private static final String TABLE_SCHEMA = "{\"type\":\"record\",\"name\":\"trip\",\"fields\":["
      + "{\"name\":\"id\",\"type\":\"string\"},"
      + "{\"name\":\"name\",\"type\":\"string\"},"
      + "{\"name\":\"city\",\"type\":\"string\"}]}";

  private final SecondaryIndexManager manager = SecondaryIndexManager.getInstance();

  private HoodieTableConfig tableConfig;
  private HoodieTableMetaClient metaClient;
  private MockedStatic<HoodieTableConfig> tableConfigStatic;

  @BeforeEach
  public void setUp() {
    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.CREATE_SCHEMA, TABLE_SCHEMA);

    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    when(activeTimeline.getLastCommitMetadataWithValidSchema(false)).thenReturn(Option.empty());

    metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(metaClient.getMetaPath()).thenReturn(new StoragePath("/tmp/dummy/.hoodie"));

    tableConfigStatic = Mockito.mockStatic(HoodieTableConfig.class, Mockito.CALLS_REAL_METHODS);
    tableConfigStatic.when(() -> HoodieTableConfig.update(any(), any(), any()))
        .thenAnswer(invocation -> {
          tableConfig.setAll(invocation.getArgument(2));
          return null;
        });
    tableConfigStatic.when(() -> HoodieTableConfig.delete(any(), any(), any()))
        .thenAnswer(invocation -> {
          Set<String> propsToDelete = invocation.getArgument(2);
          propsToDelete.forEach(key -> tableConfig.getProps(false).remove(key));
          return null;
        });
  }

  @AfterEach
  public void tearDown() {
    tableConfigStatic.close();
  }

  @Test
  public void testGetInstanceReturnsSingleton() {
    assertSame(SecondaryIndexManager.getInstance(), SecondaryIndexManager.getInstance());
  }

  @Test
  public void testCreateAddsSecondaryIndexMetadata() {
    LinkedHashMap<String, Map<String, String>> columns = SecondaryIndexTestUtils.singleColumn("name");
    manager.create(metaClient, "idx_name", "lucene", false, columns, Collections.emptyMap());

    Option<List<HoodieSecondaryIndex>> indexes = manager.show(metaClient);
    assertTrue(indexes.isPresent());
    assertEquals(1, indexes.get().size());
    HoodieSecondaryIndex index = indexes.get().get(0);
    assertEquals("idx_name", index.getIndexName());
    assertEquals(SecondaryIndexType.LUCENE, index.getIndexType());
    assertEquals(columns.keySet(), index.getColumns().keySet());
  }

  @Test
  public void testCreateThrowsWhenColumnNotInSchema() {
    LinkedHashMap<String, Map<String, String>> columns = SecondaryIndexTestUtils.singleColumn("unknown_col");

    HoodieSecondaryIndexException e = assertThrows(HoodieSecondaryIndexException.class,
        () -> manager.create(metaClient, "idx_unknown", "lucene", false, columns, Collections.emptyMap()));
    assertTrue(e.getMessage().contains("Field not exists"));
  }

  @Test
  public void testCreateThrowsWhenIndexNameAlreadyExists() {
    LinkedHashMap<String, Map<String, String>> columns = SecondaryIndexTestUtils.singleColumn("name");
    manager.create(metaClient, "idx_name", "lucene", false, columns, Collections.emptyMap());

    HoodieSecondaryIndexException e = assertThrows(HoodieSecondaryIndexException.class,
        () -> manager.create(metaClient, "idx_name", "lucene", false, columns, Collections.emptyMap()));
    assertTrue(e.getMessage().contains("already exists"));
  }

  @Test
  public void testCreateIgnoresWhenIndexNameAlreadyExistsAndIgnoreFlagSet() {
    LinkedHashMap<String, Map<String, String>> columns = SecondaryIndexTestUtils.singleColumn("name");
    manager.create(metaClient, "idx_name", "lucene", false, columns, Collections.emptyMap());

    assertDoesNotThrow(() -> manager.create(metaClient, "idx_name", "lucene", true, columns, Collections.emptyMap()));
    assertEquals(1, manager.show(metaClient).get().size());
  }

  @Test
  public void testCreateThrowsWhenSameTypeAndColumnsUnderDifferentName() {
    LinkedHashMap<String, Map<String, String>> columns = SecondaryIndexTestUtils.singleColumn("name");
    manager.create(metaClient, "idx_1", "lucene", false, columns, Collections.emptyMap());

    // Same index type and columns, but a different name: should be treated as a duplicate.
    HoodieSecondaryIndexException e = assertThrows(HoodieSecondaryIndexException.class,
        () -> manager.create(metaClient, "idx_2", "lucene", false, columns, Collections.emptyMap()));
    assertTrue(e.getMessage().contains("already exists"));
  }

  @Test
  public void testCreateMultipleDistinctIndexesAreSortedByName() {
    LinkedHashMap<String, Map<String, String>> nameCol = SecondaryIndexTestUtils.singleColumn("name");
    LinkedHashMap<String, Map<String, String>> cityCol = SecondaryIndexTestUtils.singleColumn("city");

    manager.create(metaClient, "idx_z", "lucene", false, nameCol, Collections.emptyMap());
    manager.create(metaClient, "idx_a", "lucene", false, cityCol, Collections.emptyMap());

    List<HoodieSecondaryIndex> indexes = manager.show(metaClient).get();
    assertEquals(2, indexes.size());
    assertEquals("idx_a", indexes.get(0).getIndexName());
    assertEquals("idx_z", indexes.get(1).getIndexName());
  }

  @Test
  public void testShowReturnsEmptyWhenNoSecondaryIndexes() {
    assertFalse(manager.show(metaClient).isPresent());
  }

  @Test
  public void testDropThrowsWhenIndexNotExists() {
    HoodieSecondaryIndexException e = assertThrows(HoodieSecondaryIndexException.class,
        () -> manager.drop(metaClient, "idx_missing", false));
    assertTrue(e.getMessage().contains("not exists"));
  }

  @Test
  public void testDropIgnoresWhenIndexNotExistsAndIgnoreFlagSet() {
    assertDoesNotThrow(() -> manager.drop(metaClient, "idx_missing", true));
  }

  @Test
  public void testDropRemovesSecondaryIndexMetadataEntirelyWhenLastIndexDropped() {
    LinkedHashMap<String, Map<String, String>> columns = SecondaryIndexTestUtils.singleColumn("name");
    manager.create(metaClient, "idx_name", "lucene", false, columns, Collections.emptyMap());

    manager.drop(metaClient, "idx_name", false);

    assertFalse(manager.show(metaClient).isPresent());
  }

  @Test
  public void testDropKeepsRemainingIndexesWhenOtherIndexesExist() {
    LinkedHashMap<String, Map<String, String>> nameCol = SecondaryIndexTestUtils.singleColumn("name");
    LinkedHashMap<String, Map<String, String>> cityCol = SecondaryIndexTestUtils.singleColumn("city");

    manager.create(metaClient, "idx_name", "lucene", false, nameCol, Collections.emptyMap());
    manager.create(metaClient, "idx_city", "lucene", false, cityCol, Collections.emptyMap());

    manager.drop(metaClient, "idx_name", false);

    List<HoodieSecondaryIndex> remaining = manager.show(metaClient).get();
    assertEquals(1, remaining.size());
    assertEquals("idx_city", remaining.get(0).getIndexName());
  }
}
