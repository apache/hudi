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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieSecondaryIndexException;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link SecondaryIndexDefinitionUtils}.
 */
public class TestSecondaryIndexDefinitionUtils {

  private static HoodieSecondaryIndex newIndex(String name) {
    return SecondaryIndexTestUtils.newLuceneIndex(name, "name", Collections.singletonMap("k", "v"));
  }

  private static HoodieTableMetaClient metaClientWithTableConfig(HoodieTableConfig tableConfig) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    return metaClient;
  }

  @Test
  public void testToJsonStringAndFromJsonStringRoundTrip() {
    List<HoodieSecondaryIndex> original = Arrays.asList(newIndex("idx_1"), newIndex("idx_2"));
    String json = SecondaryIndexDefinitionUtils.toJsonString(original);

    List<HoodieSecondaryIndex> parsed = SecondaryIndexDefinitionUtils.fromJsonString(json);
    assertEquals(2, parsed.size());
    assertEquals("idx_1", parsed.get(0).getIndexName());
    assertEquals(SecondaryIndexType.LUCENE, parsed.get(0).getIndexType());
    assertEquals(original.get(0).getColumns(), parsed.get(0).getColumns());
    assertEquals(original.get(0).getOptions(), parsed.get(0).getOptions());
  }

  @Test
  public void testFromJsonStringThrowsOnMalformedJson() {
    HoodieSecondaryIndexException e = assertThrows(HoodieSecondaryIndexException.class,
        () -> SecondaryIndexDefinitionUtils.fromJsonString("not a valid json"));
    assertTrue(e.getMessage().contains("Fail to get secondary indexes"));
  }

  @Test
  public void testFromJsonStringWithTypeReferenceReturnsNullForEmptyInput() throws Exception {
    assertNull(SecondaryIndexDefinitionUtils.fromJsonString(null, new TypeReference<List<HoodieSecondaryIndex>>() { }));
    assertNull(SecondaryIndexDefinitionUtils.fromJsonString("", new TypeReference<List<HoodieSecondaryIndex>>() { }));
  }

  @Test
  public void testObjectMapperIgnoresUnknownProperties() throws Exception {
    String jsonWithUnknownField = "[{\"indexName\":\"idx_1\",\"indexType\":\"LUCENE\","
        + "\"columns\":{\"name\":{}},\"options\":{},\"unknownField\":\"shouldBeIgnored\"}]";

    List<HoodieSecondaryIndex> parsed = SecondaryIndexDefinitionUtils.fromJsonString(jsonWithUnknownField);
    assertEquals(1, parsed.size());
    assertEquals("idx_1", parsed.get(0).getIndexName());
  }

  @Test
  public void testGetSecondaryIndexesReturnsEmptyWhenNotSet() {
    HoodieTableMetaClient metaClient = metaClientWithTableConfig(new HoodieTableConfig());
    assertFalse(SecondaryIndexDefinitionUtils.getSecondaryIndexes(metaClient).isPresent());
  }

  @Test
  public void testGetSecondaryIndexesReturnsParsedListWhenSet() {
    List<HoodieSecondaryIndex> original = Collections.singletonList(newIndex("idx_1"));
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.SECONDARY_INDEXES_METADATA, SecondaryIndexDefinitionUtils.toJsonString(original));
    HoodieTableMetaClient metaClient = metaClientWithTableConfig(tableConfig);

    Option<List<HoodieSecondaryIndex>> indexes = SecondaryIndexDefinitionUtils.getSecondaryIndexes(metaClient);
    assertTrue(indexes.isPresent());
    assertEquals("idx_1", indexes.get().get(0).getIndexName());
  }
}
