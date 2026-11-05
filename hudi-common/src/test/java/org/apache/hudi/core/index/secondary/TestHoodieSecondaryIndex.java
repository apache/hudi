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

import org.apache.hudi.exception.HoodieSecondaryIndexException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieSecondaryIndex}.
 */
public class TestHoodieSecondaryIndex {

  @Test
  public void testBuilderPopulatesAllFields() {
    LinkedHashMap<String, Map<String, String>> columns = new LinkedHashMap<>();
    columns.put("name", Collections.singletonMap("order", "1"));

    HoodieSecondaryIndex index = HoodieSecondaryIndex.builder()
        .setIndexName("idx_name")
        .setIndexType("lucene")
        .setColumns(columns)
        .setOptions(Collections.singletonMap("k", "v"))
        .build();

    assertEquals("idx_name", index.getIndexName());
    assertEquals(SecondaryIndexType.LUCENE, index.getIndexType());
    assertEquals(columns, index.getColumns());
    assertEquals(Collections.singletonMap("k", "v"), index.getOptions());
  }

  @Test
  public void testBuilderIndexTypeIsCaseInsensitive() {
    HoodieSecondaryIndex index = HoodieSecondaryIndex.builder()
        .setIndexName("idx_name")
        .setIndexType("LUCENE")
        .setColumns(SecondaryIndexTestUtils.singleColumn("name"))
        .setOptions(Collections.emptyMap())
        .build();

    assertEquals(SecondaryIndexType.LUCENE, index.getIndexType());
  }

  @Test
  public void testLuceneIndexWithMultipleColumnsThrows() {
    LinkedHashMap<String, Map<String, String>> columns = new LinkedHashMap<>();
    columns.put("name", Collections.emptyMap());
    columns.put("city", Collections.emptyMap());

    HoodieSecondaryIndexException e = assertThrows(HoodieSecondaryIndexException.class,
        () -> new HoodieSecondaryIndex("idx_name", SecondaryIndexType.LUCENE, columns, Collections.emptyMap()));
    assertTrue(e.getMessage().contains("Lucene index only support single column"));
  }

  @Test
  public void testToStringContainsAllFields() {
    HoodieSecondaryIndex index = SecondaryIndexTestUtils.newLuceneIndex("idx_name", "name");

    String str = index.toString();
    assertTrue(str.contains("idx_name"));
    assertTrue(str.contains("LUCENE"));
    assertTrue(str.contains("name"));
  }

  @Test
  public void testHoodieIndexCompactorSortsByIndexName() {
    HoodieSecondaryIndex idxB = SecondaryIndexTestUtils.newLuceneIndex("idx_b", "name");
    HoodieSecondaryIndex idxA = SecondaryIndexTestUtils.newLuceneIndex("idx_a", "name");

    List<HoodieSecondaryIndex> sorted = Arrays.asList(idxB, idxA);
    sorted.sort(new HoodieSecondaryIndex.HoodieIndexCompactor());

    assertEquals("idx_a", sorted.get(0).getIndexName());
    assertEquals("idx_b", sorted.get(1).getIndexName());
  }
}
