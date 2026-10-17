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

package org.apache.hudi.index;

import org.apache.hudi.common.index.vector.VectorIndexOptions;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.exception.HoodieMetadataIndexException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_VECTOR_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_VECTOR_INDEX_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

/** Tests vector index definition validation at the DDL boundary. */
class TestVectorIndexDefinition {

  private HoodieTableMetaClient metaClient;
  private HoodieTableConfig tableConfig;

  @BeforeEach
  void setUp() {
    metaClient = mock(HoodieTableMetaClient.class);
    tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getMetadataPartitions()).thenReturn(Collections.emptySet());
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.current());
  }

  @Test
  void createsNormalizedDefinitionFromVectorSchema() throws Exception {
    Map<String, Map<String, String>> columns = singleColumn("embedding");
    Map<String, String> options = new LinkedHashMap<>();
    options.put(VectorIndexOptions.NUM_CLUSTERS, "4");
    options.put(VectorIndexOptions.QUERY_NUM_PROBES, "2");

    try (MockedConstruction<TableSchemaResolver> ignored = schemaResolverFor(
        HoodieSchemaField.of("embedding", HoodieSchema.createVector(128)))) {
      HoodieIndexDefinition definition = HoodieIndexUtils.getVectorIndexDefinition(
          metaClient, "embedding_idx", columns, options);

      assertEquals(PARTITION_NAME_VECTOR_INDEX_PREFIX + "embedding_idx", definition.getIndexName());
      assertEquals(PARTITION_NAME_VECTOR_INDEX, definition.getIndexType());
      assertEquals(Collections.singletonList("embedding"), definition.getSourceFields());
      assertEquals("4", definition.getIndexOptions().get(VectorIndexOptions.NUM_CLUSTERS));
      assertEquals("2", definition.getIndexOptions().get(VectorIndexOptions.QUERY_NUM_PROBES));
      assertFalse(definition.getIndexOptions().containsKey("vector.dimension"));
    }
  }

  @Test
  void rejectsNonVectorColumn() {
    try (MockedConstruction<TableSchemaResolver> ignored = schemaResolverFor(
        HoodieSchemaField.of("embedding", HoodieSchema.create(HoodieSchemaType.STRING)))) {
      assertThrows(HoodieMetadataIndexException.class,
          () -> HoodieIndexUtils.getVectorIndexDefinition(
              metaClient, "embedding_idx", singleColumn("embedding"), Collections.emptyMap()));
    }
  }

  @Test
  void rejectsUnknownOptions() {
    try (MockedConstruction<TableSchemaResolver> ignored = schemaResolverFor(
        HoodieSchemaField.of("embedding", HoodieSchema.createVector(128)))) {
      assertThrows(IllegalArgumentException.class,
          () -> HoodieIndexUtils.getVectorIndexDefinition(
              metaClient,
              "embedding_idx",
              singleColumn("embedding"),
              Collections.singletonMap("vector.dimension", "128")));
    }
  }

  @Test
  void rejectsMultipleColumns() {
    Map<String, Map<String, String>> columns = singleColumn("embedding");
    columns.put("other", Collections.emptyMap());
    assertThrows(IllegalArgumentException.class,
        () -> HoodieIndexUtils.getVectorIndexDefinition(
            metaClient, "embedding_idx", columns, Collections.emptyMap()));
  }

  private MockedConstruction<TableSchemaResolver> schemaResolverFor(HoodieSchemaField field) {
    HoodieSchema schema = HoodieSchema.createRecord(
        "VectorRecord", null, null, Collections.singletonList(field));
    return mockConstruction(
        TableSchemaResolver.class,
        (resolver, context) -> when(resolver.getTableSchema()).thenReturn(schema));
  }

  private static Map<String, Map<String, String>> singleColumn(String name) {
    Map<String, Map<String, String>> columns = new LinkedHashMap<>();
    columns.put(name, Collections.emptyMap());
    return columns;
  }
}
