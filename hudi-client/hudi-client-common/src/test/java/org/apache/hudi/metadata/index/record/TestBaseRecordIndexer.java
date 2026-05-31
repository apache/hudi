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

package org.apache.hudi.metadata.index.record;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

class TestBaseRecordIndexer {

  private static final String SIMPLE_SCHEMA_JSON =
      "{\"type\":\"record\",\"name\":\"Test\",\"namespace\":\"test\","
          + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";

  @Test
  void resolveDataSchemaForRLIBootstrap_usesConfigSchemaWhenPresent() throws Exception {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeConfig.getWriteSchema()).thenReturn(SIMPLE_SCHEMA_JSON);
    when(writeConfig.allowOperationMetadataField()).thenReturn(false);

    HoodieSchema result = BaseRecordIndexer.resolveDataSchemaForRLIBootstrap(metaClient, writeConfig);

    assertNotNull(result);
    assertTrue(result.getFields().stream().anyMatch(f -> f.name().startsWith("_hoodie_")));
  }

  @Test
  void resolveDataSchemaForRLIBootstrap_fallsBackToTableSchemaResolverWhenNull() throws Exception {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeConfig.getWriteSchema()).thenReturn(null);
    when(writeConfig.allowOperationMetadataField()).thenReturn(false);

    HoodieSchema tableSchema = HoodieSchema.parse(SIMPLE_SCHEMA_JSON);
    try (MockedConstruction<TableSchemaResolver> mockedResolver =
        mockConstruction(TableSchemaResolver.class,
            (resolver, ctx) -> when(resolver.getTableSchema(false)).thenReturn(tableSchema))) {

      HoodieSchema result = BaseRecordIndexer.resolveDataSchemaForRLIBootstrap(metaClient, writeConfig);

      assertNotNull(result);
      assertTrue(result.getFields().stream().anyMatch(f -> f.name().startsWith("_hoodie_")));
      assertEquals(1, mockedResolver.constructed().size());
    }
  }

}
