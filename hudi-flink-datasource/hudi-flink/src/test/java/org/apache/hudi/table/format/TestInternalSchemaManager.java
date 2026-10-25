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

package org.apache.hudi.table.format;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.schema.internal.Types;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/**
 * Tests read-time schema reconciliation in {@link InternalSchemaManager}.
 */
class TestInternalSchemaManager {

  @Test
  void testDisabledManagerUsesEmptySchema() {
    assertTrue(InternalSchemaManager.DISABLED.getQuerySchema().isEmptySchema());
    assertTrue(InternalSchemaManager.DISABLED
        .getMergeSchema("file-id_1-0-1_001.parquet").isEmptySchema());

    org.apache.hadoop.conf.Configuration hadoopConf =
        new org.apache.hadoop.conf.Configuration();
    hadoopConf.setBoolean(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), false);
    assertSame(
        InternalSchemaManager.DISABLED,
        InternalSchemaManager.get(
            new HadoopStorageConfiguration(hadoopConf),
            mock(org.apache.hudi.common.table.HoodieTableMetaClient.class)));
  }

  @Test
  void testGetCastMapForChangedSelectedField() {
    InternalSchema querySchema = schema(
        Types.Field.get(0, false, "id", Types.LongType.get()),
        Types.Field.get(1, true, "new_name", Types.StringType.get()));
    InternalSchema fileSchema = schema(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "old_name", Types.StringType.get()));
    InternalSchemaManager manager = manager(querySchema);
    DataType[] queryTypes = {DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.BOOLEAN()};

    CastMap castMap = manager.getCastMap(
        fileSchema,
        new String[] {"id", "new_name", "extra"},
        queryTypes,
        new int[] {0, 1});

    assertEquals(DataTypes.INT().notNull(), castMap.getFileFieldTypes()[0]);
    assertEquals(DataTypes.STRING(), castMap.getFileFieldTypes()[1]);
    assertEquals(DataTypes.BOOLEAN(), castMap.getFileFieldTypes()[2]);
    assertEquals(7L, castMap.castIfNeeded(0, 7));
    assertTrue(castMap.toRowDataProjection(new int[] {0, 1}).isPresent());
  }

  @Test
  void testGetCastMapWithoutSelectedChangedField() {
    InternalSchema querySchema = schema(
        Types.Field.get(0, false, "id", Types.LongType.get()),
        Types.Field.get(1, true, "name", Types.StringType.get()));
    InternalSchema fileSchema = schema(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "name", Types.StringType.get()));
    InternalSchemaManager manager = manager(querySchema);

    CastMap castMap = manager.getCastMap(
        fileSchema,
        new String[] {"id", "name"},
        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
        new int[] {1});

    assertEquals(DataTypes.INT().notNull(), castMap.getFileFieldTypes()[0]);
    assertFalse(castMap.toRowDataProjection(new int[] {1}).isPresent());
  }

  @Test
  void testGetCastMapWhenTypesAreUnchanged() {
    InternalSchema schema = schema(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "name", Types.StringType.get()));
    InternalSchemaManager manager = manager(schema);
    DataType[] queryTypes = {DataTypes.INT(), DataTypes.STRING()};

    CastMap castMap = manager.getCastMap(
        schema,
        new String[] {"id", "name"},
        queryTypes,
        new int[] {0, 1});

    assertArrayEquals(queryTypes, castMap.getFileFieldTypes());
    assertFalse(castMap.toRowDataProjection(new int[] {0, 1}).isPresent());
  }

  @Test
  void testGetMergeFieldNamesResolvesRenames() {
    InternalSchema querySchema = schema(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "new_name", Types.StringType.get()));
    InternalSchema fileSchema = schema(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "old_name", Types.StringType.get()));
    InternalSchemaManager manager = manager(querySchema);

    assertArrayEquals(
        new String[] {"id", "old_name", "extra"},
        manager.getMergeFieldNames(
            fileSchema, new String[] {"id", "new_name", "extra"}));
    assertArrayEquals(
        new String[] {"id", "new_name"},
        manager.getMergeFieldNames(
            querySchema, new String[] {"id", "new_name"}));
  }

  @Test
  void testGetMergeSchemaLoadsSchemaForFileVersion() {
    InternalSchema querySchema =
        schema(Types.Field.get(0, false, "id", Types.IntType.get()));
    InternalSchemaManager manager = manager(querySchema);
    HoodieStorage storage = mock(HoodieStorage.class);

    try (MockedStatic<HoodieStorageUtils> storageUtils = mockStatic(HoodieStorageUtils.class);
         MockedStatic<InternalSchemaCache> schemaCache = mockStatic(InternalSchemaCache.class)) {
      storageUtils.when(
          () -> HoodieStorageUtils.getStorage((String) null, null)).thenReturn(storage);
      schemaCache.when(
          () -> InternalSchemaCache.getInternalSchemaByVersionId(
              1L, null, storage, null, null, null)).thenReturn(querySchema);

      assertTrue(
          manager.getMergeSchema("file-id_1-0-1_001.parquet").isEmptySchema());
    }
  }

  @Test
  void testSchemaArgumentsMustBeNonEmpty() {
    InternalSchemaManager emptyManager =
        manager(InternalSchema.getEmptyInternalSchema());
    DataType[] dataTypes = {DataTypes.INT()};

    assertThrows(
        IllegalArgumentException.class,
        () -> emptyManager.getCastMap(
            schema(Types.Field.get(0, false, "id", Types.IntType.get())),
            new String[] {"id"},
            dataTypes,
            new int[] {0}));
    assertThrows(
        IllegalArgumentException.class,
        () -> emptyManager.getMergeFieldNames(
            schema(Types.Field.get(0, false, "id", Types.IntType.get())),
            new String[] {"id"}));

    InternalSchemaManager nonEmptyManager =
        manager(schema(Types.Field.get(0, false, "id", Types.IntType.get())));
    assertThrows(
        IllegalArgumentException.class,
        () -> nonEmptyManager.getCastMap(
            InternalSchema.getEmptyInternalSchema(),
            new String[] {"id"},
            dataTypes,
            new int[] {0}));
  }

  private static InternalSchemaManager manager(InternalSchema querySchema) {
    return new InternalSchemaManager(null, querySchema, null, null, null, null);
  }

  private static InternalSchema schema(Types.Field... fields) {
    return new InternalSchema(Types.RecordType.get(fields));
  }
}
