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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.read.ParquetRowIndexBasedSchemaHandler.getPositionalMergeField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestParquetRowIndexBasedSchemaHandler extends SchemaHandlerTestBase {

  @Test
  public void testCowBootstrapWithPositionMerge() {
    HoodieTableConfig hoodieTableConfig = mock(HoodieTableConfig.class);
    when(hoodieTableConfig.populateMetaFields()).thenReturn(true);
    HoodieReaderContext<String> readerContext = createReaderContext(hoodieTableConfig, true, false, true, false, null);
    Schema requestedSchema = generateProjectionSchema("begin_lat", "tip_history", "_hoodie_record_key", "rider");
    FileGroupReaderSchemaHandler schemaHandler = createSchemaHandler(readerContext, DATA_SCHEMA, requestedSchema, hoodieTableConfig, true);
    assertTrue(readerContext.getNeedsBootstrapMerge());
    //meta cols must go first in the required schema
    Schema expectedRequiredSchema = generateProjectionSchema("_hoodie_record_key", "begin_lat", "tip_history", "rider");
    assertEquals(expectedRequiredSchema, schemaHandler.getRequiredSchema());
    Pair<List<Schema.Field>, List<Schema.Field>> bootstrapFields = schemaHandler.getBootstrapRequiredFields();
    assertEquals(Arrays.asList(getField("_hoodie_record_key"), getPositionalMergeField()), bootstrapFields.getLeft());
    assertEquals(Arrays.asList(getField("begin_lat"), getField("tip_history"), getField("rider"), getPositionalMergeField()), bootstrapFields.getRight());

    schemaHandler = createSchemaHandler(readerContext, DATA_SCHEMA, DATA_COLS_ONLY_SCHEMA, hoodieTableConfig, true);
    assertFalse(readerContext.getNeedsBootstrapMerge());
    assertEquals(DATA_COLS_ONLY_SCHEMA, schemaHandler.getRequiredSchema());
    bootstrapFields = schemaHandler.getBootstrapRequiredFields();
    assertTrue(bootstrapFields.getLeft().isEmpty());
    assertEquals(Arrays.asList(getField("begin_lat"), getField("tip_history"), getField("rider")), bootstrapFields.getRight());

    schemaHandler = createSchemaHandler(readerContext, DATA_SCHEMA, META_COLS_ONLY_SCHEMA, hoodieTableConfig, true);
    assertFalse(readerContext.getNeedsBootstrapMerge());
    assertEquals(META_COLS_ONLY_SCHEMA, schemaHandler.getRequiredSchema());
    bootstrapFields = schemaHandler.getBootstrapRequiredFields();
    assertEquals(Arrays.asList(getField("_hoodie_commit_seqno"), getField("_hoodie_record_key")), bootstrapFields.getLeft());
    assertTrue(bootstrapFields.getRight().isEmpty());
  }

  private static Stream<Arguments> testMorParams() {
    return testMorParams(true);
  }

  @ParameterizedTest
  @MethodSource("testMorParams")
  public void testMor(RecordMergeMode mergeMode,
                      boolean hasPrecombine,
                      boolean isProjectionCompatible,
                      boolean mergeUseRecordPosition,
                      boolean supportsParquetRowIndex,
                      boolean hasBuiltInDelete) throws IOException {
    super.testMor(mergeMode, hasPrecombine, isProjectionCompatible, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete);
  }

  @ParameterizedTest
  @MethodSource("testMorParams")
  public void testMorBootstrap(RecordMergeMode mergeMode,
                               boolean hasPrecombine,
                               boolean isProjectionCompatible,
                               boolean mergeUseRecordPosition,
                               boolean supportsParquetRowIndex,
                               boolean hasBuiltInDelete) throws IOException {
    super.testMorBootstrap(mergeMode, hasPrecombine, isProjectionCompatible, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete);
  }

  @Override
  FileGroupReaderSchemaHandler createSchemaHandler(HoodieReaderContext<String> readerContext, Schema dataSchema, Schema requestedSchema, HoodieTableConfig hoodieTableConfig,
                                                   boolean supportsParquetRowIndex) {
    return new ParquetRowIndexBasedSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), hoodieTableConfig, new TypedProperties());
  }
}
