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

package org.apache.hudi.client.common;

import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.client.utils.SparkInternalSchemaConverter;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.schema.internal.Types;
import org.apache.hudi.common.schema.internal.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.common.schema.internal.utils.SerDeHelper;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.InternalSchemaHistory;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.inline.InLineFileSystem;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader;
import org.apache.spark.sql.hudi.SparkAdapter;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestSparkReaderContextFactory extends HoodieClientTestBase {
  @Test
  void testGetSchemaEvolutionConfigurations() throws Exception {
    InternalSchema internalSchema = new InternalSchema(Types.RecordType.get(Collections.singletonList(
        Types.Field.get(0, "col1", Types.BooleanType.get()))));
    internalSchema.setSchemaId(100L);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata(SerDeHelper.LATEST_SCHEMA, SerDeHelper.toJson(internalSchema));
    FileBasedInternalSchemaStorageManager schemaManager = new FileBasedInternalSchemaStorageManager(metaClient);
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    String history = SerDeHelper.inheritSchemas(internalSchema, "");
    schemaManager.persistHistorySchemaStr("100", history);
    testTable.addCommit("100", Option.of(commitMetadata));
    schemaManager.persistHistorySchemaStr("200", history);
    testTable.addCommit("200", Option.of(commitMetadata));
    metaClient.reloadActiveTimeline();
    TableSchemaResolver schemaResolver = mock(TableSchemaResolver.class);
    when(schemaResolver.getTableInternalSchemaFromCommitMetadata()).thenReturn(Option.of(internalSchema));
    SparkAdapter sparkAdapter = mock(SparkAdapter.class);
    scala.collection.immutable.Map<String, String> options =
        scala.collection.immutable.Map$.MODULE$.<String, String>empty()
            .$plus(new Tuple2<>(FileFormat.OPTION_RETURNING_BATCH(), Boolean.toString(true)));
    ArgumentCaptor<Configuration> configurationArgumentCaptor = ArgumentCaptor.forClass(Configuration.class);
    SparkColumnarFileReader sparkParquetReader = mock(SparkColumnarFileReader.class);
    when(sparkAdapter.createParquetFileReader(eq(false), eq(context.getSqlContext().sparkSession().sessionState().conf()), eq(options), configurationArgumentCaptor.capture()))
        .thenReturn(sparkParquetReader);

    SparkReaderContextFactory sparkHoodieReaderContextFactory = new SparkReaderContextFactory(context, metaClient, schemaResolver, sparkAdapter);
    HoodieReaderContext<InternalRow> readerContext = sparkHoodieReaderContextFactory.getContext();

    Configuration createdConfig = readerContext.getStorageConfiguration().unwrapAs(Configuration.class);
    assertEquals(createdConfig, configurationArgumentCaptor.getValue());

    assertFalse(createdConfig.getBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED().key(), true));
    assertFalse(createdConfig.getBoolean(SQLConf.CASE_SENSITIVE().key(), true));
    assertFalse(createdConfig.getBoolean(SQLConf.PARQUET_BINARY_AS_STRING().key(), true));
    assertTrue(createdConfig.getBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), false));
    assertFalse(createdConfig.getBoolean("spark.sql.legacy.parquet.nanosAsLong", true));
    if (HoodieSparkUtils.gteqSpark3_4()) {
      assertFalse(createdConfig.getBoolean("spark.sql.parquet.inferTimestampNTZ.enabled", true));
    }

    String inlineClassName = createdConfig.get("fs." + InLineFileSystem.SCHEME + ".impl");
    assertEquals(InLineFileSystem.class.getName(), inlineClassName);

    // Internal write-side reads must pin CONTENT; a DESCRIPTOR leak here drops blob bytes (#19232).
    assertEquals(
        HoodieReaderConfig.BLOB_INLINE_READ_MODE_CONTENT,
        createdConfig.get(HoodieReaderConfig.BLOB_INLINE_READ_MODE.key()));

    assertEquals(
        metaClient.getBasePath().toString(),
        createdConfig.get(SparkInternalSchemaConverter.HOODIE_TABLE_PATH));
    assertTrue(InternalSchemaHistory.isPresentIn(createdConfig::get));
    assertEquals(internalSchema, InternalSchemaHistory.resolve(createdConfig::get, 200L));
    assertNull(createdConfig.get(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST));
  }
}
