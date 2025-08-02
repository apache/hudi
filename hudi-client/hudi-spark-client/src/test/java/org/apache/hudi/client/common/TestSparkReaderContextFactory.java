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
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantFileNameGeneratorV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantGeneratorV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.inline.InLineFileSystem;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader;
import org.apache.spark.sql.hudi.SparkAdapter;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestSparkReaderContextFactory extends HoodieClientTestBase {
  @Test
  void testGetSchemaEvolutionConfigurations() {
    TableSchemaResolver schemaResolver = mock(TableSchemaResolver.class);
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    InstantFileNameGenerator fileNameGenerator = new InstantFileNameGeneratorV2();
    String basePath = "any_table_path";
    metaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    when(metaClient.getBasePath()).thenReturn(new StoragePath(basePath));
    when(metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants()).thenReturn(timeline);
    when(metaClient.getTimelineLayout().getInstantFileNameGenerator()).thenReturn(fileNameGenerator);

    InstantGeneratorV2 instantGen = new InstantGeneratorV2();
    Types.RecordType record = Types.RecordType.get(Collections.singletonList(
        Types.Field.get(0, "col1", Types.BooleanType.get())));
    List<HoodieInstant> instants = Arrays.asList(
        instantGen.createNewInstant(
            HoodieInstant.State.COMPLETED, ActionType.deltacommit.name(), "0001", "0005"),
        instantGen.createNewInstant(
            HoodieInstant.State.COMPLETED, ActionType.deltacommit.name(), "0002", "0006"),
        instantGen.createNewInstant(
            HoodieInstant.State.COMPLETED, ActionType.compaction.name(), "0003", "0007"));
    InternalSchema internalSchema = new InternalSchema(record);
    when(schemaResolver.getTableInternalSchemaFromCommitMetadata()).thenReturn(Option.of(internalSchema));
    when(timeline.getInstants()).thenReturn(instants);
    SparkAdapter sparkAdapter = mock(SparkAdapter.class);
    scala.collection.immutable.Map<String, String> options =
        scala.collection.immutable.Map$.MODULE$.<String, String>empty()
            .$plus(new Tuple2<>(FileFormat.OPTION_RETURNING_BATCH(), Boolean.toString(true)));
    ArgumentCaptor<Configuration> configurationArgumentCaptor = ArgumentCaptor.forClass(Configuration.class);
    SparkColumnarFileReader sparkParquetReader = mock(SparkColumnarFileReader.class);
    when(sparkAdapter.createParquetFileReader(eq(false), eq(context.getSqlContext().sessionState().conf()), eq(options), configurationArgumentCaptor.capture()))
        .thenReturn(sparkParquetReader);

    SparkReaderContextFactory sparkHoodieReaderContextFactory = new SparkReaderContextFactory(context, metaClient, schemaResolver, sparkAdapter, metaClient.getTableConfig().populateMetaFields());
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

    assertEquals(
        "0001_0005.deltacommit,0002_0006.deltacommit,0003_0007.commit",
        createdConfig.get(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST));
    assertEquals(
        basePath,
        createdConfig.get(SparkInternalSchemaConverter.HOODIE_TABLE_PATH));
  }
}
