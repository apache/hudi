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

package org.apache.hudi.table;

import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.client.utils.SparkInternalSchemaConverter;
import org.apache.hudi.common.model.ActionType;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestSparkBroadcastManager {
  @Test
  void testGetStorageConfiguration() {
    Configuration config = new Configuration(false);
    Configuration createdConfig = SparkBroadcastManager.getHadoopConfiguration(config);
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
  }

  @Test
  void testExtraConfigsAdded() {
    Map<String, String> extraConfigs = new HashMap<>();
    extraConfigs.put("K1", "V1");
    Configuration configs = new Configuration(false);
    SparkBroadcastManager.addSchemaEvolutionConfigs(configs, extraConfigs);
    assertEquals("V1", configs.get("K1"));
  }

  @Test
  void testGetSchemaEvolutionConfigurations() {
    TableSchemaResolver schemaResolver = mock(TableSchemaResolver.class);
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    InstantFileNameGenerator fileNameGenerator = new InstantFileNameGeneratorV2();
    String basePath = "any_table_path";

    // Test when InternalSchema is empty.
    when(schemaResolver.getTableInternalSchemaFromCommitMetadata()).thenReturn(Option.empty());
    Map<String, String> schemaEvolutionConfigs = SparkBroadcastManager.getSchemaEvolutionConfigs(
        schemaResolver, timeline, fileNameGenerator, basePath);
    assertTrue(schemaEvolutionConfigs.isEmpty());

    // Test when InternalSchema is not empty.
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
    schemaEvolutionConfigs = SparkBroadcastManager.getSchemaEvolutionConfigs(
        schemaResolver, timeline, fileNameGenerator, basePath);
    assertFalse(schemaEvolutionConfigs.isEmpty());
    assertEquals(
        "0001_0005.deltacommit,0002_0006.deltacommit,0003_0007.commit",
        schemaEvolutionConfigs.get(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST));
    assertEquals(
        "any_table_path",
        schemaEvolutionConfigs.get(SparkInternalSchemaConverter.HOODIE_TABLE_PATH));
  }
}
