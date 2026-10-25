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

package org.apache.hudi.util;

import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link HoodiePipeline}. */
class TestHoodiePipeline {

  @TempDir
  File tempDir;

  @Test
  void testBuildDescriptorFromColumns() {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempDir.toURI().toString());
    options.put(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ);

    HoodiePipeline.TableDescriptor descriptor = HoodiePipeline.builder("orders")
        .column("id BIGINT NOT NULL")
        .column("name STRING")
        .column("dt STRING")
        .pk("id")
        .partition("dt")
        .options(options)
        .option(FlinkOptions.WRITE_TASKS, 3)
        .option("custom.option", "custom-value")
        .getTableDescriptor();

    ResolvedCatalogTable table = descriptor.getResolvedCatalogTable();
    assertEquals("orders", descriptor.getTableId().getObjectName());
    assertEquals(Arrays.asList("id", "name", "dt"), table.getResolvedSchema().getColumnNames());
    assertEquals(Collections.singletonList("id"),
        table.getResolvedSchema().getPrimaryKey().get().getColumns());
    assertEquals(Collections.singletonList("dt"), table.getPartitionKeys());
    assertEquals("3", table.getOptions().get(FlinkOptions.WRITE_TASKS.key()));
    assertEquals("custom-value", table.getOptions().get("custom.option"));
  }

  @Test
  void testBuildDescriptorFromSchema() {
    Schema schema = Schema.newBuilder()
        .column("id", DataTypes.INT().notNull())
        .column("name", DataTypes.STRING())
        .primaryKey("id")
        .build();

    HoodiePipeline.TableDescriptor descriptor = HoodiePipeline.builder("schema_table")
        .schema(schema)
        .option(FlinkOptions.PATH, tempDir.toURI().toString())
        .getTableDescriptor();

    ResolvedCatalogTable table = descriptor.getResolvedCatalogTable();
    assertEquals(Arrays.asList("id", "name"), table.getResolvedSchema().getColumnNames());
    assertTrue(table.getResolvedSchema().getPrimaryKey().isPresent());
    assertEquals(Collections.singletonList("id"),
        table.getResolvedSchema().getPrimaryKey().get().getColumns());
    assertTrue(table.getPartitionKeys().isEmpty());

    HoodiePipeline.TableDescriptor noPrimaryKey = HoodiePipeline.builder("no_pk_table")
        .column("payload STRING")
        .option(FlinkOptions.PATH, tempDir.toURI().toString())
        .getTableDescriptor();
    assertTrue(noPrimaryKey.getResolvedCatalogTable().getResolvedSchema().getPrimaryKey().isEmpty());
  }
}
