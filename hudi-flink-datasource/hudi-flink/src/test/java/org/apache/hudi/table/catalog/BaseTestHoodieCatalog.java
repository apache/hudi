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

package org.apache.hudi.table.catalog;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieCatalogException;
import org.apache.hudi.utils.CatalogUtils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for Flink Hoodie Catalog tests.
 */
public abstract class BaseTestHoodieCatalog {

  protected static final String TEST_DEFAULT_DATABASE = "default";

  protected static final List<Column> CREATE_COLUMNS_WITH_METADATA = Arrays.asList(
      Column.metadata(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.STRING(), null, true),
      Column.metadata(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, DataTypes.STRING(), null, true),
      Column.metadata(HoodieRecord.RECORD_KEY_METADATA_FIELD, DataTypes.STRING(), null, true),
      Column.metadata(HoodieRecord.PARTITION_PATH_METADATA_FIELD, DataTypes.STRING(), null, true),
      Column.metadata(HoodieRecord.FILENAME_METADATA_FIELD, DataTypes.STRING(), null, true),
      Column.metadata(HoodieRecord.OPERATION_METADATA_FIELD, DataTypes.STRING(), null, true),
      Column.physical("uuid", DataTypes.VARCHAR(20)),
      Column.physical("name", DataTypes.VARCHAR(20)),
      Column.physical("age", DataTypes.INT()),
      Column.physical("tss", DataTypes.TIMESTAMP(3)),
      Column.physical("partition", DataTypes.VARCHAR(10))
  );

  protected static final Map<String, String> EXPECTED_OPTIONS = new HashMap<>();

  static {
    EXPECTED_OPTIONS.put(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    EXPECTED_OPTIONS.put(FlinkOptions.INDEX_GLOBAL_ENABLED.key(), "false");
    EXPECTED_OPTIONS.put(FlinkOptions.PRE_COMBINE.key(), "true");
    EXPECTED_OPTIONS.put(FactoryUtil.CONNECTOR.key(), "hudi");
  }

  protected static final UniqueConstraint CONSTRAINTS = UniqueConstraint.primaryKey("uuid", Arrays.asList("uuid"));

  protected static final ResolvedSchema CREATE_TABLE_SCHEMA_WITH_METADATA =
      new ResolvedSchema(
          CREATE_COLUMNS_WITH_METADATA,
          Collections.emptyList(),
          CONSTRAINTS);

  protected static final ResolvedCatalogTable EXPECTED_CATALOG_TABLE_WITH_METADATA = new ResolvedCatalogTable(
      CatalogUtils.createCatalogTable(
          Schema.newBuilder().fromResolvedSchema(CREATE_TABLE_SCHEMA_WITH_METADATA).build(),
          Arrays.asList("partition"),
          EXPECTED_OPTIONS,
          "test_metadata"),
      CREATE_TABLE_SCHEMA_WITH_METADATA
  );

  abstract AbstractCatalog getCatalog();

  @Test
  void testCreateTableWithInvalidMetadata() {
    AbstractCatalog catalog = getCatalog();
    // validate create table with metadata columns
    ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1_with_invalid_metadata");
    // invalid schema1: invalid metadata column name
    final List<Column> schema = Arrays.asList(
        Column.metadata("invalid_meta_col", DataTypes.STRING(), null, true),
        Column.physical("uuid", DataTypes.VARCHAR(20)),
        Column.physical("name", DataTypes.VARCHAR(20)),
        Column.physical("age", DataTypes.INT()),
        Column.physical("tss", DataTypes.TIMESTAMP(3)),
        Column.physical("partition", DataTypes.VARCHAR(10))
    );
    // test create table
    assertThrows(HoodieCatalogException.class, () -> catalog.createTable(tablePath, createCatalogTable(schema), true),
        "Hudi catalog only supports VIRTUAL metadata column, valid metadata columns: ["
            + "_hoodie_commit_time, _hoodie_partition_path, _hoodie_operation, _hoodie_record_key, _hoodie_commit_seqno, _hoodie_file_name]");

    // invalid schema2: do not support metadata key, like `ts` BIGINT METADATA FROM '_hoodie_commit_time' VIRTUAL
    final List<Column> schema2 = Arrays.asList(
        Column.metadata(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.STRING(), "ts", true),
        Column.physical("uuid", DataTypes.VARCHAR(20)),
        Column.physical("name", DataTypes.VARCHAR(20)),
        Column.physical("age", DataTypes.INT()),
        Column.physical("tss", DataTypes.TIMESTAMP(3)),
        Column.physical("partition", DataTypes.VARCHAR(10))
    );
    // test create table
    assertThrows(HoodieCatalogException.class, () -> catalog.createTable(tablePath, createCatalogTable(schema2), true),
        "Hudi catalog doesn't support metadata key, usage: `_hoodie_commit_time STRING METADATA VIRTUAL`.");

    // invalid schema3: do not support non-virtual metadata column, like `ts` BIGINT METADATA FROM '_hoodie_commit_time'
    final List<Column> schema3 = Arrays.asList(
        Column.metadata(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.STRING(), null, false),
        Column.physical("uuid", DataTypes.VARCHAR(20)),
        Column.physical("name", DataTypes.VARCHAR(20)),
        Column.physical("age", DataTypes.INT()),
        Column.physical("tss", DataTypes.TIMESTAMP(3)),
        Column.physical("partition", DataTypes.VARCHAR(10))
    );
    // test create table
    assertThrows(HoodieCatalogException.class, () -> catalog.createTable(tablePath, createCatalogTable(schema3), true),
        "Hudi catalog only supports VIRTUAL metadata column, usage: `_hoodie_commit_time STRING METADATA VIRTUAL`.");
  }

  @Test
  void testCreateTableWithMetaCols() throws Exception {
    AbstractCatalog catalog = getCatalog();
    // validate create table with metadata columns
    ObjectPath tablePath1 = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1_with_metadata");
    // test create table
    catalog.createTable(tablePath1, EXPECTED_CATALOG_TABLE_WITH_METADATA, true);

    // test table exist
    assertTrue(catalog.tableExists(tablePath1));
    CatalogBaseTable actual = catalog.getTable(tablePath1);

    Stream<Schema.UnresolvedColumn> actualVirtualCols =
        actual.getUnresolvedSchema().getColumns().stream().filter(c -> c instanceof Schema.UnresolvedMetadataColumn);
    assertTrue(actualVirtualCols.allMatch(c ->
        HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(c.getName()) && ((Schema.UnresolvedMetadataColumn) c).isVirtual()));
  }

  protected ResolvedCatalogTable createCatalogTable(List<Column> schema) {
    ResolvedSchema resolvedSchema =
        new ResolvedSchema(
            schema,
            Collections.emptyList(),
            CONSTRAINTS);
    return new ResolvedCatalogTable(
        CatalogUtils.createCatalogTable(
            Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
            Arrays.asList("partition"),
            EXPECTED_OPTIONS,
            "test_metadata"),
        resolvedSchema
    );
  }
}
