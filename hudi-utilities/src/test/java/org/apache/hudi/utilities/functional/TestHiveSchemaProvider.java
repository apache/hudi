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

package org.apache.hudi.utilities.functional;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.HiveSchemaProvider;
import org.apache.hudi.utilities.testutils.SparkClientFunctionalTestHarnessWithHiveSupport;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Basic tests against {@link HiveSchemaProvider}.
 */
@Tag("functional")
public class TestHiveSchemaProvider extends SparkClientFunctionalTestHarnessWithHiveSupport {
  private static final Logger LOG = LoggerFactory.getLogger(TestHiveSchemaProvider.class);
  private static final TypedProperties PROPS = new TypedProperties();
  private static final String SOURCE_SCHEMA_TABLE_NAME = "schema_registry.source_schema_tab";
  private static final String TARGET_SCHEMA_TABLE_NAME = "schema_registry.target_schema_tab";

  @BeforeAll
  public static void init() {
    Pair<String, String> dbAndTableName = paresDBAndTableName(SOURCE_SCHEMA_TABLE_NAME);
    PROPS.setProperty("hoodie.streamer.schemaprovider.source.schema.hive.database", dbAndTableName.getLeft());
    PROPS.setProperty("hoodie.streamer.schemaprovider.source.schema.hive.table", dbAndTableName.getRight());
  }

  @Disabled
  @Test
  public void testSourceSchema() throws Exception {
    try {
      createSchemaTable(SOURCE_SCHEMA_TABLE_NAME);
      HoodieSchema sourceSchema = UtilHelpers.createSchemaProvider(HiveSchemaProvider.class.getName(), PROPS, jsc()).getSourceHoodieSchema();

      HoodieSchema originalSchema = HoodieSchema.parse(
              UtilitiesTestBase.Helpers.readFile("streamer-config/hive_schema_provider_source.avsc")
      );
      for (HoodieSchemaField field : sourceSchema.getFields()) {
        HoodieSchemaField originalField = originalSchema.getField(field.name()).get();
        assertNotNull(originalField);
      }
    } catch (HoodieException e) {
      LOG.error("Failed to get source schema. ", e);
      throw e;
    }
  }

  @Disabled
  @Test
  public void testTargetSchema() throws Exception {
    try {
      Pair<String, String> dbAndTableName = paresDBAndTableName(TARGET_SCHEMA_TABLE_NAME);
      PROPS.setProperty("hoodie.streamer.schemaprovider.target.schema.hive.database", dbAndTableName.getLeft());
      PROPS.setProperty("hoodie.streamer.schemaprovider.target.schema.hive.table", dbAndTableName.getRight());
      createSchemaTable(SOURCE_SCHEMA_TABLE_NAME);
      createSchemaTable(TARGET_SCHEMA_TABLE_NAME);
      HoodieSchema targetSchema = UtilHelpers.createSchemaProvider(HiveSchemaProvider.class.getName(), PROPS, jsc()).getTargetHoodieSchema();
      HoodieSchema originalSchema = HoodieSchema.parse(
          UtilitiesTestBase.Helpers.readFile("streamer-config/hive_schema_provider_target.avsc"));
      for (HoodieSchemaField field : targetSchema.getFields()) {
        HoodieSchemaField originalField = originalSchema.getField(field.name()).get();
        assertNotNull(originalField);
      }
    } catch (HoodieException e) {
      LOG.error("Failed to get source/target schema. ", e);
      throw e;
    }
  }

  @Disabled
  @Test
  public void testNotExistTable() {
    String wrongName = "wrong_schema_tab";
    PROPS.setProperty("hoodie.streamer.schemaprovider.source.schema.hive.table", wrongName);
    Assertions.assertThrows(NoSuchTableException.class, () -> {
      try {
        UtilHelpers.createSchemaProvider(HiveSchemaProvider.class.getName(), PROPS, jsc()).getSourceHoodieSchema();
      } catch (Throwable exception) {
        while (exception.getCause() != null) {
          exception = exception.getCause();
        }
        throw exception;
      }
    });
  }

  private static Pair<String, String> paresDBAndTableName(String fullName) {
    String[] dbAndTableName = fullName.split("\\.");
    if (dbAndTableName.length > 1) {
      return new ImmutablePair<>(dbAndTableName[0], dbAndTableName[1]);
    } else {
      return new ImmutablePair<>("default", dbAndTableName[0]);
    }
  }

  private void createSchemaTable(String fullName) throws IOException {
    SparkSession spark = spark();
    String createTableSQL = UtilitiesTestBase.Helpers.readFile(String.format("streamer-config/%s.sql", fullName));
    Pair<String, String> dbAndTableName = paresDBAndTableName(fullName);
    spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s", dbAndTableName.getLeft()));
    spark.sql(createTableSQL);
  }
}
