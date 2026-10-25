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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests {@link OneToTwoUpgradeHandler}. The handler is exercised directly rather than through
 * {@link UpgradeDowngrade}, which rejects any table below version six.
 */
class TestOneToTwoUpgradeHandler extends HoodieClientTestBase {

  @Test
  void testUpgradeRecordsKeySchemaAndOrderingField() {
    Map<ConfigProperty, String> tableProps = upgrade(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, "timestamp");
    assertEquals("uuid", tableProps.get(HoodieTableConfig.RECORDKEY_FIELDS));
    assertEquals("partition_path", tableProps.get(HoodieTableConfig.PARTITION_FIELDS));
    assertEquals(HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().name(), tableProps.get(HoodieTableConfig.BASE_FILE_FORMAT));
    assertEquals("timestamp", tableProps.get(HoodieTableConfig.PRECOMBINE_FIELD));
  }

  /**
   * A nested ordering field is recorded as configured. The schema check exists to catch a
   * materialized default, and a default is never nested, so an explicitly configured nested field
   * is taken at face value.
   */
  @ParameterizedTest
  @ValueSource(strings = {"fare.amount", "not_a_record.not_a_column"})
  void testRecordsNestedOrderingFieldAsConfigured(String orderingField) {
    assertEquals(orderingField,
        upgrade(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, orderingField).get(HoodieTableConfig.PRECOMBINE_FIELD));
  }

  /** A table that already records ordering fields keeps them, even if the writer configures another. */
  @Test
  void testLeavesRecordedOrderingFieldsAlone() {
    Properties recordedProps = new Properties();
    recordedProps.setProperty(HoodieTableConfig.PRECOMBINE_FIELD.key(), "timestamp");
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), recordedProps);

    assertNull(upgrade(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, "_row_key").get(HoodieTableConfig.PRECOMBINE_FIELD));
  }

  /** Every field of a multi field ordering config has to resolve for any of it to be recorded. */
  @Test
  void testRecordsMultipleOrderingFields() {
    assertEquals("timestamp,_row_key",
        upgrade(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, "timestamp,_row_key").get(HoodieTableConfig.PRECOMBINE_FIELD));
    assertNull(upgrade(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, "timestamp,not_a_column").get(HoodieTableConfig.PRECOMBINE_FIELD));
  }

  /**
   * An ordering field the schema cannot resolve is left unrecorded, so the table config never ends
   * up with one no reader can resolve.
   */
  @ParameterizedTest
  @ValueSource(strings = {"not_a_column", "ts"})
  void testSkipsOrderingFieldTheSchemaCannotResolve(String orderingField) {
    Map<ConfigProperty, String> tableProps = upgrade(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, orderingField);
    assertEquals("uuid", tableProps.get(HoodieTableConfig.RECORDKEY_FIELDS));
    assertNull(tableProps.get(HoodieTableConfig.PRECOMBINE_FIELD));
  }

  @Test
  void testSkipsEmptyOrderingField() {
    assertNull(upgrade(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, "").get(HoodieTableConfig.PRECOMBINE_FIELD));
  }

  /** With no committed data and no writer schema there is nothing to resolve the field against. */
  @Test
  void testSkipsOrderingFieldWhenNoSchemaIsAvailable() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath).forTable("test-trip-table").withProps(keySchemaParams("timestamp")).build();
    Map<ConfigProperty, String> tableProps = new OneToTwoUpgradeHandler()
        .upgrade(config, context, null, SparkUpgradeDowngradeHelper.getInstance()).propertiesToUpdate();
    assertNull(tableProps.get(HoodieTableConfig.PRECOMBINE_FIELD));
  }

  private Map<ConfigProperty, String> upgrade(String schema, String orderingField) {
    HoodieWriteConfig config = getConfigBuilder(schema).withProps(keySchemaParams(orderingField)).build();
    return new OneToTwoUpgradeHandler()
        .upgrade(config, context, null, SparkUpgradeDowngradeHelper.getInstance())
        .propertiesToUpdate();
  }

  private Map<String, String> keySchemaParams(String orderingField) {
    Map<String, String> params = new HashMap<>();
    params.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "uuid");
    params.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    params.put(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().name());
    params.put(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), orderingField);
    return params;
  }
}
