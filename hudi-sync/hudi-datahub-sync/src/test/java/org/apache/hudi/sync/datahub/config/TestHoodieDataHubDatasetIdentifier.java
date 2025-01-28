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

package org.apache.hudi.sync.datahub.config;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_DATAPLATFORM_NAME;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_DATASET_ENV;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieDataHubDatasetIdentifier {

  private Properties props;

  @BeforeEach
  void setUp() {
    props = new Properties();
  }

  @Test
  @DisplayName("Test constructor with default values")
  void testConstructorWithDefaultValues() {
    // Given
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "test_db");
    props.setProperty(META_SYNC_TABLE_NAME.key(), "test_table");

    // When
    HoodieDataHubDatasetIdentifier identifier = new HoodieDataHubDatasetIdentifier(props);

    // Then
    DatasetUrn datasetUrn = identifier.getDatasetUrn();
    assertNotNull(datasetUrn);
    assertEquals(HoodieDataHubDatasetIdentifier.DEFAULT_HOODIE_DATAHUB_PLATFORM_NAME,
        datasetUrn.getPlatformEntity().getId());
    assertEquals("test_db.test_table", datasetUrn.getDatasetNameEntity());
    assertEquals(HoodieDataHubDatasetIdentifier.DEFAULT_DATAHUB_ENV, datasetUrn.getOriginEntity());
  }

  @Test
  @DisplayName("Test constructor with custom values")
  void testConstructorWithCustomValues() {
    // Given
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "custom_db");
    props.setProperty(META_SYNC_TABLE_NAME.key(), "custom_table");
    props.setProperty(META_SYNC_DATAHUB_DATAPLATFORM_NAME.key(), "custom_platform");
    props.setProperty(META_SYNC_DATAHUB_DATASET_ENV.key(), "PROD");

    // When
    HoodieDataHubDatasetIdentifier identifier = new HoodieDataHubDatasetIdentifier(props);

    // Then
    DatasetUrn datasetUrn = identifier.getDatasetUrn();
    assertNotNull(datasetUrn);
    assertEquals("custom_platform", datasetUrn.getPlatformEntity().getId());
    assertEquals("custom_db.custom_table", datasetUrn.getDatasetNameEntity());
    assertEquals(FabricType.PROD, datasetUrn.getOriginEntity());
  }

  @Test
  @DisplayName("Test getDatabaseUrn")
  void testGetDatabaseUrn() {
    // Given
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "test_db");
    props.setProperty(META_SYNC_TABLE_NAME.key(), "test_table");
    props.setProperty(META_SYNC_DATAHUB_DATASET_ENV.key(), "PROD");

    // When
    HoodieDataHubDatasetIdentifier identifier = new HoodieDataHubDatasetIdentifier(props);

    // Then
    Urn databaseUrn = identifier.getDatabaseUrn();
    assertNotNull(databaseUrn);
    assertFalse(databaseUrn.toString().contains("test_db"));
    assertFalse(databaseUrn.toString().contains("PROD"));
    assertTrue(databaseUrn.toString().startsWith("urn:li:container:"));
  }

  @Test
  @DisplayName("Test getTableName")
  void testGetTableName() {
    // Given
    String tableName = "test_table";
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "test_db");
    props.setProperty(META_SYNC_TABLE_NAME.key(), tableName);

    // When
    HoodieDataHubDatasetIdentifier identifier = new HoodieDataHubDatasetIdentifier(props);

    // Then
    assertEquals(tableName, identifier.getTableName());
  }

  @Test
  @DisplayName("Test constructor with missing required properties")
  void testConstructorWithMissingProperties() {
    // Given empty properties

    // Then
    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieDataHubDatasetIdentifier(props);
    });
  }

  @Test
  @DisplayName("Test constructor with invalid environment")
  void testConstructorWithInvalidEnvironment() {
    // Given
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "test_db");
    props.setProperty(META_SYNC_TABLE_NAME.key(), "test_table");
    props.setProperty(META_SYNC_DATAHUB_DATASET_ENV.key(), "INVALID_ENV");

    // Then
    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieDataHubDatasetIdentifier(props);
    });
  }
}