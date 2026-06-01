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

package org.apache.hudi.aws.sync;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.GlueCatalogSyncClientConfig;
import org.apache.hudi.config.HoodieAWSConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.sts.StsClient;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Base class for AWS Glue integration tests using moto for AWS service mocking.
 * Provides common setup for Glue client configuration, database management,
 * and reusable test utilities.
 */
public abstract class AWSGlueIntegrationTestBase {

  protected static final String AWS_ACCESS_KEY = "dummy";
  protected static final String AWS_SECRET_KEY = "dummy";
  protected static final String AWS_SESSION_TOKEN = "dummy";
  protected static final String AWS_REGION = "eu-west-1";
  protected static final String CATALOG_ID = "123456789012";
  protected static final String GLUE_ENDPOINT = "http://localhost:5002";
  protected static final String STS_ENDPOINT = "http://localhost:5002";
  protected static final String DB_NAME = "db_name";

  protected TypedProperties hiveSyncProps;
  protected HiveSyncConfig hiveSyncConfig;
  protected AWSGlueCatalogSyncClient glueSync;
  protected GlueClient glueClient;
  protected StsClient stsClient;
  protected String tablePath;
  protected HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws Exception {
    // Create temporary table path
    Path tempDir = Files.createTempDirectory("hivesynctest" + System.nanoTime());
    tablePath = tempDir.toUri().toString() + "/tbl_name";

    // Create a Hudi table with schema first (needed for getTableDoc())
    HudiTestTableUtils.createDefaultHudiTable(tablePath);

    // Initialize Hive sync properties with AWS Glue configuration
    hiveSyncProps = createHiveSyncProperties();
    hiveSyncConfig = new HiveSyncConfig(hiveSyncProps, new Configuration());

    // Create meta client using the exact original working pattern but for existing table
    StorageConfiguration<?> configuration = HadoopFSUtils.getStorageConf(new Configuration());
    metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName("test_table")
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(configuration, tablePath);

    // Create AWS clients
    glueClient = createGlueClient();
    stsClient = createStsClient();

    // Create AWS Glue sync client using exact original pattern
    glueSync = new AWSGlueCatalogSyncClient(hiveSyncConfig, metaClient);

    // Create test database
    createTestDatabase();
  }

  @AfterEach
  public void tearDown() throws Exception {
    try {
      // Clean up test database
      deleteTestDatabase();
    } finally {
      // Close clients
      if (glueClient != null) {
        glueClient.close();
      }
      if (stsClient != null) {
        stsClient.close();
      }
    }
  }

  /**
   * Creates TypedProperties with AWS Glue configuration for moto integration testing.
   */
  protected TypedProperties createHiveSyncProperties() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieSyncConfig.META_SYNC_BASE_PATH.key(), tablePath);
    props.setProperty(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key(), DB_NAME);
    props.setProperty(HoodieAWSConfig.AWS_GLUE_ENDPOINT.key(), GLUE_ENDPOINT);
    props.setProperty(HoodieAWSConfig.AWS_GLUE_REGION.key(), AWS_REGION);
    props.setProperty(HoodieAWSConfig.AWS_ACCESS_KEY.key(), AWS_ACCESS_KEY);
    props.setProperty(HoodieAWSConfig.AWS_SECRET_KEY.key(), AWS_SECRET_KEY);
    props.setProperty(HoodieAWSConfig.AWS_SESSION_TOKEN.key(), AWS_SESSION_TOKEN);
    props.setProperty(HoodieAWSConfig.AWS_STS_ENDPOINT.key(), STS_ENDPOINT);
    props.setProperty(HoodieAWSConfig.AWS_STS_REGION.key(), AWS_REGION);
    props.setProperty(GlueCatalogSyncClientConfig.GLUE_CATALOG_ID.key(), CATALOG_ID);
    return props;
  }

  /**
   * Creates a Glue client configured for moto integration testing.
   */
  protected GlueClient createGlueClient() {
    return GlueClient.builder()
        .endpointOverride(URI.create(GLUE_ENDPOINT))
        .region(Region.of(AWS_REGION))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(AWS_ACCESS_KEY, AWS_SECRET_KEY)))
        .build();
  }

  /**
   * Creates an STS client configured for moto integration testing.
   */
  protected StsClient createStsClient() {
    return StsClient.builder()
        .endpointOverride(URI.create(STS_ENDPOINT))
        .region(Region.of(AWS_REGION))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(AWS_ACCESS_KEY, AWS_SECRET_KEY)))
        .build();
  }

  /**
   * Creates the test database in AWS Glue.
   */
  protected void createTestDatabase() {
    DatabaseInput databaseInput = DatabaseInput.builder()
        .name(DB_NAME)
        .description("Test database for AWS Glue integration tests")
        .build();

    CreateDatabaseRequest request = CreateDatabaseRequest.builder()
        .catalogId(CATALOG_ID)
        .databaseInput(databaseInput)
        .build();

    glueClient.createDatabase(request);
  }

  /**
   * Deletes the test database from AWS Glue.
   */
  protected void deleteTestDatabase() {
    try {
      DeleteDatabaseRequest request = DeleteDatabaseRequest.builder()
          .catalogId(CATALOG_ID)
          .name(DB_NAME)
          .build();
      glueClient.deleteDatabase(request);
    } catch (Exception e) {
      // Ignore errors during cleanup
    }
  }

  /**
   * Gets the table path for the current test.
   */
  protected String getTablePath() {
    return tablePath;
  }

  /**
   * Gets the database name used in tests.
   */
  protected String getDatabaseName() {
    return DB_NAME;
  }
}