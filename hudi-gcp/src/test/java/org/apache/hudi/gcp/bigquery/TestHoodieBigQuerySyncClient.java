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

package org.apache.hudi.gcp.bigquery;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.nio.file.Path;
import java.util.Properties;

import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_BILLING_PROJECT_ID;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_PROJECT_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieBigQuerySyncClient {
  private static final String PROJECT_ID = "test_project";
  private static final String MANIFEST_FILE_URI = "file:/manifest_file";
  private static final String SOURCE_PREFIX = "file:/manifest_file/date=*";
  private static final String TEST_TABLE = "test_table";
  private static final String TEST_DATASET = "test_dataset";

  static @TempDir Path tempDir;

  private static String basePath;
  private static final String BILLING_PROJECT_ID = "test_billing_project";
  private final BigQuery mockBigQuery = mock(BigQuery.class);
  private HoodieBigQuerySyncClient client;

  @BeforeAll
  static void setupOnce() throws Exception {
    basePath = tempDir.toString();
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(TEST_TABLE)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(new Configuration(), basePath);
  }

  @BeforeEach
  void setup() {
    Properties properties = new Properties();
    properties.setProperty(BigQuerySyncConfig.BIGQUERY_SYNC_PROJECT_ID.key(), PROJECT_ID);
    properties.setProperty(BigQuerySyncConfig.BIGQUERY_SYNC_DATASET_NAME.key(), TEST_DATASET);
    properties.setProperty(BigQuerySyncConfig.BIGQUERY_SYNC_BILLING_PROJECT_ID.key(), BILLING_PROJECT_ID);
    properties.setProperty(HoodieSyncConfig.META_SYNC_BASE_PATH.key(), tempDir.toString());
    BigQuerySyncConfig config = new BigQuerySyncConfig(properties);
    client = new HoodieBigQuerySyncClient(config, mockBigQuery);
  }

  @Test
  void createTableWithManifestFile_partitioned() throws Exception {
    Schema schema = Schema.of(Field.of("field", StandardSQLTypeName.STRING));
    ArgumentCaptor<JobInfo> jobInfoCaptor = ArgumentCaptor.forClass(JobInfo.class);
    Job mockJob = mock(Job.class);
    when(mockBigQuery.create(jobInfoCaptor.capture())).thenReturn(mockJob);
    Job mockJobFinished = mock(Job.class);
    when(mockJob.waitFor()).thenReturn(mockJobFinished);
    JobStatus mockJobStatus = mock(JobStatus.class);
    when(mockJobFinished.getStatus()).thenReturn(mockJobStatus);
    when(mockJobStatus.getError()).thenReturn(null);
    client.createTableUsingBqManifestFile(TEST_TABLE, MANIFEST_FILE_URI, SOURCE_PREFIX, schema);

    QueryJobConfiguration configuration = jobInfoCaptor.getValue().getConfiguration();
    assertEquals(configuration.getQuery(),
        String.format("CREATE EXTERNAL TABLE `%s.%s.%s` ( field STRING ) WITH PARTITION COLUMNS OPTIONS (enable_list_inference=true, "
            + "hive_partition_uri_prefix=\"%s\", uris=[\"%s\"], format=\"PARQUET\", "
            + "file_set_spec_type=\"NEW_LINE_DELIMITED_MANIFEST\")", PROJECT_ID, TEST_DATASET, TEST_TABLE, SOURCE_PREFIX, MANIFEST_FILE_URI));
  }

  @Test
  void createTableWithManifestFile_nonPartitioned() throws Exception {
    Schema schema = Schema.of(Field.of("field", StandardSQLTypeName.STRING));
    ArgumentCaptor<JobInfo> jobInfoCaptor = ArgumentCaptor.forClass(JobInfo.class);
    Job mockJob = mock(Job.class);
    when(mockBigQuery.create(jobInfoCaptor.capture())).thenReturn(mockJob);
    Job mockJobFinished = mock(Job.class);
    when(mockJob.waitFor()).thenReturn(mockJobFinished);
    JobStatus mockJobStatus = mock(JobStatus.class);
    when(mockJobFinished.getStatus()).thenReturn(mockJobStatus);
    when(mockJobStatus.getError()).thenReturn(null);
    client.createTableUsingBqManifestFile(TEST_TABLE, MANIFEST_FILE_URI, "", schema);

    QueryJobConfiguration configuration = jobInfoCaptor.getValue().getConfiguration();
    assertEquals(configuration.getQuery(),
        String.format("CREATE EXTERNAL TABLE `%s.%s.%s` ( field STRING ) OPTIONS (enable_list_inference=true, uris=[\"%s\"], format=\"PARQUET\", "
            + "file_set_spec_type=\"NEW_LINE_DELIMITED_MANIFEST\")", PROJECT_ID, TEST_DATASET, TEST_TABLE, MANIFEST_FILE_URI));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateOrUpdateTableUsingManifestWithBillingProjectId(boolean setBillingProjectId) {
    Properties props = new Properties();
    props.setProperty(BIGQUERY_SYNC_PROJECT_ID.key(), PROJECT_ID);
    if (setBillingProjectId) {
      props.setProperty(BIGQUERY_SYNC_BILLING_PROJECT_ID.key(), BILLING_PROJECT_ID);
    }
    props.setProperty(BigQuerySyncConfig.BIGQUERY_SYNC_DATASET_NAME.key(), TEST_DATASET);
    props.setProperty(HoodieSyncConfig.META_SYNC_BASE_PATH.key(), tempDir.toString());
    BigQuerySyncConfig syncConfig = new BigQuerySyncConfig(props);
    Job mockJob = mock(Job.class);
    ArgumentCaptor<JobInfo> jobInfoCaptor = ArgumentCaptor.forClass(JobInfo.class);
    when(mockBigQuery.create(jobInfoCaptor.capture())).thenReturn(mockJob);

    Schema schema = Schema.of(Field.of("field", StandardSQLTypeName.STRING));
    client.createTableUsingBqManifestFile(TEST_TABLE, MANIFEST_FILE_URI, "", schema);

    assertEquals(
        setBillingProjectId ? BILLING_PROJECT_ID : PROJECT_ID,
        jobInfoCaptor.getValue().getJobId().getProject());
  }

}