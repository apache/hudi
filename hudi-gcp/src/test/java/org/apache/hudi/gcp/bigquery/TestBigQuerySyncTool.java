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

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.sync.common.util.ManifestFileWriter;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TestBigQuerySyncTool {
  private static final String TEST_TABLE = "test_table";
  private final ManifestFileWriter mockManifestFileWriter = mock(ManifestFileWriter.class);
  private final HoodieBigQuerySyncClient mockBqSyncClient = mock(HoodieBigQuerySyncClient.class);
  private final BigQuerySchemaResolver mockBqSchemaResolver = mock(BigQuerySchemaResolver.class);
  private final HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
  private final Properties properties = new Properties();

  private final Schema schema = Schema.of(Field.of("id", StandardSQLTypeName.STRING));

  @BeforeEach
  void setup() {
    // add default properties
    properties.setProperty(BigQuerySyncConfig.BIGQUERY_SYNC_TABLE_NAME.key(), TEST_TABLE);
  }

  @Test
  void missingDatasetCausesFailure() {
    when(mockBqSyncClient.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);
    when(mockBqSyncClient.datasetExists()).thenReturn(false);
    BigQuerySyncTool tool = new BigQuerySyncTool(properties, mockManifestFileWriter, mockBqSyncClient, mockMetaClient, mockBqSchemaResolver);
    assertThrows(HoodieBigQuerySyncException.class, tool::syncHoodieTable);
    verifyNoInteractions(mockManifestFileWriter, mockBqSchemaResolver);
  }

  @Test
  void useBQManifestFile_newTablePartitioned() {
    properties.setProperty(BigQuerySyncConfig.BIGQUERY_SYNC_USE_BQ_MANIFEST_FILE.key(), "true");
    String prefix = "file:///local/prefix";
    properties.setProperty(BigQuerySyncConfig.BIGQUERY_SYNC_SOURCE_URI_PREFIX.key(), prefix);
    properties.setProperty(BigQuerySyncConfig.BIGQUERY_SYNC_PARTITION_FIELDS.key(), "datestr,type");
    when(mockBqSyncClient.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);
    when(mockBqSyncClient.datasetExists()).thenReturn(true);
    when(mockBqSyncClient.tableNotExistsOrDoesNotMatchSpecification(TEST_TABLE)).thenReturn(true);
    Path manifestPath = new Path("file:///local/path");
    when(mockManifestFileWriter.getManifestSourceUri(true)).thenReturn(manifestPath.toUri().getPath());
    when(mockBqSchemaResolver.getTableSchema(any(), eq(Arrays.asList("datestr", "type")))).thenReturn(schema);
    BigQuerySyncTool tool = new BigQuerySyncTool(properties, mockManifestFileWriter, mockBqSyncClient, mockMetaClient, mockBqSchemaResolver);
    tool.syncHoodieTable();
    verify(mockBqSyncClient).createOrUpdateTableUsingBqManifestFile(TEST_TABLE, manifestPath.toUri().getPath(), prefix, schema);
    verify(mockManifestFileWriter).writeManifestFile(true);
  }

  @Test
  void useBQManifestFile_newTableNonPartitioned() {
    properties.setProperty(BigQuerySyncConfig.BIGQUERY_SYNC_USE_BQ_MANIFEST_FILE.key(), "true");
    when(mockBqSyncClient.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);
    when(mockBqSyncClient.datasetExists()).thenReturn(true);
    when(mockBqSyncClient.tableNotExistsOrDoesNotMatchSpecification(TEST_TABLE)).thenReturn(true);
    Path manifestPath = new Path("file:///local/path");
    when(mockManifestFileWriter.getManifestSourceUri(true)).thenReturn(manifestPath.toUri().getPath());
    when(mockBqSchemaResolver.getTableSchema(any(), eq(Collections.emptyList()))).thenReturn(schema);
    BigQuerySyncTool tool = new BigQuerySyncTool(properties, mockManifestFileWriter, mockBqSyncClient, mockMetaClient, mockBqSchemaResolver);
    tool.syncHoodieTable();
    verify(mockBqSyncClient).createOrUpdateTableUsingBqManifestFile(TEST_TABLE, manifestPath.toUri().getPath(), null, schema);
    verify(mockManifestFileWriter).writeManifestFile(true);
  }

  @Test
  void useBQManifestFile_existingPartitionedTable() {
    properties.setProperty(BigQuerySyncConfig.BIGQUERY_SYNC_USE_BQ_MANIFEST_FILE.key(), "true");
    String prefix = "file:///local/prefix";
    properties.setProperty(BigQuerySyncConfig.BIGQUERY_SYNC_SOURCE_URI_PREFIX.key(), prefix);
    properties.setProperty(BigQuerySyncConfig.BIGQUERY_SYNC_PARTITION_FIELDS.key(), "datestr,type");
    when(mockBqSyncClient.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);
    when(mockBqSyncClient.datasetExists()).thenReturn(true);
    when(mockBqSyncClient.tableNotExistsOrDoesNotMatchSpecification(TEST_TABLE)).thenReturn(false);
    Path manifestPath = new Path("file:///local/path");
    when(mockManifestFileWriter.getManifestSourceUri(true)).thenReturn(manifestPath.toUri().getPath());
    List<String> partitionFields = Arrays.asList("datestr", "type");
    when(mockBqSchemaResolver.getTableSchema(any(), eq(partitionFields))).thenReturn(schema);
    BigQuerySyncTool tool = new BigQuerySyncTool(properties, mockManifestFileWriter, mockBqSyncClient, mockMetaClient, mockBqSchemaResolver);
    tool.syncHoodieTable();
    verify(mockBqSyncClient).updateTableSchema(TEST_TABLE, schema, partitionFields);
    verify(mockManifestFileWriter).writeManifestFile(true);
  }

  @Test
  void useBQManifestFile_existingNonPartitionedTable() {
    properties.setProperty(BigQuerySyncConfig.BIGQUERY_SYNC_USE_BQ_MANIFEST_FILE.key(), "true");
    when(mockBqSyncClient.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);
    when(mockBqSyncClient.datasetExists()).thenReturn(true);
    when(mockBqSyncClient.tableNotExistsOrDoesNotMatchSpecification(TEST_TABLE)).thenReturn(false);
    Path manifestPath = new Path("file:///local/path");
    when(mockManifestFileWriter.getManifestSourceUri(true)).thenReturn(manifestPath.toUri().getPath());
    when(mockBqSchemaResolver.getTableSchema(any(), eq(Collections.emptyList()))).thenReturn(schema);
    BigQuerySyncTool tool = new BigQuerySyncTool(properties, mockManifestFileWriter, mockBqSyncClient, mockMetaClient, mockBqSchemaResolver);
    tool.syncHoodieTable();
    verify(mockBqSyncClient).updateTableSchema(TEST_TABLE, schema, Collections.emptyList());
    verify(mockManifestFileWriter).writeManifestFile(true);
  }
}
