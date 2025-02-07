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

package org.apache.hudi.sync.datahub;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.sync.datahub.config.DataHubSyncConfig;

import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_SYNC_SUPPRESS_EXCEPTIONS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestDataHubSyncClient {

  @Mock
  RestEmitter restEmitterMock;

  @TempDir
  static java.nio.file.Path tmpDir;

  private static String TRIP_EXAMPLE_SCHEMA;
  private static Schema avroSchema;
  private static String tableBasePath;

  @BeforeAll
  public static void beforeAll() throws IOException {
    TRIP_EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"triprec\",\"fields\": [ "
            + "{\"name\": \"ts\",\"type\": \"long\"}]}";

    avroSchema = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);

    Properties props = new Properties();
    props.put("hoodie.table.name", "some_table");
    tableBasePath = Paths.get(tmpDir.toString(), "some_table").toString();
    HoodieTableMetaClient.newTableBuilder()
            .fromProperties(props)
            .setTableType(HoodieTableType.MERGE_ON_READ.name())
            .initTable(HadoopFSUtils.getStorageConf(new Configuration()), tableBasePath);
  }

  @BeforeEach
  public void beforeEach() {
    MockitoAnnotations.initMocks(this);
  }

  @AfterEach
  public void afterEach() {
  }

  @Test
  public void testUpdateTableSchemaInvokesRestEmitter() throws IOException {
    Properties props = new Properties();
    props.put(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), DummyPartitionValueExtractor.class.getName());
    props.put(META_SYNC_BASE_PATH.key(), tableBasePath);

    Mockito.when(
            restEmitterMock.emit(any(MetadataChangeProposalWrapper.class), Mockito.any())
    ).thenReturn(
            CompletableFuture.completedFuture(MetadataWriteResponse.builder().build())
    );

    DatahubSyncConfigStub configStub = new DatahubSyncConfigStub(props, restEmitterMock);
    DataHubSyncClientStub dhClient = new DataHubSyncClientStub(configStub);

    dhClient.updateTableSchema("some_table", null, null);
    verify(restEmitterMock, times(9)).emit(any(MetadataChangeProposalWrapper.class),
            Mockito.any());
  }

  @Test
  public void testUpdateTableProperties() throws Exception {
    Properties props = new Properties();
    props.put(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), DummyPartitionValueExtractor.class.getName());
    props.put(META_SYNC_BASE_PATH.key(), tableBasePath);

    when(restEmitterMock.emit(any(MetadataChangeProposal.class), any()))
            .thenReturn(CompletableFuture.completedFuture(MetadataWriteResponse.builder().build()));

    DatahubSyncConfigStub configStub = new DatahubSyncConfigStub(props, restEmitterMock);
    DataHubSyncClientStub dhClient = new DataHubSyncClientStub(configStub);

    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "value1");
    properties.put("key2", "value2");

    boolean result = dhClient.updateTableProperties("some_table", properties);
    assertTrue(result);
    verify(restEmitterMock, times(1)).emit(any(MetadataChangeProposal.class), any());
  }

  @Test
  public void testUpdateTablePropertiesFailure() throws Exception {
    Properties props = new Properties();
    props.put(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), DummyPartitionValueExtractor.class.getName());
    props.put(META_SYNC_BASE_PATH.key(), tableBasePath);
    props.put(META_SYNC_DATAHUB_SYNC_SUPPRESS_EXCEPTIONS.key(), "false");

    CompletableFuture<MetadataWriteResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new IOException("Emission failed"));
    when(restEmitterMock.emit(any(MetadataChangeProposalWrapper.class), any()))
        .thenReturn(failedFuture);

    DatahubSyncConfigStub configStub = new DatahubSyncConfigStub(props, restEmitterMock);
    DataHubSyncClientStub dhClient = new DataHubSyncClientStub(configStub);

    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "value1");

    assertThrows(HoodieDataHubSyncException.class, () ->
            dhClient.updateTableProperties("some_table", properties));
  }

  @Test
  public void testGetLastCommitTimeSynced() {
    Properties props = new Properties();
    props.put(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), DummyPartitionValueExtractor.class.getName());
    props.put(META_SYNC_BASE_PATH.key(), tableBasePath);

    DatahubSyncConfigStub configStub = new DatahubSyncConfigStub(props, restEmitterMock);
    DataHubSyncClientStub dhClient = new DataHubSyncClientStub(configStub);

    assertThrows(UnsupportedOperationException.class, () ->
            dhClient.getLastCommitTimeSynced("some_table"));
  }

  @Test
  public void testGetMetastoreSchema() {
    Properties props = new Properties();
    props.put(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), DummyPartitionValueExtractor.class.getName());
    props.put(META_SYNC_BASE_PATH.key(), tableBasePath);

    DatahubSyncConfigStub configStub = new DatahubSyncConfigStub(props, restEmitterMock);
    DataHubSyncClientStub dhClient = new DataHubSyncClientStub(configStub);

    assertThrows(UnsupportedOperationException.class, () ->
            dhClient.getMetastoreSchema("some_table"));
  }

  @Test
  public void testUpdateTableSchemaWithEmitterFailure() throws Exception {
    Properties props = new Properties();
    props.put(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), DummyPartitionValueExtractor.class.getName());
    props.put(META_SYNC_BASE_PATH.key(), tableBasePath);
    props.put(META_SYNC_DATAHUB_SYNC_SUPPRESS_EXCEPTIONS.key(), "false");

    // Create a failed future that will throw when accessed
    CompletableFuture<MetadataWriteResponse> future = new CompletableFuture<>();
    future.completeExceptionally(new ExecutionException("Emission failed", new IOException()));

    // Configure mock to return the failed future for ALL calls
    when(restEmitterMock.emit((MetadataChangeProposalWrapper) any(), any())).thenReturn(future);

    DatahubSyncConfigStub configStub = new DatahubSyncConfigStub(props, restEmitterMock);
    DataHubSyncClientStub dhClient = new DataHubSyncClientStub(configStub);

    assertThrows(HoodieDataHubSyncException.class, () ->
            dhClient.updateTableSchema("some_table", null, null));
  }

  @Test
  public void testUpdateLastCommitTimeSynced() throws Exception {
    Properties props = new Properties();
    props.put(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), DummyPartitionValueExtractor.class.getName());
    props.put(META_SYNC_BASE_PATH.key(), tableBasePath);

    when(restEmitterMock.emit(any(MetadataChangeProposal.class), any()))
            .thenReturn(CompletableFuture.completedFuture(MetadataWriteResponse.builder().build()));

    DatahubSyncConfigStub configStub = new DatahubSyncConfigStub(props, restEmitterMock);
    DataHubSyncClientStub dhClient = new DataHubSyncClientStub(configStub);

    dhClient.updateLastCommitTimeSynced("some_table");
    verify(restEmitterMock, times(2)).emit(any(MetadataChangeProposal.class), any());
  }

  public class DataHubSyncClientStub extends DataHubSyncClient {

    public DataHubSyncClientStub(DataHubSyncConfig config) {
      super(config, mock(HoodieTableMetaClient.class));
    }

    @Override
    Schema getAvroSchemaWithoutMetadataFields(HoodieTableMetaClient metaClient) {
      return avroSchema;
    }

    @Override
    protected Option<String> getLastCommitTime() {
      return Option.of("1000");
    }

    @Override
    protected Option<String> getLastCommitCompletionTime() {
      return Option.of("1000");
    }

  }

  public class DatahubSyncConfigStub extends DataHubSyncConfig {

    private final RestEmitter emitterMock;

    public DatahubSyncConfigStub(Properties props, RestEmitter emitterMock) {
      super(props);
      this.emitterMock = emitterMock;
    }

    @Override
    public RestEmitter getRestEmitter() {
      return emitterMock;
    }

  }

}