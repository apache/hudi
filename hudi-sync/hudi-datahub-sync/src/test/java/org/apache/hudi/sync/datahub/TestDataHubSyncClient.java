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

import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.sync.datahub.config.DataHubSyncConfig;
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
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestDataHubSyncClient {

  @Mock
  DataHubSyncConfig configMock;

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
    HoodieTableMetaClient.initTableAndGetMetaClient(new Configuration(),
            tableBasePath, props);
  }

  @BeforeEach
  public void beforeEach() {
    MockitoAnnotations.initMocks(this);
  }

  @AfterEach
  public void afterEach() {
  }

  @Test
  public void testUpdateTableSchemaInvokesRestEmiiter() throws IOException {
    Properties props = new Properties();
    props.put(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), DummyPartitionValueExtractor.class.getName());
    props.put(META_SYNC_BASE_PATH.key(), tableBasePath);

    Mockito.when(restEmitterMock.emit(any(MetadataChangeProposalWrapper.class),
            Mockito.any())).thenReturn(CompletableFuture.completedFuture(MetadataWriteResponse.
            builder().build()));

    DatahubSyncConfigStub configStub = new DatahubSyncConfigStub(props, restEmitterMock);
    DataHubSyncClientStub dhClient = new DataHubSyncClientStub(configStub);

    dhClient.updateTableSchema("someTable", null);
    verify(restEmitterMock, times(2)).emit(any(MetadataChangeProposalWrapper.class),
            Mockito.any());
  }

  public class DataHubSyncClientStub extends DataHubSyncClient {

    public DataHubSyncClientStub(DataHubSyncConfig config) {
      super(config);
    }

    @Override
    Schema getAvroSchemaWithoutMetadataFields(HoodieTableMetaClient metaClient) {
      return avroSchema;
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
