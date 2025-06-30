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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for schema evolution client api.
 */
public class TestSchemaEvolutionClient extends HoodieJavaClientTestHarness {

  private static final Schema SCHEMA = getSchemaFromResource(TestSchemaEvolutionClient.class, "/exampleSchema.avsc");

  @BeforeEach
  public void setUpClient() throws IOException {
    HoodieJavaWriteClient<RawTripTestPayload> writeClient = getWriteClient();
    this.writeClient = writeClient;
    prepareTable(writeClient);
  }

  @AfterEach
  public void closeClient() {
    if (writeClient != null) {
      writeClient.close();
    }
  }

  @Test
  public void testUpdateColumnType() {
    writeClient.updateColumnType("number", Types.LongType.get());
    assertEquals(Types.LongType.get(), getFieldByName("number").type());
  }

  private HoodieJavaWriteClient<RawTripTestPayload> getWriteClient() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withPath(basePath)
        .withSchema(SCHEMA.toString())
        .withProps(Collections.singletonMap(HoodieWriteConfig.TBL_NAME.key(), "hoodie_test_table"))
        .build();
    return new HoodieJavaWriteClient<>(context, config);
  }

  private void prepareTable(HoodieJavaWriteClient<RawTripTestPayload> writeClient) throws IOException {
    String commitTime = writeClient.startCommit();
    String jsonRow = "{\"_row_key\": \"1\", \"time\": \"2000-01-01T00:00:00.000Z\", \"number\": 1}";
    RawTripTestPayload payload = new RawTripTestPayload(jsonRow);
    HoodieAvroRecord<RawTripTestPayload> record = new HoodieAvroRecord<>(
        new HoodieKey(payload.getRowKey(), payload.getPartitionPath()), payload);
    writeClient.commit(commitTime, writeClient.insert(Collections.singletonList(record), commitTime), Option.empty(), COMMIT_ACTION, Collections.emptyMap());
  }

  private Types.Field getFieldByName(String fieldName) {
    return new TableSchemaResolver(metaClient)
        .getTableInternalSchemaFromCommitMetadata()
        .get()
        .findField(fieldName);
  }
}
