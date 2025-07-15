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

package org.apache.hudi.common.table;

import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;

class TestHoodieTableConfig {
  @Test
  void testVersionLessThanNineReturnsNone() {
    HoodieTableConfig config = Mockito.spy(new HoodieTableConfig());
    doReturn(HoodieTableVersion.EIGHT).when(config).getTableVersion();
    assertEquals(PartialUpdateMode.NONE, config.getPartialUpdateMode());
  }

  @Test
  void testEmptyPayloadUsesConfigValue() {
    HoodieTableConfig config = Mockito.spy(new HoodieTableConfig());
    doReturn(HoodieTableVersion.NINE).when(config).getTableVersion();
    doReturn(null).when(config).getPayloadClass();
    doReturn("IGNORE_DEFAULTS").when(config).getStringOrDefault(HoodieTableConfig.PARTIAL_UPDATE_MODE);
    assertEquals(PartialUpdateMode.IGNORE_DEFAULTS, config.getPartialUpdateMode());
  }

  @Test
  void testKnownPayloadOverwriteNonDefaults() {
    HoodieTableConfig config = Mockito.spy(new HoodieTableConfig());
    doReturn(HoodieTableVersion.NINE).when(config).getTableVersion();
    doReturn(OverwriteNonDefaultsWithLatestAvroPayload.class.getName()).when(config).getPayloadClass();
    assertEquals(PartialUpdateMode.IGNORE_DEFAULTS, config.getPartialUpdateMode());
  }

  @Test
  void testKnownPayloadPartialUpdateAvro() {
    HoodieTableConfig config = Mockito.spy(new HoodieTableConfig());
    doReturn(HoodieTableVersion.NINE).when(config).getTableVersion();
    doReturn(PartialUpdateAvroPayload.class.getName()).when(config).getPayloadClass();
    assertEquals(PartialUpdateMode.IGNORE_DEFAULTS, config.getPartialUpdateMode());
  }

  @Test
  void testKnownPayloadPostgresDebezium() {
    HoodieTableConfig config = Mockito.spy(new HoodieTableConfig());
    doReturn(HoodieTableVersion.NINE).when(config).getTableVersion();
    doReturn(PostgresDebeziumAvroPayload.class.getName()).when(config).getPayloadClass();
    assertEquals(PartialUpdateMode.IGNORE_MARKERS, config.getPartialUpdateMode());
  }

  @Test
  void testUnknownPayloadFallsBackToConfig() {
    HoodieTableConfig config = Mockito.spy(new HoodieTableConfig());
    doReturn(HoodieTableVersion.NINE).when(config).getTableVersion();
    doReturn("some.custom.Payload").when(config).getPayloadClass();
    doReturn("IGNORE_DEFAULTS").when(config).getStringOrDefault(HoodieTableConfig.PARTIAL_UPDATE_MODE);
    assertEquals(PartialUpdateMode.IGNORE_DEFAULTS, config.getPartialUpdateMode());
  }
}
