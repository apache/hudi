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

package org.apache.hudi.metadata;

import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestMetadataTableServiceRequest {

  @Test
  void defaultsToAllServicesAndBothPhases() {
    MetadataTableServiceRequest request = MetadataTableServiceRequest.newBuilder().build();

    assertEquals(MetadataTableServiceMode.SCHEDULE_AND_EXECUTE, request.getMode());
    assertEquals(EnumSet.of(TableServiceType.COMPACT, TableServiceType.LOG_COMPACT,
        TableServiceType.CLEAN, TableServiceType.ARCHIVE), request.getServices());
    assertFalse(request.shouldDisableTableServiceManagerDelegation());
  }

  @Test
  void preservesExecutionInstantAndDelegationFlag() {
    MetadataTableServiceRequest request = MetadataTableServiceRequest.newBuilder()
        .withMode(MetadataTableServiceMode.EXECUTE)
        .withServices(EnumSet.of(TableServiceType.COMPACT))
        .withInstantTime(Option.of("compaction-instant"))
        .disableTableServiceManagerDelegation(true)
        .build();

    assertEquals("compaction-instant", request.getInstantTime().get());
    assertTrue(request.shouldDisableTableServiceManagerDelegation());
  }

  @Test
  void copiesRequestWithDifferentMode() {
    MetadataTableServiceRequest request = MetadataTableServiceRequest.newBuilder()
        .withServices(EnumSet.of(TableServiceType.COMPACT))
        .disableTableServiceManagerDelegation(true)
        .build();

    MetadataTableServiceRequest copy = request.copy(MetadataTableServiceMode.EXECUTE);

    assertEquals(MetadataTableServiceMode.EXECUTE, copy.getMode());
    assertEquals(request.getServices(), copy.getServices());
    assertTrue(copy.shouldDisableTableServiceManagerDelegation());
  }

  @Test
  void rejectsUnsupportedOrAmbiguousRequests() {
    assertThrows(IllegalArgumentException.class, () -> MetadataTableServiceRequest.newBuilder()
        .withServices(EnumSet.of(TableServiceType.CLUSTER))
        .build());
    assertThrows(IllegalArgumentException.class, () -> MetadataTableServiceRequest.newBuilder()
        .withInstantTime(Option.of("instant"))
        .build());
  }
}
