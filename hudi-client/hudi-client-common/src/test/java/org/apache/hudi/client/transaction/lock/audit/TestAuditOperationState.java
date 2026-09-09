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

package org.apache.hudi.client.transaction.lock.audit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests the {@link AuditOperationState} lifecycle enum.
 */
public class TestAuditOperationState {

  @Test
  void declaresExpectedStatesInOrder() {
    assertArrayEquals(
        new AuditOperationState[] {
            AuditOperationState.START,
            AuditOperationState.RENEW,
            AuditOperationState.END
        },
        AuditOperationState.values());
  }

  @Test
  void valueOfResolvesEachState() {
    for (AuditOperationState state : AuditOperationState.values()) {
      assertSame(state, AuditOperationState.valueOf(state.name()));
    }
  }

  @Test
  void valueOfRejectsUnknownState() {
    assertThrows(IllegalArgumentException.class, () -> AuditOperationState.valueOf("PAUSE"));
  }
}
