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

package org.apache.hudi.hive.transaction.lock;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TestHeartbeat {

  @Test
  void runDoesNotRethrowWhenHeartbeatFails() throws TException {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    doThrow(new TException("transient failure")).when(client).heartbeat(anyLong(), anyLong());

    Heartbeat heartbeat = new Heartbeat(client, 7L);

    // Rethrowing here would cancel every subsequent execution of a scheduleAtFixedRate task,
    // silently stopping lock renewal. The fix must swallow the failure so the next tick retries.
    assertDoesNotThrow(heartbeat::run);
  }

  @Test
  void runHeartbeatsTheLockOnSuccess() throws TException {
    IMetaStoreClient client = mock(IMetaStoreClient.class);

    new Heartbeat(client, 99L).run();

    verify(client, times(1)).heartbeat(0L, 99L);
  }
}
