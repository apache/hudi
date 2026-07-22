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

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

@Slf4j
class Heartbeat implements Runnable {
  private final IMetaStoreClient client;
  private final long lockId;

  Heartbeat(IMetaStoreClient client, long lockId) {
    this.client = client;
    this.lockId = lockId;
  }

  @Override
  public void run() {
    try {
      client.heartbeat(0, lockId);
    } catch (Exception e) {
      // Do not rethrow. This task is scheduled via ScheduledExecutorService.scheduleAtFixedRate,
      // where a thrown exception permanently cancels all subsequent executions and is only
      // observable through the (unread) ScheduledFuture. Swallowing a transient failure here keeps
      // the lock heartbeated on the next tick instead of silently stopping renewal altogether.
      log.warn("Failed to heartbeat for lock: {}", lockId, e);
    }
  }
}
