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
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;

import java.util.function.Consumer;

@Slf4j
class Heartbeat implements Runnable {
  private final IMetaStoreClient client;
  private final long lockId;
  private final Consumer<Exception> onLockLost;
  // Latches the terminal failure so that a tick already queued when the schedule was cancelled
  // does not issue another doomed heartbeat or report the loss a second time.
  private volatile boolean lockLost = false;

  Heartbeat(IMetaStoreClient client, long lockId, Consumer<Exception> onLockLost) {
    this.client = client;
    this.lockId = lockId;
    this.onLockLost = onLockLost;
  }

  @Override
  public void run() {
    if (lockLost) {
      return;
    }
    try {
      client.heartbeat(0, lockId);
    } catch (NoSuchLockException | NoSuchTxnException | TxnAbortedException e) {
      // Terminal. The metastore has already expired or aborted this lock, so no later tick can
      // renew it. Retrying would only log once per interval while the writer keeps believing it
      // holds exclusivity, so report the loss to the owner, which stops the schedule and drops
      // the lock.
      lockLost = true;
      onLockLost.accept(e);
    } catch (Exception e) {
      // Do not rethrow. This task is scheduled via ScheduledExecutorService.scheduleAtFixedRate,
      // where a thrown exception permanently cancels all subsequent executions and is only
      // observable through the (unread) ScheduledFuture. Swallowing a transient failure here keeps
      // the lock heartbeated on the next tick instead of silently stopping renewal altogether.
      log.warn("Failed to heartbeat for lock: {}", lockId, e);
    }
  }
}
