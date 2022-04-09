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

package org.apache.hudi.client;

import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Run one round of compaction.
 */
public abstract class BaseCompactor<T extends HoodieRecordPayload, I, K, O> implements Serializable {

  private static final long serialVersionUID = 1L;

  protected final transient Object writeClientUpdateLock = new Object();
  protected final transient List<BaseHoodieWriteClient<T, I, K, O>> oldCompactionClientList = new ArrayList<>();

  protected transient BaseHoodieWriteClient<T, I, K, O> compactionClient;

  protected boolean isCompactionRunning = false;

  public BaseCompactor(BaseHoodieWriteClient<T, I, K, O> compactionClient) {
    this.compactionClient = compactionClient;
  }

  public abstract void compact(HoodieInstant instant) throws IOException;

  public void updateWriteClient(BaseHoodieWriteClient<T, I, K, O> writeClient) {
    synchronized (writeClientUpdateLock) {
      if (!isCompactionRunning) {
        this.compactionClient.close();
      } else {
        // Store the old compaction client so that they can be closed
        // at the end of the compaction execution
        this.oldCompactionClientList.add(this.compactionClient);
      }
      this.compactionClient = writeClient;
    }
  }

}
