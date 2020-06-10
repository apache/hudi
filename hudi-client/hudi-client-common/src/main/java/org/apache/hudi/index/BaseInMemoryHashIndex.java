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

package org.apache.hudi.index;

import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.BaseHoodieTable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Hoodie Index implementation backed by an in-memory Hash map.
 * <p>
 * ONLY USE FOR LOCAL TESTING
 */
public abstract class BaseInMemoryHashIndex<T extends HoodieRecordPayload, I, K, O, P> extends HoodieIndex<T, I, K, O, P> {

  protected static ConcurrentMap<HoodieKey, HoodieRecordLocation> recordLocationMap;

  public BaseInMemoryHashIndex(HoodieWriteConfig config) {
    super(config);
    synchronized (BaseInMemoryHashIndex.class) {
      if (recordLocationMap == null) {
        recordLocationMap = new ConcurrentHashMap<>();
      }
    }
  }

  @Override
  public P fetchRecordLocation(K hoodieKeys,
                               HoodieEngineContext context, BaseHoodieTable<T, I, K, O, P> hoodieTable) {
    throw new UnsupportedOperationException("InMemory index does not implement check exist yet");
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    return true;
  }

  /**
   * Only looks up by recordKey.
   */
  @Override
  public boolean isGlobal() {
    return true;
  }

  /**
   * Mapping is available in HBase already.
   */
  @Override
  public boolean canIndexLogFiles() {
    return true;
  }

  /**
   * Index needs to be explicitly updated after storage write.
   */
  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }

}
