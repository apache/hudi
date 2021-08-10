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

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.io.Serializable;

/**
 * Base class for different types of secondary indexes. 
 * This is different from primary index because
 * a) index lookup operation is very different (using predicates instead of sending all keys).
 * b) There can be multiple secondary index configured for same table. (one column may use range index/some other column may use bloom index)
 *
 * @param <T> Sub type of HoodieRecordPayload
 * @param <I> Type of inputs
 * @param <K> Type of keys
 * @param <O> Type of outputs
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public abstract class HoodieSecondaryIndex<T extends HoodieRecordPayload, I, K, O> implements Serializable {

  protected final HoodieWriteConfig config;

  protected final HoodieEngineContext engineContext;

  protected HoodieSecondaryIndex(HoodieWriteConfig config, HoodieEngineContext engineContext) {
    this.config = config;
    this.engineContext = engineContext;
  }

  /**
   * Update index to quickly identify records.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract void updateIndex(HoodieWriteMetadata<O> writeMetadata, String instantTime, HoodieEngineContext context,
                                HoodieTable<T, I, K, O> hoodieTable) throws HoodieIndexException;

  /**
   * Rollback the effects of the commit made at instantTime.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract boolean rollbackCommit(String instantTime);

  /**
   * TODO figure out the signature for this
    public abstract List<HoodieFileGroup> findMatchingDataFiles(List<Predicate> predicates)
   */
  /**
   * Each index type should implement it's own logic to release any resources acquired during the process.
   */
  public void close() {
  }

  public enum SecondaryIndexType {
    /* TODO: also support multiple secondary index on same table - different columns can have different values */
    RANGE,
    BLOOM, /* TODO: we can leverage same bloom index used for primary key */
    CUCKOO, /* TODO: implementation, For medium cardinality, cuckoo index likely performs better than bloom index https://www.vldb.org/pvldb/vol13/p3559-kipf.pdf */
    POINT /* TODO: Finding exact value quickly. maybe useful for primary key lookups too. */
  }

  public HoodieWriteConfig getWriteConfig() {
    return this.config;
  }

  public HoodieEngineContext getEngineContext() {
    return this.engineContext;
  }
}
