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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.table.HoodieTable;

import java.io.Serializable;

/**
 * Base class for different types of indexes to determine the mapping from uuid.
 *
 * @param <I> Type of inputs for deprecated APIs
 * @param <O> Type of outputs for deprecated APIs
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public abstract class HoodieIndex<I, O> implements Serializable {

  protected final HoodieWriteConfig config;

  protected HoodieIndex(HoodieWriteConfig config) {
    this.config = config;
  }

  /**
   * Looks up the index and tags each incoming record with a location of a file that contains the row (if it is actually
   * present).
   */
  @Deprecated
  @PublicAPIMethod(maturity = ApiMaturityLevel.DEPRECATED)
  public I tagLocation(I records, HoodieEngineContext context,
                       HoodieTable hoodieTable) throws HoodieIndexException {
    throw new HoodieNotSupportedException("Deprecated API should not be called");
  }

  /**
   * Extracts the location of written records, and updates the index.
   */
  @Deprecated
  @PublicAPIMethod(maturity = ApiMaturityLevel.DEPRECATED)
  public O updateLocation(O writeStatuses, HoodieEngineContext context,
                          HoodieTable hoodieTable) throws HoodieIndexException {
    throw new HoodieNotSupportedException("Deprecated API should not be called");
  }

  /**
   * Looks up the index and tags each incoming record with a location of a file that contains
   * the row (if it is actually present).
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
      HoodieTable hoodieTable) throws HoodieIndexException;

  /**
   * Looks up the index and tags each incoming record with a location of a file that contains
   * the row (if it is actually present).
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public <R> HoodiePairData<HoodieKey, HoodieRecord<R>> tagLocationX(
      HoodieEngineContext context, HoodiePairData<HoodieKey, HoodieRecord<R>> records,
      HoodieTable hoodieTable) throws HoodieIndexException {
    throw new UnsupportedOperationException("not implemented");
  }

  /**
   * Extracts the location of written records, and updates the index.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract HoodieData<WriteStatus> updateLocation(
      HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context,
      HoodieTable hoodieTable) throws HoodieIndexException;

  /**
   * Rollback the effects of the commit made at instantTime.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public abstract boolean rollbackCommit(String instantTime);

  /**
   * An index is `global` if {@link HoodieKey} to fileID mapping, does not depend on the `partitionPath`. Such an
   * implementation is able to obtain the same mapping, for two hoodie keys with same `recordKey` but different
   * `partitionPath`
   *
   * @return whether or not, the index implementation is global in nature
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public abstract boolean isGlobal();

  /**
   * This is used by storage to determine, if its safe to send inserts, straight to the log, i.e having a
   * {@link FileSlice}, with no data file.
   *
   * @return Returns true/false depending on whether the impl has this capability
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract boolean canIndexLogFiles();

  /**
   * An index is "implicit" with respect to storage, if just writing new data to a file slice, updates the index as
   * well. This is used by storage, to save memory footprint in certain cases.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public abstract boolean isImplicitWithStorage();

  /**
   * To indicate if a operation type requires location tagging before writing
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public boolean requiresTagging(WriteOperationType operationType) {
    switch (operationType) {
      case DELETE:
      case UPSERT:
        return true;
      default:
        return false;
    }
  }

  /**
   * Each index type should implement it's own logic to release any resources acquired during the process.
   */
  public void close() {
  }

  public enum IndexType {
    HBASE, INMEMORY, BLOOM, GLOBAL_BLOOM, SIMPLE, GLOBAL_SIMPLE, BUCKET, FLINK_STATE
  }

  public enum BucketIndexEngineType {
    SIMPLE, CONSISTENT_HASHING
  }
}
