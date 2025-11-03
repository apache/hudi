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
import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;
import org.apache.hudi.common.data.HoodieData;
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
   * Extracts the location of written records, and updates the index.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract HoodieData<WriteStatus> updateLocation(
      HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context,
      HoodieTable hoodieTable) throws HoodieIndexException;


  /**
   * Extracts the location of written records, and updates the index.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public HoodieData<WriteStatus> updateLocation(
      HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context,
      HoodieTable hoodieTable, String instant) throws HoodieIndexException {
    return updateLocation(writeStatuses, context, hoodieTable);
  }


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
   * @return whether the index implementation is global in nature
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public abstract boolean isGlobal();

  /**
   * This is used by storage to determine, if it is safe to send inserts, straight to the log, i.e. having a
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
   * To indicate if an operation type requires location tagging before writing
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public boolean requiresTagging(WriteOperationType operationType) {
    switch (operationType) {
      case DELETE:
      case DELETE_PREPPED:
      case UPSERT:
        return true;
      default:
        return false;
    }
  }

  /**
   * Each index type should implement its own logic to release any resources acquired during the process.
   */
  public void close() {
  }

  @EnumDescription("Determines how input records are indexed, i.e., looked up based on the key "
      + "for the location in the existing table. Default is SIMPLE on Spark engine, and INMEMORY "
      + "on Flink and Java engines.")
  public enum IndexType {
    @EnumFieldDescription("Uses in-memory hashmap in Spark and Java engine and Flink in-memory "
        + "state in Flink for indexing.")
    INMEMORY,

    @EnumFieldDescription("Employs bloom filters built out of the record keys, "
        + "optionally also pruning candidate files using record key ranges. "
        + "Key uniqueness is enforced inside partitions.")
    BLOOM,

    @EnumFieldDescription("Employs bloom filters built out of the record keys, "
        + "optionally also pruning candidate files using record key ranges. "
        + "Key uniqueness is enforced across all partitions in the table. ")
    GLOBAL_BLOOM,

    @EnumFieldDescription("Performs a lean join of the incoming update/delete records "
        + "against keys extracted from the table on storage."
        + "Key uniqueness is enforced inside partitions.")
    SIMPLE,

    @EnumFieldDescription("Performs a lean join of the incoming update/delete records "
        + "against keys extracted from the table on storage."
        + "Key uniqueness is enforced across all partitions in the table.")
    GLOBAL_SIMPLE,

    @EnumFieldDescription("locates the file group containing the record fast by using bucket "
        + "hashing, particularly beneficial in large scale. Use `hoodie.index.bucket.engine` to "
        + "choose bucket engine type, i.e., how buckets are generated.")
    BUCKET,

    @EnumFieldDescription("Internal Config for indexing based on Flink state.")
    FLINK_STATE,

    @Deprecated
    @EnumFieldDescription("Index which saves the record key to location mappings in the "
        + "HUDI Metadata Table. Record index is a global index, enforcing key uniqueness across all "
        + "partitions in the table. Supports sharding to achieve very high scale. For a table with "
        + "keys that are only unique inside each partition, use `RECORD_LEVEL_INDEX` instead. "
        + "This enum is deprecated. Use GLOBAL_RECORD_LEVEL_INDEX for global uniqueness of record " +
        "keys or RECORD_LEVEL_INDEX for partition-level uniqueness of record keys.")
    RECORD_INDEX,

    @EnumFieldDescription("Index which saves the record key to location mappings in the "
        + "HUDI Metadata Table. Record index is a global index, enforcing key uniqueness across all "
        + "partitions in the table. Supports sharding to achieve very high scale. For a table with "
        + "keys that are only unique inside each partition, use `RECORD_LEVEL_INDEX` instead.")
    GLOBAL_RECORD_LEVEL_INDEX,

    @EnumFieldDescription("Index which saves the record key to location mappings in the "
        + "HUDI Metadata Table. Supports sharding to achieve very high scale. This is a "
        + "non global index, where keys can be replicated across partitions, since a "
        + "pair of partition path and record keys will uniquely map to a location using "
        + "this index. If users expect record keys to be unique across all partitions, "
        + "use `GLOBAL_RECORD_LEVEL_INDEX` instead.")
    RECORD_LEVEL_INDEX
  }

  @EnumDescription("Determines the type of bucketing or hashing to use when `hoodie.index.type`"
      + " is set to `BUCKET`.")
  public enum BucketIndexEngineType {

    @EnumFieldDescription("Uses a fixed number of buckets for file groups which cannot shrink or "
        + "expand. This works for both COW and MOR tables.")
    SIMPLE,

    @EnumFieldDescription("Supports dynamic number of buckets with bucket resizing to properly "
        + "size each bucket. This solves potential data skew problem where one bucket can be "
        + "significantly larger than others in SIMPLE engine type. This only works with MOR tables.")
    CONSISTENT_HASHING
  }
}
