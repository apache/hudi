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

package org.apache.hudi.index.bucket;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.LazyIterableIterator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.hudi.index.HoodieIndexUtils.tagAsNewRecordIfNeeded;

/**
 * Hash indexing mechanism.
 */
public abstract class HoodieBucketIndex extends HoodieIndex<Object, Object> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieBucketIndex.class);

  protected final int numBuckets;
  protected final List<String> indexKeyFields;

  public HoodieBucketIndex(HoodieWriteConfig config) {
    super(config);

    this.numBuckets = config.getBucketIndexNumBuckets();
    this.indexKeyFields = Arrays.asList(config.getBucketIndexHashField().split(","));
    LOG.info("Use bucket index, numBuckets = " + numBuckets + ", indexFields: " + indexKeyFields);
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses,
                                                HoodieEngineContext context,
                                                HoodieTable hoodieTable)
      throws HoodieIndexException {
    return writeStatuses;
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
      HoodieTable hoodieTable)
      throws HoodieIndexException {

    // Get bucket location mapper
    GlobalIndexLocationFunction locFunc = new GlobalIndexLocationFunction(hoodieTable);

    return records.mapPartitions(iterator ->
        new LazyIterableIterator<HoodieRecord<R>, HoodieRecord<R>>(iterator) {
          @Override
          protected HoodieRecord<R> computeNext() {
            // TODO maybe batch the operation to improve performance
            HoodieRecord record = inputItr.next();
            Option<HoodieRecordLocation> loc = locFunc.apply(record);
            return tagAsNewRecordIfNeeded(record, loc);
          }
        }, false
    );
  }

  /**
   * Global lazy-loading index location function. The index location function can be applied to a hoodie record to get its location under the partition.
   * The per-partition index location functions are cached for better performance.
   */
  class GlobalIndexLocationFunction implements Function<HoodieRecord, Option<HoodieRecordLocation>>, Serializable {

    private final HoodieTable table;
    private final Map<String/*partition path*/, Function<HoodieRecord, Option<HoodieRecordLocation>>/*location func per partition*/> partitionToIndexFunctionMap;

    public GlobalIndexLocationFunction(HoodieTable table) {
      this.table = table;
      this.partitionToIndexFunctionMap = new HashMap<>();
    }

    @Override
    public Option<HoodieRecordLocation> apply(HoodieRecord record) {
      String partitionPath = record.getPartitionPath();
      if (!partitionToIndexFunctionMap.containsKey(partitionPath)) {
        partitionToIndexFunctionMap.put(partitionPath, getIndexLocationFunctionForPartition(table, partitionPath));
      }
      return partitionToIndexFunctionMap.get(partitionPath).apply(record);
    }
  }

  /**
   * Returns the index location function for the give partition path {@code partitionPath}.
   */
  protected abstract Function<HoodieRecord, Option<HoodieRecordLocation>> getIndexLocationFunctionForPartition(HoodieTable table, String partitionPath);

  @Override
  public boolean requiresTagging(WriteOperationType operationType) {
    switch (operationType) {
      case INSERT:
      case INSERT_OVERWRITE:
      case UPSERT:
      case DELETE:
      case DELETE_PREPPED:
      case BULK_INSERT:
        return true;
      default:
        return false;
    }
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    return true;
  }

  @Override
  public boolean isGlobal() {
    return false;
  }

  @Override
  public boolean canIndexLogFiles() {
    return true;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }

  public int getNumBuckets() {
    return numBuckets;
  }
}
