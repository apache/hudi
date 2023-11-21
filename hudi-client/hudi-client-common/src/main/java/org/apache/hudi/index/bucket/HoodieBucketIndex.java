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
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

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
