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

package org.apache.hudi.index.bloom;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import java.io.Serializable;

/**
 * Helper for {@link HoodieMetadataBloomIndex} containing engine-specific logic.
 */
public abstract class MetadataBaseHoodieBloomIndexHelper implements Serializable {
  /**
   * Build the IndexFileFilter for all the affected partitions from the column stats index.
   *
   * @param config                  - Write config
   * @param context                 - {@link HoodieEngineContext} instance to use
   * @param hoodieTable             - {@link HoodieTable} instance to use
   * @param partitionRecordKeyPairs - Partition and record key pairs
   * @return Partition and its IndexFileFilter pair data
   */
  protected abstract HoodieData<Pair<String, IndexFileFilter>> buildPartitionIndexFileFilter(
      HoodieWriteConfig config, HoodieEngineContext context, HoodieTable hoodieTable,
      HoodiePairData<String, String> partitionRecordKeyPairs);

  /**
   * Find HoodieKey and its record location for the requested keys.
   *
   * @param config                  - Write config
   * @param context                 - {@link HoodieEngineContext} instance to use
   * @param hoodieTable             - {@link HoodieTable} instance to use
   * @param partitionRecordKeyPairs - Pairs of partition path and record key
   * @param fileCandidateKeyPairs   - Pairs of filename and record key
   * @return {@link HoodiePairData} of {@link HoodieKey} and {@link HoodieRecordLocation} pairs
   */
  public abstract HoodiePairData<HoodieKey, HoodieRecordLocation> findMatchingFilesForRecordKeys(
      HoodieWriteConfig config, HoodieEngineContext context, HoodieTable hoodieTable,
      HoodiePairData<String, String> partitionRecordKeyPairs,
      HoodieData<ImmutablePair<String, HoodieKey>> fileCandidateKeyPairs);
}
