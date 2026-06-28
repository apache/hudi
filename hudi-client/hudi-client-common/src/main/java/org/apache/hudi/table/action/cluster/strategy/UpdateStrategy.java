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

package org.apache.hudi.table.action.cluster.strategy;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.table.HoodieTable;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 * When file groups in clustering, write records to these file group need to check.
 */
public abstract class UpdateStrategy<T, I> implements Serializable {

  protected final transient HoodieEngineContext engineContext;
  protected HoodieTable table;
  protected Set<HoodieFileGroupId> fileGroupsInPendingClustering;
  protected Set<HoodieFileGroupId> fileGroupsToBeReplaced;

  public UpdateStrategy(HoodieEngineContext engineContext, HoodieTable table,
                        Set<HoodieFileGroupId> fileGroupsInPendingClustering,
                        Set<HoodieFileGroupId> fileGroupsToBeReplaced) {
    this.engineContext = engineContext;
    this.table = table;
    this.fileGroupsInPendingClustering = fileGroupsInPendingClustering;
    this.fileGroupsToBeReplaced = fileGroupsToBeReplaced;
  }

  /**
   * Backward-compatible 3-arg constructor for custom {@code hoodie.clustering.updates.strategy}
   * classes that pre-date the addition of {@code fileGroupsToBeReplaced}. Delegates to the 4-arg
   * form with an empty replaced set so the existing reflection lookup keeps working.
   */
  public UpdateStrategy(HoodieEngineContext engineContext, HoodieTable table,
                        Set<HoodieFileGroupId> fileGroupsInPendingClustering) {
    this(engineContext, table, fileGroupsInPendingClustering, Collections.emptySet());
  }

  /**
   * Check the update records to the file group in clustering.
   * @param taggedRecordsRDD the records to write, tagged with target file id,
   *                         future can update tagged records location to a different fileId.
   * @return the recordsRDD strategy updated and a set of file groups to be updated while pending clustering.
   */
  public abstract Pair<I, Set<HoodieFileGroupId>> handleUpdate(I taggedRecordsRDD);

}
