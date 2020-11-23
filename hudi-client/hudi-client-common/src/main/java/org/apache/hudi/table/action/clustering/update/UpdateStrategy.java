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

package org.apache.hudi.table.action.clustering.update;

import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.table.WorkloadProfile;

import java.util.List;

/**
 * When file groups in clustering, write records to these file group need to check.
 */
public interface UpdateStrategy  {

  /**
   * check the update records to the file group in clustering.
   * @param fileGroupsInPendingClustering
   * @param workloadProfile workloadProfile have the records update info,
   *                       just like BaseSparkCommitActionExecutor.getUpsertPartitioner use it.
   */
  void apply(List<Pair<HoodieFileGroupId, HoodieInstant>> fileGroupsInPendingClustering, WorkloadProfile workloadProfile);

}
