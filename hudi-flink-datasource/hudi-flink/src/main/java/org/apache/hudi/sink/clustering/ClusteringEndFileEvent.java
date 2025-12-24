/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.clustering;

import org.apache.hudi.common.model.lsm.HoodieLSMLogFile;
import org.apache.hudi.common.util.collection.Pair;

import java.util.Set;

/**
 * Represents a cluster command from the clustering plan task {@link ClusteringPlanSourceFunction}.
 */
public class ClusteringEndFileEvent extends ClusteringFileEvent {
  private static final long serialVersionUID = 1L;

  public ClusteringEndFileEvent() {
  }

  public ClusteringEndFileEvent(HoodieLSMLogFile logFile, String fileID, String partitionPath, Pair<Set<String>, Set<String>> missingAndCompletedInstants) {
    super(logFile, fileID, partitionPath, null, missingAndCompletedInstants);
  }
}
