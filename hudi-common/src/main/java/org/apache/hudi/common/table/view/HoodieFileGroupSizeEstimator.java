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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.util.ObjectSizeCalculator;
import org.apache.hudi.common.util.SizeEstimator;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A size estimator that is used alongside of {@link org.apache.hudi.common.serialization.HoodieFileGroupSerializer} to
 * estimate the size of list of file groups within a {@link SpillableMapBasedFileSystemView}.
 */
class HoodieFileGroupSizeEstimator implements SizeEstimator<List<HoodieFileGroup>>, Serializable {

  @Override
  public long sizeEstimate(List<HoodieFileGroup> hoodieFileGroups) {
    if (hoodieFileGroups.isEmpty()) {
      return 0;
    }
    long sizeOfFileGroupIds = ObjectSizeCalculator.getObjectSize(hoodieFileGroups.get(0).getFileGroupId()) * hoodieFileGroups.size();
    long sizeOfFileSlices = ObjectSizeCalculator.getObjectSize(hoodieFileGroups.stream().flatMap(HoodieFileGroup::getAllFileSlices).collect(Collectors.toList()));
    return sizeOfFileSlices + sizeOfFileGroupIds;
  }
}
