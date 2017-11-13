/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.table;

import com.uber.hoodie.common.model.HoodieRecordLocation;
import java.io.Serializable;
import java.util.HashMap;

/**
 * Wraps stats about a single partition path.
 */
public class WorkloadStat implements Serializable {

  private long numInserts = 0L;

  private long numUpdates = 0L;

  private HashMap<String, Long> updateLocationToCount;

  public WorkloadStat() {
    updateLocationToCount = new HashMap<>();
  }

  long addInserts(long numInserts) {
    return this.numInserts += numInserts;
  }

  long addUpdates(HoodieRecordLocation location, long numUpdates) {
    updateLocationToCount.put(location.getFileId(), numUpdates);
    return this.numUpdates += numUpdates;
  }

  public long getNumUpdates() {
    return numUpdates;
  }

  public long getNumInserts() {
    return numInserts;
  }

  public HashMap<String, Long> getUpdateLocationToCount() {
    return updateLocationToCount;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("WorkloadStat {");
    sb.append("numInserts=").append(numInserts).append(", ");
    sb.append("numUpdates=").append(numUpdates);
    sb.append('}');
    return sb.toString();
  }
}
