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

package org.apache.hudi.table;

import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Wraps stats about a single partition path.
 */
public class WorkloadStat implements Serializable {

  private long numInserts = 0L;

  private long numUpdates = 0L;

  private HashMap<String, Pair<String, Long>> insertLocationToCount;

  private HashMap<String, Pair<String, Long>> updateLocationToCount;

  public WorkloadStat() {
    insertLocationToCount = new HashMap<>();
    updateLocationToCount = new HashMap<>();
  }

  public long addInserts(long numInserts) {
    return this.numInserts += numInserts;
  }

  public long addInserts(HoodieRecordLocation location, long numInserts) {
    long accNumInserts = 0;
    if (insertLocationToCount.containsKey(location.getFileId())) {
      accNumInserts = insertLocationToCount.get(location.getFileId()).getRight();
    }
    insertLocationToCount.put(
        location.getFileId(),
        Pair.of(location.getInstantTime(), numInserts + accNumInserts));
    return this.numInserts += numInserts;
  }

  public long addUpdates(HoodieRecordLocation location, long numUpdates) {
    long accNumUpdates = 0;
    if (updateLocationToCount.containsKey(location.getFileId())) {
      accNumUpdates = updateLocationToCount.get(location.getFileId()).getRight();
    }
    updateLocationToCount.put(
        location.getFileId(),
        Pair.of(location.getInstantTime(), numUpdates + accNumUpdates));
    return this.numUpdates += numUpdates;
  }

  public long getNumUpdates() {
    return numUpdates;
  }

  public long getNumInserts() {
    return numInserts;
  }

  public HashMap<String, Pair<String, Long>> getUpdateLocationToCount() {
    return updateLocationToCount;
  }

  public HashMap<String, Pair<String, Long>> getInsertLocationToCount() {
    return insertLocationToCount;
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
