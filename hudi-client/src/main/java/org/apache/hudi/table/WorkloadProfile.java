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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;

import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import scala.Tuple2;

/**
 * Information about incoming records for upsert/insert obtained either via sampling or introspecting the data fully.
 * <p>
 * TODO(vc): Think about obtaining this directly from index.tagLocation
 */
public class WorkloadProfile<T extends HoodieRecordPayload> implements Serializable {

  /**
   * Input workload.
   */
  private final JavaRDD<HoodieRecord<T>> taggedRecords;

  /**
   * Computed workload profile.
   */
  private final HashMap<String, WorkloadStat> partitionPathStatMap;


  private final WorkloadStat globalStat;

  public WorkloadProfile(JavaRDD<HoodieRecord<T>> taggedRecords) {
    this.taggedRecords = taggedRecords;
    this.partitionPathStatMap = new HashMap<>();
    this.globalStat = new WorkloadStat();
    buildProfile();
  }

  private void buildProfile() {

    Map<Tuple2<String, Option<HoodieRecordLocation>>, Long> partitionLocationCounts = taggedRecords
        .mapToPair(record -> new Tuple2<>(
            new Tuple2<>(record.getPartitionPath(), Option.ofNullable(record.getCurrentLocation())), record))
        .countByKey();

    for (Map.Entry<Tuple2<String, Option<HoodieRecordLocation>>, Long> e : partitionLocationCounts.entrySet()) {
      String partitionPath = e.getKey()._1();
      Long count = e.getValue();
      Option<HoodieRecordLocation> locOption = e.getKey()._2();

      if (!partitionPathStatMap.containsKey(partitionPath)) {
        partitionPathStatMap.put(partitionPath, new WorkloadStat());
      }

      if (locOption.isPresent()) {
        // update
        partitionPathStatMap.get(partitionPath).addUpdates(locOption.get(), count);
        globalStat.addUpdates(locOption.get(), count);
      } else {
        // insert
        partitionPathStatMap.get(partitionPath).addInserts(count);
        globalStat.addInserts(count);
      }
    }
  }

  public WorkloadStat getGlobalStat() {
    return globalStat;
  }

  public Set<String> getPartitionPaths() {
    return partitionPathStatMap.keySet();
  }

  public WorkloadStat getWorkloadStat(String partitionPath) {
    return partitionPathStatMap.get(partitionPath);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("WorkloadProfile {");
    sb.append("globalStat=").append(globalStat).append(", ");
    sb.append("partitionStat=").append(partitionPathStatMap);
    sb.append('}');
    return sb.toString();
  }
}
