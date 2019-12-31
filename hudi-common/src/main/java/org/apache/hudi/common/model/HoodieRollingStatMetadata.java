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

package org.apache.hudi.common.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class holds statistics about files belonging to a dataset.
 */
public class HoodieRollingStatMetadata implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieRollingStatMetadata.class);
  protected Map<String, Map<String, HoodieRollingStat>> partitionToRollingStats;
  private String actionType = "DUMMY_ACTION";
  public static final String ROLLING_STAT_METADATA_KEY = "ROLLING_STAT";

  public void addRollingStat(String partitionPath, HoodieRollingStat stat) {
    if (!partitionToRollingStats.containsKey(partitionPath)) {
      partitionToRollingStats.put(partitionPath, new RollingStatsHashMap<>());
    }
    partitionToRollingStats.get(partitionPath).put(stat.getFileId(), stat);
  }

  public HoodieRollingStatMetadata() {
    partitionToRollingStats = new HashMap<>();
  }

  public HoodieRollingStatMetadata(String actionType) {
    this();
    this.actionType = actionType;
  }

  class RollingStatsHashMap<K, V> extends HashMap<K, V> {

    @Override
    public V put(K key, V value) {
      V v = this.get(key);
      if (v == null) {
        super.put(key, value);
      } else if (v instanceof HoodieRollingStat) {
        long inserts = ((HoodieRollingStat) v).getInserts();
        long upserts = ((HoodieRollingStat) v).getUpserts();
        long deletes = ((HoodieRollingStat) v).getDeletes();
        ((HoodieRollingStat) value).addInserts(inserts);
        ((HoodieRollingStat) value).addUpserts(upserts);
        ((HoodieRollingStat) value).addDeletes(deletes);
        super.put(key, value);
      }
      return value;
    }
  }

  public static HoodieRollingStatMetadata fromBytes(byte[] bytes) throws IOException {
    return HoodieCommitMetadata.fromBytes(bytes, HoodieRollingStatMetadata.class);
  }

  public String toJsonString() throws IOException {
    if (partitionToRollingStats.containsKey(null)) {
      LOG.info("partition path is null for {}", partitionToRollingStats.get(null));
      partitionToRollingStats.remove(null);
    }
    return HoodieCommitMetadata.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  public HoodieRollingStatMetadata merge(HoodieRollingStatMetadata rollingStatMetadata) {
    for (Map.Entry<String, Map<String, HoodieRollingStat>> stat : rollingStatMetadata.partitionToRollingStats
        .entrySet()) {
      for (Map.Entry<String, HoodieRollingStat> innerStat : stat.getValue().entrySet()) {
        this.addRollingStat(stat.getKey(), innerStat.getValue());
      }
    }
    return this;
  }

  public Map<String, Map<String, HoodieRollingStat>> getPartitionToRollingStats() {
    return partitionToRollingStats;
  }

  public String getActionType() {
    return actionType;
  }
}
