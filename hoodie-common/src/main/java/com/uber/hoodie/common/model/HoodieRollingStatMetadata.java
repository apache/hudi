package com.uber.hoodie.common.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HoodieRollingStatMetadata implements Serializable {

  private static volatile Logger log = LogManager.getLogger(HoodieRollingStatMetadata.class);
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
        if (HoodieActiveTimeline.COMMIT_ACTION.equals(actionType)) {
          return super.remove(key);
        } else {
          long inserts = ((HoodieRollingStat) v).getInserts();
          long upserts = ((HoodieRollingStat) v).getUpserts();
          ((HoodieRollingStat) value).updateInserts(inserts);
          ((HoodieRollingStat) value).updateUpserts(upserts);
          super.put(key, value);
        }
      }
      return value;
    }
  }

  public static HoodieRollingStatMetadata fromBytes(byte[] bytes) throws IOException {
    return fromString(new String(bytes, Charset.forName("utf-8")));
  }

  public static HoodieRollingStatMetadata fromString(String jsonStr) throws IOException {
    return getObjectMapper().readValue(jsonStr, HoodieRollingStatMetadata.class);
  }

  public String toJsonString() throws IOException {
    if (partitionToRollingStats.containsKey(null)) {
      log.info("partition path is null for " + partitionToRollingStats.get(null));
      partitionToRollingStats.remove(null);
    }
    return getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  private static ObjectMapper getObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    return mapper;
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
