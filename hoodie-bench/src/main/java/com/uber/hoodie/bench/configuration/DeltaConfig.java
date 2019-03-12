/*
 *  Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.bench.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.uber.hoodie.bench.DeltaInputFormat;
import com.uber.hoodie.bench.DeltaSinkType;
import com.uber.hoodie.common.SerializableConfiguration;
import com.uber.hoodie.common.util.collection.Pair;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * Configuration to hold the delta sink type and delta format
 */
public class DeltaConfig implements Serializable {

  private final DeltaSinkType deltaSinkType;
  private final DeltaInputFormat deltaInputFormat;
  private final SerializableConfiguration configuration;

  public DeltaConfig(DeltaSinkType deltaSinkType, DeltaInputFormat deltaInputFormat,
      SerializableConfiguration configuration) {
    this.deltaSinkType = deltaSinkType;
    this.deltaInputFormat = deltaInputFormat;
    this.configuration = configuration;
  }

  public DeltaSinkType getDeltaSinkType() {
    return deltaSinkType;
  }

  public DeltaInputFormat getDeltaInputFormat() {
    return deltaInputFormat;
  }

  public Configuration getConfiguration() {
    return configuration.get();
  }

  /**
   * Represents any kind of workload operation for new data. Each workload also contains a set of optional sequence of
   * actions that can be executed in parallel.
   */
  public static class Config {

    public static final String CONFIG_NAME = "config";
    public static final String TYPE = "type";
    public static final String NODE_NAME = "name";
    public static final String DEPENDENCIES = "deps";
    public static final String CHILDREN = "children";
    public static final String HIVE_QUERIES = "hive_queries";
    private static String NUM_RECORDS_INSERT = "num_records_insert";
    private static String NUM_RECORDS_UPSERT = "num_records_upsert";
    private static String REPEAT_COUNT = "repeat_count";
    private static String RECORD_SIZE = "record_size";
    private static String NUM_PARTITIONS_INSERT = "num_partitions_insert";
    private static String NUM_PARTITIONS_UPSERT = "num_partitions_upsert";
    private static String NUM_FILES_UPSERT = "num_files_upsert";
    private static String FRACTION_UPSERT_PER_FILE = "fraction_upsert_per_file";
    private static String DISABLE_GENERATE = "disable_generate";
    private static String DISABLE_INGEST = "disable_ingest";
    private static String HIVE_LOCAL = "hive_local";
    private static String HIVE_QUEUE_NAME = "queue_name";
    private static String HIVE_EXECUTION_ENGINE = "query_engine";

    private Map<String, Object> configsMap;

    @VisibleForTesting
    public Config(Map<String, Object> configsMap) {
      this.configsMap = configsMap;
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public long getNumRecordsInsert() {
      return Long.valueOf(configsMap.getOrDefault(NUM_RECORDS_INSERT, 0).toString());
    }

    public long getNumRecordsUpsert() {
      return Long.valueOf(configsMap.getOrDefault(NUM_RECORDS_UPSERT, 0).toString());
    }

    public int getRecordSize() {
      return Integer.valueOf(configsMap.getOrDefault(RECORD_SIZE, 1024).toString());
    }

    public int getNumInsertPartitions() {
      return Integer.valueOf(configsMap.getOrDefault(NUM_PARTITIONS_INSERT, 1).toString());
    }

    public int getRepeatCount() {
      return Integer.valueOf(configsMap.getOrDefault(REPEAT_COUNT, 1).toString());
    }

    public int getNumUpsertPartitions() {
      return Integer.valueOf(configsMap.getOrDefault(NUM_PARTITIONS_UPSERT, 0).toString());
    }

    public int getNumUpsertFiles() {
      return Integer.valueOf(configsMap.getOrDefault(NUM_FILES_UPSERT, 1).toString());
    }

    public double getFractionUpsertPerFile() {
      return Double.valueOf(configsMap.getOrDefault(FRACTION_UPSERT_PER_FILE, 0.0).toString());
    }

    public boolean isDisableGenerate() {
      return Boolean.valueOf(configsMap.getOrDefault(DISABLE_GENERATE, false).toString());
    }

    public boolean isDisableIngest() {
      return Boolean.valueOf(configsMap.getOrDefault(DISABLE_INGEST, false).toString());
    }

    public Map<String, Object> getOtherConfigs() {
      if (configsMap == null) {
        return new HashMap<>();
      }
      return configsMap;
    }

    public List<Pair<String, Integer>> getHiveQueries() {
      try {
        System.out.println(this.configsMap);
        return (List<Pair<String, Integer>>) this.configsMap.getOrDefault("hive_queries", new ArrayList<>());
      } catch (Exception e) {
        throw new RuntimeException("unable to get hive queries from configs");
      }
    }

    public boolean isHiveLocal() {
      return Boolean.valueOf(configsMap.getOrDefault(HIVE_LOCAL, false).toString());
    }

    public String getHiveQueueName() {
      Object name = configsMap.getOrDefault(HIVE_QUEUE_NAME, null);
      return name == null ? null : (String) name;
    }

    public String getHiveExecutionEngine() {
      Object name = configsMap.getOrDefault(HIVE_EXECUTION_ENGINE, null);
      return name == null ? null : (String) name;
    }


    @Override
    public String toString() {
      try {
        return new ObjectMapper().writeValueAsString(this.configsMap);
      } catch (Exception e) {
        throw new RuntimeException("unable to generate string representation of config");
      }
    }

    public static class Builder {

      private Map<String, Object> configsMap = new HashMap<>();

      public Builder() {
      }

      public Builder withNumRecordsToInsert(long numRecordsInsert) {
        this.configsMap.put(NUM_RECORDS_INSERT, numRecordsInsert);
        return this;
      }

      public Builder withNumRecordsToUpdate(long numRecordsUpsert) {
        this.configsMap.put(NUM_RECORDS_UPSERT, numRecordsUpsert);
        return this;
      }

      public Builder withNumInsertPartitions(int numInsertPartitions) {
        this.configsMap.put(NUM_PARTITIONS_INSERT, numInsertPartitions);
        return this;
      }

      public Builder withNumUpsertPartitions(int numUpsertPartitions) {
        this.configsMap.put(NUM_PARTITIONS_UPSERT, numUpsertPartitions);
        return this;
      }

      public Builder withNumUpsertFiles(int numUpsertFiles) {
        this.configsMap.put(NUM_FILES_UPSERT, numUpsertFiles);
        return this;
      }

      public Builder withFractionUpsertPerFile(double fractionUpsertPerFile) {
        this.configsMap.put(FRACTION_UPSERT_PER_FILE, fractionUpsertPerFile);
        return this;
      }

      public Builder withNumTimesToRepeat(int repeatCount) {
        this.configsMap.put(REPEAT_COUNT, repeatCount);
        return this;
      }

      public Builder withRecordSize(int recordSize) {
        this.configsMap.put(RECORD_SIZE, recordSize);
        return this;
      }

      public Builder disableGenerate(boolean generate) {
        this.configsMap.put(DISABLE_GENERATE, generate);
        return this;
      }

      public Builder disableIngest(boolean ingest) {
        this.configsMap.put(DISABLE_INGEST, ingest);
        return this;
      }


      public Builder withConfig(String name, Object value) {
        this.configsMap.put(name, value);
        return this;
      }

      public Builder withHiveQueryAndResults(List<Pair<String, Integer>> hiveQueryConfig) {
        this.configsMap.put(HIVE_QUERIES, hiveQueryConfig);
        return this;
      }

      public Builder withHiveLocal(boolean startHiveLocal) {
        this.configsMap.put(HIVE_LOCAL, startHiveLocal);
        return this;
      }

      public Builder withHiveQueueName(String queueName) {
        this.configsMap.put(HIVE_QUEUE_NAME, queueName);
        return this;
      }

      public Builder withHiveExecutionEngine(String executionEngine) {
        this.configsMap.put(HIVE_EXECUTION_ENGINE, executionEngine);
        return this;
      }

      public Builder withConfigsMap(Map<String, Object> configsMap) {
        this.configsMap = configsMap;
        return this;
      }

      public Config build() {
        return new Config(configsMap);
      }

    }
  }
}
