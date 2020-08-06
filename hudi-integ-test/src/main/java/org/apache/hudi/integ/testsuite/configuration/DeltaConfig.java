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

package org.apache.hudi.integ.testsuite.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.integ.testsuite.reader.DeltaInputType;
import org.apache.hudi.integ.testsuite.writer.DeltaOutputMode;

/**
 * Configuration to hold the delta output type and delta input format.
 */
public class DeltaConfig implements Serializable {

  private final DeltaOutputMode deltaOutputMode;
  private final DeltaInputType deltaInputType;
  private final SerializableConfiguration configuration;

  public DeltaConfig(DeltaOutputMode deltaOutputMode, DeltaInputType deltaInputType,
      SerializableConfiguration configuration) {
    this.deltaOutputMode = deltaOutputMode;
    this.deltaInputType = deltaInputType;
    this.configuration = configuration;
  }

  public DeltaOutputMode getDeltaOutputMode() {
    return deltaOutputMode;
  }

  public DeltaInputType getDeltaInputType() {
    return deltaInputType;
  }

  public Configuration getConfiguration() {
    return configuration.get();
  }

  /**
   * Represents any kind of workload operation for new data. Each workload also contains a set of Option sequence of
   * actions that can be executed in parallel.
   */
  public static class Config {

    public static final String CONFIG_NAME = "config";
    public static final String TYPE = "type";
    public static final String NODE_NAME = "name";
    public static final String DEPENDENCIES = "deps";
    public static final String CHILDREN = "children";
    public static final String HIVE_QUERIES = "hive_queries";
    public static final String HIVE_PROPERTIES = "hive_props";
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

    private Map<String, Object> configsMap;

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
        return (List<Pair<String, Integer>>) this.configsMap.getOrDefault("hive_queries", new ArrayList<>());
      } catch (Exception e) {
        throw new RuntimeException("unable to get hive queries from configs");
      }
    }

    public boolean isHiveLocal() {
      return Boolean.valueOf(configsMap.getOrDefault(HIVE_LOCAL, false).toString());
    }

    public List<String> getHiveProperties() {
      return (List<String>) this.configsMap.getOrDefault(HIVE_PROPERTIES, new ArrayList<>());
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

      public Builder withHiveQueryAndResults(List<Pair<String, Integer>> hiveQueries) {
        this.configsMap.put(HIVE_QUERIES, hiveQueries);
        return this;
      }

      public Builder withHiveLocal(boolean startHiveLocal) {
        this.configsMap.put(HIVE_LOCAL, startHiveLocal);
        return this;
      }

      public Builder withHiveProperties(List<String> hiveProperties) {
        this.configsMap.put(HIVE_PROPERTIES, hiveProperties);
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
