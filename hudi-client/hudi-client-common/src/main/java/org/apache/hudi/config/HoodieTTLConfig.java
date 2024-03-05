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

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.table.action.ttl.strategy.PartitionTTLStrategyType;

import javax.annotation.concurrent.Immutable;

import java.util.Properties;

/**
 * Hoodie Configs for partition/record level ttl management.
 */
@Immutable
@ConfigClassProperty(name = "TTL management Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Data ttl management")
public class HoodieTTLConfig extends HoodieConfig {

  public static final String PARTITION_TTL_STRATEGY_PARAM_PREFIX = "hoodie.partition.ttl.strategy.";

  public static final String KEEP_BY_TIME_PARTITION_TTL_STRATEGY =
      "org.apache.hudi.table.action.ttl.strategy.KeepByTimeStrategy";
  public static final ConfigProperty<Boolean> INLINE_PARTITION_TTL = ConfigProperty
      .key("hoodie.partition.ttl.inline")
      .defaultValue(false)
      .sinceVersion("1.0.0")
      .markAdvanced()
      .withDocumentation("When enabled, the partition ttl management service is invoked immediately after each commit, "
          + "to delete exipired partitions");

  public static final ConfigProperty<String> PARTITION_TTL_STRATEGY_CLASS_NAME = ConfigProperty
      .key("hoodie.partition.ttl.strategy.class")
      .noDefaultValue()
      .sinceVersion("1.0.0")
      .markAdvanced()
      .withDocumentation("Config to provide a strategy class (subclass of PartitionTTLStrategy) to get the expired partitions");

  public static final ConfigProperty<String> PARTITION_TTL_STRATEGY_TYPE = ConfigProperty
      .key("hoodie.partition.ttl.management.strategy.type")
      .defaultValue(PartitionTTLStrategyType.KEEP_BY_TIME.name())
      .sinceVersion("1.0.0")
      .markAdvanced()
      .withDocumentation("Partition ttl management strategy type to determine the strategy class");

  public static final ConfigProperty<Integer> DAYS_RETAIN = ConfigProperty
      .key(PARTITION_TTL_STRATEGY_PARAM_PREFIX + "days.retain")
      .defaultValue(-1)
      .sinceVersion("1.0.0")
      .markAdvanced()
      .withDocumentation("Partition ttl management KEEP_BY_TIME strategy days retain");

  public static final ConfigProperty<String> PARTITION_SELECTED = ConfigProperty
      .key(PARTITION_TTL_STRATEGY_PARAM_PREFIX + "partition.selected")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Partitions to manage ttl");

  public static final ConfigProperty<Integer> MAX_PARTITION_TO_DELETE = ConfigProperty
      .key(PARTITION_TTL_STRATEGY_PARAM_PREFIX + "max.delete.partitions")
      .defaultValue(1000)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("max partitions to delete in partition ttl management");

  public static class Builder {
    private final HoodieTTLConfig ttlConfig = new HoodieTTLConfig();

    public HoodieTTLConfig.Builder withTTLPartitionSelected(String partitionSelected) {
      ttlConfig.setValue(PARTITION_SELECTED, partitionSelected);
      return this;
    }

    public HoodieTTLConfig.Builder withTTLDaysRetain(Integer daysRetain) {
      ttlConfig.setValue(DAYS_RETAIN, daysRetain.toString());
      return this;
    }

    public HoodieTTLConfig.Builder enableInlinePartitionTTL(Boolean enable) {
      ttlConfig.setValue(INLINE_PARTITION_TTL, enable.toString());
      return this;
    }

    public HoodieTTLConfig.Builder withTTLStrategyClass(String clazz) {
      ttlConfig.setValue(PARTITION_TTL_STRATEGY_CLASS_NAME, clazz);
      return this;
    }

    public HoodieTTLConfig.Builder withTTLStrategyType(PartitionTTLStrategyType ttlStrategyType) {
      ttlConfig.setValue(PARTITION_TTL_STRATEGY_TYPE, ttlStrategyType.name());
      return this;
    }

    public HoodieTTLConfig.Builder fromProperties(Properties props) {
      this.ttlConfig.getProps().putAll(props);
      return this;
    }

    public HoodieTTLConfig build() {
      ttlConfig.setDefaults(HoodieTTLConfig.class.getName());
      return ttlConfig;
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

}
