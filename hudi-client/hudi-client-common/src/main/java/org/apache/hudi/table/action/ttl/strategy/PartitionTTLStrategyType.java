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

package org.apache.hudi.table.action.ttl.strategy;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorType;

import lombok.Getter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.config.HoodieTTLConfig.PARTITION_TTL_STRATEGY_CLASS_NAME;
import static org.apache.hudi.config.HoodieTTLConfig.PARTITION_TTL_STRATEGY_TYPE;

/**
 * Types of {@link PartitionTTLStrategy}.
 */
public enum PartitionTTLStrategyType {
  KEEP_BY_TIME("org.apache.hudi.table.action.ttl.strategy.KeepByTimeStrategy"),
  KEEP_BY_CREATION_TIME("org.apache.hudi.table.action.ttl.strategy.KeepByCreationTimeStrategy");

  @Getter
  private final String className;

  PartitionTTLStrategyType(String className) {
    this.className = className;
  }

  public static PartitionTTLStrategyType fromClassName(String className) {
    for (PartitionTTLStrategyType type : PartitionTTLStrategyType.values()) {
      if (type.getClassName().equals(className)) {
        return type;
      }
    }
    throw new IllegalArgumentException("No PartitionTTLStrategyType found for class name: " + className);
  }

  public static List<String> getPartitionTTLStrategyNames() {
    List<String> names = new ArrayList<>(PartitionTTLStrategyType.values().length);
    Arrays.stream(PartitionTTLStrategyType.values())
        .forEach(x -> names.add(x.name()));
    return names;
  }

  @Nullable
  public static String getPartitionTTLStrategyClassName(HoodieConfig config) {
    if (config.contains(PARTITION_TTL_STRATEGY_CLASS_NAME)) {
      return config.getString(PARTITION_TTL_STRATEGY_CLASS_NAME);
    } else if (config.contains(PARTITION_TTL_STRATEGY_TYPE)) {
      return KeyGeneratorType.valueOf(config.getString(PARTITION_TTL_STRATEGY_TYPE)).getClassName();
    }
    return null;
  }
}
