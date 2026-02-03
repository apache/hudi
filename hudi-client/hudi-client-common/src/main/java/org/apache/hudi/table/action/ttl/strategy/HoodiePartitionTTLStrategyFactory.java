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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieTTLConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Locale;

import static org.apache.hudi.config.HoodieTTLConfig.PARTITION_TTL_STRATEGY_CLASS_NAME;
import static org.apache.hudi.config.HoodieTTLConfig.PARTITION_TTL_STRATEGY_TYPE;

/**
 * Factory help to create {@link PartitionTTLStrategy}.
 * <p>
 * This factory will try {@link HoodieTTLConfig#PARTITION_TTL_STRATEGY_CLASS_NAME} firstly,
 * this ensures the class prop will not be overwritten by {@link PartitionTTLStrategyType}
 */
@Slf4j
public class HoodiePartitionTTLStrategyFactory {

  public static PartitionTTLStrategy createStrategy(HoodieTable hoodieTable, TypedProperties props, String instantTime) throws IOException {
    String strategyClassName = getPartitionTTLStrategyClassName(props);
    try {
      return (PartitionTTLStrategy) ReflectionUtils.loadClass(strategyClassName,
          new Class<?>[] {HoodieTable.class, String.class}, hoodieTable, instantTime);
    } catch (Throwable e) {
      throw new IOException("Could not load partition ttl management strategy class " + strategyClassName, e);
    }
  }

  private static String getPartitionTTLStrategyClassName(TypedProperties props) {
    String strategyClassName =
        props.getString(PARTITION_TTL_STRATEGY_CLASS_NAME.key(), null);
    if (StringUtils.isNullOrEmpty(strategyClassName)) {
      String strategyType = props.getString(PARTITION_TTL_STRATEGY_TYPE.key(),
          PARTITION_TTL_STRATEGY_TYPE.defaultValue());
      PartitionTTLStrategyType strategyTypeEnum;
      try {
        strategyTypeEnum = PartitionTTLStrategyType.valueOf(strategyType.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new HoodieException("Unsupported PartitionTTLStrategy Type " + strategyType);
      }
      strategyClassName = getPartitionTTLStrategyFromType(strategyTypeEnum);
    }
    return strategyClassName;
  }

  /**
   * @param type {@link PartitionTTLStrategyType} enum.
   * @return The partition ttl management strategy class name based on the {@link PartitionTTLStrategyType}.
   */
  public static String getPartitionTTLStrategyFromType(PartitionTTLStrategyType type) {
    switch (type) {
      case KEEP_BY_TIME:
        return KeepByTimeStrategy.class.getName();
      case KEEP_BY_CREATION_TIME:
        return KeepByCreationTimeStrategy.class.getName();
      default:
        throw new HoodieException("Unsupported PartitionTTLStrategy Type " + type);
    }
  }

}
