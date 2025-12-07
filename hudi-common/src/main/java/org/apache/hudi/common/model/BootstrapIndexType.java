/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hudi.common.bootstrap.index.NoOpBootstrapIndex;
import org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex;
import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;
import org.apache.hudi.common.config.HoodieConfig;

import lombok.Getter;

import static org.apache.hudi.common.table.HoodieTableConfig.BOOTSTRAP_INDEX_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.BOOTSTRAP_INDEX_ENABLE;
import static org.apache.hudi.common.table.HoodieTableConfig.BOOTSTRAP_INDEX_TYPE;

@Getter
@EnumDescription("Bootstrap index type to use for mapping between skeleton and actual data files.")
public enum BootstrapIndexType {
  @EnumFieldDescription("Maintains mapping in HFile format.")
  HFILE(HFileBootstrapIndex.class.getName()),
  @EnumFieldDescription("No-op, an empty implementation.")
  NONE(NoOpBootstrapIndex.class.getName());

  private final String className;

  BootstrapIndexType(String className) {
    this.className = className;
  }

  public static BootstrapIndexType fromClassName(String className) {
    for (BootstrapIndexType type : BootstrapIndexType.values()) {
      if (type.getClassName().equals(className)) {
        return type;
      }
    }
    throw new IllegalArgumentException("No BootstrapIndexType found for class name: " + className);
  }

  public static String getBootstrapIndexClassName(HoodieConfig config) {
    if (!config.getBooleanOrDefault(BOOTSTRAP_INDEX_ENABLE)) {
      return BootstrapIndexType.NONE.getClassName();
    }
    if (config.contains(BOOTSTRAP_INDEX_CLASS_NAME)) {
      return config.getString(BOOTSTRAP_INDEX_CLASS_NAME);
    } else if (config.contains(BOOTSTRAP_INDEX_TYPE)) {
      return BootstrapIndexType.valueOf(config.getString(BOOTSTRAP_INDEX_TYPE)).getClassName();
    }
    return getDefaultBootstrapIndexClassName(config);
  }

  public static String getDefaultBootstrapIndexClassName(HoodieConfig config) {
    return getBootstrapIndexType(config).getClassName();
  }

  public static BootstrapIndexType getBootstrapIndexType(HoodieConfig config) {
    if (!config.getBooleanOrDefault(BOOTSTRAP_INDEX_ENABLE)) {
      return BootstrapIndexType.NONE;
    }
    return BootstrapIndexType.valueOf(BOOTSTRAP_INDEX_TYPE.defaultValue());
  }
}
