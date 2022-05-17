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

package org.apache.hudi.table.storage;

import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.Set;

/**
 * Storage layout when using bucket index. Data distribution and files organization are in a specific way.
 */
public class HoodieSimpleBucketLayout extends HoodieStorageLayout {

  public static final Set<WriteOperationType> SUPPORTED_OPERATIONS = CollectionUtils.createImmutableSet(
      WriteOperationType.INSERT,
      WriteOperationType.INSERT_PREPPED,
      WriteOperationType.UPSERT,
      WriteOperationType.UPSERT_PREPPED,
      WriteOperationType.INSERT_OVERWRITE,
      WriteOperationType.DELETE,
      WriteOperationType.COMPACT,
      WriteOperationType.DELETE_PARTITION
  );

  public HoodieSimpleBucketLayout(HoodieWriteConfig config) {
    super(config);
  }

  /**
   * Bucketing controls the number of file groups directly.
   */
  @Override
  public boolean determinesNumFileGroups() {
    return true;
  }

  @Override
  public Option<String> layoutPartitionerClass() {
    return config.contains(HoodieLayoutConfig.LAYOUT_PARTITIONER_CLASS_NAME)
        ? Option.of(config.getString(HoodieLayoutConfig.LAYOUT_PARTITIONER_CLASS_NAME.key()))
        : Option.empty();
  }

  @Override
  public boolean writeOperationSupported(WriteOperationType operationType) {
    return SUPPORTED_OPERATIONS.contains(operationType);
  }
}
