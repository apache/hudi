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

package org.apache.hudi.common.config;

import java.util.Properties;

public class HoodieBuildConfig extends HoodieConfig {
  public static final ConfigProperty<String> PARTITION_SELECTED = ConfigProperty
      .key("hoodie.build.partition.selected")
      .noDefaultValue()
      .sinceVersion("0.13.0")
      .withDocumentation("Partitions to run build");

  public static final ConfigProperty<Integer> BUILD_BATCH_ADD_SIZE = ConfigProperty
      .key("hoodie.build.batch.add.size")
      .defaultValue(1000)
      .withDocumentation("Batch size when add records to index builder");

  public HoodieBuildConfig() {
  }

  public HoodieBuildConfig(Properties props) {
    super(props);
  }

  public int getPartitionSelected() {
    return getIntOrDefault(PARTITION_SELECTED);
  }

  public int getBatchSize() {
    return getIntOrDefault(BUILD_BATCH_ADD_SIZE);
  }
}
