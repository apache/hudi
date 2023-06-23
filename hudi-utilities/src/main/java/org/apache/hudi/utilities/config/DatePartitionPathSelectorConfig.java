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

package org.apache.hudi.utilities.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

/**
 * Date Partition Path Selector Configs
 */
@Immutable
@ConfigClassProperty(name = "Date Partition Path Selector Configs",
    groupName = ConfigGroups.Names.DELTA_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling the behavior of date partition path selector "
        + "for DFS source in Deltastreamer.")
public class DatePartitionPathSelectorConfig extends HoodieConfig {

  public static final ConfigProperty<String> DATE_FORMAT = ConfigProperty
      .key("hoodie.deltastreamer.source.dfs.datepartitioned.date.format")
      .defaultValue("yyyy-MM-dd")
      .markAdvanced()
      .withDocumentation("Date format.");

  public static final ConfigProperty<Integer> DATE_PARTITION_DEPTH = ConfigProperty
      .key("hoodie.deltastreamer.source.dfs.datepartitioned.selector.depth")
      .defaultValue(0)
      .markAdvanced()
      .withDocumentation("Depth of the files to scan. 0 implies no (date) partition.");

  public static final ConfigProperty<Integer> LOOKBACK_DAYS = ConfigProperty
      .key("hoodie.deltastreamer.source.dfs.datepartitioned.selector.lookback.days")
      .defaultValue(2)
      .markAdvanced()
      .withDocumentation("The maximum look-back days for scanning.");

  public static final ConfigProperty<String> CURRENT_DATE = ConfigProperty
      .key("hoodie.deltastreamer.source.dfs.datepartitioned.selector.currentdate")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Current date.");

  public static final ConfigProperty<Integer> PARTITIONS_LIST_PARALLELISM = ConfigProperty
      .key("hoodie.deltastreamer.source.dfs.datepartitioned.selector.parallelism")
      .defaultValue(20)
      .markAdvanced()
      .withDocumentation("Parallelism for listing partitions.");
}
