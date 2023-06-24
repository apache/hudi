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

package org.apache.hudi.hive.replication;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.hive.HiveSyncConfig;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.concurrent.Immutable;

import java.util.Properties;

@Immutable
@ConfigClassProperty(name = "Global Hive Sync Configs",
    groupName = ConfigGroups.Names.META_SYNC,
    description = "Global replication configurations used by the Hudi to sync metadata to Hive Metastore.")
public class GlobalHiveSyncConfig extends HiveSyncConfig {

  public static final ConfigProperty<String> META_SYNC_GLOBAL_REPLICATE_TIMESTAMP = ConfigProperty
      .key("hoodie.meta_sync.global.replicate.timestamp")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("");

  public GlobalHiveSyncConfig(Properties props, Configuration hadoopConf) {
    super(props, hadoopConf);
  }

  public static class GlobalHiveSyncConfigParams {

    @ParametersDelegate()
    public final HiveSyncConfigParams hiveSyncConfigParams = new HiveSyncConfigParams();

    @Parameter(names = {"--replicated-timestamp"}, description = "Add globally replicated timestamp to enable consistent reads across clusters")
    public String globallyReplicatedTimeStamp;

    public boolean isHelp() {
      return hiveSyncConfigParams.isHelp();
    }

    public TypedProperties toProps() {
      final TypedProperties props = hiveSyncConfigParams.toProps();
      props.setPropertyIfNonNull(META_SYNC_GLOBAL_REPLICATE_TIMESTAMP.key(), globallyReplicatedTimeStamp);
      return props;
    }
  }

}
