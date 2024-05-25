/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.meta;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * A factory to generate {@link CkpMetadata} instance based on whether {@link HoodieWriteConfig#INSTANT_STATE_TIMELINE_SERVER_BASED} enabled.
 */
public class CkpMetadataFactory {
  public static CkpMetadata getCkpMetadata(HoodieWriteConfig writeConfig, Configuration conf) {
    FileSystem fs = HadoopFSUtils.getFs(conf.getString(FlinkOptions.PATH), HadoopConfigurations.getHadoopConf(conf));
    String basePath = conf.getString(FlinkOptions.PATH);
    String uniqueId = conf.getString(FlinkOptions.WRITE_CLIENT_ID);
    if (writeConfig.isEmbeddedTimelineServerEnabled() && writeConfig.isTimelineServerBasedInstantStateEnabled()) {
      return new TimelineBasedCkpMetadata(fs, basePath, uniqueId, writeConfig);
    } else {
      return new CkpMetadata(fs, basePath, uniqueId);
    }
  }
}
