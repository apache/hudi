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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.MarkerFiles;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Downgrade handle to assist in downgrading hoodie table from version 2 to 1.
 */
public class TwoToOneDowngradeHandler implements DowngradeHandler {

  @Override
  public void downgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime) {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(context.getHadoopConf().get()).setBasePath(config.getBasePath())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion()))).build();
    Properties properties = metaClient.getTableConfig().getProps();
    properties.remove(HoodieTableConfig.HOODIE_TABLE_PARTITION_FIELDS_PROP.key());
    properties.remove(HoodieTableConfig.HOODIE_TABLE_RECORDKEY_FIELDS.key());
    properties.remove(HoodieTableConfig.HOODIE_BASE_FILE_FORMAT_PROP.key());

    // serialize the updated properties
    // since the metaclient object is not shared between AbstractUpgradeDownGrade, we have to serialize the properties here
    Path propertyFile = new Path(metaClient.getMetaPath() + "/" + HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    try (FSDataOutputStream os = metaClient.getFs().create(propertyFile)) {
      properties.store(os, "");
    } catch (IOException e) {
      throw new HoodieIOException("Updating hoodie.properties file to remove some of the new props failed ", e);
    }
  }
}