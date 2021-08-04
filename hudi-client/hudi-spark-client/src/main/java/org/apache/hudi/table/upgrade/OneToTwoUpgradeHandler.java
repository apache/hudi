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
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Properties;

/**
 * Upgrade handle to assist in upgrading hoodie table from version 1 to 2.
 */
public class OneToTwoUpgradeHandler implements UpgradeHandler {

  @Override
  public void upgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime) {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(context.getHadoopConf().get()).setBasePath(config.getBasePath())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion()))).build();
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    tableConfig.setValue(HoodieTableConfig.HOODIE_TABLE_NAME_PROP, config.getTableName());
    tableConfig.setValue(HoodieTableConfig.HOODIE_TABLE_PARTITION_FIELDS_PROP, config.getString(HoodieTableConfig.HOODIE_TABLE_PARTITION_FIELDS_PROP));
    tableConfig.setValue(HoodieTableConfig.HOODIE_TABLE_RECORDKEY_FIELDS, config.getString(HoodieTableConfig.HOODIE_TABLE_RECORDKEY_FIELDS));
    tableConfig.setValue(HoodieTableConfig.HOODIE_BASE_FILE_FORMAT_PROP, config.getString(HoodieTableConfig.HOODIE_BASE_FILE_FORMAT_PROP));

    // serialize the updated properties
    // since the metaclient object is not shared between AbstractUpgradeDownGrade, we have to serialize the properties here
    Path propertyFile = new Path(metaClient.getMetaPath() + "/" + HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    try (FSDataOutputStream os = metaClient.getFs().create(propertyFile)) {
      metaClient.getTableConfig().getProps().store(os, "");
    } catch (IOException e) {
      throw new HoodieIOException("Upgrading hoodie.properties file with new props failed ", e);
    }
  }
}
