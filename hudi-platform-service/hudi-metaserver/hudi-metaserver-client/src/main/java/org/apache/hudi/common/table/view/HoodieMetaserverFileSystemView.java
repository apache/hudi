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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.config.HoodieMetaserverConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metaserver.client.HoodieMetaserverClient;
import org.apache.hudi.metaserver.client.HoodieMetaserverClientProxy;

/**
 * TableFileSystemView Implementations based on in-memory storage and
 * is specifically for hoodie table whose metadata is stored in the hoodie metaserver.
 */
public class HoodieMetaserverFileSystemView extends HoodieTableFileSystemView {
  private final String databaseName;
  private final String tableName;

  private final HoodieMetaserverClient metaserverClient;

  public HoodieMetaserverFileSystemView(HoodieTableMetaClient metaClient,
                                        HoodieTimeline visibleActiveTimeline, HoodieMetaserverConfig config) {
    super(new FileSystemBackedTableMetadata(new HoodieLocalEngineContext(metaClient.getStorageConf()), metaClient.getTableConfig(), metaClient.getStorage(),
        metaClient.getBasePath().toString()), metaClient, visibleActiveTimeline);
    this.metaserverClient = HoodieMetaserverClientProxy.getProxy(config);
    this.databaseName = metaClient.getTableConfig().getDatabaseName();
    this.tableName = metaClient.getTableConfig().getTableName();
  }
}
