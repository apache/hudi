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

package org.apache.hudi.testutils.providers;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public interface HoodieMetaClientProvider {

  HoodieTableMetaClient getHoodieMetaClient(StorageConfiguration<?> storageConf, String basePath, Properties props) throws IOException;

  HoodieTableMetaClient getHoodieMetaClient(StorageConfiguration<?> storageConf, String basePath, Properties props, HoodieTableType tableType) throws IOException;

  default HoodieTableFileSystemView getHoodieTableFileSystemView(
      HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline,
      List<StoragePathInfo> pathInfoList) {
    return new HoodieTableFileSystemView(metaClient, visibleActiveTimeline, pathInfoList);
  }

  default SyncableFileSystemView getFileSystemViewWithUnCommittedSlices(HoodieTableMetaClient metaClient) {
    try {
      return new HoodieTableFileSystemView(metaClient,
          metaClient.getActiveTimeline(),
          HoodieTestTable.of(metaClient).listAllBaseAndLogFiles()
      );
    } catch (IOException ioe) {
      throw new HoodieIOException("Error getting file system view", ioe);
    }
  }
}
