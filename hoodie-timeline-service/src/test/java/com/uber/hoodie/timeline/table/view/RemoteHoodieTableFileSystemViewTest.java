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

package com.uber.hoodie.timeline.table.view;

import com.uber.hoodie.common.SerializableConfiguration;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.SyncableFileSystemView;
import com.uber.hoodie.common.table.view.FileSystemViewManager;
import com.uber.hoodie.common.table.view.FileSystemViewStorageConfig;
import com.uber.hoodie.common.table.view.FileSystemViewStorageType;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemViewTest;
import com.uber.hoodie.common.table.view.RemoteHoodieTableFileSystemView;
import com.uber.hoodie.timeline.service.TimelineService;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Bring up a remote Timeline Server and run all test-cases of HoodieTableFileSystemViewTest against it
 */
public class RemoteHoodieTableFileSystemViewTest extends HoodieTableFileSystemViewTest {

  private static Logger log = LogManager.getLogger(RemoteHoodieTableFileSystemViewTest.class);

  private TimelineService server;
  private RemoteHoodieTableFileSystemView view;

  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline) {
    FileSystemViewStorageConfig sConf = FileSystemViewStorageConfig.newBuilder()
        .withStorageType(FileSystemViewStorageType.SPILLABLE_DISK).build();
    try {
      server =
          new TimelineService(0,
              FileSystemViewManager.createViewManager(new SerializableConfiguration(metaClient.getHadoopConf()),
                  sConf));
      server.startService();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    log.info("Connecting to Timeline Server :" + server.getServerPort());
    view = new RemoteHoodieTableFileSystemView("localhost", server.getServerPort(), metaClient);
    return view;
  }
}
