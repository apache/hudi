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

package org.apache.hudi.timeline.service.handlers;

import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.timeline.service.TimelineService;

public abstract class Handler {

  private static final ThreadLocal<SyncableFileSystemView> BOUND_VIEW = new ThreadLocal<>();

  protected final StorageConfiguration<?> conf;
  protected final TimelineService.Config timelineServiceConfig;
  protected final FileSystemViewManager viewManager;

  public Handler(StorageConfiguration<?> conf, TimelineService.Config timelineServiceConfig,
                 FileSystemViewManager viewManager) {
    this.conf = conf.newInstance();
    this.timelineServiceConfig = timelineServiceConfig;
    this.viewManager = viewManager;
  }

  protected HoodieStorage getStorage(String path) {
    return HoodieStorageUtils.getStorage(path, conf);
  }

  /**
   * Binds the view the current request reads until {@link #unbindView()}; the handlers of the request read this view,
   * which is the view the request holds a lease of.
   */
  public static void bindView(SyncableFileSystemView view) {
    BOUND_VIEW.set(view);
  }

  public static void unbindView() {
    BOUND_VIEW.remove();
  }

  /**
   * The view bound to the current request, or the manager's view of the table when none is bound.
   */
  protected SyncableFileSystemView viewFor(String basePath) {
    SyncableFileSystemView bound = BOUND_VIEW.get();
    return bound != null ? bound : viewManager.getFileSystemView(basePath);
  }
}
