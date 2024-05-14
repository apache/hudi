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
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.timeline.service.TimelineService;

import java.io.IOException;

public abstract class Handler {

  protected final StorageConfiguration<?> conf;
  protected final TimelineService.Config timelineServiceConfig;
  protected final HoodieStorage storage;
  protected final FileSystemViewManager viewManager;

  public Handler(StorageConfiguration<?> conf, TimelineService.Config timelineServiceConfig,
                 HoodieStorage storage, FileSystemViewManager viewManager) throws IOException {
    this.conf = conf;
    this.timelineServiceConfig = timelineServiceConfig;
    this.storage = storage;
    this.viewManager = viewManager;
  }
}
