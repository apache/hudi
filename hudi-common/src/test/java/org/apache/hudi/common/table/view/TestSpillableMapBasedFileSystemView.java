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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

/**
 * Tests spillable map based file system view {@link SpillableMapBasedFileSystemView}.
 */
public class TestSpillableMapBasedFileSystemView extends TestHoodieTableFileSystemView {

  @Override
  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline) {
    return new SpillableMapBasedFileSystemView(metaClient, timeline, FileSystemViewStorageConfig.newBuilder()
        // pure disk base View
        .withStorageType(FileSystemViewStorageType.SPILLABLE_DISK).withMaxMemoryForView(0L).build(),
        HoodieCommonConfig.newBuilder().build());
  }
}
