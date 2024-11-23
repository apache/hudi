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

package org.apache.hudi.common.table.timeline.versioning.v1;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.TimelinePathProvider;
import org.apache.hudi.storage.StoragePath;

/**
 * For Timeline version 1, .hoodie is the timeline folder.
 */
public class TimelinePathProviderV1 implements TimelinePathProvider {

  @Override
  public StoragePath getTimelinePath(HoodieTableConfig tableConfig, StoragePath basePath) {
    return new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
  }

  @Override
  public StoragePath getTimelineHistoryPath(HoodieTableConfig tableConfig, StoragePath basePath) {
    String archiveFolder = tableConfig.getArchivelogFolder();
    return new StoragePath(getTimelinePath(tableConfig, basePath), archiveFolder);
  }
}
