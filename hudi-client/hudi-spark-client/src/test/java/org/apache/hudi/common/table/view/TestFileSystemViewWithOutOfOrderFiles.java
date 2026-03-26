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

import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.metadata.HoodieTableMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Test utility that returns file groups in reverse order to simulate out-of-order
 * iteration from the underlying file system view.
 * <p>
 * This is used to test that code correctly handles unsorted file groups/slices,
 * particularly for RLI lookups where consistent ordering is required.
 */
public class TestFileSystemViewWithOutOfOrderFiles extends HoodieTableFileSystemView {

  public TestFileSystemViewWithOutOfOrderFiles(HoodieTableMetadata tableMetadata,
                                               HoodieTableMetaClient metaClient,
                                               HoodieTimeline visibleActiveTimeline) {
    super(tableMetadata, metaClient, visibleActiveTimeline);
  }

  @Override
  Stream<HoodieFileGroup> fetchAllStoredFileGroups(String partition) {
    final List<HoodieFileGroup> fileGroups = new ArrayList<>(partitionToFileGroupsMap.get(partition));
    // Reverse the order to simulate out-of-order iteration
    Collections.reverse(fileGroups);
    return fileGroups.stream();
  }
}
