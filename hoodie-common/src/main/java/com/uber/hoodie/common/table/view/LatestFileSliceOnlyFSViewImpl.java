/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.table.view;

import com.uber.hoodie.common.model.HoodieFileGroup;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;

/**
 * A file-system view implementataion containing only the latest file-slice for each file-group
 */
public class LatestFileSliceOnlyFSViewImpl extends HoodieTableFileSystemView implements
    SealedLatestVersionOnlyFSView {

  // State to track when the cache is fully populated.
  private boolean sealed = false;
  private final boolean errorIfSealBroken;

  public LatestFileSliceOnlyFSViewImpl(HoodieTableMetaClient metaClient,
      HoodieTimeline visibleActiveTimeline, boolean errorIfSealBroken) {
    super(metaClient, visibleActiveTimeline);
    this.errorIfSealBroken = errorIfSealBroken;
  }

  public LatestFileSliceOnlyFSViewImpl(HoodieTableMetaClient metaClient,
      HoodieTimeline visibleActiveTimeline, FileStatus[] fileStatuses, boolean errorIfSealBroken) {
    super(metaClient, visibleActiveTimeline, fileStatuses);
    this.errorIfSealBroken = errorIfSealBroken;
  }

  protected HoodieFileGroup preProcessFileGroup(HoodieFileGroup group, boolean isCompactionPending) {
    HoodieFileGroup newGroup = new HoodieFileGroup(group.getPartitionPath(), group.getId(), group.getTimeline());
    // We allow latest 2 file-slices to be added to create a complete file-version here
    int maxNumberOfFileSlicesAdded = isCompactionPending ? 2 : 1;
    group.getAllFileSlices().limit(maxNumberOfFileSlicesAdded).forEach(fs -> {
      newGroup.addNewFileSliceAtInstant(fs.getBaseInstantTime());
      if (fs.getDataFile().isPresent()) {
        newGroup.addDataFile(fs.getDataFile().get());
      }
      fs.getLogFiles().forEach(lf -> {
        newGroup.addLogFile(lf);
      });
    });
    return newGroup;
  }

  protected List<HoodieFileGroup> addFilesToView(FileStatus[] statuses) {
    if (!sealed || !errorIfSealBroken) {
      return super.addFilesToView(statuses);
    }
    throw new IllegalStateException("Internal Exception : Files can not be added after this view is sealed."
        + "Trying to add statuses :" + Arrays.toString(statuses));
  }

  public void seal() {
    sealed = true;
  }
}