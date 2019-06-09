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

package com.uber.hoodie.cli.utils;

import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import java.io.IOException;
import java.util.List;

public class CommitUtil {

  public static long countNewRecords(HoodieTableMetaClient target, List<String> commitsToCatchup) throws IOException {
    long totalNew = 0;
    HoodieTimeline timeline = target.getActiveTimeline().reload().getCommitTimeline().filterCompletedInstants();
    for (String commit : commitsToCatchup) {
      HoodieCommitMetadata c = HoodieCommitMetadata.fromBytes(
          timeline.getInstantDetails(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commit)).get(),
          HoodieCommitMetadata.class);
      totalNew += c.fetchTotalRecordsWritten() - c.fetchTotalUpdateRecordsWritten();
    }
    return totalNew;
  }
}
