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

package org.apache.hudi.table.action.restore;

import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;

import java.io.IOException;

public class RestoreUtils {

  /**
   * Get Latest version of Restore plan corresponding to a restore instant.
   *
   * @param metaClient      Hoodie Table Meta Client
   * @param restoreInstant Instant referring to restore action
   * @return Rollback plan corresponding to rollback instant
   * @throws IOException
   */
  public static HoodieRestorePlan getRestorePlan(HoodieTableMetaClient metaClient, HoodieInstant restoreInstant)
      throws IOException {
    final HoodieInstant requested = HoodieTimeline.getRollbackRequestedInstant(restoreInstant);
    return TimelineMetadataUtils.deserializeAvroMetadata(
        metaClient.getActiveTimeline().readRestoreInfoAsBytes(requested).get(), HoodieRestorePlan.class);
  }
}
