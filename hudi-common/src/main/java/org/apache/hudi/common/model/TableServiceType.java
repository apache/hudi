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

package org.apache.hudi.common.model;

import org.apache.hudi.common.table.timeline.HoodieTimeline;

/**
 * Supported runtime table services.
 */
public enum TableServiceType {
  ARCHIVE, COMPACT, CLUSTER, CLEAN, LOG_COMPACT;

  public String getAction() {
    switch (this) {
      case ARCHIVE:
        // for table service type completeness; there is no timeline action associated with archive
        return "NONE";
      case COMPACT:
        return HoodieTimeline.COMPACTION_ACTION;
      case CLEAN:
        return HoodieTimeline.CLEAN_ACTION;
      case CLUSTER:
        return HoodieTimeline.REPLACE_COMMIT_ACTION;
      case LOG_COMPACT:
        return HoodieTimeline.LOG_COMPACTION_ACTION;
      default:
        throw new IllegalArgumentException("Unknown table service " + this);
    }
  }
}
