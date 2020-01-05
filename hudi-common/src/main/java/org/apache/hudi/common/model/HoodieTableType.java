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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;

/**
 * Type of the Hoodie Table.
 * <p>
 * Currently, 2 types are supported.
 * <p>
 * COPY_ON_WRITE - Performs upserts by versioning entire files, with later versions containing newer value of a record.
 * <p>
 * MERGE_ON_READ - Speeds up upserts, by delaying merge until enough work piles up.
 * <p>
 * In the future, following might be added.
 * <p>
 * SIMPLE_LSM - A simple 2 level LSM tree.
 */
public enum HoodieTableType {

  COPY_ON_WRITE {

    @Override
    public HoodieTimeline getCommitsTimeline(HoodieTableMetaClient metaClient) {
      return metaClient.getActiveTimeline().getCommitTimeline();
    }

    @Override
    public HoodieTimeline getCommitsAndCompactionTimeline(HoodieTableMetaClient metaClient) {
      return metaClient.getActiveTimeline().getCommitTimeline();
    }

    @Override
    public HoodieTimeline getCommitTimeline(HoodieTableMetaClient metaClient) {
      return metaClient.getActiveTimeline().getCommitTimeline();
    }

    @Override
    public String getCommitActionType() {
      return HoodieActiveTimeline.COMMIT_ACTION;
    }
  },

  MERGE_ON_READ {

    @Override
    public HoodieTimeline getCommitsTimeline(HoodieTableMetaClient metaClient) {
      // We need to include the parquet files written out in delta commits
      // Include commit action to be able to start doing a MOR over a COW dataset - no
      // migration required
      return metaClient.getActiveTimeline().getCommitsTimeline();
    }

    @Override
    public HoodieTimeline getCommitsAndCompactionTimeline(HoodieTableMetaClient metaClient) {
      return metaClient.getActiveTimeline().getCommitsAndCompactionTimeline();
    }

    @Override
    public HoodieTimeline getCommitTimeline(HoodieTableMetaClient metaClient) {
      // We need to include the parquet files written out in delta commits in tagging
      return metaClient.getActiveTimeline().getCommitTimeline();
    }

    @Override
    public String getCommitActionType() {
      return HoodieActiveTimeline.DELTA_COMMIT_ACTION;
    }
  };

  public abstract HoodieTimeline getCommitsTimeline(HoodieTableMetaClient metaClient);

  public abstract HoodieTimeline getCommitsAndCompactionTimeline(HoodieTableMetaClient metaClient);

  public abstract HoodieTimeline getCommitTimeline(HoodieTableMetaClient metaClient);

  public abstract String getCommitActionType();
}
