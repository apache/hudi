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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;

/**
 * Interface that supports updating metadata for a given table, as actions complete.
 */
public interface HoodieTableMetadataWriter extends Serializable, AutoCloseable {

  void update(HoodieCommitMetadata commitMetadata, String instantTime);

  void update(HoodieCleanerPlan cleanerPlan, String instantTime);

  void update(HoodieCleanMetadata cleanMetadata, String instantTime);

  void update(HoodieRestoreMetadata restoreMetadata, String instantTime);

  void update(HoodieRollbackMetadata rollbackMetadata, String instantTime);

  /**
   * Return the timestamp of the latest instant synced to the metadata table.
   */
  Option<String> getLatestSyncedInstantTime();
}
