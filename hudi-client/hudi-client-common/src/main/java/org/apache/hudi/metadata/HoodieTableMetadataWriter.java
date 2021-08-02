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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieMetadataException;

import java.io.Serializable;

/**
 * Interface that supports updating metadata for a given table, as actions complete.
 */
public interface HoodieTableMetadataWriter extends Serializable, AutoCloseable {

  // Update the metadata table due to a COMMIT operation
  void update(HoodieCommitMetadata option, String instantTime);

  // Update the metadata table due to a CLEAN operation
  void update(HoodieCleanMetadata cleanMetadata, String instantTime);

  // Update the metadata table due to a RESTORE operation
  void update(HoodieRestoreMetadata restoreMetadata, String instantTime);

  // Update the metadata table due to a ROLLBACK operation
  void update(HoodieRollbackMetadata rollbackMetadata, String instantTime);

  /**
   * Return the timestamp of the latest instant synced to the metadata table.
   */
  Option<String> getLatestSyncedInstantTime();

  /**
   * Remove the metadata table for the dataset.
   *
   * @param basePath base path of the dataset
   * @param context
   */
  static void removeMetadataTable(String basePath, HoodieEngineContext context) {
    final String metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
    FileSystem fs = FSUtils.getFs(metadataTablePath, context.getHadoopConf().get());
    try {
      fs.delete(new Path(metadataTablePath), true);
    } catch (Exception e) {
      throw new HoodieMetadataException("Failed to remove metadata table from path " + metadataTablePath, e);
    }
  }
}
