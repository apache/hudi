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

package org.apache.hudi.table.action;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.InstantFileNameParser;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieTable;

import java.io.Serializable;

public abstract class BaseActionExecutor<T, I, K, O, R> implements Serializable {

  private static final long serialVersionUID = 1L;
  protected final transient HoodieEngineContext context;
  protected final transient StorageConfiguration<?> storageConf;

  protected final HoodieWriteConfig config;

  protected final HoodieTable<T, I, K, O> table;
  protected final InstantGenerator instantGenerator;
  protected final InstantFileNameParser instantFileNameParser;
  protected final InstantFileNameGenerator instantFileNameGenerator;
  protected final String instantTime;

  public BaseActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table, String instantTime) {
    this.context = context;
    this.storageConf = context.getStorageConf();
    this.config = config;
    this.table = table;
    this.instantGenerator = table.getInstantGenerator();
    this.instantFileNameGenerator = table.getInstantFileNameGenerator();
    this.instantFileNameParser = table.getInstantFileNameParser();
    this.instantTime = instantTime;
  }

  public abstract R execute();

  /**
   * Writes commits metadata to table metadata.
   *
   * @param metadata commit metadata of interest.
   */
  protected final void writeTableMetadata(HoodieCommitMetadata metadata, String actionType) {
    // Recreate MDT for insert_overwrite_table operation.
    if (table.getConfig().isMetadataTableEnabled()
        && WriteOperationType.INSERT_OVERWRITE_TABLE == metadata.getOperationType()) {
      HoodieTableMetadataUtil.deleteMetadataTable(table.getMetaClient(), table.getContext(), false);
    }

    // MDT should be recreated if it has been deleted for insert_overwrite_table operation.
    Option<HoodieTableMetadataWriter> metadataWriterOpt = table.getMetadataWriter(instantTime);
    if (metadataWriterOpt.isPresent()) {
      try (HoodieTableMetadataWriter metadataWriter = metadataWriterOpt.get()) {
        metadataWriter.update(metadata, instantTime);
      } catch (Exception e) {
        handleMetadataException(e, "Failed to apply " + actionType + " in metadata");
      }
    }
  }

  /**
   * Writes clean metadata to table metadata.
   * @param metadata clean metadata of interest.
   */
  protected final void writeTableMetadata(HoodieCleanMetadata metadata, String instantTime) {
    Option<HoodieTableMetadataWriter> metadataWriterOpt = table.getMetadataWriter(instantTime);
    if (metadataWriterOpt.isPresent()) {
      try (HoodieTableMetadataWriter metadataWriter = metadataWriterOpt.get()) {
        metadataWriter.update(metadata, instantTime);
      } catch (Exception e) {
        handleMetadataException(e, "Failed to apply clean commit to metadata");
      }
    }
  }

  /**
   * Writes rollback metadata to table metadata.
   * @param metadata rollback metadata of interest.
   */
  protected final void writeTableMetadata(HoodieRollbackMetadata metadata) {
    Option<HoodieTableMetadataWriter> metadataWriterOpt = table.getMetadataWriter(instantTime);
    if (metadataWriterOpt.isPresent()) {
      try (HoodieTableMetadataWriter metadataWriter = metadataWriterOpt.get()) {
        metadataWriter.update(metadata, instantTime);
      } catch (Exception e) {
        handleMetadataException(e, "Failed to apply rollbacks in metadata");
      }
    }
  }

  /**
   * Writes restore metadata to table metadata.
   * @param metadata restore metadata of interest.
   */
  protected final void writeTableMetadata(HoodieRestoreMetadata metadata) {
    Option<HoodieTableMetadataWriter> metadataWriterOpt = table.getMetadataWriter(instantTime);
    if (metadataWriterOpt.isPresent()) {
      try (HoodieTableMetadataWriter metadataWriter = metadataWriterOpt.get()) {
        dropIndexOnRestore();
        metadataWriter.update(metadata, instantTime);
      } catch (Exception e) {
        handleMetadataException(e, "Failed to apply restore to metadata");
      }
    }
  }

  /**
   * Helper method to handle exceptions from metadata operations.
   */
  private void handleMetadataException(Exception e, String errorMessage) {
    if (e instanceof HoodieException) {
      throw (HoodieException) e;
    } else {
      throw new HoodieException(errorMessage, e);
    }
  }

  /**
   * Drop metadata partition, for restore operation for certain metadata partitions.
   */
  protected final void dropIndexOnRestore() {
    for (String partitionPath : table.getMetaClient().getTableConfig().getMetadataPartitions()) {
      if (MetadataPartitionType.shouldDeletePartitionOnRestore(partitionPath)) {
        // setting backup to true as this delete is part of restore operation
        HoodieTableMetadataUtil.deleteMetadataTablePartition(table.getMetaClient(), context, partitionPath, true);
      }
    }
  }
}
