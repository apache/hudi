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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import java.io.Serializable;
import java.util.List;

import static org.apache.hudi.metadata.MetadataPartitionType.FUNCTIONAL_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.SECONDARY_INDEX;

public abstract class BaseActionExecutor<T, I, K, O, R> implements Serializable {

  private static final long serialVersionUID = 1L;
  protected final transient HoodieEngineContext context;
  protected final transient StorageConfiguration<?> storageConf;

  protected final HoodieWriteConfig config;

  protected final HoodieTable<T, I, K, O> table;

  protected final String instantTime;

  public BaseActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table, String instantTime) {
    this.context = context;
    this.storageConf = context.getStorageConf();
    this.config = config;
    this.table = table;
    this.instantTime = instantTime;
  }

  public abstract R execute();

  /**
   * Writes commits metadata to table metadata.
   *
   * @param metadata commit metadata of interest.
   */
  protected final void writeTableMetadata(HoodieCommitMetadata metadata, HoodieData<WriteStatus> writeStatus, String operationType) {
    Option<HoodieTableMetadataWriter> metadataWriterOpt = table.getMetadataWriter(instantTime);
    if (metadataWriterOpt.isPresent()) {
      try (HoodieTableMetadataWriter metadataWriter = metadataWriterOpt.get()) {
        if (operationType.equals("restore")) {
          dropIndexInfo();
        } else {
          metadataWriter.updateFromWriteStatuses(metadata, writeStatus, instantTime);
        }
      } catch (Exception e) {
        if (e instanceof HoodieException) {
          throw (HoodieException) e;
        } else {
          throw new HoodieException("Failed to update metadata", e);
        }
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
        if (e instanceof HoodieException) {
          throw (HoodieException) e;
        } else {
          throw new HoodieException("Failed to apply clean commit to metadata", e);
        }
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
        if (e instanceof HoodieException) {
          throw (HoodieException) e;
        } else {
          throw new HoodieException("Failed to apply rollbacks in metadata", e);
        }
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
        metadataWriter.update(metadata, instantTime);
      } catch (Exception e) {
        if (e instanceof HoodieException) {
          throw (HoodieException) e;
        } else {
          throw new HoodieException("Failed to apply restore to metadata", e);
        }
      }
    }
  }

  /**
   * Drop indexes information, for e.g., restore operation.
   */
  protected final void dropIndexInfo() {
    StoragePath metadataTableBasePath =
        HoodieTableMetadata.getMetadataTableBasePath(table.getMetaClient().getBasePath());
    List<String> partitionPaths = FSUtils.getAllPartitionPaths(context, table.getStorage(), metadataTableBasePath, false);
    for (String partitionPath : partitionPaths) {
      if (FUNCTIONAL_INDEX != MetadataPartitionType.fromPartitionPath(partitionPath)
          && SECONDARY_INDEX != MetadataPartitionType.fromPartitionPath(partitionPath)) {
        HoodieTableMetadataUtil.deleteMetadataTablePartition(
            table.getMetaClient(), context, partitionPath, true);
      } else {
        // Delete records in functional and secondary indexes, but keep the definitions.
      }
    }
  }
}
