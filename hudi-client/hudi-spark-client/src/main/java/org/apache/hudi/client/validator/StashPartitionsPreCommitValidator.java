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

package org.apache.hudi.client.validator;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A special-purpose pre-commit validator that moves partition files to a stash
 * location before a deletePartitions commit completes.
 *
 * <p><strong>WARNING: This is NOT a standard validator pattern.</strong>
 * Unlike typical pre-commit validators that are read-only checks, this validator
 * performs destructive side-effects (moving/deleting files). It is designed exclusively
 * for use with the {@code HoodieStashPartitionsTool} and the DELETE_PARTITION operation.
 * Do NOT use this as a pattern for other validators.</p>
 *
 * <h3>Why move files inside a pre-commit validator?</h3>
 *
 * <p>The stash operation needs to both move data files to a backup location and mark
 * the partitions as deleted via a replace commit. These two actions must be coupled:
 * if the replace commit lands on the timeline but the file move has not happened yet,
 * the Hudi cleaner will eventually delete the physical data files for the replaced
 * file groups (based on its retention policy), permanently losing the data that was
 * intended to be stashed.
 *
 * <p>By performing the file move inside this pre-commit validator, the move happens
 * <b>before</b> the replace commit is finalized. If the move fails, the validator
 * throws an exception, the commit does not land, and the table remains unchanged.
 * This guarantees that data is never in a state where it has been marked as deleted
 * but has not been safely backed up to the stash location.</p>
 *
 * <p>This validator requires the following configs to be set:
 * <ul>
 *   <li>{@link #STASH_PATH_CONFIG} — the target stash directory</li>
 *   <li>{@link #RENAME_HELPER_CLASS_CONFIG} (optional) — custom rename helper class</li>
 * </ul>
 *
 * <p>Guards:
 * <ul>
 *   <li>Fails if RLI (Record Level Index) is enabled on the table</li>
 *   <li>Must only be used with DELETE_PARTITION operation</li>
 * </ul>
 */
public class StashPartitionsPreCommitValidator<T, I, K, O extends HoodieData<WriteStatus>>
    extends SparkPreCommitValidator<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(StashPartitionsPreCommitValidator.class);

  /**
   * Config key for the stash target directory path.
   */
  public static final String STASH_PATH_CONFIG = "hoodie.stash.partitions.target.path";

  /**
   * Config key for the custom rename helper class. Defaults to {@link DefaultStashPartitionRenameHelper}.
   */
  public static final String RENAME_HELPER_CLASS_CONFIG = "hoodie.stash.rename.helper.class";

  public StashPartitionsPreCommitValidator(HoodieSparkTable<T> table,
                                           HoodieEngineContext engineContext,
                                           HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  /**
   * Override validate to extract affected partitions from partitionToReplaceFileIds rather than
   * write stats, since deletePartitions produces empty write stats but populates the replacement map.
   */
  @Override
  public void validate(String instantTime, HoodieWriteMetadata<O> writeResult,
                       Dataset<Row> before, Dataset<Row> after) throws HoodieValidationException {
    Set<String> partitionsAffected = new HashSet<>(writeResult.getPartitionToReplaceFileIds().keySet());
    validateRecordsBeforeAndAfter(before, after, partitionsAffected);
  }

  /**
   * Moves partition files to the stash location before the replace commit lands.
   *
   * <p><b>Partial failure semantics:</b> if the move fails partway through the partition list
   * (e.g., partition k succeeded but partition k+1 throws), the exception aborts the commit —
   * but partitions 0..k have already had their files physically moved to the stash location.
   * The on-disk layout is now partially mutated even though no commit landed. This validator
   * does NOT attempt to roll back already-moved partitions on failure.
   *
   * <p>The {@code HoodieStashPartitionsTool} compensates for this via its pre-check, which
   * detects partially stashed partitions and restores them before retrying. Callers using this
   * validator outside the tool must perform their own recovery (restore files from stash back
   * to source) before any subsequent write to the affected partitions.
   */
  @Override
  protected void validateRecordsBeforeAndAfter(Dataset<Row> before,
                                                Dataset<Row> after,
                                                Set<String> partitionsAffected) {
    // Guard: ensure RLI and secondary index are not enabled
    HoodieMetadataConfig metadataConfig = getWriteConfig().getMetadataConfig();
    ValidationUtils.checkState(
        !metadataConfig.isRecordLevelIndexEnabled() && !metadataConfig.isGlobalRecordLevelIndexEnabled(),
        "StashPartitionsPreCommitValidator does not support tables with Record Level Index (RLI) enabled. "
            + "Disable RLI before using the stash partitions tool.");
    ValidationUtils.checkState(
        !metadataConfig.isSecondaryIndexEnabled(),
        "StashPartitionsPreCommitValidator does not support tables with Secondary Index enabled. "
            + "Disable secondary index before using the stash partitions tool.");

    // Read stash path from config
    String stashPath = getWriteConfig().getProps().getProperty(STASH_PATH_CONFIG);
    ValidationUtils.checkState(!StringUtils.isNullOrEmpty(stashPath),
        "Stash path must be configured via " + STASH_PATH_CONFIG);

    // Instantiate rename helper
    StashPartitionRenameHelper renameHelper = createRenameHelper();

    HoodieStorage storage = getHoodieTable().getStorage();
    StoragePath basePath = getHoodieTable().getMetaClient().getBasePath();

    LOG.info("Stashing {} partition(s) to {}", partitionsAffected.size(), stashPath);

    for (String partition : partitionsAffected) {
      StoragePath sourcePartitionPath = new StoragePath(basePath, partition);
      StoragePath targetPartitionPath = new StoragePath(stashPath, partition);

      try {
        // Check if source directory exists and has files
        if (!storage.exists(sourcePartitionPath) || isDirectoryEmpty(storage, sourcePartitionPath)) {
          LOG.info("Partition {} source path is empty or does not exist, skipping (already stashed).", partition);
          continue;
        }

        LOG.info("Moving partition {} files from {} to {}", partition, sourcePartitionPath, targetPartitionPath);
        renameHelper.stashPartitionFiles(storage, sourcePartitionPath, targetPartitionPath);
        LOG.info("Successfully moved partition {} to stash location.", partition);
      } catch (IOException e) {
        LOG.error("Failed to stash partition {}", partition, e);
        throw new HoodieValidationException(
            "Failed to move partition files for partition: " + partition + " from " + sourcePartitionPath + " to " + targetPartitionPath, e);
      }
    }

    LOG.info("All partitions stashed successfully.");
  }

  private boolean isDirectoryEmpty(HoodieStorage storage, StoragePath path) throws IOException {
    List<StoragePathInfo> entries = storage.listDirectEntries(path);
    return entries.isEmpty();
  }

  private StashPartitionRenameHelper createRenameHelper() {
    String helperClass = getWriteConfig().getProps().getProperty(RENAME_HELPER_CLASS_CONFIG);
    if (StringUtils.isNullOrEmpty(helperClass)) {
      return new DefaultStashPartitionRenameHelper();
    }
    LOG.info("Using custom rename helper: {}", helperClass);
    return (StashPartitionRenameHelper) ReflectionUtils.loadClass(helperClass);
  }
}
