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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Defines the interface through which file listing metadata is created, updated and accessed. This metadata is
 * saved within an internal Metadata Table.
 *
 * If file listing metadata is disabled, the functions default to using listStatus(...) RPC calls to retrieve
 * file listings from the file system.
 */
public class HoodieMetadata {
  private static final Logger LOG = LogManager.getLogger(HoodieMetadata.class);

  // Base path of the Metadata Table relative to the dataset (.hoodie/metadata)
  private static final String METADATA_TABLE_REL_PATH = HoodieTableMetaClient.METAFOLDER_NAME + Path.SEPARATOR
      + "metadata";

  // Instances of metadata for each basePath
  private static Map<String, HoodieMetadataImpl> instances = new HashMap<>();

  // A base directory where the metadata tables should be saved outside the dataset directory.
  private static String metadataBaseDirectory;

  /**
   * Initialize the Metadata Table.
   *  - If the table does not exist it is created for the first time.
   *  - If the table exists, it is synced with the instants on the dataset.
   *
   * @param jsc {@code JavaSparkContext}
   * @param writeConfig {@code HoodieWriteConfig} for the writer from which relevant configuration is inherited
   */
  public static synchronized void init(JavaSparkContext jsc, HoodieWriteConfig writeConfig) {
    try {
      final String basePath = writeConfig.getBasePath();
      if (instances.containsKey(basePath)) {
        LOG.warn("Duplicate initialization of metadata for basePath " + basePath);
      }
      if (writeConfig.useFileListingMetadata()) {
        instances.put(basePath, new HoodieMetadataImpl(jsc, jsc.hadoopConfiguration(), basePath,
            getMetadataTableBasePath(basePath), writeConfig, false));
      } else {
        if (!isMetadataTable(writeConfig.getBasePath())) {
          LOG.info("Metadata table is disabled in write config.");
        }
      }
    } catch (IOException e) {
      throw new HoodieMetadataException("Failed to initialize metadata table", e);
    }
  }

  /**
   * Initialize the Metadata Table in read-only mode.
   *
   * If the table does not exist, a warning is logged and RPC calls are used to retrieve file listings
   * from the file system. No updates are applied to the table and it is not synced.
   *
   * @param hadoopConf {@code Configuration}
   * @param basePath The basePath for the dataset
   */
  public static synchronized void init(Configuration hadoopConf, String basePath) {
    try {
      if (instances.containsKey(basePath)) {
        LOG.warn("Duplicate initialization of metadata for basePath " + basePath);
      }
      instances.put(basePath, new HoodieMetadataImpl(null, hadoopConf, basePath, getMetadataTableBasePath(basePath),
          null, true));
    } catch (TableNotFoundException e) {
      LOG.warn("Metadata table was not found at path " + getMetadataTableBasePath(basePath));
    } catch (IOException e) {
      throw new HoodieMetadataException("Failed to initialize metadata table", e);
    }
  }

  /**
   * Returns true if the Metadata Table for the given basePath has been initialized.
   */
  public static synchronized boolean exists(String basePath) {
    return instances.containsKey(basePath);
  }

  /**
   * Remove the Metadata Table object for the given basePath.
   *
   * This can be used to reinitialize the metadata table.
   */
  public static synchronized void remove(String basePath) {
    instances.remove(basePath);
  }

  /**
   * Update the metadata from a commit, delta-commit or compaction on the dataset.
   *
   * Updates are only performed if the Metadata Table was initialized in read-write mode.
   *
   * @param writeConfig {@code HoodieWriteConfig} for the writer
   * @param commitMetadata {@code HoodieCommitMetadata}
   * @param instantTime The instant at which the commit took place
   */
  public static void update(HoodieWriteConfig writeConfig, HoodieCommitMetadata commitMetadata, String instantTime) {
    if (writeConfig.useFileListingMetadata() && instances.containsKey(writeConfig.getBasePath())) {
      instances.get(writeConfig.getBasePath()).update(commitMetadata, instantTime);
    }
  }

  /**
   * Update the metadata from a clean on the dataset.
   *
   * Updates are only performed if the Metadata Table was initialized in read-write mode.
   *
   * @param writeConfig {@code HoodieWriteConfig} for the writer
   * @param cleanerPlan {@code HoodieCleanerPlan}
   * @param instantTime The instant at which the cleaner plan was generated
   */
  public static void update(HoodieWriteConfig writeConfig, HoodieCleanerPlan cleanerPlan, String instantTime) {
    if (writeConfig.useFileListingMetadata() && instances.containsKey(writeConfig.getBasePath())) {
      instances.get(writeConfig.getBasePath()).update(cleanerPlan, instantTime);
    }
  }

  /**
   * Update the metadata from a restore on the dataset.
   *
   * Updates are only performed if the Metadata Table was initialized in read-write mode.
   *
   * @param writeConfig {@code HoodieWriteConfig} for the writer
   * @param restoreMetadata {@code HoodieRestoreMetadata}
   * @param instantTime The instant at which the restore took place
   */
  public static void update(HoodieWriteConfig writeConfig, HoodieRestoreMetadata restoreMetadata, String instantTime) {
    if (writeConfig.useFileListingMetadata() && instances.containsKey(writeConfig.getBasePath())) {
      instances.get(writeConfig.getBasePath()).update(restoreMetadata, instantTime);
    }
  }

  /**
   * Update the metadata from a rollback on the dataset.
   *
   * Updates are only performed if the Metadata Table was initialized in read-write mode.
   *
   * @param writeConfig {@code HoodieWriteConfig} for the writer
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param instantTime The instant at which the rollback took place
   */
  public static void update(HoodieWriteConfig writeConfig, HoodieRollbackMetadata rollbackMetadata, String instantTime) {
    if (writeConfig.useFileListingMetadata() && instances.containsKey(writeConfig.getBasePath())) {
      instances.get(writeConfig.getBasePath()).update(rollbackMetadata, instantTime);
    }
  }

  /**
   * Return the base path of the Metadata Table.
   *
   * @param tableBasePath The base path of the dataset
   */
  public static String getMetadataTableBasePath(String tableBasePath) {
    if (metadataBaseDirectory != null) {
      return metadataBaseDirectory;
    }

    return tableBasePath + Path.SEPARATOR + METADATA_TABLE_REL_PATH;
  }

  /**
   * Return the list of partitions in the dataset.
   *
   * If the Metadata Table is enabled, the listing is retrieved from the stored metadata. Otherwise, the list of
   * partitions is retrieved directly from the underlying {@code FileSystem}.
   *
   * On any errors retrieving the listing from the metadata, defaults to using the file system listings.
   *
   * @param fs The {@code FileSystem}
   * @param basePath Base path of the dataset
   * @param assumeDatePartitioning True if the dataset uses date based partitioning
   */
  public static List<String> getAllPartitionPaths(FileSystem fs, String basePath, boolean assumeDatePartitioning)
      throws IOException {
    try {
      if (instances.containsKey(basePath)) {
        return instances.get(basePath).getAllPartitionPaths();
      }
    } catch (Exception e) {
      LOG.error("Failed to retrive all partition paths from metadata: " + e);
    }

    return FSUtils.getAllPartitionPaths(fs, basePath, assumeDatePartitioning);
  }

  /**
   * Return the list of files in a partition.
   *
   * If the Metadata Table is enabled, the listing is retrived from the stored metadata. Otherwise, the list of
   * partitions is retrieved directly from the underlying {@code FileSystem}.
   *
   * On any errors retrieving the listing from the metadata, defaults to using the file system listings.
   *
   * @param hadoopConf {@code Configuration}
   * @param partitionPath The absolute path of the partition to list
   */
  public static FileStatus[] getAllFilesInPartition(Configuration hadoopConf, String basePath, Path partitionPath)
      throws IOException {
    try {
      if (instances.containsKey(basePath)) {
        return instances.get(basePath).getAllFilesInPartition(partitionPath);
      }
    } catch (Exception e) {
      LOG.error("Failed to retrive partition file listing from metadata: " + e);
    }

    return FSUtils.getFs(partitionPath.toString(), hadoopConf).listStatus(partitionPath);
  }

  /**
   * Returns the current state of sync.
   *
   * @return True if all the instants on the dataset have been synced to the Metadata Table
   */
  public static boolean isInSync(String basePath) {
    return instances.containsKey(basePath) && instances.get(basePath).isInSync();
  }

  /**
   * Returns the timestamp of the latest compaction on the Metadata Table.
   */
  public static Option<String> getLatestCompactionTimestamp(String basePath) {
    return instances.containsKey(basePath) ? instances.get(basePath).getLatestCompactionTimestamp() : Option.empty();
  }

  /**
   * Return an ordered list of instants which have not been synced to the Metadata Table.

   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset
   */
  public static List<HoodieInstant> findInstantsToSync(Configuration hadoopConf, String basePath) {
    if (!instances.containsKey(basePath)) {
      return Collections.emptyList();
    }

    HoodieTableMetaClient datasetMetaClient = new HoodieTableMetaClient(hadoopConf, basePath);
    return instances.get(basePath).findInstantsToSync(datasetMetaClient);
  }

  /**
   * Returns {@code True} if the given path contains a metadata table.
   *
   * @param basePath The base path to check
   */
  public static boolean isMetadataTable(String basePath) {
    return basePath.endsWith(METADATA_TABLE_REL_PATH);
  }

  /**
   * Returns the {@code HoodieWriteConfig} in use on the Metadata Table.
   *
   * @return {@code HoodieWriteConfig} if Metadata Table is enabled, {@code null} otherwise.
   */
  public static HoodieWriteConfig getWriteConfig(String basePath) {
    return instances.containsKey(basePath) ? instances.get(basePath).getWriteConfig() : null;
  }

  /**
   * Returns stats from a Metadata Table.
   *
   * @param detailed If true, also returns stats which are calculated by reading from the metadata table.
   * @throws IOException
   */
  public static Map<String, String> getStats(String basePath, boolean detailed) throws IOException {
    return instances.containsKey(basePath) ? instances.get(basePath).getStats(detailed) : Collections.emptyMap();
  }

  /**
   * Sets the directory to store Metadata Table.
   *
   * This can be used to store the metadata table away from the dataset directory.
   *  - Useful for testing as well as for using via the HUDI CLI so that the actual dataset is not written to.
   *  - Useful for testing Metadata Table performance and operations on existing datasets before enabling.
   */
  public static void setMetadataBaseDirectory(String metadataDir) {
    ValidationUtils.checkState(metadataBaseDirectory == null,
        "metadataBaseDirectory is already set to " + metadataBaseDirectory);
    metadataBaseDirectory = metadataDir;
  }
}
