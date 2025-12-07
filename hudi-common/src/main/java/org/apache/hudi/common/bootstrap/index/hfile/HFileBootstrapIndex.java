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

package org.apache.hudi.common.bootstrap.index.hfile;

import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Maintains mapping from skeleton file id to external bootstrap file.
 * It maintains 2 physical indices.
 *  (a) At partition granularity to lookup all indices for each partition.
 *  (b) At file-group granularity to lookup bootstrap mapping for an individual file-group.
 *
 * This implementation uses HFile as physical storage of index. FOr the initial run, bootstrap
 * mapping for the entire dataset resides in a single file but care has been taken in naming
 * the index files in the same way as Hudi data files so that we can reuse file-system abstraction
 * on these index files to manage multiple file-groups.
 */
@Getter
public class HFileBootstrapIndex extends BootstrapIndex {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(HFileBootstrapIndex.class);

  public static final String BOOTSTRAP_INDEX_FILE_ID = "00000000-0000-0000-0000-000000000000-0";

  private static final String PARTITION_KEY_PREFIX = "part";
  private static final String FILE_ID_KEY_PREFIX = "fileid";
  private static final String KEY_VALUE_SEPARATOR = "=";
  private static final String KEY_PARTS_SEPARATOR = ";";
  // This is part of the suffix that HFIle appends to every key
  public static final String HFILE_CELL_KEY_SUFFIX_PART = "//LATEST_TIMESTAMP/Put/vlen";

  // Additional Metadata written to HFiles.
  public static final String INDEX_INFO_KEY_STRING = "INDEX_INFO";
  public static final byte[] INDEX_INFO_KEY = getUTF8Bytes(INDEX_INFO_KEY_STRING);

  private final boolean isPresent;

  public HFileBootstrapIndex(HoodieTableMetaClient metaClient) {
    super(metaClient);
    StoragePath indexByPartitionPath = partitionIndexPath(metaClient);
    StoragePath indexByFilePath = fileIdIndexPath(metaClient);
    try {
      HoodieStorage storage = metaClient.getStorage();
      // The metadata table is never bootstrapped, so the bootstrap index is always absent
      // for the metadata table.  The fs.exists calls are avoided for metadata table.
      isPresent = !metaClient.isMetadataTable() && storage.exists(indexByPartitionPath) && storage.exists(indexByFilePath);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Returns partition-key to be used in HFile.
   * @param partition Partition-Path
   * @return
   */
  static String getPartitionKey(String partition) {
    return getKeyValueString(PARTITION_KEY_PREFIX, partition);
  }

  /**
   * Returns file group key to be used in HFile.
   * @param fileGroupId File Group Id.
   * @return
   */
  static String getFileGroupKey(HoodieFileGroupId fileGroupId) {
    return getPartitionKey(fileGroupId.getPartitionPath()) + KEY_PARTS_SEPARATOR
        + getKeyValueString(FILE_ID_KEY_PREFIX, fileGroupId.getFileId());
  }

  static String getPartitionFromKey(String key) {
    String[] parts = key.split("=", 2);
    ValidationUtils.checkArgument(parts[0].equals(PARTITION_KEY_PREFIX));
    return parts[1];
  }

  private static String getFileIdFromKey(String key) {
    String[] parts = key.split("=", 2);
    ValidationUtils.checkArgument(parts[0].equals(FILE_ID_KEY_PREFIX));
    return parts[1];
  }

  static HoodieFileGroupId getFileGroupFromKey(String key) {
    String[] parts = key.split(KEY_PARTS_SEPARATOR, 2);
    return new HoodieFileGroupId(getPartitionFromKey(parts[0]), getFileIdFromKey(parts[1]));
  }

  private static String getKeyValueString(String key, String value) {
    return key + KEY_VALUE_SEPARATOR + value;
  }

  static StoragePath partitionIndexPath(HoodieTableMetaClient metaClient) {
    return new StoragePath(metaClient.getBootstrapIndexByPartitionFolderPath(),
        FSUtils.makeBootstrapIndexFileName(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, BOOTSTRAP_INDEX_FILE_ID,
            HoodieFileFormat.HFILE.getFileExtension()));
  }

  static StoragePath fileIdIndexPath(HoodieTableMetaClient metaClient) {
    return new StoragePath(metaClient.getBootstrapIndexByFileIdFolderNameFolderPath(),
        FSUtils.makeBootstrapIndexFileName(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, BOOTSTRAP_INDEX_FILE_ID,
            HoodieFileFormat.HFILE.getFileExtension()));
  }

  @Override
  public BootstrapIndex.IndexReader createReader() {
    return new HFileBootstrapIndexReader(metaClient);
  }

  @Override
  public BootstrapIndex.IndexWriter createWriter(String bootstrapBasePath) {
    return new HFileBootstrapIndexWriter(bootstrapBasePath, metaClient);
  }

  @Override
  public void dropIndex() {
    try {
      StoragePath[] indexPaths = new StoragePath[] {partitionIndexPath(metaClient), fileIdIndexPath(metaClient)};
      for (StoragePath indexPath : indexPaths) {
        if (metaClient.getStorage().exists(indexPath)) {
          LOG.info("Dropping bootstrap index. Deleting file: {}", indexPath);
          metaClient.getStorage().deleteDirectory(indexPath);
        }
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }
}
