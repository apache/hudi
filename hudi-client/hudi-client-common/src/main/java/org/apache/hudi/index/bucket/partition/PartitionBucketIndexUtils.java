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

package org.apache.hudi.index.bucket.partition;

import org.apache.hudi.common.model.PartitionBucketIndexHashingConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PartitionBucketIndexUtils {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionBucketIndexUtils.class);

  public static boolean isPartitionSimpleBucketIndex(Configuration conf, String basePath) {
    return isPartitionSimpleBucketIndex(HadoopFSUtils.getStorageConf(conf), basePath);
  }

  public static boolean isPartitionSimpleBucketIndex(StorageConfiguration conf, String basePath) {
    StoragePath storagePath = PartitionBucketIndexHashingConfig.getHashingConfigStorageFolder(basePath);
    try (HoodieHadoopStorage storage = new HoodieHadoopStorage(storagePath, conf)) {
      return storage.exists(storagePath);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to list PARTITION_BUCKET_INDEX_HASHING_FOLDER folder ", e);
    }
  }

  /**
   * @return all File id in current table using `partitionPath + FileId` format.
   */
  @VisibleForTesting
  public static List<String> getAllFileIDWithPartition(HoodieTableMetaClient metaClient) throws IOException {
    List<StoragePathInfo> allFiles = metaClient.getStorage().listDirectEntries(metaClient.getBasePath()).stream().flatMap(path -> {
      try {
        return metaClient.getStorage().listDirectEntries(path.getPath()).stream();
      } catch (IOException e) {
        return Stream.empty();
      }
    }).collect(Collectors.toList());

    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants(), allFiles);
    return fsView.getAllFileGroups().map(group -> {
      return group.getPartitionPath() + group.getFileGroupId().getFileId().split("-")[0];
    }).collect(Collectors.toList());
  }
}
