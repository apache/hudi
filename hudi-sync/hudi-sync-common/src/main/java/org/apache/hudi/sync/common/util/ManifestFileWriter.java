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

package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ManifestFileWriter {

  public static final String MANIFEST_FOLDER_NAME = "manifest";
  public static final String ABSOLUTE_PATH_MANIFEST_FOLDER_NAME = "absolute-path-manifest";
  public static final String MANIFEST_FILE_NAME = "latest-snapshot.csv";
  private static final Logger LOG = LoggerFactory.getLogger(ManifestFileWriter.class);

  private final HoodieTableMetaClient metaClient;
  private final boolean useFileListingFromMetadata;

  protected ManifestFileWriter(HoodieTableMetaClient metaClient, boolean useFileListingFromMetadata) {
    this.metaClient = metaClient;
    this.useFileListingFromMetadata = useFileListingFromMetadata;
  }

  /**
   * Write all the latest base file names to the manifest file.
   */
  public synchronized void writeManifestFile(boolean useAbsolutePath) {
    try {
      List<String> baseFiles = fetchLatestBaseFilesForAllPartitions(useAbsolutePath)
          .collect(Collectors.toList());
      if (baseFiles.isEmpty()) {
        LOG.warn("No base file to generate manifest file.");
        return;
      } else {
        LOG.info("Writing base file names to manifest file: {}", baseFiles.size());
      }
      final StoragePath manifestFilePath = getManifestFilePath(useAbsolutePath);
      try (OutputStream outputStream = metaClient.getStorage().create(manifestFilePath, true);
           BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {
        for (String f : baseFiles) {
          writer.write(f);
          writer.write("\n");
        }
      }
    } catch (Exception e) {
      throw new HoodieException("Error in writing manifest file.", e);
    }
  }

  @VisibleForTesting
  public Stream<String> fetchLatestBaseFilesForAllPartitions(boolean useAbsolutePath) {
    try {
      StorageConfiguration storageConf = metaClient.getStorageConf();
      HoodieLocalEngineContext engContext = new HoodieLocalEngineContext(storageConf);
      boolean canUseMetadataTable = useFileListingFromMetadata && metaClient.getTableConfig().isMetadataTableAvailable();
      return getLatestBaseFiles(canUseMetadataTable, engContext, metaClient, useAbsolutePath);
    } catch (Exception e) {
      throw new HoodieException("Error in fetching latest base files.", e);
    }
  }

  public StoragePath getManifestFolder(boolean useAbsolutePath) {
    return new StoragePath(metaClient.getMetaPath(), useAbsolutePath ? ABSOLUTE_PATH_MANIFEST_FOLDER_NAME : MANIFEST_FOLDER_NAME);
  }

  @VisibleForTesting
  static Stream<String> getLatestBaseFiles(boolean canUseMetadataTable, HoodieEngineContext engContext, HoodieTableMetaClient metaClient,
                                           boolean useAbsolutePath) {
    List<String> partitions = FSUtils.getAllPartitionPaths(engContext, metaClient, canUseMetadataTable);
    LOG.info("Retrieve all partitions: {}", partitions.size());
    HoodieTableFileSystemView fsView = null;
    try {
      fsView = FileSystemViewManager.createInMemoryFileSystemViewWithTimeline(engContext, metaClient,
          HoodieMetadataConfig.newBuilder().enable(canUseMetadataTable).build(),
          metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
      if (canUseMetadataTable) {
        // incase of MDT, we can load all partitions at once. If not for MDT, we can rely on fsView.getLatestBaseFiles(partition) for each partition to load from FS.
        fsView.loadAllPartitions();
      }
      HoodieTableFileSystemView finalFsView = fsView;
      // if we do not collect and return stream directly, lazy evaluation happens and we end up closing the fsview in finally block which later
      // fails the getLatestBaseFiles call. Hence we collect and return a stream.
      return partitions.parallelStream().flatMap(partition -> finalFsView.getLatestBaseFiles(partition)
          .map(useAbsolutePath ? HoodieBaseFile::getPath : HoodieBaseFile::getFileName)).collect(Collectors.toList()).stream();
    } finally {
      fsView.close();
    }
  }

  public StoragePath getManifestFilePath(boolean useAbsolutePath) {
    return new StoragePath(getManifestFolder(useAbsolutePath), MANIFEST_FILE_NAME);
  }

  public String getManifestSourceUri(boolean useAbsolutePath) {
    return new Path(getManifestFolder(useAbsolutePath).toString(), "*").toUri().toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link ManifestFileWriter}.
   */
  public static class Builder {
    private boolean useFileListingFromMetadata;
    private HoodieTableMetaClient metaClient;

    public Builder setUseFileListingFromMetadata(boolean useFileListingFromMetadata) {
      this.useFileListingFromMetadata = useFileListingFromMetadata;
      return this;
    }

    public Builder setMetaClient(HoodieTableMetaClient metaClient) {
      this.metaClient = metaClient;
      return this;
    }

    public ManifestFileWriter build() {
      ValidationUtils.checkArgument(metaClient != null, "MetaClient needs to be set to init ManifestFileGenerator");
      return new ManifestFileWriter(metaClient, useFileListingFromMetadata);
    }
  }
}
