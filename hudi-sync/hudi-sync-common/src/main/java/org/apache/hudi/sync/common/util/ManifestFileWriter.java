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
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ManifestFileWriter {

  public static final String MANIFEST_FOLDER_NAME = "manifest";
  public static final String MANIFEST_FILE_NAME = "latest-snapshot.csv";
  private static final Logger LOG = LogManager.getLogger(ManifestFileWriter.class);

  private final HoodieTableMetaClient metaClient;
  private final boolean useFileListingFromMetadata;
  private final boolean assumeDatePartitioning;

  private ManifestFileWriter(Configuration hadoopConf, String basePath, boolean useFileListingFromMetadata, boolean assumeDatePartitioning) {
    this.metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
    this.useFileListingFromMetadata = useFileListingFromMetadata;
    this.assumeDatePartitioning = assumeDatePartitioning;
  }

  /**
   * Write all the latest base file names to the manifest file.
   */
  public synchronized void writeManifestFile() {
    try {
      List<String> baseFiles = fetchLatestBaseFilesForAllPartitions(metaClient, useFileListingFromMetadata, assumeDatePartitioning)
          .collect(Collectors.toList());
      if (baseFiles.isEmpty()) {
        LOG.warn("No base file to generate manifest file.");
        return;
      } else {
        LOG.info("Writing base file names to manifest file: " + baseFiles.size());
      }
      final Path manifestFilePath = getManifestFilePath();
      try (FSDataOutputStream outputStream = metaClient.getFs().create(manifestFilePath, true);
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

  public static Stream<String> fetchLatestBaseFilesForAllPartitions(HoodieTableMetaClient metaClient,
      boolean useFileListingFromMetadata, boolean assumeDatePartitioning) {
    try {
      List<String> partitions = FSUtils.getAllPartitionPaths(new HoodieLocalEngineContext(metaClient.getHadoopConf()),
          metaClient.getBasePath(), useFileListingFromMetadata, assumeDatePartitioning);
      LOG.info("Retrieve all partitions: " + partitions.size());
      return partitions.parallelStream().flatMap(p -> {
        Configuration hadoopConf = metaClient.getHadoopConf();
        HoodieLocalEngineContext engContext = new HoodieLocalEngineContext(hadoopConf);
        HoodieMetadataFileSystemView fsView = new HoodieMetadataFileSystemView(engContext, metaClient,
            metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants(),
            HoodieMetadataConfig.newBuilder().enable(useFileListingFromMetadata).withAssumeDatePartitioning(assumeDatePartitioning).build());
        return fsView.getLatestBaseFiles(p).map(HoodieBaseFile::getFileName);
      });
    } catch (Exception e) {
      throw new HoodieException("Error in fetching latest base files.", e);
    }
  }

  public Path getManifestFolder() {
    return new Path(metaClient.getMetaPath(), MANIFEST_FOLDER_NAME);
  }

  public Path getManifestFilePath() {
    return new Path(getManifestFolder(), MANIFEST_FILE_NAME);
  }

  public String getManifestSourceUri() {
    return new Path(getManifestFolder(), "*").toUri().toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link ManifestFileWriter}.
   */
  public static class Builder {

    private Configuration conf;
    private String basePath;
    private boolean useFileListingFromMetadata;
    private boolean assumeDatePartitioning;

    public Builder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder setBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public Builder setUseFileListingFromMetadata(boolean useFileListingFromMetadata) {
      this.useFileListingFromMetadata = useFileListingFromMetadata;
      return this;
    }

    public Builder setAssumeDatePartitioning(boolean assumeDatePartitioning) {
      this.assumeDatePartitioning = assumeDatePartitioning;
      return this;
    }

    public ManifestFileWriter build() {
      ValidationUtils.checkArgument(conf != null, "Configuration needs to be set to init ManifestFileGenerator");
      ValidationUtils.checkArgument(basePath != null, "basePath needs to be set to init ManifestFileGenerator");
      return new ManifestFileWriter(conf, basePath, useFileListingFromMetadata, assumeDatePartitioning);
    }
  }
}
