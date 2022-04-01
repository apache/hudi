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
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE;

public class ManifestFileUtil {
  private static final Logger LOG = LogManager.getLogger(ManifestFileUtil.class);
  private static final String MANIFEST_FOLDER_NAME = "manifest";
  private static final String MANIFEST_FILE_NAME = "latest-snapshot.csv";
  private static final String DELIMITER = "\n";
  private final SerializableConfiguration hadoopConf;
  private final String basePath;
  private final transient HoodieLocalEngineContext engineContext;
  private final HoodieTableMetaClient metaClient;

  private ManifestFileUtil(Configuration conf, String basePath) {
    this.hadoopConf = new SerializableConfiguration(conf);
    this.basePath = basePath;
    this.engineContext = new HoodieLocalEngineContext(conf);
    this.metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf.get()).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
  }

  public synchronized void writeManifestFile() {
    try {
      Path manifestFilePath = new Path(getManifestFolder(), MANIFEST_FILE_NAME);
      Option<byte[]> content = Option.of(fetchLatestBaseFilesForAllPartitions().collect(Collectors.joining(DELIMITER)).getBytes());
      FileIOUtils.createFileInPath(metaClient.getFs(), manifestFilePath, content);
    } catch (Exception e) {
      String msg = "Error writing manifest file";
      LOG.error(msg, e);
      throw new HoodieException(msg, e);
    }
  }

  public Stream<String> fetchLatestBaseFilesForAllPartitions() {
    try {
      HoodieMetadataConfig metadataConfig = buildMetadataConfig(hadoopConf.get());

      List<String> partitions = FSUtils.getAllPartitionPaths(engineContext, metadataConfig, basePath);

      return partitions.parallelStream().flatMap(p -> {
        HoodieLocalEngineContext engContext = new HoodieLocalEngineContext(hadoopConf.get());
        HoodieMetadataFileSystemView fsView =
            new HoodieMetadataFileSystemView(engContext, metaClient, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants(), metadataConfig);
        return fsView.getLatestBaseFiles(p).map(HoodieBaseFile::getFileName);
      });
    } catch (Exception e) {
      String msg = "Error checking path :" + basePath;
      LOG.error(msg, e);
      throw new HoodieException(msg, e);
    }
  }

  private static HoodieMetadataConfig buildMetadataConfig(Configuration conf) {
    return HoodieMetadataConfig.newBuilder()
        .enable(conf.getBoolean(ENABLE.key(), DEFAULT_METADATA_ENABLE_FOR_READERS))
        .build();
  }

  /**
   * @return Manifest File folder
   */
  public Path getManifestFolder() {
    return new Path(metaClient.getMetaPath(), MANIFEST_FOLDER_NAME);
  }

  /**
   * @return Manifest File Full Path
   */
  public Path getManifestFilePath() {
    return new Path(getManifestFolder(), MANIFEST_FILE_NAME);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link ManifestFileUtil}.
   */
  public static class Builder {

    private Configuration conf;
    private String basePath;

    public Builder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder setBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public ManifestFileUtil build() {
      ValidationUtils.checkArgument(conf != null, "Configuration needs to be set to init ManifestFileGenerator");
      ValidationUtils.checkArgument(basePath != null, "basePath needs to be set to init ManifestFileGenerator");
      return new ManifestFileUtil(conf, basePath);
    }
  }
}
