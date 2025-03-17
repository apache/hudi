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

package org.apache.hudi.index.bucket;

import org.apache.hudi.common.model.PartitionBucketIndexHashingConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieInstantWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class SimpleBucketIndexUtils {
  public static final String INITIAL_HASHING_CONFIG_INSTANT = HoodieTimeline.INIT_INSTANT_TS;

  private static final Logger LOG = LoggerFactory.getLogger(SimpleBucketIndexUtils.class);

  public static boolean initHashingConfig(HoodieTableMetaClient metaClient,
                                              String expressions,
                                              String rule,
                                              int defaultBucketNumber,
                                              String instant){
    if (StringUtils.isNullOrEmpty(expressions)) {
      return false;
    }
    String hashingInstant = StringUtils.isNullOrEmpty(instant) ? INITIAL_HASHING_CONFIG_INSTANT : instant;
    HoodieStorage storage = metaClient.getStorage();
    PartitionBucketIndexHashingConfig hashingConfig =
        new PartitionBucketIndexHashingConfig(expressions, defaultBucketNumber, rule, PartitionBucketIndexHashingConfig.CURRENT_VERSION, hashingInstant);
    StoragePath hashingConfigPath = new StoragePath(metaClient.getHashingMetadataConfigPath(), hashingConfig.getFilename());

    try {
      Option<byte []> content = Option.of(hashingConfig.toJsonString().getBytes(StandardCharsets.UTF_8));
      storage.createImmutableFileInPath(hashingConfigPath, content.map(HoodieInstantWriter::convertByteArrayToWriter));
    } catch (IOException ioe) {
      throw new HoodieIOException("Failed to initHashingConfig ", ioe);
    }
    return true;
  }

  public static Option<PartitionBucketIndexHashingConfig> loadHashingConfig(HoodieStorage storage,
                                                                            StoragePathInfo hashingConfig) {
    if (hashingConfig == null) {
      return Option.empty();
    }
    try (InputStream is = storage.open(hashingConfig.getPath())) {
      byte[] content = FileIOUtils.readAsByteArray(is);
      return Option.of(PartitionBucketIndexHashingConfig.fromBytes(content));
    } catch (IOException e) {
      LOG.error("Error when loading hashing config, for path: " + hashingConfig.getPath().getName(), e);
      throw new HoodieIOException("Error while loading hashing config", e);
    }
  }
}
