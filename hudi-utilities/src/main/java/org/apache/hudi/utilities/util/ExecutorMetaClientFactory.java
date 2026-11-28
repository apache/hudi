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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FileSystemRetryConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.storage.StorageConfiguration;

import java.io.Serializable;

/**
 * Everything a phase-2 executor needs in order to build a meta client for storage and the archive,
 * and nothing that drags a timeline along with it.
 *
 * <p><b>Why this exists rather than shipping the driver's meta client.</b> Both dimensions read the
 * shipped client for exactly one thing — its storage configuration — but
 * {@code HoodieTableMetaClient.activeTimeline} and {@code ActiveTimelineV2.metaClient} are both
 * non-transient, so the two form a cycle and shipping either ships a whole active timeline. In the
 * FS-MDT diff that meant a closure already carrying {@code T_aft} also carried {@code T_prev}.
 *
 * <p><b>The configs are carried deliberately.</b> {@code HoodieTableMetaClient.Builder.build()}
 * defaults the consistency guard, the retry policy and the time generator when they are not set, so a
 * client built on an executor from conf and base path alone runs every storage call <em>unretried</em>
 * while the driver's client keeps whatever the operator configured. {@code HoodieTableMetaClient.reload}
 * carries the same three for the same reason.
 *
 * <p>The metaserver config is deliberately not carried: no phase-2 path reaches the metaserver, and
 * {@code build()} supplies the same default the validator's own client is constructed with.
 *
 * <p><b>The active timeline is never loaded</b> by the client this produces. That is the invariant
 * both dimensions' adjudication paths rest on: an executor must not build an active timeline, because
 * hoisting the timeline reads to the driver is what makes phase 2 finish at all.
 */
public final class ExecutorMetaClientFactory implements Serializable {

  private static final long serialVersionUID = 1L;

  private final StorageConfiguration<?> storageConf;
  private final String basePath;
  private final ConsistencyGuardConfig consistencyGuardConfig;
  private final FileSystemRetryConfig fileSystemRetryConfig;
  private final HoodieTimeGeneratorConfig timeGeneratorConfig;

  private ExecutorMetaClientFactory(StorageConfiguration<?> storageConf, String basePath,
                                    ConsistencyGuardConfig consistencyGuardConfig,
                                    FileSystemRetryConfig fileSystemRetryConfig,
                                    HoodieTimeGeneratorConfig timeGeneratorConfig) {
    this.storageConf = storageConf;
    this.basePath = basePath;
    this.consistencyGuardConfig = consistencyGuardConfig;
    this.fileSystemRetryConfig = fileSystemRetryConfig;
    this.timeGeneratorConfig = timeGeneratorConfig;
  }

  /** Reads the driver's client for what an executor needs, and for nothing that ties it to a timeline. */
  public static ExecutorMetaClientFactory from(HoodieTableMetaClient driverClient) {
    return new ExecutorMetaClientFactory(
        driverClient.getStorageConf(),
        driverClient.getBasePath().toString(),
        driverClient.getConsistencyGuardConfig(),
        driverClient.getFileSystemRetryConfig(),
        driverClient.getTimeGeneratorConfig());
  }

  /**
   * A meta client for this task alone, for storage and the archive.
   *
   * <p>Built WITHOUT the active timeline, and no code reachable from an adjudication path may call
   * {@code getActiveTimeline()} on the returned client — that would build an active timeline on the
   * executor.
   */
  public HoodieTableMetaClient newStorageOnlyClient() {
    return HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance())
        .setBasePath(basePath)
        .setLoadActiveTimelineOnLoad(false)
        .setConsistencyGuardConfig(consistencyGuardConfig)
        .setFileSystemRetryConfig(fileSystemRetryConfig)
        .setTimeGeneratorConfig(timeGeneratorConfig)
        .build();
  }

  public String getBasePath() {
    return basePath;
  }
}
