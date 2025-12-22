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

package org.apache.hudi.cli;

import org.apache.hudi.client.HoodieTimelineArchiver;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;

import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Archive Utils.
 */
public final class ArchiveExecutorUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ArchiveExecutorUtils.class);

  private ArchiveExecutorUtils() {
  }

  public static int archive(JavaSparkContext jsc,
       int minCommits,
       int maxCommits,
       int commitsRetained,
       boolean enableMetadata,
       String basePath) throws IOException {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(minCommits, maxCommits).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(commitsRetained).build())
        .withEmbeddedTimelineServerEnabled(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadata).build())
        .build();
    HoodieEngineContext context = new HoodieSparkEngineContext(jsc);
    HoodieSparkTable<HoodieAvroPayload> table = HoodieSparkTable.create(config, context);
    try {
      HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(config, table);
      archiver.archiveIfRequired(context, true);
    } catch (IOException ioe) {
      LOG.error("Failed to archive with IOException: {}", ioe.getMessage());
      throw ioe;
    }
    return 0;
  }
}
