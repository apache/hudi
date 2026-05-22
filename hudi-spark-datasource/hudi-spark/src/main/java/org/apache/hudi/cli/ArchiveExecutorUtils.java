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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.timeline.HoodieTimelineArchiver;
import org.apache.hudi.client.timeline.TimelineArchivers;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.util.CommonClientUtils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Map;

/**
 * Archive Utils.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class ArchiveExecutorUtils {

  public static int archive(JavaSparkContext jsc,
                            int minCommits,
                            int maxCommits,
                            int commitsRetained,
                            boolean enableMetadata,
                            String basePath,
                            Map<String, String> options) throws IOException {
    // NOTE on builder ordering:
    //   `withArchivalConfig`/`withCleanConfig`/`withMetadataConfig` each call
    //   `putAll(subConfig.getProps())` onto `writeConfig.getProps()`, which
    //   includes every key filled in by `setDefaults` during the sub-config's
    //   `build()`. If `withProps(conf)` ran BEFORE them, those defaults would
    //   overwrite the user's options (e.g. `hoodie.keep.min.commits`).
    //
    //   Therefore `withProps(conf)` is intentionally placed LAST so user-supplied
    //   options reliably win over sub-config defaults. Named procedure params
    //   (min/max/retain/enableMetadata) are forwarded via the dedicated builders
    //   below; if the caller wants those to win over a same-name key in `conf`,
    //   the procedure layer is responsible for not putting that key into `conf`.
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(minCommits, maxCommits).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(commitsRetained).build())
        .withEmbeddedTimelineServerEnabled(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadata).build())
        .withProps(options)
        .build();
    HoodieEngineContext context = new HoodieSparkEngineContext(jsc);
    HoodieSparkTable<HoodieAvroPayload> table = HoodieSparkTable.create(config, context);
    CommonClientUtils.validateTableVersion(table.getMetaClient().getTableConfig(), config);
    try {
      HoodieTimelineArchiver<HoodieAvroPayload, HoodieData<HoodieRecord<HoodieAvroPayload>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> archiver =
          TimelineArchivers.getInstance(table.getMetaClient().getTimelineLayoutVersion(), config, table);
      archiver.archiveIfRequired(context, true);
    } catch (IOException ioe) {
      log.error("Failed to archive with IOException: {}", ioe.getMessage());
      throw ioe;
    }
    return 0;
  }
}
