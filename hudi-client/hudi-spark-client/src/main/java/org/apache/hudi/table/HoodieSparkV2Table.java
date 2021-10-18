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

package org.apache.hudi.table;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

/**
 * TODO(rxu) add more from {@link HoodieSparkTable}.
 */
public abstract class HoodieSparkV2Table<I, K, O> extends HoodieBaseTable<I, K, O> {

  protected HoodieSparkV2Table(HoodieWriteConfig config, HoodieEngineContext context,
      HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  public static HoodieSparkV2Table create(HoodieWriteConfig config, HoodieEngineContext context) {
    return create(config, context, false);
  }

  public static HoodieSparkV2Table create(HoodieWriteConfig config, HoodieEngineContext context,
      boolean refreshTimeline) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(context.getHadoopConf().get()).setBasePath(config.getBasePath())
            .setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
            .setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion()))).build();
    return create(config, (HoodieSparkEngineContext) context, metaClient, refreshTimeline);
  }

  public static HoodieSparkV2Table create(HoodieWriteConfig config,
      HoodieSparkEngineContext context,
      HoodieTableMetaClient metaClient) {
    return create(config, context, metaClient, false);
  }

  public static HoodieSparkV2Table create(HoodieWriteConfig config,
      HoodieSparkEngineContext context,
      HoodieTableMetaClient metaClient,
      boolean refreshTimeline) {
    HoodieSparkV2Table hoodieSparkTable;
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        hoodieSparkTable = new HoodieSparkV2CopyOnWriteTable(config, context, metaClient);
        break;
      case MERGE_ON_READ:
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
    if (refreshTimeline) {
      hoodieSparkTable.getHoodieView().sync();
    }
    return hoodieSparkTable;
  }
}
