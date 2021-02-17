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

package org.apache.hudi.table;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.JavaHoodieIndex;
import org.apache.hudi.index.HoodieIndex;

import java.util.List;

public abstract class HoodieJavaTable<T extends HoodieRecordPayload>
    extends HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {
  protected HoodieJavaTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  public static <T extends HoodieRecordPayload> HoodieJavaTable<T> create(HoodieWriteConfig config, HoodieEngineContext context) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(context.getHadoopConf().get()).setBasePath(config.getBasePath())
            .setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
            .setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion()))).build();
    return HoodieJavaTable.create(config, (HoodieJavaEngineContext) context, metaClient);
  }

  public static <T extends HoodieRecordPayload> HoodieJavaTable<T> create(HoodieWriteConfig config,
                                                                          HoodieJavaEngineContext context,
                                                                          HoodieTableMetaClient metaClient) {
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        return new HoodieJavaCopyOnWriteTable<>(config, context, metaClient);
      case MERGE_ON_READ:
        throw new HoodieNotSupportedException("MERGE_ON_READ is not supported yet");
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
  }

  @Override
  protected HoodieIndex<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> getIndex(HoodieWriteConfig config, HoodieEngineContext context) {
    return JavaHoodieIndex.createIndex(config);
  }
}
