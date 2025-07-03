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
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.SparkMetadataTableSecondaryDeltaCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkMetadataTableFirstDeltaCommitActionExecutor;

import java.util.List;

/**
 * MOR table used for Metadata writes. Has special handing of upserts to support streaming writes to metadata table.
 * @param <T>
 */
public class HoodieSparkMergeOnReadMetadataTable<T> extends HoodieSparkMergeOnReadTable<T> {

  HoodieSparkMergeOnReadMetadataTable(HoodieWriteConfig config, HoodieEngineContext context,
                                      HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  public HoodieWriteMetadata<HoodieData<WriteStatus>> upsertPrepped(HoodieEngineContext context, String instantTime,
                                                                    HoodieData<HoodieRecord<T>> preppedRecords,
                                                                    List<HoodieFileGroupId> hoodieFileGroupIdListOpt) {
    // upsert partitioner for metadata table when all records are upsert and locations are known upfront.
    // this is expected to be invoked first during streaming writes to metadata table.
    return new SparkMetadataTableFirstDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime,
        preppedRecords, hoodieFileGroupIdListOpt).execute();
  }

  public HoodieWriteMetadata<HoodieData<WriteStatus>> upsertPrepped(HoodieEngineContext context, String instantTime,
                                                                    HoodieData<HoodieRecord<T>> preppedRecords,
                                                                    boolean initialCall) {
    // upsert partitioner for metadata table when all records are upsert and locations are known upfront.
    // this is expected to be invoked second during streaming writes to metadata table.
    return new SparkMetadataTableSecondaryDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime,
        preppedRecords, initialCall).execute();
  }
}
