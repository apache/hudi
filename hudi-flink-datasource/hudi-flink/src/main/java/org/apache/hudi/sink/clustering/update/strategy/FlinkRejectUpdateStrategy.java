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

package org.apache.hudi.sink.clustering.update.strategy;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieClusteringUpdateException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Set;

public class FlinkRejectUpdateStrategy<T extends HoodieRecordPayload> extends BaseFlinkUpdateStrategy<T> {

  private static final Logger LOG = LogManager.getLogger(FlinkRejectUpdateStrategy.class);

  public FlinkRejectUpdateStrategy(HoodieEngineContext engineContext) {
    super(engineContext);
  }

  @Override
  protected Pair<List<RecordsInstantPair>, Set<HoodieFileGroupId>> doHandleUpdate(HoodieFileGroupId fileId, RecordsInstantPair records) {
    String msg = String.format("Not allowed to update the clustering file group %s. "
            + "For pending clustering operations, we are not going to support update for now. "
            + "If you are using ConsistentHashing Bucket Index, try set CLUSTERING_UPDATE_STRATEGY to %s "
            + "to enable concurrent update & clustering.", fileId.toString(), FlinkConsistentBucketDuplicateUpdateStrategy.class.getName());
    LOG.error(msg);
    throw new HoodieClusteringUpdateException(msg);
  }
}
