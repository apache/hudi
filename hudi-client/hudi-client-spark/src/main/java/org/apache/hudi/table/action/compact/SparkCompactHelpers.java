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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.util.List;

/**
 * A spark implementation of {@link AbstractCompactHelpers}.
 *
 * @param <T>
 */
public class SparkCompactHelpers<T extends HoodieRecordPayload> extends
    AbstractCompactHelpers<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private SparkCompactHelpers() {
  }

  private static class CompactHelperHolder {
    private static final SparkCompactHelpers SPARK_COMPACT_HELPERS = new SparkCompactHelpers();
  }

  public static SparkCompactHelpers newInstance() {
    return CompactHelperHolder.SPARK_COMPACT_HELPERS;
  }

  @Override
  public HoodieCommitMetadata createCompactionMetadata(HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table,
                                                       String compactionInstantTime,
                                                       JavaRDD<WriteStatus> writeStatuses,
                                                       String schema) throws IOException {
    byte[] planBytes = table.getActiveTimeline().readCompactionPlanAsBytes(
        HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime)).get();
    HoodieCompactionPlan compactionPlan = TimelineMetadataUtils.deserializeCompactionPlan(planBytes);
    List<HoodieWriteStat> updateStatusMap = writeStatuses.map(WriteStatus::getStat).collect();
    org.apache.hudi.common.model.HoodieCommitMetadata metadata = new org.apache.hudi.common.model.HoodieCommitMetadata(true);
    for (HoodieWriteStat stat : updateStatusMap) {
      metadata.addWriteStat(stat.getPartitionPath(), stat);
    }
    metadata.addMetadata(org.apache.hudi.common.model.HoodieCommitMetadata.SCHEMA_KEY, schema);
    if (compactionPlan.getExtraMetadata() != null) {
      compactionPlan.getExtraMetadata().forEach(metadata::addMetadata);
    }
    return metadata;
  }
}
