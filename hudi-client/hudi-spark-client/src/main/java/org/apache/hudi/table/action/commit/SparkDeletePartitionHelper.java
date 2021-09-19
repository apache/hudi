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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.SparkHoodieRDDData;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaSparkContext;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

public class SparkDeletePartitionHelper<T extends HoodieRecordPayload<T>>
    extends BaseDeletePartitionHelper<T> {

  private SparkDeletePartitionHelper() {
  }

  private static class DeletePartitionHelperHolder {
    private static final SparkDeletePartitionHelper SPARK_DELETE_PARTITION_HELPER = new SparkDeletePartitionHelper();
  }

  public static SparkDeletePartitionHelper newInstance() {
    return SparkDeletePartitionHelper.DeletePartitionHelperHolder.SPARK_DELETE_PARTITION_HELPER;
  }

  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute(
      String instantTime, HoodieEngineContext context, HoodieTable table, List<String> partitions,
      BaseCommitHelper<T> commitHelper) {
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
    HoodieTimer timer = new HoodieTimer().startTimer();
    Map<String, List<String>> partitionToReplaceFileIds = jsc.parallelize(partitions, partitions.size()).distinct()
        .mapToPair(partitionPath -> new Tuple2<>(partitionPath, getAllExistingFileIds(table, partitionPath))).collectAsMap();
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = new HoodieWriteMetadata<>();
    result.setPartitionToReplaceFileIds(partitionToReplaceFileIds);
    result.setIndexUpdateDuration(Duration.ofMillis(timer.endTimer()));

    result.setWriteStatuses(SparkHoodieRDDData.of(jsc.emptyRDD()));
    commitHelper.saveWorkloadProfileMetadataToInflight(
        new WorkloadProfile(Pair.of(new HashMap<>(), new WorkloadStat())), instantTime);
    commitHelper.commitOnAutoCommit(result);
    return result;
  }

  protected List<String> getAllExistingFileIds(HoodieTable table, String partitionPath) {
    // because new commit is not complete. it is safe to mark all existing file Ids as old files
    return table.getSliceView().getLatestFileSlices(partitionPath).map(
        fg -> fg.getFileId()).distinct().collect(Collectors.toList());
  }
}
