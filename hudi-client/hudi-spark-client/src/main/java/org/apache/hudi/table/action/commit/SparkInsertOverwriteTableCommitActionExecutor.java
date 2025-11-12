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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SparkInsertOverwriteTableCommitActionExecutor<T>
    extends SparkInsertOverwriteCommitActionExecutor<T> {

  public SparkInsertOverwriteTableCommitActionExecutor(HoodieEngineContext context,
                                                       HoodieWriteConfig config, HoodieTable table,
                                                       String instantTime, HoodieData<HoodieRecord<T>> inputRecordsRDD) {
    super(context, config, table, instantTime, inputRecordsRDD, WriteOperationType.INSERT_OVERWRITE_TABLE);
  }

  @Override
  protected Map<String, List<String>> getPartitionToReplacedFileIds(HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata) {
    List<String> partitionPaths = FSUtils.getAllPartitionPaths(context, table.getMetaClient(), config.getMetadataConfig());
    if (partitionPaths == null || partitionPaths.isEmpty()) {
      return Collections.emptyMap();
    }
    context.setJobStatus(this.getClass().getSimpleName(), "Getting ExistingFileIds of all partitions");
    return HoodieJavaPairRDD.getJavaPairRDD(context.parallelize(partitionPaths, partitionPaths.size()).mapToPair(
        partitionPath -> Pair.of(partitionPath, getAllExistingFileIds(partitionPath)))).collectAsMap();
  }
}
