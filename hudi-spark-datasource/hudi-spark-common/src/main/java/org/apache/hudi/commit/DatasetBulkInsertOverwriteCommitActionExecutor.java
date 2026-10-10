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

package org.apache.hudi.commit;

import org.apache.hudi.HoodieDatasetBulkInsertHelper;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieInternalConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DatasetBulkInsertOverwriteCommitActionExecutor extends BaseDatasetBulkInsertCommitActionExecutor {

  public DatasetBulkInsertOverwriteCommitActionExecutor(HoodieWriteConfig config,
                                                        SparkRDDWriteClient writeClient, String instantTime) {
    super(config, writeClient, instantTime);
  }

  @Override
  protected Option<HoodieData<WriteStatus>> doExecute(Dataset<Row> records, boolean arePartitionRecordsSorted) {
    table.getActiveTimeline().transitionRequestedToInflight(table.getMetaClient().createNewInstant(HoodieInstant.State.REQUESTED,
        getCommitActionType(), instantTime), Option.empty());
    return Option.of(HoodieDatasetBulkInsertHelper
        .bulkInsert(records, instantTime, table, writeConfig, arePartitionRecordsSorted, false));
  }

  /**
   * For INSERT_OVERWRITE: enumerate latest file groups in the targeted partitions so the caller
   * can reject overlap with pending clustering before the bulk-insert materializes. Mirrors
   * {@code SparkInsertOverwriteCommitActionExecutor#getFileGroupsBeingReplaced}; called by the
   * base class on the prepared dataset (after {@code prepareForBulkInsert}), so dynamic partition
   * resolution can read the rows' partition paths.
   */
  @Override
  protected Set<HoodieFileGroupId> getFileGroupsBeingReplaced(Dataset<Row> preparedRecords) {
    List<String> partitionPaths = resolveTargetPartitions(preparedRecords);
    if (partitionPaths.isEmpty()) {
      return Collections.emptySet();
    }
    return partitionPaths.stream()
        .flatMap(partitionPath -> table.getSliceView().getLatestFileSlices(partitionPath)
            .map(FileSlice::getFileGroupId))
        .collect(Collectors.toSet());
  }

  /**
   * Resolves the partition paths this overwrite will replace. Subclasses override for the
   * table-wide variant (enumerate every partition).
   */
  protected List<String> resolveTargetPartitions(Dataset<Row> preparedRecords) {
    if (!table.isPartitioned()) {
      return Collections.singletonList(StringUtils.EMPTY_STRING);
    }
    String staticOverwritePartitionPaths = writeConfig.getStringOrDefault(HoodieInternalConfig.STATIC_OVERWRITE_PARTITION_PATHS);
    if (StringUtils.nonEmpty(staticOverwritePartitionPaths)) {
      return Arrays.asList(staticOverwritePartitionPaths.split(","));
    }
    if (!writeConfig.populateMetaFields()) {
      // Without meta fields, HoodieDatasetBulkInsertHelper.prepareForBulkInsert stubs every meta
      // column with null, and the writer derives each row's partition path from the key generator
      // instead (BulkInsertDataInternalWriterHelper#extractPartitionPath), so do the same here.
      return resolvePartitionPathsWithKeyGenerator(preparedRecords, writeConfig.getProps());
    }
    // Dynamic partition path: read the populated _hoodie_partition_path meta field. The base
    // class invokes this hook after HoodieDatasetBulkInsertHelper.prepareForBulkInsert, which
    // populates the field through the configured key generator when meta fields are populated.
    return preparedRecords.select(HoodieRecord.PARTITION_PATH_METADATA_FIELD)
        .distinct()
        .as(Encoders.STRING())
        .collectAsList();
  }

  /**
   * Computes the distinct partition paths of the prepared rows with the configured key generator,
   * the same way the bulk-insert writer does when the {@code _hoodie_partition_path} meta field is
   * not populated. Static, so the Spark closure captures only the schema and the properties.
   */
  private static List<String> resolvePartitionPathsWithKeyGenerator(Dataset<Row> preparedRecords, TypedProperties props) {
    StructType schema = preparedRecords.schema();
    return preparedRecords.queryExecution().toRdd().toJavaRDD()
        .mapPartitions(rows -> {
          Option<BuiltinKeyGenerator> keyGeneratorOpt = HoodieSparkKeyGeneratorFactory.getKeyGenerator(props);
          Set<String> partitionPaths = new HashSet<>();
          rows.forEachRemaining(row -> partitionPaths.add(keyGeneratorOpt.isPresent()
              ? keyGeneratorOpt.get().getPartitionPath(row, schema).toString()
              : StringUtils.EMPTY_STRING));
          return partitionPaths.iterator();
        })
        .distinct()
        .collect();
  }

  @Override
  public WriteOperationType getWriteOperationType() {
    return WriteOperationType.INSERT_OVERWRITE;
  }

  @Override
  protected Map<String, List<String>> getPartitionToReplacedFileIds(HoodieData<WriteStatus> writeStatuses) {
    if (!table.isPartitioned()) {
      // Short-circuit for unpartitioned tables - only one partition path: empty string
      return Collections.singletonMap(StringUtils.EMPTY_STRING, getAllExistingFileIds(StringUtils.EMPTY_STRING));
    }

    String staticOverwritePartition = writeConfig.getStringOrDefault(HoodieInternalConfig.STATIC_OVERWRITE_PARTITION_PATHS);
    if (StringUtils.nonEmpty(staticOverwritePartition)) {
      // static insert overwrite partitions
      List<String> partitionPaths = Arrays.asList(staticOverwritePartition.split(","));
      table.getContext().setJobStatus(this.getClass().getSimpleName(), "Getting ExistingFileIds of matching static partitions");
      return HoodieJavaPairRDD.getJavaPairRDD(table.getContext().parallelize(partitionPaths, partitionPaths.size()).mapToPair(
          partitionPath -> Pair.of(partitionPath, getAllExistingFileIds(partitionPath)))).collectAsMap();
    } else {
      // dynamic insert overwrite partitions
      return HoodieJavaPairRDD.getJavaPairRDD(writeStatuses.map(status -> status.getStat().getPartitionPath()).distinct().mapToPair(partitionPath ->
          Pair.of(partitionPath, getAllExistingFileIds(partitionPath)))).collectAsMap();
    }
  }

  protected List<String> getAllExistingFileIds(String partitionPath) {
    // because new commit is not complete. it is safe to mark all existing file Ids as old files
    return table.getSliceView().getLatestFileSlices(partitionPath).map(FileSlice::getFileId).distinct().collect(Collectors.toList());
  }
}
