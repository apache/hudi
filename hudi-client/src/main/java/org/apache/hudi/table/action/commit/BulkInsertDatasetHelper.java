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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hudi.client.EncodableWriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

public class BulkInsertDatasetHelper {

  private static final Logger LOG = LogManager.getLogger(BulkInsertDatasetHelper.class);

  public static <T extends HoodieRecordPayload<T>> HoodieWriteMetadata bulkInsertDataset(
      Dataset<Row> rowDataset, String instantTime,
      HoodieTable<T> table, HoodieWriteConfig config,
      BulkInsertDatasetCommitActionExecutor<T> executor, boolean performDedupe,
      Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    HoodieWriteMetadata result = new HoodieWriteMetadata();

    // De-dupe/merge if needed
    Dataset<Row> dedupedRecords = rowDataset;

    /*if (performDedupe) {
      dedupedRecords = WriteHelper.combineOnCondition(config.shouldCombineBeforeInsert(), inputRecords,
          config.getInsertShuffleParallelism(), ((HoodieTable<T>)table));
    }*/

    // no user defined repartitioning support yet

    final Dataset<Row> rows = dedupedRecords
        .sort(config.getPartitionPathFieldProp(), config.getRecordKeyFieldProp())
        .coalesce(config.getBulkInsertShuffleParallelism());

    Dataset<Row> repartitionedRecords = rows
        .withColumn(HoodieRecord.FILENAME_METADATA_FIELD,
            functions.lit("").cast(DataTypes.StringType))
        .withColumn(HoodieRecord.PARTITION_PATH_METADATA_FIELD,
            functions.lit("").cast(DataTypes.StringType))
        .withColumn(HoodieRecord.RECORD_KEY_METADATA_FIELD,
            functions.lit("").cast(DataTypes.StringType))
        .withColumn(HoodieRecord.COMMIT_TIME_METADATA_FIELD,
            functions.lit("").cast(DataTypes.StringType))
        .withColumn(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
            functions.lit("").cast(DataTypes.StringType));

    // since we can't get partition index in scala mapPartition func, we have to generate these fileIds within
    // mapPartition functions
    /* // generate new file ID prefixes for each output partition
    final List<String> fileIDPrefixes =
        IntStream.range(0, parallelism).mapToObj(i -> FSUtils.createNewFileIdPfx()).collect(Collectors.toList());*/

    table.getActiveTimeline()
        .transitionRequestedToInflight(new HoodieInstant(HoodieInstant.State.REQUESTED,
            table.getMetaClient().getCommitActionType(), instantTime), Option.empty());

    // Generate encoder for Row
    ExpressionEncoder encoder = getEncoder(repartitionedRecords.schema());

    try {
      Dataset<EncodableWriteStatus> encWriteStatusDataset = repartitionedRecords.mapPartitions(
          new BulkInsertDatasetMapFunction<>(instantTime, config, table, encoder),
          Encoders.bean(EncodableWriteStatus.class));

      executor.updateIndexAndCommitIfNeeded(encWriteStatusDataset, result);
      return result;
    } catch (Throwable e) {
      LOG.error("Throwable thrwon in map partition func ", e);
      throw e;
    }
  }

  private static ExpressionEncoder getEncoder(StructType schema) {
    List<Attribute> attributes = JavaConversions.asJavaCollection(schema.toAttributes()).stream()
        .map(Attribute::toAttribute).collect(Collectors.toList());
    return RowEncoder.apply(schema)
        .resolveAndBind(JavaConverters.asScalaBufferConverter(attributes).asScala().toSeq(),
            SimpleAnalyzer$.MODULE$);
  }
}
