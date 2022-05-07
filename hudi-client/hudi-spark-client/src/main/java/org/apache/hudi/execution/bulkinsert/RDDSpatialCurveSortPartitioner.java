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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.RewriteAvroPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.sort.SpaceCurveSortingHelper;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;

/**
 * A partitioner that does spatial curve optimization sorting based on specified column values for each RDD partition.
 * support z-curve optimization, hilbert will come soon.
 * @param <T> HoodieRecordPayload type
 */
public class RDDSpatialCurveSortPartitioner<T extends HoodieRecordPayload>
    implements BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> {

  private final transient HoodieSparkEngineContext sparkEngineContext;
  private final String[] orderByColumns;
  private final SerializableSchema schema;
  private final HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy;
  private final HoodieClusteringConfig.SpatialCurveCompositionStrategyType curveCompositionStrategyType;

  public RDDSpatialCurveSortPartitioner(HoodieSparkEngineContext sparkEngineContext,
                                        String[] orderByColumns,
                                        HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy,
                                        HoodieClusteringConfig.SpatialCurveCompositionStrategyType curveCompositionStrategyType,
                                        Schema schema) {
    this.sparkEngineContext = sparkEngineContext;
    this.orderByColumns = orderByColumns;
    this.layoutOptStrategy = layoutOptStrategy;
    this.curveCompositionStrategyType = curveCompositionStrategyType;
    this.schema = new SerializableSchema(schema);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records, int outputSparkPartitions) {
    JavaRDD<GenericRecord> genericRecordsRDD =
        records.map(f -> (GenericRecord) f.getData().getInsertValue(schema.get()).get());

    Dataset<Row> sourceDataset =
        AvroConversionUtils.createDataFrame(
            genericRecordsRDD.rdd(),
            schema.toString(),
            sparkEngineContext.getSqlContext().sparkSession()
        );

    Dataset<Row> sortedDataset = reorder(sourceDataset, outputSparkPartitions);

    return HoodieSparkUtils.createRdd(sortedDataset, schema.get().getName(), schema.get().getNamespace(), false, Option.empty())
        .toJavaRDD()
        .map(record -> {
          String key = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
          String partition = record.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
          HoodieKey hoodieKey = new HoodieKey(key, partition);
          HoodieRecord hoodieRecord = new HoodieAvroRecord(hoodieKey, new RewriteAvroPayload(record));
          return hoodieRecord;
        });
  }

  private Dataset<Row> reorder(Dataset<Row> dataset, int numOutputGroups) {
    if (orderByColumns.length == 0) {
      // No-op
      return dataset;
    }

    List<String> orderedCols = Arrays.asList(orderByColumns);

    switch (curveCompositionStrategyType) {
      case DIRECT:
        return SpaceCurveSortingHelper.orderDataFrameByMappingValues(dataset, layoutOptStrategy, orderedCols, numOutputGroups);
      case SAMPLE:
        return SpaceCurveSortingHelper.orderDataFrameBySamplingValues(dataset, layoutOptStrategy, orderedCols, numOutputGroups);
      default:
        throw new UnsupportedOperationException(String.format("Unsupported space-curve curve building strategy (%s)", curveCompositionStrategyType));
    }
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
