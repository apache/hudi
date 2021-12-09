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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.sort.SpaceCurveSortingHelper;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;

/**
 * A partitioner that does spatial curve optimization sorting based on specified column values for each RDD partition.
 * support z-curve optimization, hilbert will come soon.
 * @param <T> HoodieRecordPayload type
 */
public class RDDSpatialCurveOptimizationSortPartitioner<T extends HoodieRecordPayload>
    implements BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> {
  private final HoodieSparkEngineContext sparkEngineContext;
  private final SerializableSchema serializableSchema;
  private final HoodieWriteConfig config;

  public RDDSpatialCurveOptimizationSortPartitioner(HoodieSparkEngineContext sparkEngineContext, HoodieWriteConfig config, Schema schema) {
    this.sparkEngineContext = sparkEngineContext;
    this.config = config;
    this.serializableSchema = new SerializableSchema(schema);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records, int outputSparkPartitions) {
    String payloadClass = config.getPayloadClass();
    // do sort
    JavaRDD<GenericRecord> preparedRecord = prepareGenericRecord(records, outputSparkPartitions, serializableSchema.get());
    return preparedRecord.map(record -> {
      String key = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      String partition = record.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
      HoodieKey hoodieKey = new HoodieKey(key, partition);
      HoodieRecordPayload avroPayload = ReflectionUtils.loadPayload(payloadClass,
          new Object[] {Option.of(record)}, Option.class);
      HoodieRecord hoodieRecord = new HoodieRecord(hoodieKey, avroPayload);
      return hoodieRecord;
    });
  }

  private JavaRDD<GenericRecord> prepareGenericRecord(JavaRDD<HoodieRecord<T>> inputRecords, final int numOutputGroups, final Schema schema) {
    SerializableSchema serializableSchema = new SerializableSchema(schema);
    JavaRDD<GenericRecord> genericRecordJavaRDD =  inputRecords.map(f -> (GenericRecord) f.getData().getInsertValue(serializableSchema.get()).get());
    Dataset<Row> originDF =
        AvroConversionUtils.createDataFrame(
            genericRecordJavaRDD.rdd(),
            schema.toString(),
            sparkEngineContext.getSqlContext().sparkSession()
        );

    Dataset<Row> sortedDF = reorder(originDF, numOutputGroups);

    return HoodieSparkUtils.createRdd(sortedDF, schema.getName(),
        schema.getNamespace(), false, org.apache.hudi.common.util.Option.empty()).toJavaRDD();
  }

  private Dataset<Row> reorder(Dataset<Row> originDF, int numOutputGroups) {
    String orderedColumnsListConfig = config.getClusteringSortColumns();

    if (isNullOrEmpty(orderedColumnsListConfig) || numOutputGroups <= 0) {
      // No-op
      return originDF;
    }

    List<String> orderedCols =
        Arrays.stream(orderedColumnsListConfig.split(","))
            .map(String::trim)
            .collect(Collectors.toList());

    HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy =
        HoodieClusteringConfig.LayoutOptimizationStrategy.fromValue(config.getLayoutOptimizationStrategy());

    HoodieClusteringConfig.BuildCurveStrategyType curveBuildStrategyType = config.getLayoutOptimizationCurveBuildMethod();

    switch (curveBuildStrategyType) {
      case DIRECT:
        return SpaceCurveSortingHelper.orderDataFrameByMappingValues(originDF, layoutOptStrategy, orderedCols, numOutputGroups);
      case SAMPLE:
        return SpaceCurveSortingHelper.orderDataFrameBySamplingValues(originDF, layoutOptStrategy, orderedCols, numOutputGroups);
      default:
        throw new UnsupportedOperationException(String.format("Unsupported space-curve curve building strategy (%s)", curveBuildStrategyType));
    }
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
