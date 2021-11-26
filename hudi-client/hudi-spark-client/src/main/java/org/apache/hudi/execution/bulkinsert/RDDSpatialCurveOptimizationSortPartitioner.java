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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.OrderingIndexHelper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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
    Dataset<Row> originDF =  AvroConversionUtils.createDataFrame(genericRecordJavaRDD.rdd(), schema.toString(), sparkEngineContext.getSqlContext().sparkSession());
    Dataset<Row> zDataFrame;

    switch (config.getLayoutOptimizationCurveBuildMethod()) {
      case DIRECT:
        zDataFrame = OrderingIndexHelper
            .createOptimizedDataFrameByMapValue(originDF, config.getClusteringSortColumns(), numOutputGroups, config.getLayoutOptimizationStrategy());
        break;
      case SAMPLE:
        zDataFrame = OrderingIndexHelper
            .createOptimizeDataFrameBySample(originDF, config.getClusteringSortColumns(), numOutputGroups, config.getLayoutOptimizationStrategy());
        break;
      default:
        throw new HoodieException("Not a valid build curve method for doWriteOperation: ");
    }
    return HoodieSparkUtils.createRdd(zDataFrame, schema.getName(),
        schema.getNamespace(), false, org.apache.hudi.common.util.Option.empty()).toJavaRDD();
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
