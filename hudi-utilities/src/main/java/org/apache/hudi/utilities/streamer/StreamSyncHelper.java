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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.hudi.utilities.UtilHelpers.createRecordMerger;
import static org.apache.hudi.utilities.UtilHelpers.getPartitionColumns;

/**
 * Stream sync helper function that will APIs that help in fetching data from source, apply key gen etc.,
 */
public class StreamSyncHelper {

  public static JavaRDD<HoodieRecord> applyKeyGeneration(HoodieStreamer.Config cfg, TypedProperties props, SchemaProvider schemaProvider,
                                                         Option<JavaRDD<GenericRecord>> avroRDDOptional,
                                                         Option<Dataset<Row>> rowDatasetOptional,
                                                         boolean autoGenerateRecordKeys, String instantTime) {

    HoodieRecordType recordType = createRecordMerger(props).getRecordType();
    boolean shouldCombine = cfg.filterDupes || cfg.operation.equals(WriteOperationType.UPSERT);
    Set<String> partitionColumns = getPartitionColumns(props);
    JavaRDD<HoodieRecord> records = null;
    SerializableSchema avroSchema = new SerializableSchema(schemaProvider.getTargetSchema());
    SerializableSchema processedAvroSchema = new SerializableSchema(UtilHelpers.isDropPartitionColumns(props)
        ? HoodieAvroUtils.removeMetadataFields(avroSchema.get()) : avroSchema.get());
    try {
      if (recordType == HoodieRecordType.SPARK) {
        assert (rowDatasetOptional.isPresent());
        StructType baseStructType = AvroConversionUtils.convertAvroSchemaToStructType(processedAvroSchema.get());
        StructType targetStructType = UtilHelpers.isDropPartitionColumns(props) ? AvroConversionUtils
            .convertAvroSchemaToStructType(HoodieAvroUtils.removeFields(processedAvroSchema.get(), partitionColumns)) : baseStructType;

        Dataset<Row> df = rowDatasetOptional.get();
        records = df.queryExecution().toRdd()
            .toJavaRDD()
            .mapPartitions(itr -> {
              if (autoGenerateRecordKeys) {
                props.setProperty(KeyGenUtils.RECORD_KEY_GEN_PARTITION_ID_CONFIG, String.valueOf(TaskContext.getPartitionId()));
                props.setProperty(KeyGenUtils.RECORD_KEY_GEN_INSTANT_TIME_CONFIG, instantTime);
              }
              BuiltinKeyGenerator builtinKeyGenerator = (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
              return new CloseableMappingIterator<>(ClosableIterator.wrap(itr), rec -> {
                String recordKey = builtinKeyGenerator.getRecordKey(rec, baseStructType).toString();
                String partitionPath = builtinKeyGenerator.getPartitionPath(rec, baseStructType).toString();
                return new HoodieSparkRecord(new HoodieKey(recordKey, partitionPath),
                    HoodieInternalRowUtils.getCachedUnsafeProjection(baseStructType, targetStructType).apply(rec), targetStructType, false);
              });
            });
      } else {
        assert (avroRDDOptional.isPresent());
        JavaRDD<GenericRecord> avroRDD = avroRDDOptional.get();
        if (recordType == HoodieRecordType.AVRO) {
          records = avroRDD.mapPartitions(
              (FlatMapFunction<Iterator<GenericRecord>, HoodieRecord>) genericRecordIterator -> {
                if (autoGenerateRecordKeys) {
                  props.setProperty(KeyGenUtils.RECORD_KEY_GEN_PARTITION_ID_CONFIG, String.valueOf(TaskContext.getPartitionId()));
                  props.setProperty(KeyGenUtils.RECORD_KEY_GEN_INSTANT_TIME_CONFIG, instantTime);
                }
                BuiltinKeyGenerator builtinKeyGenerator = (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
                List<HoodieRecord> avroRecords = new ArrayList<>();
                while (genericRecordIterator.hasNext()) {
                  GenericRecord genRec = genericRecordIterator.next();
                  HoodieKey hoodieKey = new HoodieKey(builtinKeyGenerator.getRecordKey(genRec), builtinKeyGenerator.getPartitionPath(genRec));
                  GenericRecord gr = UtilHelpers.isDropPartitionColumns(props) ? HoodieAvroUtils.removeFields(genRec, partitionColumns) : genRec;
                  HoodieRecordPayload payload = shouldCombine ? DataSourceUtils.createPayload(cfg.payloadClassName, gr,
                      (Comparable) HoodieAvroUtils.getNestedFieldVal(gr, cfg.sourceOrderingField, false, props.getBoolean(
                          KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
                          Boolean.parseBoolean(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()))))
                      : DataSourceUtils.createPayload(cfg.payloadClassName, gr);
                  avroRecords.add(new HoodieAvroRecord<>(hoodieKey, payload));
                }
                return avroRecords.iterator();
              });
        } else {
          throw new UnsupportedOperationException(recordType.name());
        }
      }
    } catch (Exception e) {
      throw new HoodieException("Error converting input data to hoodie records.", e);
    }
    return records;
  }
}
