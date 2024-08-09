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
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Either;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.exception.HoodieRecordCreationException;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.util.SparkKeyGenUtils;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.avro.HoodieAvroDeserializer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.DROP_PARTITION_COLUMNS;
import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_ENABLE_VALIDATE_RECORD_CREATION;

/**
 * Util class for HoodieStreamer.
 */
public class HoodieStreamerUtils {

  /**
   * Generates HoodieRecords for the avro data read from source.
   * Takes care of dropping columns, precombine, auto key generation.
   * Both AVRO and SPARK record types are supported.
   */
  public static Option<JavaRDD<HoodieRecord>> createHoodieRecords(HoodieStreamer.Config cfg, TypedProperties props, Option<JavaRDD<GenericRecord>> avroRDDOptional,
                                                                  SchemaProvider schemaProvider, HoodieRecord.HoodieRecordType recordType, boolean autoGenerateRecordKeys,
                                                                  String instantTime, Option<BaseErrorTableWriter> errorTableWriter) {
    boolean shouldCombine = cfg.filterDupes || cfg.operation.equals(WriteOperationType.UPSERT);
    boolean shouldErrorTable = errorTableWriter.isPresent() && props.getBoolean(ERROR_ENABLE_VALIDATE_RECORD_CREATION.key(), ERROR_ENABLE_VALIDATE_RECORD_CREATION.defaultValue());
    boolean useConsistentLogicalTimestamp = ConfigUtils.getBooleanWithAltKeys(
        props, KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED);
    Set<String> partitionColumns = getPartitionColumns(props);
    return avroRDDOptional.map(avroRDD -> {
      SerializableSchema avroSchema = new SerializableSchema(schemaProvider.getTargetSchema());
      SerializableSchema processedAvroSchema = new SerializableSchema(isDropPartitionColumns(props) ? HoodieAvroUtils.removeMetadataFields(avroSchema.get()) : avroSchema.get());
      JavaRDD<Either<HoodieRecord,String>> records;
      if (recordType == HoodieRecord.HoodieRecordType.AVRO) {
        records = avroRDD.mapPartitions(
            (FlatMapFunction<Iterator<GenericRecord>, Either<HoodieRecord,String>>) genericRecordIterator -> {
              if (autoGenerateRecordKeys) {
                props.setProperty(KeyGenUtils.RECORD_KEY_GEN_PARTITION_ID_CONFIG, String.valueOf(TaskContext.getPartitionId()));
                props.setProperty(KeyGenUtils.RECORD_KEY_GEN_INSTANT_TIME_CONFIG, instantTime);
              }
              BuiltinKeyGenerator builtinKeyGenerator = (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
              return new CloseableMappingIterator<>(ClosableIterator.wrap(genericRecordIterator), genRec -> {
                try {
                  HoodieKey hoodieKey = new HoodieKey(builtinKeyGenerator.getRecordKey(genRec), builtinKeyGenerator.getPartitionPath(genRec));
                  GenericRecord gr = isDropPartitionColumns(props) ? HoodieAvroUtils.removeFields(genRec, partitionColumns) : genRec;
                  HoodieRecordPayload payload = shouldCombine ? DataSourceUtils.createPayload(cfg.payloadClassName, gr,
                      (Comparable) HoodieAvroUtils.getNestedFieldVal(gr, cfg.sourceOrderingField, false, useConsistentLogicalTimestamp))
                      : DataSourceUtils.createPayload(cfg.payloadClassName, gr);
                  return Either.left(new HoodieAvroRecord<>(hoodieKey, payload));
                } catch (Exception e) {
                  return generateErrorRecordOrThrowException(genRec, e, shouldErrorTable);
                }
              });
            });

      } else if (recordType == HoodieRecord.HoodieRecordType.SPARK) {
        // TODO we should remove it if we can read InternalRow from source.

        records = avroRDD.mapPartitions(itr -> {
          if (autoGenerateRecordKeys) {
            props.setProperty(KeyGenUtils.RECORD_KEY_GEN_PARTITION_ID_CONFIG, String.valueOf(TaskContext.getPartitionId()));
            props.setProperty(KeyGenUtils.RECORD_KEY_GEN_INSTANT_TIME_CONFIG, instantTime);
          }
          BuiltinKeyGenerator builtinKeyGenerator = (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
          StructType baseStructType = AvroConversionUtils.convertAvroSchemaToStructType(processedAvroSchema.get());
          StructType targetStructType = isDropPartitionColumns(props) ? AvroConversionUtils
              .convertAvroSchemaToStructType(HoodieAvroUtils.removeFields(processedAvroSchema.get(), partitionColumns)) : baseStructType;
          HoodieAvroDeserializer deserializer = SparkAdapterSupport$.MODULE$.sparkAdapter().createAvroDeserializer(processedAvroSchema.get(), baseStructType);

          return new CloseableMappingIterator<>(ClosableIterator.wrap(itr), rec -> {
            InternalRow row = (InternalRow) deserializer.deserialize(rec).get();
            try {
              String recordKey = builtinKeyGenerator.getRecordKey(row, baseStructType).toString();
              String partitionPath = builtinKeyGenerator.getPartitionPath(row, baseStructType).toString();
              return Either.left(new HoodieSparkRecord(new HoodieKey(recordKey, partitionPath),
                  HoodieInternalRowUtils.getCachedUnsafeProjection(baseStructType, targetStructType).apply(row), targetStructType, false));
            } catch (Exception e) {
              return generateErrorRecordOrThrowException(rec, e, shouldErrorTable);
            }
          });

        });
      } else {
        throw new UnsupportedOperationException(recordType.name());
      }
      if (shouldErrorTable) {
        errorTableWriter.get().addErrorEvents(records.filter(Either::isRight).map(Either::asRight).map(evStr -> new ErrorEvent<>(evStr,
            ErrorEvent.ErrorReason.RECORD_CREATION)));
      }
      return records.filter(Either::isLeft).map(Either::asLeft);
    });
  }

  /**
   * @param genRec Avro {@link GenericRecord} instance.
   * @return the representation of error record (empty {@link HoodieRecord} and the error record
   * String) for writing to error table.
   */
  private static Either<HoodieRecord, String> generateErrorRecordOrThrowException(GenericRecord genRec, Exception e, boolean shouldErrorTable) {
    if (!shouldErrorTable) {
      if (e instanceof HoodieKeyException) {
        throw (HoodieKeyException) e;
      } else if (e instanceof HoodieKeyGeneratorException) {
        throw (HoodieKeyGeneratorException) e;
      } else {
        throw new HoodieRecordCreationException("Failed to create Hoodie Record", e);
      }
    }
    try {
      return Either.right(HoodieAvroUtils.safeAvroToJsonString(genRec));
    } catch (Exception ex) {
      throw new HoodieException("Failed to convert illegal record to json", ex);
    }
  }

  /**
   * Set based on hoodie.datasource.write.drop.partition.columns config.
   * When set to true, will not write the partition columns into the table.
   */
  static Boolean isDropPartitionColumns(TypedProperties props) {
    return props.getBoolean(DROP_PARTITION_COLUMNS.key(), DROP_PARTITION_COLUMNS.defaultValue());
  }

  /**
   * Get the partition columns as a set of strings.
   *
   * @param props TypedProperties
   * @return Set of partition columns.
   */
  static Set<String> getPartitionColumns(TypedProperties props) {
    String partitionColumns = SparkKeyGenUtils.getPartitionColumns(props);
    return Arrays.stream(partitionColumns.split(",")).collect(Collectors.toSet());
  }

}
