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
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.utilities.exception.HoodieStreamerException;
import org.apache.hudi.utilities.schema.LazyCastingIterator;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.HoodieAvroDeserializer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import scala.Tuple2;

import static org.apache.hudi.avro.AvroSchemaUtils.getAvroRecordQualifiedName;
import static org.apache.hudi.utilities.UtilHelpers.createRecordMerger;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME;

public class StreamSyncAvro extends StreamSync<JavaRDD<HoodieRecord>> {

  @Deprecated
  public StreamSyncAvro(HoodieStreamer.Config cfg, SparkSession sparkSession, SchemaProvider schemaProvider,
                        TypedProperties props, JavaSparkContext jssc, FileSystem fs, Configuration conf,
                        Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient)
      throws IOException {
    super(cfg, sparkSession, schemaProvider, props, jssc, fs, conf, onInitializingHoodieWriteClient);
  }

  public StreamSyncAvro(HoodieStreamer.Config cfg, SparkSession sparkSession, SchemaProvider schemaProvider,
                        TypedProperties props, HoodieSparkEngineContext hoodieSparkContext, FileSystem fs, Configuration conf,
                        Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient)
      throws IOException {
    super(cfg, sparkSession, schemaProvider, props, hoodieSparkContext, fs, conf, onInitializingHoodieWriteClient);
  }

  @Override
  protected InputBatch<JavaRDD<HoodieRecord>> fetchFromSourceWithTransformerAndUserSchemaProvider(Option<String> resumeCheckpointStr,
                                                                                                  boolean reconcileSchema, String instantTime) {
    InputBatch<Dataset<Row>> inputBatch = fetchDataAndTransform(resumeCheckpointStr);
    if (inputBatch == null) {
      return null;
    }
    // If the target schema is specified through Avro schema,
    // pass in the schema for the Row-to-Avro conversion
    // to avoid nullability mismatch between Avro schema and Row schema
    final Option<JavaRDD<GenericRecord>> avroRDDOptional;
    if (errorTableWriter.isPresent()
        && props.getBoolean(HoodieErrorTableConfig.ERROR_ENABLE_VALIDATE_TARGET_SCHEMA.key(),
        HoodieErrorTableConfig.ERROR_ENABLE_VALIDATE_TARGET_SCHEMA.defaultValue())) {
      // If the above conditions are met, trigger error events for the rows whose conversion to
      // avro records fails.
      avroRDDOptional = inputBatch.getBatch().map(
          rowDataset -> {
            Tuple2<RDD<GenericRecord>, RDD<String>> safeCreateRDDs = HoodieSparkUtils.safeCreateRDD(rowDataset,
                HOODIE_RECORD_STRUCT_NAME, HOODIE_RECORD_NAMESPACE, reconcileSchema,
                Option.of(this.userProvidedSchemaProvider.getTargetSchema()));
            errorTableWriter.get().addErrorEvents(safeCreateRDDs._2().toJavaRDD()
                .map(evStr -> new ErrorEvent<>(evStr,
                    ErrorEvent.ErrorReason.AVRO_DESERIALIZATION_FAILURE)));
            return safeCreateRDDs._1.toJavaRDD();
          });
    } else {
      avroRDDOptional = inputBatch.getBatch().map(rowDataset -> getTransformedRDD(rowDataset, reconcileSchema, this.userProvidedSchemaProvider.getTargetSchema()));
    }
    schemaProvider = this.userProvidedSchemaProvider;
    return convertGenericRecordToHoodie(new InputBatch<>(avroRDDOptional, inputBatch.getCheckpointForNextBatch(), this.userProvidedSchemaProvider), instantTime);
  }

  @Override
  protected InputBatch<JavaRDD<HoodieRecord>> fetchFromSourceWithTransformerWithoutUserSchemaProvider(Option<String> resumeCheckpointStr,
                                                                                                      HoodieTableMetaClient metaClient, boolean reconcileSchema, String instantTime) {
    InputBatch<Dataset<Row>> inputBatch = fetchDataAndTransform(resumeCheckpointStr);
    if (inputBatch == null) {
      return null;
    }
    Option<Schema> incomingSchemaOpt = inputBatch.getBatch().map(df ->
        AvroConversionUtils.convertStructTypeToAvroSchema(df.schema(), getAvroRecordQualifiedName(cfg.targetTableName)));
    schemaProvider = incomingSchemaOpt.map(incomingSchema -> getDeducedSchemaProvider(incomingSchema, inputBatch.getSchemaProvider(), metaClient))
        .orElse(inputBatch.getSchemaProvider());
    InputBatch<JavaRDD<GenericRecord>> outputBatch = new InputBatch<>(inputBatch.getBatch().map(t -> getTransformedRDD(t, reconcileSchema, schemaProvider.getTargetSchema())),
        inputBatch.getCheckpointForNextBatch(), schemaProvider);
    return convertGenericRecordToHoodie(outputBatch, instantTime);
  }

  @Override
  protected InputBatch<JavaRDD<HoodieRecord>> fetchFromSourceWithoutTransformer(Option<String> resumeCheckpointStr, HoodieTableMetaClient metaClient, String instantTime) {
    InputBatch<JavaRDD<GenericRecord>> dataAndCheckpoint = fetchNewDataInAvroFormat(resumeCheckpointStr, cfg.sourceLimit);
    if (dataAndCheckpoint == null) {
      return null;
    }
    schemaProvider = dataAndCheckpoint.getSchemaProvider();
    schemaProvider = getDeducedSchemaProvider(dataAndCheckpoint.getSchemaProvider().getSourceSchema(), dataAndCheckpoint.getSchemaProvider(), metaClient);
    String serializedTargetSchema = schemaProvider.getTargetSchema().toString();
    InputBatch<JavaRDD<GenericRecord>> outputBatch = new InputBatch<>(
        dataAndCheckpoint.getBatch().map(t -> t.mapPartitions(iterator -> new LazyCastingIterator(iterator, serializedTargetSchema))),
        dataAndCheckpoint.getCheckpointForNextBatch(), schemaProvider);
    return convertGenericRecordToHoodie(outputBatch, instantTime);
  }

  @Override
  protected boolean isEmpty(InputBatch<JavaRDD<HoodieRecord>> inputBatch) {
    return inputBatch.getBatch().get().isEmpty();
  }

  @Override
  protected void setupWriteClient(InputBatch<JavaRDD<HoodieRecord>> inputBatch) throws IOException {
    setupWriteClient(inputBatch.getBatch().get());
  }

  @Override
  protected void reInitWriteClient(Schema sourceSchema, Schema targetSchema, InputBatch<JavaRDD<HoodieRecord>> inputBatch) throws IOException {
    reInitWriteClient(sourceSchema, targetSchema, inputBatch.getBatch().get());
  }

  private InputBatch<JavaRDD<HoodieRecord>> convertGenericRecordToHoodie(InputBatch<JavaRDD<GenericRecord>> dataAndCheckpoint, String instantTime) {
    hoodieSparkContext.setJobStatus(this.getClass().getSimpleName(), "Checking if input is empty");
    if ((!dataAndCheckpoint.getBatch().isPresent()) || (dataAndCheckpoint.getBatch().get().isEmpty())) {
      LOG.info("No new data, perform empty commit.");
      return new InputBatch<>(Option.of(hoodieSparkContext.emptyRDD()), dataAndCheckpoint.getCheckpointForNextBatch(), schemaProvider);
    }

    HoodieRecord.HoodieRecordType recordType = createRecordMerger(props).getRecordType();
    boolean shouldCombine = cfg.filterDupes || cfg.operation.equals(WriteOperationType.UPSERT);
    Set<String> partitionColumns = getPartitionColumns(props);
    JavaRDD<HoodieRecord> records;
    SerializableSchema avroSchema = new SerializableSchema(schemaProvider.getTargetSchema());
    SerializableSchema processedAvroSchema = new SerializableSchema(isDropPartitionColumns() ? HoodieAvroUtils.removeMetadataFields(avroSchema.get()) : avroSchema.get());

    JavaRDD<GenericRecord> avroRDD = dataAndCheckpoint.getBatch().get();
    if (recordType == HoodieRecord.HoodieRecordType.AVRO) {
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
              GenericRecord gr = isDropPartitionColumns() ? HoodieAvroUtils.removeFields(genRec, partitionColumns) : genRec;
              HoodieRecordPayload payload = shouldCombine ? DataSourceUtils.createPayload(cfg.payloadClassName, gr,
                  (Comparable) HoodieAvroUtils.getNestedFieldVal(gr, cfg.sourceOrderingField, false, props.getBoolean(
                      KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
                      Boolean.parseBoolean(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()))))
                  : DataSourceUtils.createPayload(cfg.payloadClassName, gr);
              avroRecords.add(new HoodieAvroRecord<>(hoodieKey, payload));
            }
            return avroRecords.iterator();
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
        StructType targetStructType = isDropPartitionColumns() ? AvroConversionUtils
            .convertAvroSchemaToStructType(HoodieAvroUtils.removeFields(processedAvroSchema.get(), partitionColumns)) : baseStructType;
        HoodieAvroDeserializer deserializer = SparkAdapterSupport$.MODULE$.sparkAdapter().createAvroDeserializer(processedAvroSchema.get(), baseStructType);

        return new CloseableMappingIterator<>(ClosableIterator.wrap(itr), rec -> {
          InternalRow row = (InternalRow) deserializer.deserialize(rec).get();
          String recordKey = builtinKeyGenerator.getRecordKey(row, baseStructType).toString();
          String partitionPath = builtinKeyGenerator.getPartitionPath(row, baseStructType).toString();
          return new HoodieSparkRecord(new HoodieKey(recordKey, partitionPath),
              HoodieInternalRowUtils.getCachedUnsafeProjection(baseStructType, targetStructType).apply(row), targetStructType, false);
        });
      });
    } else {
      throw new UnsupportedOperationException(recordType.name());
    }

    return new InputBatch<>(Option.of(records), dataAndCheckpoint.getCheckpointForNextBatch(), schemaProvider);
  }

  @Override
  protected Pair<JavaRDD<WriteStatus>,Map<String, List<String>>> doWrite(String instantTime, InputBatch<JavaRDD<HoodieRecord>> inputBatch) {
    JavaRDD<HoodieRecord> records = inputBatch.getBatch().get();
    if (cfg.filterDupes) {
      records = DataSourceUtils.dropDuplicates(hoodieSparkContext.jsc(), records, writeClient.getConfig());
    }

    HoodieWriteResult writeResult;
    Map<String, List<String>> partitionToReplacedFileIds = Collections.emptyMap();
    JavaRDD<WriteStatus> writeStatusRDD;
    switch (cfg.operation) {
      case INSERT:
        writeStatusRDD = writeClient.insert(records, instantTime);
        break;
      case UPSERT:
        writeStatusRDD = writeClient.upsert(records, instantTime);
        break;
      case BULK_INSERT:
        writeStatusRDD = writeClient.bulkInsert(records, instantTime);
        break;
      case INSERT_OVERWRITE:
        writeResult = writeClient.insertOverwrite(records, instantTime);
        partitionToReplacedFileIds = writeResult.getPartitionToReplaceFileIds();
        writeStatusRDD = writeResult.getWriteStatuses();
        break;
      case INSERT_OVERWRITE_TABLE:
        writeResult = writeClient.insertOverwriteTable(records, instantTime);
        partitionToReplacedFileIds = writeResult.getPartitionToReplaceFileIds();
        writeStatusRDD = writeResult.getWriteStatuses();
        break;
      case DELETE_PARTITION:
        List<String> partitions = records.map(record -> record.getPartitionPath()).distinct().collect();
        writeResult = writeClient.deletePartitions(partitions, instantTime);
        partitionToReplacedFileIds = writeResult.getPartitionToReplaceFileIds();
        writeStatusRDD = writeResult.getWriteStatuses();
        break;
      default:
        throw new HoodieStreamerException("Unknown operation : " + cfg.operation);
    }

    return Pair.of(writeStatusRDD, partitionToReplacedFileIds);
  }

}
