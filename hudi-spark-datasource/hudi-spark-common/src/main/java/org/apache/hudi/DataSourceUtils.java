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

package org.apache.hudi;

import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.TablePathUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.common.util.CommitUtils.getCheckpointValueAsString;

/**
 * Utilities used throughout the data source.
 */
public class DataSourceUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DataSourceUtils.class);

  @VisibleForTesting
  public static Pair<String, StructField> getSchemaForField(StructType schema, String fieldName) {
    return getSchemaForField(schema, fieldName, StringUtils.EMPTY_STRING);
  }

  @VisibleForTesting
  public static Pair<String, StructField> getSchemaForField(StructType schema, String fieldName, String prefix) {
    if (!fieldName.contains(".")) {
      return Pair.of(prefix + schema.fields()[schema.fieldIndex(fieldName)].name(), schema.fields()[schema.fieldIndex(fieldName)]);
    } else {
      int rootFieldIndex = fieldName.indexOf(".");
      StructField rootField = schema.fields()[schema.fieldIndex(fieldName.substring(0, rootFieldIndex))];
      if (rootField == null) {
        throw new HoodieException("Failed to find " + fieldName + " in the table schema ");
      }
      return getSchemaForField((StructType) rootField.dataType(), fieldName.substring(rootFieldIndex + 1), prefix + fieldName.substring(0, rootFieldIndex + 1));
    }
  }

  public static String getTablePath(HoodieStorage storage,
                                    List<StoragePath> userProvidedPaths) throws IOException {
    LOG.info("Getting table path..");
    for (StoragePath path : userProvidedPaths) {
      try {
        Option<StoragePath> tablePath = TablePathUtils.getTablePath(storage, path);
        if (tablePath.isPresent()) {
          return tablePath.get().toString();
        }
      } catch (HoodieException he) {
        LOG.warn("Error trying to get table path from " + path.toString(), he);
      }
    }

    throw new TableNotFoundException(userProvidedPaths.stream()
        .map(StoragePath::toString).collect(Collectors.joining(",")));
  }

  /**
   * Create a UserDefinedBulkInsertPartitioner class via reflection,
   * <br>
   * if the class name of UserDefinedBulkInsertPartitioner is configured through the HoodieWriteConfig.
   *
   * @see HoodieWriteConfig#getUserDefinedBulkInsertPartitionerClass()
   */
  public static Option<BulkInsertPartitioner> createUserDefinedBulkInsertPartitioner(HoodieWriteConfig config)
      throws HoodieException {
    String bulkInsertPartitionerClass = config.getUserDefinedBulkInsertPartitionerClass();
    try {
      return StringUtils.isNullOrEmpty(bulkInsertPartitionerClass)
          ? Option.empty() :
          Option.of((BulkInsertPartitioner) ReflectionUtils.loadClass(bulkInsertPartitionerClass, config));
    } catch (Throwable e) {
      throw new HoodieException("Could not create UserDefinedBulkInsertPartitioner class " + bulkInsertPartitionerClass, e);
    }
  }

  /**
   * Create a UserDefinedBulkInsertPartitionerRows class via reflection,
   * <br>
   * if the class name of UserDefinedBulkInsertPartitioner is configured through the HoodieWriteConfig.
   *
   * @see HoodieWriteConfig#getUserDefinedBulkInsertPartitionerClass()
   */
  public static Option<BulkInsertPartitioner<Dataset<Row>>> createUserDefinedBulkInsertPartitionerWithRows(HoodieWriteConfig config)
      throws HoodieException {
    String bulkInsertPartitionerClass = config.getUserDefinedBulkInsertPartitionerClass();
    try {
      return StringUtils.isNullOrEmpty(bulkInsertPartitionerClass)
          ? Option.empty() :
          Option.of((BulkInsertPartitioner) ReflectionUtils.loadClass(bulkInsertPartitionerClass, config));
    } catch (Throwable e) {
      throw new HoodieException("Could not create UserDefinedBulkInsertPartitionerRows class " + bulkInsertPartitionerClass, e);
    }
  }

  /**
   * Create a payload class via reflection, passing in an ordering/precombine value.
   */
  public static HoodieRecordPayload createPayload(String payloadClass, GenericRecord record, Comparable orderingVal)
      throws IOException {
    try {
      return (HoodieRecordPayload) ReflectionUtils.loadClass(payloadClass,
          new Class<?>[] {GenericRecord.class, Comparable.class}, record, orderingVal);
    } catch (Throwable e) {
      throw new IOException("Could not create payload for class: " + payloadClass, e);
    }
  }

  public static Map<String, String> getExtraMetadata(Map<String, String> properties) {
    Map<String, String> extraMetadataMap = new HashMap<>();
    if (properties.containsKey(DataSourceWriteOptions.COMMIT_METADATA_KEYPREFIX().key())) {
      properties.entrySet().forEach(entry -> {
        if (entry.getKey().startsWith(properties.get(DataSourceWriteOptions.COMMIT_METADATA_KEYPREFIX().key()))) {
          extraMetadataMap.put(entry.getKey(), entry.getValue());
        }
      });
    }
    if (properties.containsKey(HoodieSparkSqlWriter.SPARK_STREAMING_BATCH_ID())) {
      extraMetadataMap.put(HoodieStreamingSink.SINK_CHECKPOINT_KEY(),
          getCheckpointValueAsString(properties.getOrDefault(DataSourceWriteOptions.STREAMING_CHECKPOINT_IDENTIFIER().key(),
                  DataSourceWriteOptions.STREAMING_CHECKPOINT_IDENTIFIER().defaultValue()),
              properties.get(HoodieSparkSqlWriter.SPARK_STREAMING_BATCH_ID())));
    }
    return extraMetadataMap;
  }

  /**
   * Create a payload class via reflection, do not ordering/precombine value.
   */
  public static HoodieRecordPayload createPayload(String payloadClass, GenericRecord record)
      throws IOException {
    try {
      return (HoodieRecordPayload) ReflectionUtils.loadClass(payloadClass,
          new Class<?>[] {Option.class}, Option.of(record));
    } catch (Throwable e) {
      throw new IOException("Could not create payload for class: " + payloadClass, e);
    }
  }

  public static HoodieWriteConfig createHoodieConfig(String schemaStr, String basePath,
                                                     String tblName, Map<String, String> parameters) {
    boolean asyncCompact = Boolean.parseBoolean(parameters.get(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE().key()));
    boolean inlineCompact = false;
    if (parameters.containsKey(HoodieCompactionConfig.INLINE_COMPACT.key())) {
      // if inline is set, fetch the value from it.
      inlineCompact = Boolean.parseBoolean(parameters.get(HoodieCompactionConfig.INLINE_COMPACT.key()));
    }
    // if inline is false, derive the value from asyncCompact and table type
    if (!inlineCompact) {
      inlineCompact = !asyncCompact && parameters.get(DataSourceWriteOptions.TABLE_TYPE().key())
          .equals(DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL());
    }
    // insert/bulk-insert combining to be true, if filtering for duplicates
    boolean combineInserts = Boolean.parseBoolean(parameters.get(DataSourceWriteOptions.INSERT_DROP_DUPS().key()));
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withPath(basePath).withAutoCommit(false).combineInput(combineInserts, true);
    if (schemaStr != null) {
      builder = builder.withSchema(schemaStr);
    }

    return builder.forTable(tblName)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(inlineCompact).build())
        .withPayloadConfig(HoodiePayloadConfig.newBuilder()
            // For Spark SQL INSERT INTO and MERGE INTO, custom payload classes are used
            // to realize the SQL functionality, so the write config needs to be fetched first.
            .withPayloadClass(parameters.getOrDefault(DataSourceWriteOptions.PAYLOAD_CLASS_NAME().key(),
                parameters.getOrDefault(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), HoodieTableConfig.DEFAULT_PAYLOAD_CLASS_NAME)))
            .withPayloadOrderingField(parameters.get(DataSourceWriteOptions.PRECOMBINE_FIELD().key()))
            .build())
        // override above with Hoodie configs specified as options.
        .withProps(parameters).build();
  }

  public static SparkRDDWriteClient createHoodieClient(JavaSparkContext jssc, String schemaStr, String basePath,
                                                       String tblName, Map<String, String> parameters) {
    return new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jssc), createHoodieConfig(schemaStr, basePath, tblName, parameters));
  }

  public static HoodieWriteResult doWriteOperation(SparkRDDWriteClient client, JavaRDD<HoodieRecord> hoodieRecords,
                                                   String instantTime, WriteOperationType operation, Boolean isPrepped) throws HoodieException {
    switch (operation) {
      case BULK_INSERT:
        Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner =
                createUserDefinedBulkInsertPartitioner(client.getConfig());
        return new HoodieWriteResult(client.bulkInsert(hoodieRecords, instantTime, userDefinedBulkInsertPartitioner));
      case INSERT:
        return new HoodieWriteResult(client.insert(hoodieRecords, instantTime));
      case UPSERT:
        if (isPrepped) {
          return new HoodieWriteResult(client.upsertPreppedRecords(hoodieRecords, instantTime));
        }

        return new HoodieWriteResult(client.upsert(hoodieRecords, instantTime));
      case INSERT_OVERWRITE:
        return client.insertOverwrite(hoodieRecords, instantTime);
      case INSERT_OVERWRITE_TABLE:
        return client.insertOverwriteTable(hoodieRecords, instantTime);
      default:
        throw new HoodieException("Not a valid operation type for doWriteOperation: " + operation.toString());
    }
  }

  public static HoodieWriteResult doDeleteOperation(SparkRDDWriteClient client, JavaRDD<Tuple2<HoodieKey, scala.Option<HoodieRecordLocation>>> hoodieKeysAndLocations,
      String instantTime, boolean isPrepped) {

    if (isPrepped) {
      HoodieRecord.HoodieRecordType recordType = client.getConfig().getRecordMerger().getRecordType();
      JavaRDD<HoodieRecord> records = hoodieKeysAndLocations.map(tuple -> {
        HoodieRecord record = recordType == HoodieRecord.HoodieRecordType.AVRO
            ? new HoodieAvroRecord(tuple._1, new EmptyHoodieRecordPayload())
            : new HoodieSparkRecord(tuple._1, null, false);
        record.setCurrentLocation(tuple._2.get());
        return record;
      });
      return new HoodieWriteResult(client.deletePrepped(records, instantTime));
    }

    return new HoodieWriteResult(client.delete(hoodieKeysAndLocations.map(tuple -> tuple._1()), instantTime));
  }

  public static HoodieWriteResult doDeletePartitionsOperation(SparkRDDWriteClient client, List<String> partitionsToDelete,
                                                    String instantTime) {
    return client.deletePartitions(partitionsToDelete, instantTime);
  }

  public static HoodieRecord createHoodieRecord(GenericRecord gr, Comparable orderingVal, HoodieKey hKey,
      String payloadClass, scala.Option<HoodieRecordLocation> recordLocation) throws IOException {
    HoodieRecordPayload payload = DataSourceUtils.createPayload(payloadClass, gr, orderingVal);
    HoodieAvroRecord record = new HoodieAvroRecord<>(hKey, payload);
    if (recordLocation.isDefined()) {
      record.setCurrentLocation(recordLocation.get());
    }
    return record;
  }

  public static HoodieRecord createHoodieRecord(GenericRecord gr, HoodieKey hKey,
                                                String payloadClass, scala.Option<HoodieRecordLocation> recordLocation) throws IOException {
    HoodieRecordPayload payload = DataSourceUtils.createPayload(payloadClass, gr);
    HoodieAvroRecord record = new HoodieAvroRecord<>(hKey, payload);
    if (recordLocation.isDefined()) {
      record.setCurrentLocation(recordLocation.get());
    }
    return record;
  }

  /**
   * Drop records already present in the dataset.
   *
   * @param engineContext the spark engine context
   * @param incomingHoodieRecords HoodieRecords to deduplicate
   * @param writeConfig HoodieWriteConfig
   */
  @SuppressWarnings("unchecked")
  public static JavaRDD<HoodieRecord> dropDuplicates(HoodieSparkEngineContext engineContext, JavaRDD<HoodieRecord> incomingHoodieRecords,
      HoodieWriteConfig writeConfig) {
    try {
      SparkRDDReadClient client = new SparkRDDReadClient<>(engineContext, writeConfig);
      return client.tagLocation(incomingHoodieRecords)
          .filter(r -> !((HoodieRecord<HoodieRecordPayload>) r).isCurrentLocationKnown());
    } catch (TableNotFoundException e) {
      // this will be executed when there is no hoodie table yet
      // so no dups to drop
      return incomingHoodieRecords;
    }
  }

  @SuppressWarnings("unchecked")
  public static JavaRDD<HoodieRecord> dropDuplicates(JavaSparkContext jssc, JavaRDD<HoodieRecord> incomingHoodieRecords,
      Map<String, String> parameters) {
    HoodieWriteConfig writeConfig =
        HoodieWriteConfig.newBuilder().withPath(parameters.get("path")).withProps(parameters).build();
    return dropDuplicates(new HoodieSparkEngineContext(jssc), incomingHoodieRecords, writeConfig);
  }
}
