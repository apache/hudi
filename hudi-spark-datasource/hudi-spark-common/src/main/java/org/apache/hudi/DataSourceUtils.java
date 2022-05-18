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

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.TablePathUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.util.DataTypeUtils;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveExternalCatalog;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utilities used throughout the data source.
 */
public class DataSourceUtils {

  private static final Logger LOG = LogManager.getLogger(DataSourceUtils.class);

  public static String getTablePath(FileSystem fs, Path[] userProvidedPaths) throws IOException {
    LOG.info("Getting table path..");
    for (Path path : userProvidedPaths) {
      try {
        Option<Path> tablePath = TablePathUtils.getTablePath(fs, path);
        if (tablePath.isPresent()) {
          return tablePath.get().toString();
        }
      } catch (HoodieException he) {
        LOG.warn("Error trying to get table path from " + path.toString(), he);
      }
    }

    throw new TableNotFoundException("Unable to find a hudi table for the user provided paths.");
  }

  /**
   * Create a UserDefinedBulkInsertPartitioner class via reflection,
   * <br>
   * if the class name of UserDefinedBulkInsertPartitioner is configured through the HoodieWriteConfig.
   *
   * @see HoodieWriteConfig#getUserDefinedBulkInsertPartitionerClass()
   */
  private static Option<BulkInsertPartitioner> createUserDefinedBulkInsertPartitioner(HoodieWriteConfig config)
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

  public static void checkRequiredProperties(TypedProperties props, List<String> checkPropNames) {
    checkPropNames.forEach(prop -> {
      if (!props.containsKey(prop)) {
        throw new HoodieNotSupportedException("Required property " + prop + " is missing");
      }
    });
  }

  public static HoodieWriteConfig createHoodieConfig(String schemaStr, String basePath,
      String tblName, Map<String, String> parameters) {
    boolean asyncCompact = Boolean.parseBoolean(parameters.get(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE().key()));
    boolean inlineCompact = !asyncCompact && parameters.get(DataSourceWriteOptions.TABLE_TYPE().key())
        .equals(DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL());
    // insert/bulk-insert combining to be true, if filtering for duplicates
    boolean combineInserts = Boolean.parseBoolean(parameters.get(DataSourceWriteOptions.INSERT_DROP_DUPS().key()));
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withPath(basePath).withAutoCommit(false).combineInput(combineInserts, true);
    if (schemaStr != null) {
      builder = builder.withSchema(schemaStr);
    }

    return builder.forTable(tblName)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withPayloadClass(parameters.get(DataSourceWriteOptions.PAYLOAD_CLASS_NAME().key()))
            .withInlineCompaction(inlineCompact).build())
        .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadOrderingField(parameters.get(DataSourceWriteOptions.PRECOMBINE_FIELD().key()))
            .build())
        // override above with Hoodie configs specified as options.
        .withProps(parameters).build();
  }

  public static SparkRDDWriteClient createHoodieClient(JavaSparkContext jssc, String schemaStr, String basePath,
                                                       String tblName, Map<String, String> parameters) {
    return new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jssc), createHoodieConfig(schemaStr, basePath, tblName, parameters));
  }

  public static HoodieWriteResult doWriteOperation(SparkRDDWriteClient client, JavaRDD<HoodieRecord> hoodieRecords,
                                                   String instantTime, WriteOperationType operation) throws HoodieException {
    switch (operation) {
      case BULK_INSERT:
        Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner =
                createUserDefinedBulkInsertPartitioner(client.getConfig());
        return new HoodieWriteResult(client.bulkInsert(hoodieRecords, instantTime, userDefinedBulkInsertPartitioner));
      case INSERT:
        return new HoodieWriteResult(client.insert(hoodieRecords, instantTime));
      case UPSERT:
        return new HoodieWriteResult(client.upsert(hoodieRecords, instantTime));
      case INSERT_OVERWRITE:
        return client.insertOverwrite(hoodieRecords, instantTime);
      case INSERT_OVERWRITE_TABLE:
        return client.insertOverwriteTable(hoodieRecords, instantTime);
      default:
        throw new HoodieException("Not a valid operation type for doWriteOperation: " + operation.toString());
    }
  }

  public static HoodieWriteResult doDeleteOperation(SparkRDDWriteClient client, JavaRDD<HoodieKey> hoodieKeys,
      String instantTime) {
    return new HoodieWriteResult(client.delete(hoodieKeys, instantTime));
  }

  public static HoodieWriteResult doDeletePartitionsOperation(SparkRDDWriteClient client, List<String> partitionsToDelete,
                                                    String instantTime) {
    return client.deletePartitions(partitionsToDelete, instantTime);
  }

  public static HoodieRecord createHoodieRecord(GenericRecord gr, Comparable orderingVal, HoodieKey hKey,
      String payloadClass) throws IOException {
    HoodieRecordPayload payload = DataSourceUtils.createPayload(payloadClass, gr, orderingVal);
    return new HoodieAvroRecord<>(hKey, payload);
  }

  public static HoodieRecord createHoodieRecord(GenericRecord gr, HoodieKey hKey,
                                                String payloadClass) throws IOException {
    HoodieRecordPayload payload = DataSourceUtils.createPayload(payloadClass, gr);
    return new HoodieAvroRecord<>(hKey, payload);
  }

  /**
   * Drop records already present in the dataset.
   *
   * @param jssc JavaSparkContext
   * @param incomingHoodieRecords HoodieRecords to deduplicate
   * @param writeConfig HoodieWriteConfig
   */
  @SuppressWarnings("unchecked")
  public static JavaRDD<HoodieRecord> dropDuplicates(JavaSparkContext jssc, JavaRDD<HoodieRecord> incomingHoodieRecords,
      HoodieWriteConfig writeConfig) {
    try {
      HoodieReadClient client = new HoodieReadClient<>(new HoodieSparkEngineContext(jssc), writeConfig);
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
    return dropDuplicates(jssc, incomingHoodieRecords, writeConfig);
  }

  /**
   * @deprecated Use {@link HiveSyncConfig} constructor directly and provide the props,
   * and set {@link HoodieSyncConfig#META_SYNC_BASE_PATH} and {@link HoodieSyncConfig#META_SYNC_BASE_FILE_FORMAT} instead.
   */
  @Deprecated
  public static HiveSyncConfig buildHiveSyncConfig(TypedProperties props, String basePath, String baseFileFormat) {
    checkRequiredProperties(props, Collections.singletonList(DataSourceWriteOptions.HIVE_TABLE().key()));
    HiveSyncConfig hiveSyncConfig = new HiveSyncConfig();
    hiveSyncConfig.basePath = basePath;
    hiveSyncConfig.usePreApacheInputFormat =
        props.getBoolean(DataSourceWriteOptions.HIVE_USE_PRE_APACHE_INPUT_FORMAT().key(),
            Boolean.parseBoolean(DataSourceWriteOptions.HIVE_USE_PRE_APACHE_INPUT_FORMAT().defaultValue()));
    hiveSyncConfig.databaseName = props.getString(DataSourceWriteOptions.HIVE_DATABASE().key(),
        DataSourceWriteOptions.HIVE_DATABASE().defaultValue());
    hiveSyncConfig.tableName = props.getString(DataSourceWriteOptions.HIVE_TABLE().key());
    hiveSyncConfig.baseFileFormat = baseFileFormat;
    hiveSyncConfig.hiveUser =
        props.getString(DataSourceWriteOptions.HIVE_USER().key(), DataSourceWriteOptions.HIVE_USER().defaultValue());
    hiveSyncConfig.hivePass =
        props.getString(DataSourceWriteOptions.HIVE_PASS().key(), DataSourceWriteOptions.HIVE_PASS().defaultValue());
    hiveSyncConfig.jdbcUrl =
        props.getString(DataSourceWriteOptions.HIVE_URL().key(), DataSourceWriteOptions.HIVE_URL().defaultValue());
    hiveSyncConfig.metastoreUris =
            props.getString(DataSourceWriteOptions.METASTORE_URIS().key(), DataSourceWriteOptions.METASTORE_URIS().defaultValue());
    hiveSyncConfig.partitionFields =
        props.getStringList(DataSourceWriteOptions.HIVE_PARTITION_FIELDS().key(), ",", new ArrayList<>());
    hiveSyncConfig.partitionValueExtractorClass =
        props.getString(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS().key(),
            SlashEncodedDayPartitionValueExtractor.class.getName());
    hiveSyncConfig.useJdbc = Boolean.valueOf(props.getString(DataSourceWriteOptions.HIVE_USE_JDBC().key(),
        DataSourceWriteOptions.HIVE_USE_JDBC().defaultValue()));
    if (props.containsKey(DataSourceWriteOptions.HIVE_SYNC_MODE().key())) {
      hiveSyncConfig.syncMode = props.getString(DataSourceWriteOptions.HIVE_SYNC_MODE().key());
    }
    hiveSyncConfig.autoCreateDatabase = Boolean.valueOf(props.getString(DataSourceWriteOptions.HIVE_AUTO_CREATE_DATABASE().key(),
        DataSourceWriteOptions.HIVE_AUTO_CREATE_DATABASE().defaultValue()));
    hiveSyncConfig.ignoreExceptions = Boolean.valueOf(props.getString(DataSourceWriteOptions.HIVE_IGNORE_EXCEPTIONS().key(),
        DataSourceWriteOptions.HIVE_IGNORE_EXCEPTIONS().defaultValue()));
    hiveSyncConfig.skipROSuffix = Boolean.valueOf(props.getString(DataSourceWriteOptions.HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE().key(),
        DataSourceWriteOptions.HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE().defaultValue()));
    hiveSyncConfig.supportTimestamp = Boolean.valueOf(props.getString(DataSourceWriteOptions.HIVE_SUPPORT_TIMESTAMP_TYPE().key(),
        DataSourceWriteOptions.HIVE_SUPPORT_TIMESTAMP_TYPE().defaultValue()));
    hiveSyncConfig.isConditionalSync = Boolean.valueOf(props.getString(DataSourceWriteOptions.HIVE_CONDITIONAL_SYNC().key(),
        DataSourceWriteOptions.HIVE_CONDITIONAL_SYNC().defaultValue()));
    hiveSyncConfig.bucketSpec = props.getBoolean(DataSourceWriteOptions.HIVE_SYNC_BUCKET_SYNC().key(),
        DataSourceWriteOptions.HIVE_SYNC_BUCKET_SYNC().defaultValue())
        ? HiveSyncConfig.getBucketSpec(props.getString(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key()),
            props.getInteger(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key())) : null;
    if (props.containsKey(HiveExternalCatalog.CREATED_SPARK_VERSION())) {
      hiveSyncConfig.sparkVersion = props.getString(HiveExternalCatalog.CREATED_SPARK_VERSION());
    }
    hiveSyncConfig.syncComment = Boolean.valueOf(props.getString(DataSourceWriteOptions.HIVE_SYNC_COMMENT().key(),
            DataSourceWriteOptions.HIVE_SYNC_COMMENT().defaultValue()));
    return hiveSyncConfig;
  }

  // Now by default ParquetWriteSupport will write DecimalType to parquet as int32/int64 when the scale of decimalType < Decimal.MAX_LONG_DIGITS(),
  // but AvroParquetReader which used by HoodieParquetReader cannot support read int32/int64 as DecimalType.
  // try to find current schema whether contains that DecimalType, and auto set the value of "spark.sql.parquet.writeLegacyFormat"
  public static void mayBeOverwriteParquetWriteLegacyFormatProp(Configuration configuration, StructType schema) {
    if (DataTypeUtils.foundSmallPrecisionDecimalType(schema)
        && !Boolean.parseBoolean(configuration.get("spark.sql.parquet.writeLegacyFormat", "false"))) {
      configuration.set("spark.sql.parquet.writeLegacyFormat", "true");
      LOG.warn("Small Decimal Type found in current schema, auto set the value of spark.sql.parquet.writeLegacyFormat to true");
    }
  }
}
