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

import org.apache.hudi.callback.common.WriteStatusValidator;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.TablePathUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieDuplicateKeyException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.index.SparkHoodieIndexFactory;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.BulkInsertPartitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.common.util.CommitUtils.getCheckpointValueAsString;

/**
 * Utilities used throughout the data source.
 */
@Slf4j
public class DataSourceUtils {

  private static final String UPSERT_HINT = "or write with the upsert operation (hoodie.spark.sql.insert.into.operation=upsert "
      + "for a Spark SQL INSERT INTO)";
  private static final String DUPLICATE_EXISTING_KEY_DETAIL = "The table already has a record with this key. "
      + "Set hoodie.datasource.insert.dup.policy to drop to skip such records or to none to keep the duplicates, " + UPSERT_HINT
      + " to update them.";
  private static final String DUPLICATE_INCOMING_KEY_DETAIL = "More than one inserted record has this key. "
      + "Set hoodie.combine.before.insert to true to combine them or hoodie.datasource.insert.dup.policy to none to keep "
      + "the duplicates, " + UPSERT_HINT + ".";

  public static String getTablePath(HoodieStorage storage,
                                    List<StoragePath> userProvidedPaths) throws IOException {
    log.info("Getting table path..");
    for (StoragePath path : userProvidedPaths) {
      try {
        Option<StoragePath> tablePath = TablePathUtils.getTablePath(storage, path);
        if (tablePath.isPresent()) {
          return tablePath.get().toString();
        }
      } catch (Exception e) {
        log.warn("Error trying to get table path from {}", path.toString(), e);
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
        .withPath(basePath).combineInput(combineInserts, true);
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
                parameters.getOrDefault(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), HoodieTableConfig.getDefaultPayloadClassName())))
            .withPayloadOrderingFields(ConfigUtils.getOrderingFieldsStrDuringWrite(parameters))
            .build())
        // override above with Hoodie configs specified as options.
        .withProps(parameters).build();
  }

  public static SparkRDDWriteClient createHoodieClient(JavaSparkContext jssc, String schemaStr, String basePath,
                                                       String tblName, Map<String, String> parameters) {
    return createHoodieClient(jssc, schemaStr, basePath, tblName, parameters,
        (context, writeConfig) -> new SparkRDDWriteClient<>(context, writeConfig));
  }

  /**
   * Creates a write client, letting the caller construct it from the resolved write config. Callers that own
   * resources shared across clients (e.g. an embedded timeline server) use this to hand them to the client.
   */
  public static SparkRDDWriteClient createHoodieClient(JavaSparkContext jssc, String schemaStr, String basePath,
                                                       String tblName, Map<String, String> parameters,
                                                       BiFunction<HoodieSparkEngineContext, HoodieWriteConfig, SparkRDDWriteClient> clientFactory) {
    HoodieSparkEngineContext context = new HoodieSparkEngineContext(jssc);
    HoodieWriteConfig writeConfig = createHoodieConfig(schemaStr, basePath, tblName, parameters);
    // The write options carry the table config of an existing table, so only a single-field complex keygen table
    // whose encoding is not known yet pays for loading the table config here; every other write skips it.
    if (KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(writeConfig)) {
      HoodieStorage storage = HoodieStorageUtils.getStorage(basePath, context.getStorageConf());
      if (TablePathUtils.isHoodieTablePath(storage, new StoragePath(basePath))) {
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
            .setConf(context.getStorageConf()).setBasePath(basePath).build();
        KeyGenUtils.recordComplexKeygenEncodingIfMissing(metaClient, writeConfig);
      }
    }
    return clientFactory.apply(context, writeConfig);
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
        throw new HoodieException("Not a valid operation type for doWriteOperation: " + operation);
    }
  }

  public static HoodieWriteResult doDeleteOperation(SparkRDDWriteClient client, JavaRDD<Tuple2<HoodieKey, Option<HoodieRecordLocation>>> hoodieKeysAndLocations,
      String instantTime, boolean isPrepped) {

    if (isPrepped) {
      HoodieRecord.HoodieRecordType recordType = client.getConfig().getRecordMerger().getRecordType();
      JavaRDD<HoodieRecord> records = hoodieKeysAndLocations.map(tuple -> {
        HoodieRecord record = new HoodieEmptyRecord(tuple._1, recordType);
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

  /**
   * Drop records already present in the dataset if {@code failOnDuplicates} is {@code false}.
   * Otherwise, throw a {@link HoodieDuplicateKeyException} if duplicates are found in the dataset.
   *
   * @param engineContext the Spark engine context
   * @param incomingHoodieRecords the HoodieRecords to deduplicate
   * @param writeConfig the HoodieWriteConfig
   * @param failOnDuplicates a flag indicating whether to fail when duplicates are found
   * @return a JavaRDD of deduplicated HoodieRecords
   */
  @SuppressWarnings("unchecked")
  public static JavaRDD<HoodieRecord> handleDuplicates(HoodieSparkEngineContext engineContext,
                                                       JavaRDD<HoodieRecord> incomingHoodieRecords,
                                                       HoodieWriteConfig writeConfig,
                                                       boolean failOnDuplicates) {
    try {
      SparkRDDReadClient client = new SparkRDDReadClient<>(engineContext, writeConfig);
      return client.tagLocation(incomingHoodieRecords)
          .filter(r -> shouldIncludeRecord((HoodieRecord<HoodieRecordPayload>) r, failOnDuplicates));
    } catch (TableNotFoundException e) {
      // No table exists yet, so no duplicates to drop
      return incomingHoodieRecords;
    }
  }

  /**
   * Fails an insert before it writes, with a {@link HoodieDuplicateKeyException} as the cause, if a record key
   * repeats among the incoming records, unless {@code hoodie.combine.before.insert} combines them, or, with
   * {@code lookUpDataset}, is already present in the dataset.
   *
   * <p>The check runs as jobs of its own and the write gets the incoming records as they are, in their order
   * and partitioning. The incoming records are persisted for that at the level the write persists them at, so
   * that the check and the write read them once, and are released with the resources of the instant.
   */
  public static void failOnDuplicates(HoodieSparkEngineContext engineContext,
                                      JavaRDD<HoodieRecord> incomingHoodieRecords,
                                      HoodieWriteConfig writeConfig,
                                      String instantTime,
                                      boolean lookUpDataset) {
    boolean checkIncomingRecords = !writeConfig.shouldCombineBeforeInsert();
    if (!checkIncomingRecords && !lookUpDataset) {
      return;
    }
    boolean persist = incomingHoodieRecords.getStorageLevel() == StorageLevel.NONE();
    if (persist) {
      HoodieJavaRDD.of(incomingHoodieRecords).persist(writeConfig.getTaggedRecordStorageLevel(),
          engineContext, HoodieData.HoodieDataCacheKey.of(writeConfig.getBasePath(), instantTime));
    }
    try {
      if (checkIncomingRecords) {
        failOnIncomingDuplicates(incomingHoodieRecords, writeConfig);
      }
      if (lookUpDataset) {
        // An index persists the records it tags and releases them once they are tagged, so it gets a view of
        // the incoming records, which stay persisted for the write
        handleDuplicates(engineContext, incomingHoodieRecords.map(record -> record), writeConfig, true).count();
      }
    } catch (Exception e) {
      if (persist) {
        incomingHoodieRecords.unpersist();
      }
      throw new HoodieUpsertException("Failed to upsert for commit time " + instantTime, e);
    }
  }

  /**
   * The duplicate handling of an insert into a table with a record key that sets no duplicate policy:
   * {@link #failOnDuplicates}, where the dataset is looked up only when its index tells whether it holds a
   * record key, see {@link #indexTellsKeyExists}.
   */
  public static void failOnDuplicatesByDefault(HoodieSparkEngineContext engineContext,
                                               JavaRDD<HoodieRecord> incomingHoodieRecords,
                                               HoodieWriteConfig writeConfig,
                                               String instantTime) {
    failOnDuplicates(engineContext, incomingHoodieRecords, writeConfig, instantTime, indexTellsKeyExists(writeConfig));
  }

  /**
   * Whether a location the index gives a record key means that the dataset holds the key. The bucket index
   * locates any key by its hash, and the indexes that read the keys of the base files do not see the deletes
   * in the log files of a merge-on-read table, which the record level index does.
   */
  private static boolean indexTellsKeyExists(HoodieWriteConfig writeConfig) {
    if (!StringUtils.isNullOrEmpty(writeConfig.getIndexClass())) {
      return false;
    }
    switch (writeConfig.getIndexType()) {
      case RECORD_INDEX:
      case GLOBAL_RECORD_LEVEL_INDEX:
      case RECORD_LEVEL_INDEX:
        return true;
      case SIMPLE:
      case GLOBAL_SIMPLE:
      case BLOOM:
      case GLOBAL_BLOOM:
        return writeConfig.getTableType() == HoodieTableType.COPY_ON_WRITE;
      default:
        return false;
    }
  }

  /**
   * Fails on the first two incoming records with the same key, in a shuffle of the keys of the records alone.
   * The records are keyed as the index of the table keys a record: by record key for a global index, and by
   * record key and partition path otherwise.
   */
  private static void failOnIncomingDuplicates(JavaRDD<HoodieRecord> records, HoodieWriteConfig writeConfig) {
    boolean isIndexingGlobal = SparkHoodieIndexFactory.isGlobalIndex(writeConfig);
    records
        .mapToPair(record -> new Tuple2<Object, String>(
            isIndexingGlobal ? record.getRecordKey() : record.getKey(), record.getRecordKey()))
        .reduceByKey((first, second) -> {
          throw new HoodieDuplicateKeyException(first, DUPLICATE_INCOMING_KEY_DETAIL);
        }, records.getNumPartitions())
        .count();
  }

  /**
   * Determines if a record should be included in the result after deduplication.
   *
   * @param record            The Hoodie record to evaluate.
   * @param failOnDuplicates  Whether to fail on detecting duplicates.
   * @return true if the record should be included; false otherwise.
   */
  private static boolean shouldIncludeRecord(HoodieRecord<?> record, boolean failOnDuplicates) {
    if (!record.isCurrentLocationKnown()) {
      return true;
    }
    if (failOnDuplicates) {
      // Fail if duplicates are found and the flag is set
      throw new HoodieDuplicateKeyException(record.getRecordKey(), DUPLICATE_EXISTING_KEY_DETAIL);
    }
    return false;
  }

  /**
   * Spark data source WriteStatus validator.
   *
   * <ul>
   *   <li>If there are error records, prints few of them and exit;</li>
   *   <li>If not, proceeds with the commit.</li>
   * </ul>
   */
  static class SparkDataSourceWriteStatusValidator implements WriteStatusValidator {

    private final WriteOperationType writeOperationType;
    private final AtomicBoolean hasErrored;

    public SparkDataSourceWriteStatusValidator(WriteOperationType writeOperationType, AtomicBoolean hasErrored) {
      this.writeOperationType = writeOperationType;
      this.hasErrored = hasErrored;
    }

    @Override
    public boolean validate(long totalRecords, long totalErroredRecords, Option<HoodieData<WriteStatus>> writeStatusesOpt) {
      if (totalErroredRecords > 0) {
        hasErrored.set(true);
        ValidationUtils.checkArgument(writeStatusesOpt.isPresent(), "RDD <WriteStatus> expected to be present when there are errors");
        long errorCount = HoodieJavaRDD.getJavaRDD(writeStatusesOpt.get())
            .filter(WriteStatus::hasErrors)
            .count();

        String errorSummary = String.format(
            "%s operation failed with %d error(s).%n%n"
                + "Total write statuses with errors: %d%n%n"
                + "Check the driver logs for error stacktraces which provide more information on the failure.",
            writeOperationType,
            totalErroredRecords,
            errorCount);

        log.error(errorSummary);

        if (log.isTraceEnabled()) {
          log.trace("Printing out the top 100 errors");

          HoodieJavaRDD.getJavaRDD(writeStatusesOpt.get()).filter(WriteStatus::hasErrors)
              .take(100)
              .forEach(ws -> {
                log.trace("Global error:", ws.getGlobalError());
                if (!ws.getErrors().isEmpty()) {
                  ws.getErrors().forEach((k, v) -> log.trace("Error for key {}: {}", k, v));
                }
              });
        }
        return false;
      } else {
        return true;
      }
    }
  }
}

