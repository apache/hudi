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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.FutureUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig;
import org.apache.hudi.utilities.schema.SchemaProvider;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.CollectionUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.ConfigUtils.containsConfigProperty;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.CloudSourceConfig.CLOUD_INCREMENTAL_MERGE_SCHEMA;
import static org.apache.hudi.utilities.config.CloudSourceConfig.EXISTS_CHECK_PARALLELISM;
import static org.apache.hudi.utilities.config.CloudSourceConfig.EXISTS_CHECK_PARTITIONS;
import static org.apache.hudi.utilities.config.CloudSourceConfig.IGNORE_RELATIVE_PATH_PREFIX;
import static org.apache.hudi.utilities.config.CloudSourceConfig.IGNORE_RELATIVE_PATH_SUBSTR;
import static org.apache.hudi.utilities.config.CloudSourceConfig.PATH_BASED_PARTITION_FIELDS;
import static org.apache.hudi.utilities.config.CloudSourceConfig.SELECT_RELATIVE_PATH_PREFIX;
import static org.apache.hudi.utilities.config.CloudSourceConfig.SELECT_RELATIVE_PATH_REGEX;
import static org.apache.hudi.utilities.config.CloudSourceConfig.SPARK_DATASOURCE_READER_COMMA_SEPARATED_PATH_FORMAT;
import static org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig.S3_FS_PREFIX;
import static org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig.S3_IGNORE_KEY_PREFIX;
import static org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig.S3_IGNORE_KEY_SUBSTRING;
import static org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig.S3_KEY_PREFIX;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.coalesceOrRepartition;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.when;

/**
 * Generic helper methods to fetch from Cloud Storage during incremental fetch from cloud storage buckets.
 * NOTE: DO NOT use any implementation specific classes here. This class is supposed to across S3EventsSource,
 * GcsEventsSource etc...so you can't assume the classes for your specific implementation will be available here.
 */
@Slf4j
public class CloudObjectsSelectorCommon {

  public static final String S3_OBJECT_KEY = "s3.object.key";
  public static final String S3_OBJECT_SIZE = "s3.object.size";
  public static final String S3_BUCKET_NAME = "s3.bucket.name";
  public static final String GCS_OBJECT_KEY = "name";
  public static final String GCS_OBJECT_SIZE = "size";
  public static final String S3_EVENT_TIME = "eventTime";
  public static final String GCS_OBJECT_UPDATED = "updated";
  public static final String CLOUD_SOURCE_PATH_COLUMN = "_hoodie_cloud_source_path";
  private static final String SPACE_DELIMTER = " ";
  private static final String GCS_PREFIX = "gs://";

  private final TypedProperties properties;

  public CloudObjectsSelectorCommon(TypedProperties properties) {
    this.properties = properties;
  }

  /**
   * Return a function that extracts filepaths from a list of Rows.
   * Here Row is assumed to have the schema [bucket_name, filepath_relative_to_bucket, object_size]
   * @param storageUrlSchemePrefix    Eg: s3:// or gs://. The storage-provider-specific prefix to use within the URL.
   * @param storageConf               storage configuration.
   * @param checkIfExists             check if each file exists, before adding it to the returned list
   * @param existsCheckParallelism    number of threads per task for the existence checks (getObjectMetadata validates it is >= 1); 1 checks sequentially
   */
  public static MapPartitionsFunction<Row, CloudObjectMetadata> getCloudObjectMetadataPerPartition(
      String storageUrlSchemePrefix, StorageConfiguration<Configuration> storageConf,
      boolean checkIfExists, int existsCheckParallelism) {
    return rows -> {
      if (!checkIfExists || existsCheckParallelism <= 1) {
        List<CloudObjectMetadata> cloudObjectMetadataPerPartition = new ArrayList<>();
        rows.forEachRemaining(row ->
            processRow(row, storageUrlSchemePrefix, storageConf, checkIfExists).ifPresent(cloudObjectMetadataPerPartition::add));
        return cloudObjectMetadataPerPartition.iterator();
      }

      List<Row> rowList = new ArrayList<>();
      rows.forEachRemaining(rowList::add);
      if (rowList.isEmpty()) {
        return Collections.emptyIterator();
      }
      ExecutorService executor = Executors.newFixedThreadPool(Math.min(existsCheckParallelism, rowList.size()),
          new CustomizedThreadFactory("cloud-exists-check", true));
      try {
        List<CompletableFuture<Option<CloudObjectMetadata>>> futures = rowList.stream()
            .map(row -> CompletableFuture.supplyAsync(
                () -> processRow(row, storageUrlSchemePrefix, storageConf, true), executor))
            .collect(Collectors.toList());
        List<Option<CloudObjectMetadata>> results;
        try {
          // fails fast: the first failed check completes the returned future exceptionally and cancels the rest,
          // and every future is complete before this returns, so the pool can be shut down right after
          results = FutureUtils.allOf(futures).join();
        } catch (CompletionException e) {
          throw unwrapExistsCheckFailure(e);
        }
        return results.stream()
            .filter(Option::isPresent)
            .map(Option::get)
            .collect(Collectors.toList())
            .iterator();
      } finally {
        // on the failure path this also interrupts the in-flight checks that FutureUtils.allOf only cancelled
        executor.shutdownNow();
      }
    };
  }

  /**
   * {@link FutureUtils#allOf} wraps the failure of a check in one or more {@link CompletionException}s; rethrow the
   * original exception so callers see the same failure the sequential path throws, whatever the parallelism.
   */
  private static RuntimeException unwrapExistsCheckFailure(CompletionException e) {
    Throwable cause = e;
    while (cause instanceof CompletionException && cause.getCause() != null) {
      cause = cause.getCause();
    }
    if (cause instanceof Error) {
      throw (Error) cause;
    }
    return cause instanceof RuntimeException
        ? (RuntimeException) cause
        : new HoodieException("Failed during parallel cloud object existence check", cause);
  }

  /**
   * Process a single row to build a {@link CloudObjectMetadata}. Optionally checks if the file exists.
   */
  private static Option<CloudObjectMetadata> processRow(Row row, String storageUrlSchemePrefix,
                                                        StorageConfiguration<Configuration> storageConf,
                                                        boolean checkIfExists) {
    Option<String> filePathUrl = getUrlForFile(row, storageUrlSchemePrefix, storageConf, checkIfExists);
    if (!filePathUrl.isPresent()) {
      return Option.empty();
    }
    String url = filePathUrl.get();
    log.debug("Adding file: {}", url);
    long size;
    Object obj = row.get(2);
    if (obj instanceof String) {
      size = Long.parseLong((String) obj);
    } else if (obj instanceof Integer) {
      size = ((Integer) obj).longValue();
    } else if (obj instanceof Long) {
      size = (long) obj;
    } else {
      throw new HoodieIOException("unexpected object size's type in Cloud storage events: " + obj.getClass());
    }
    long modificationTime = row.size() > 3
        ? epochMillis(row.get(3)) : CloudObjectMetadata.UNKNOWN_MODIFICATION_TIME;
    return Option.of(new CloudObjectMetadata(url, size, modificationTime));
  }

  /**
   * Epoch millis for a notification timestamp, which both S3 and GCS report as an ISO-8601 string.
   * Anything unparseable yields {@link CloudObjectMetadata#UNKNOWN_MODIFICATION_TIME} rather than
   * failing the batch, since the value only orders writes to the same object.
   */
  private static long epochMillis(Object rawValue) {
    if (rawValue == null) {
      return CloudObjectMetadata.UNKNOWN_MODIFICATION_TIME;
    }
    try {
      return Instant.parse(rawValue.toString()).toEpochMilli();
    } catch (DateTimeParseException e) {
      log.warn("Ignoring unparseable cloud notification timestamp {}", rawValue);
      return CloudObjectMetadata.UNKNOWN_MODIFICATION_TIME;
    }
  }

  /**
   * Construct a full qualified URL string to a cloud file from a given Row. Optionally check if the file exists.
   * Here Row is assumed to have the schema [bucket_name, filepath_relative_to_bucket].
   * The checkIfExists logic assumes that the relevant impl classes for the storageUrlSchemePrefix are already present
   * on the classpath!
   *
   * @param storageUrlSchemePrefix Eg: s3:// or gs://. The storage-provider-specific prefix to use within the URL.
   */
  private static Option<String> getUrlForFile(Row row, String storageUrlSchemePrefix,
                                              StorageConfiguration<Configuration> storageConf,
                                              boolean checkIfExists) {
    String bucket = row.getString(0);
    String filePath = storageUrlSchemePrefix + bucket + StoragePath.SEPARATOR + row.getString(1);

    try {
      String filePathUrl = URLDecoder.decode(filePath, StandardCharsets.UTF_8.name());
      if (!checkIfExists) {
        return Option.of(filePathUrl);
      }
      boolean exists = checkIfFileExists(storageUrlSchemePrefix, bucket, filePathUrl, storageConf.unwrapCopy());
      return exists ? Option.of(filePathUrl) : Option.empty();
    } catch (Exception exception) {
      log.error("Failed to generate path to cloud file {}", filePath, exception);
      throw new HoodieException(String.format("Failed to generate path to cloud file %s", filePath), exception);
    }
  }

  /**
   * Check if file with given path URL exists
   * @param storageUrlSchemePrefix Eg: s3:// or gs://. The storage-provider-specific prefix to use within the URL.
   */
  private static boolean checkIfFileExists(String storageUrlSchemePrefix, String bucket, String filePathUrl,
                                          Configuration configuration) {
    try {
      FileSystem fs = HadoopFSUtils.getFs(storageUrlSchemePrefix + bucket, configuration);
      return fs.exists(new Path(filePathUrl));
    } catch (IOException ioe) {
      String errMsg = String.format("Error while checking path exists for %s ", filePathUrl);
      log.error(errMsg, ioe);
      throw new HoodieIOException(errMsg, ioe);
    }
  }

  public static String generateFilter(Type type,
                                      TypedProperties props) {
    return generateFilter(type, props, new ColumnarFileMaterializer(props));
  }

  /**
   * Builds the filter restricting which cloud objects a batch selects. The size and relative-path
   * restrictions are common to every payload; which object keys are eligible is not, and so comes
   * from {@code materializer}.
   */
  public static String generateFilter(Type type,
                                      TypedProperties props,
                                      CloudObjectMaterializer materializer) {
    Option<String> selectRelativePathPrefix = configuredValue(props, SELECT_RELATIVE_PATH_PREFIX);
    Option<String> ignoreRelativePathPrefix = configuredValue(props, IGNORE_RELATIVE_PATH_PREFIX);
    Option<String> ignoreRelativePathSubStr = configuredValue(props, IGNORE_RELATIVE_PATH_SUBSTR);
    Option<String> selectRelativePathRegex = configuredValue(props, SELECT_RELATIVE_PATH_REGEX);

    String objectKey;
    String objectSizeKey;
    // This is for backwards compatibility of configs for s3.
    if (type.equals(Type.S3)) {
      objectKey = S3_OBJECT_KEY;
      objectSizeKey = S3_OBJECT_SIZE;
      selectRelativePathPrefix = selectRelativePathPrefix.or(() -> configuredValue(props, S3_KEY_PREFIX));
      ignoreRelativePathPrefix = ignoreRelativePathPrefix.or(() -> configuredValue(props, S3_IGNORE_KEY_PREFIX));
      ignoreRelativePathSubStr = ignoreRelativePathSubStr.or(() -> configuredValue(props, S3_IGNORE_KEY_SUBSTRING));
    } else {
      objectKey = GCS_OBJECT_KEY;
      objectSizeKey = GCS_OBJECT_SIZE;
    }

    StringBuilder filter = new StringBuilder(String.format("%s > 0", objectSizeKey));
    if (selectRelativePathPrefix.isPresent() || selectRelativePathRegex.isPresent()) {
      String prefix = selectRelativePathPrefix.orElse("");
      String regex = selectRelativePathRegex.orElse("");

      // Update path if regex is present
      if (!regex.isEmpty()) {
        String updatedPathRegex = prefix.isEmpty() || prefix.endsWith(StoragePath.SEPARATOR)
            ? prefix + regex : prefix + StoragePath.SEPARATOR + regex;
        filter.append(SPACE_DELIMTER).append(String.format("and %s rlike '%s'", objectKey, updatedPathRegex));
      } else if (!prefix.isEmpty()) {
        // Build the condition based on whether regex or prefix is present
        filter.append(SPACE_DELIMTER).append(String.format("and %s like '%s%%'", objectKey, prefix));
      }
    }
    if (ignoreRelativePathPrefix.isPresent()) {
      filter.append(SPACE_DELIMTER).append(String.format("and %s not like '%s%%'", objectKey, ignoreRelativePathPrefix.get()));
    }
    if (ignoreRelativePathSubStr.isPresent()) {
      filter.append(SPACE_DELIMTER).append(String.format("and %s not like '%%%s%%'", objectKey, ignoreRelativePathSubStr.get()));
    }

    filter.append(materializer.objectKeyPredicate(objectKey, props));

    return filter.toString();
  }

  /**
   * Renders the file extension predicate. A comma separated value matches any one of the
   * extensions, so a prefix holding more than one file type can be selected in a single sync;
   * a single value renders exactly the predicate it always did. Empty when no usable extension
   * is configured, which leaves the filter unchanged.
   */
  static String extensionPredicate(String objectKey, String extensions) {
    List<String> predicates = Arrays.stream(extensions.split(","))
        .map(String::trim)
        .filter(extension -> !extension.isEmpty())
        .map(extension -> String.format("%s like '%%%s'", objectKey, extension))
        .collect(Collectors.toList());
    if (predicates.isEmpty()) {
      return "";
    }
    return predicates.size() == 1
        ? SPACE_DELIMTER + "and " + predicates.get(0)
        : SPACE_DELIMTER + "and (" + String.join(" or ", predicates) + ")";
  }

  /**
   * @param cloudObjectMetadataDF a Dataset that contains metadata of S3/GCS objects. Assumed to be a persisted form
   *                              of a Cloud Storage SQS/PubSub Notification event.
   * @param checkIfExists         Check if each file exists, before returning its full path
   * @return A {@link List} of {@link CloudObjectMetadata} containing file info.
   */
  public static List<CloudObjectMetadata> getObjectMetadata(
      Type type,
      JavaSparkContext jsc,
      Dataset<Row> cloudObjectMetadataDF,
      boolean checkIfExists,
      TypedProperties props
  ) {
    StorageConfiguration<Configuration> storageConf = HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration());
    int existsCheckParallelism = getIntWithAltKeys(props, EXISTS_CHECK_PARALLELISM);
    ValidationUtils.checkArgument(existsCheckParallelism >= 1,
        EXISTS_CHECK_PARALLELISM.key() + " must be >= 1, got: " + existsCheckParallelism);

    String prefix;
    String bucketCol;
    String keyCol;
    String sizeCol;
    String timeCol;
    if (type == Type.GCS) {
      prefix = GCS_PREFIX;
      bucketCol = "bucket";
      keyCol = GCS_OBJECT_KEY;
      sizeCol = GCS_OBJECT_SIZE;
      timeCol = GCS_OBJECT_UPDATED;
    } else if (type == Type.S3) {
      String s3FS = getStringWithAltKeys(props, S3_FS_PREFIX, true).toLowerCase();
      prefix = s3FS + "://";
      bucketCol = S3_BUCKET_NAME;
      keyCol = S3_OBJECT_KEY;
      sizeCol = S3_OBJECT_SIZE;
      timeCol = S3_EVENT_TIME;
    } else {
      throw new UnsupportedOperationException("Invalid cloud type " + type);
    }

    Dataset<Row> distinctObjects = selectDistinctObjects(cloudObjectMetadataDF, bucketCol, keyCol, sizeCol, timeCol);
    if (checkIfExists) {
      // The upstream Window.orderBy() in IncrSourceHelper collapses the dataset to one partition and AQE keeps the
      // distinct() output there, which would serialize every existence check on one task. Spread the checks over
      // the cluster: repartition(n) is not coalesced by AQE, and defaultParallelism tracks the registered cores.
      int numPartitions = existsCheckNumPartitions(props, jsc);
      log.info("Checking cloud object existence over {} partitions with {} threads per task", numPartitions, existsCheckParallelism);
      distinctObjects = distinctObjects.repartition(numPartitions);
    }
    return distinctObjects
        .mapPartitions(
            getCloudObjectMetadataPerPartition(prefix, storageConf, checkIfExists, existsCheckParallelism),
            Encoders.kryo(CloudObjectMetadata.class))
        .collectAsList();
  }

  /**
   * One row per cloud object, carrying the notification timestamp when the events have one.
   *
   * <p>An object written more than once inside a single batch produces one event per write. Keying
   * only on bucket and object key, and keeping the newest event, reads such an object once instead
   * of once per write. Where the metadata table predates the timestamp column the events cannot be
   * ordered, so this falls back to the previous behaviour of de-duplicating on size as well.
   */
  private static Dataset<Row> selectDistinctObjects(Dataset<Row> events, String bucketCol, String keyCol,
                                                    String sizeCol, String timeCol) {
    if (!Arrays.asList(events.schema().fieldNames()).contains(timeCol)) {
      log.warn("Cloud events carry no {} column; objects rewritten within a batch will be read once per write", timeCol);
      return events.select(bucketCol, keyCol, sizeCol).distinct();
    }
    String rank = "_hoodie_event_rank";
    // rank before projecting: the columns are nested, so selecting them first renames them to
    // their leaf names and the window would no longer resolve bucketCol or keyCol.
    // Order on the parsed instant, not the raw string: '.' sorts before 'Z', so a lexicographic
    // desc puts 10:00:00Z ahead of the later 10:00:00.500Z whenever precision varies in a second.
    return events
        .withColumn(rank, functions.row_number().over(
            Window.partitionBy(functions.col(bucketCol), functions.col(keyCol))
                .orderBy(functions.to_timestamp(functions.col(timeCol)).desc_nulls_last())))
        .filter(functions.col(rank).equalTo(1))
        .select(bucketCol, keyCol, sizeCol, timeCol);
  }

  /**
   * Number of partitions the existence checks are spread over: {@link CloudSourceConfig#EXISTS_CHECK_PARTITIONS}
   * when set to a positive value, otherwise the Spark default parallelism. Never below 1 (repartition rejects 0).
   */
  static int existsCheckNumPartitions(TypedProperties props, JavaSparkContext jsc) {
    int configured = getIntWithAltKeys(props, EXISTS_CHECK_PARTITIONS);
    return Math.max(1, configured > 0 ? configured : jsc.defaultParallelism());
  }

  public Option<Dataset<Row>> loadAsDataset(SparkSession spark, List<CloudObjectMetadata> cloudObjectMetadata,
                                            String fileFormat, Option<SchemaProvider> schemaProviderOption, int numPartitions) {
    if (log.isDebugEnabled()) {
      log.debug("Extracted distinct files {} and some samples {}",
          cloudObjectMetadata.size(), cloudObjectMetadata.stream().map(CloudObjectMetadata::getPath).limit(10).collect(Collectors.toList()));
    }

    if (isNullOrEmpty(cloudObjectMetadata)) {
      return Option.empty();
    }
    DataFrameReader reader = applyMergeSchemaOption(spark.read().format(fileFormat), fileFormat);
    String datasourceOpts = getStringWithAltKeys(properties, CloudSourceConfig.SPARK_DATASOURCE_OPTIONS, true);

    StructType rowSchema = null;
    if (schemaProviderOption.isPresent()) {
      HoodieSchema sourceSchema = schemaProviderOption.get().getSourceHoodieSchema();
      if (sourceSchema != null && !sourceSchema.equals(HoodieSchema.NULL_SCHEMA)) {
        rowSchema = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(sourceSchema);
        if (isCoalesceRequired(properties, sourceSchema)) {
          reader = reader.schema(addAliasesToRowSchema(sourceSchema, rowSchema));
        } else {
          reader = reader.schema(rowSchema);
        }
      }
    }

    if (StringUtils.isNullOrEmpty(datasourceOpts)) {
      // fall back to legacy config for BWC. TODO consolidate in HUDI-6020
      datasourceOpts = getStringWithAltKeys(properties, S3EventsHoodieIncrSourceConfig.SPARK_DATASOURCE_OPTIONS, true);
    }
    if (StringUtils.nonEmpty(datasourceOpts)) {
      final ObjectMapper mapper = new ObjectMapper();
      Map<String, String> sparkOptionsMap = null;
      try {
        sparkOptionsMap = mapper.readValue(datasourceOpts, Map.class);
      } catch (IOException e) {
        throw new HoodieException(String.format("Failed to parse sparkOptions: %s", datasourceOpts), e);
      }
      log.info("SparkOptions loaded: {}", sparkOptionsMap);
      reader = reader.options(sparkOptionsMap);
    }
    List<String> paths = new ArrayList<>();
    for (CloudObjectMetadata o : cloudObjectMetadata) {
      paths.add(o.getPath());
    }
    boolean isCommaSeparatedPathFormat = properties.getBoolean(SPARK_DATASOURCE_READER_COMMA_SEPARATED_PATH_FORMAT.key(), false);

    Dataset<Row> dataset;
    if (isCommaSeparatedPathFormat) {
      dataset = reader.load(String.join(",", paths));
    } else {
      dataset = reader.load(paths.toArray(new String[cloudObjectMetadata.size()]));
    }

    if (schemaProviderOption.isPresent()) {
      HoodieSchema sourceSchema = schemaProviderOption.get().getSourceHoodieSchema();
      if (isCoalesceRequired(properties, sourceSchema)) {
        dataset = spark.createDataFrame(coalesceAliasFields(dataset, sourceSchema).rdd(), rowSchema);
      }
    }

    // add partition column from source path if configured
    if (containsConfigProperty(properties, PATH_BASED_PARTITION_FIELDS)) {
      String[] partitionKeysToAdd = getStringWithAltKeys(properties, PATH_BASED_PARTITION_FIELDS).split(",");
      // Add partition column for all path-based partition keys. If key is not present in path, the value will be null.
      for (String partitionKey : partitionKeysToAdd) {
        String partitionPathPattern = String.format("%s=", partitionKey);
        log.info("Adding column {} to dataset", partitionKey);
        dataset = dataset.withColumn(partitionKey, split(split(input_file_name(), partitionPathPattern).getItem(1), StoragePath.SEPARATOR).getItem(0));
      }
    }

    // append the source file path if configured. input_file_name() is non-nullable, so wrap it to make the
    // column nullable (required to add it to an existing table); the wrapper also maps Spark's "unknown file"
    // empty string to null. Overwrites a same-named column, matching the partition columns above.
    if (getBooleanWithAltKeys(properties, CloudSourceConfig.INCLUDE_SOURCE_PATH_FIELD)) {
      if (rowSchema != null && !Arrays.asList(rowSchema.fieldNames()).contains(CLOUD_SOURCE_PATH_COLUMN)) {
        // the streamer rewrites row sources to the configured schema provider's schema before writing
        log.warn("Column {} is not declared in the configured schema provider's schema; it will be dropped before "
            + "the write unless it is declared there", CLOUD_SOURCE_PATH_COLUMN);
      }
      log.info("Adding column {} to dataset", CLOUD_SOURCE_PATH_COLUMN);
      dataset = dataset.withColumn(CLOUD_SOURCE_PATH_COLUMN, when(input_file_name().notEqual(""), input_file_name()));
    }

    dataset = coalesceOrRepartition(dataset, numPartitions);
    return Option.of(dataset);
  }

  private static boolean isCoalesceRequired(TypedProperties properties, HoodieSchema sourceSchema) {
    return getBooleanWithAltKeys(properties, CloudSourceConfig.SPARK_DATASOURCE_READER_COALESCE_ALIAS_COLUMNS)
        && Objects.nonNull(sourceSchema)
        && hasFieldWithAliases(sourceSchema);
  }

  /**
   * Recursively checks if a schema or any of its nested fields contain aliases.
   *
   * @param schema The schema to check.
   * @return True if the schema or any of its fields contain aliases, false otherwise.
   */
  private static boolean hasFieldWithAliases(HoodieSchema schema) {
    // If the schema is a record, check its fields recursively
    if (isNestedRecord(schema)) {
      for (HoodieSchemaField field : getRecordFields(schema)) {
        // Check if the field has aliases
        if (!field.aliases().isEmpty()) {
          return true;
        }
        // Recursively check the field's schema for aliases
        if (hasFieldWithAliases(field.schema())) {
          return true;
        }
      }
    }
    // No aliases found
    return false;
  }

  private static StructType addAliasesToRowSchema(HoodieSchema schema, StructType rowSchema) {
    Map<String, StructField> rowFieldsMap = Arrays.stream(rowSchema.fields())
        .collect(Collectors.toMap(StructField::name, Function.identity()));

    StructField[] modifiedFields = getRecordFields(schema).stream()
        .flatMap(field -> generateRowFieldsWithAliases(field, rowFieldsMap.get(field.name())).stream())
        .toArray(StructField[]::new);

    return new StructType(modifiedFields);
  }

  private static List<HoodieSchemaField> getRecordFields(HoodieSchema schema) {
    if (schema.getType() == HoodieSchemaType.RECORD) {
      return schema.getFields();
    }

    if (schema.getType() == HoodieSchemaType.UNION) {
      return schema.getTypes().stream()
          .filter(subSchema -> subSchema.getType() == HoodieSchemaType.RECORD)
          .findFirst()
          .map(HoodieSchema::getFields)
          .orElse(Collections.emptyList());
    }

    return Collections.emptyList();
  }

  /**
   * Generates a list of StructFields with aliases applied based on the provided field's schema.
   * <p>
   * This method processes a given field and its corresponding Spark SQL StructField, handling
   * nested records and aliases. If the field contains nested records, the method recursively
   * updates the schema for these records and applies any aliases defined in the schema.
   * If the field has aliases, they are added as new fields with nullable set to true and
   * appropriate metadata in the returned list. If no aliases or nesting are present, the original
   * StructField is returned unchanged.
   *
   * @param field The field from the schema to process.
   * @param rowField  The corresponding Spark SQL StructField to map the field to.
   * @return A list of StructFields with aliases applied as per the provided schema.
   */
  private static List<StructField> generateRowFieldsWithAliases(HoodieSchemaField field, StructField rowField) {
    List<StructField> fieldList = new ArrayList<>();

    // Handle nested records
    if (isNestedRecord(field.schema())) {
      StructType updatedSchema = addAliasesToRowSchema(field.schema(), (StructType) rowField.dataType());

      if (schemaModifiedOrHasAliases(field, updatedSchema, rowField)) {
        // Add the original field with the updated schema and add aliases if present
        addFieldWithAliases(fieldList, field.name(), updatedSchema, rowField.metadata(), field.aliases());
      } else {
        fieldList.add(rowField);
      }
    } else if (!field.aliases().isEmpty()) {
      // If the field has aliases, add them to the schema
      addFieldWithAliases(fieldList, field.name(), rowField.dataType(), rowField.metadata(), field.aliases());
    } else {
      // No aliases or nesting, return the original field
      fieldList.add(rowField);
    }
    return fieldList;
  }

  private static void addFieldWithAliases(List<StructField> fieldList, String fieldName, DataType dataType, Metadata metadata, Set<String> aliases) {
    fieldList.add(new StructField(fieldName, dataType, true, metadata));
    aliases.forEach(alias -> fieldList.add(new StructField(alias, dataType, true, metadata)));
  }

  private static Dataset<Row> coalesceAliasFields(Dataset<Row> dataset, HoodieSchema sourceSchema) {
    return coalesceNestedAliases(coalesceTopLevelAliases(dataset, sourceSchema), sourceSchema);
  }

  /**
   * Merges top-level fields with their aliases in the dataset.
   * <p>
   * This method goes through the top-level fields in the schema, and for any field that has aliases,
   * it combines them in the dataset using a coalesce operation. This ensures that if a field is null,
   * the value from its alias is used instead.
   *
   * @param dataset      The dataset to process.
   * @param sourceSchema The schema defining the fields and their aliases.
   * @return A dataset with fields merged with their aliases.
   */
  private static Dataset<Row> coalesceTopLevelAliases(Dataset<Row> dataset, HoodieSchema sourceSchema) {
    return getRecordFields(sourceSchema).stream()
        .filter(field -> !field.aliases().isEmpty())
        .reduce(dataset,
            (ds, field) -> coalesceAndDropAliasFields(ds, field.name(), field.aliases()), (ds1, ds2) -> ds1);
  }

  private static Dataset<Row> coalesceAndDropAliasFields(Dataset<Row> dataset, String fieldName, Set<String> aliases) {
    List<Column> columns = new ArrayList<>();
    columns.add(dataset.col(fieldName));
    aliases.forEach(alias -> columns.add(dataset.col(alias)));

    return dataset.withColumn(fieldName, functions.coalesce(columns.toArray(new Column[0])))
        .drop(aliases.toArray(new String[0]));
  }

  /**
   * Merges nested fields with their aliases in the dataset.
   * <p>
   * This method iterates through the fields of the provided schema and checks if they represent
   * nested records. For each nested record, it verifies if there are any alias fields present. If
   * aliases are found, the method generates a list of nested fields, coalescing them with their aliases,
   * and creates a new column in the dataset with the merged data.
   *
   * @param dataset      The dataset to process.
   * @param sourceSchema The schema defining the structure and aliases of the data.
   * @return A dataset with nested fields merged with their aliases.
   */
  private static Dataset<Row> coalesceNestedAliases(Dataset<Row> dataset, HoodieSchema sourceSchema) {
    for (HoodieSchemaField field : getRecordFields(sourceSchema)) {
      // check if this is a nested record and contains an alias field within
      if (isNestedRecord(field.schema()) && hasFieldWithAliases(field.schema())) {
        dataset = dataset.withColumn(field.name(), functions.struct(getNestedFields("", field, dataset)));
      }
    }
    return dataset;
  }

  private static Column[] getNestedFields(String parentField, HoodieSchemaField field, Dataset<Row> dataset) {
    return getRecordFields(field.schema()).stream()
        .map(schemaField -> {
          List<Column> columns = new ArrayList<>();
          String newParentField = getFullName(parentField, field.name());
          if (isNestedRecord(schemaField.schema())) {
            // if field is nested, recursively fetch nested column
            columns.add(functions.struct(getNestedFields(newParentField, schemaField, dataset)));
          } else {
            columns.add(dataset.col(getFullName(newParentField, schemaField.name())));
          }
          schemaField.aliases().forEach(alias -> columns.add(dataset.col(getFullName(newParentField, alias))));
          // if field contains aliases, coalesce the column with others matching the aliases otherwise return actual column
          return schemaField.aliases().isEmpty() ? columns.get(0)
              : functions.coalesce(columns.toArray(new Column[0])).alias(schemaField.name());
        }).toArray(Column[]::new);
  }

  private static boolean isNestedRecord(HoodieSchema schema) {
    if (schema.getType() == HoodieSchemaType.RECORD) {
      return true;
    }

    if (schema.getType() == HoodieSchemaType.UNION) {
      return schema.getTypes().stream()
          .anyMatch(subSchema -> subSchema.getType() == HoodieSchemaType.RECORD);
    }

    return false;
  }

  private static String getFullName(String namespace, String fieldName) {
    return namespace.isEmpty() ? fieldName : namespace + "." + fieldName;
  }

  private static boolean schemaModifiedOrHasAliases(HoodieSchemaField field, StructType modifiedNestedSchema, StructField rowField) {
    return !modifiedNestedSchema.equals(rowField.dataType()) || !field.aliases().isEmpty();
  }

  static Option<String> configuredValue(TypedProperties props, ConfigProperty<String> configProperty) {
    String value = getStringWithAltKeys(props, configProperty, true);
    if (!StringUtils.isNullOrEmpty(value)) {
      return Option.of(value);
    }

    return Option.empty();
  }

  /**
   * Enables Spark {@code mergeSchema} for cloud object batches of Parquet or ORC files when configured, so
   * heterogeneous files in one sync round share a merged struct type. Applied before user
   * {@link CloudSourceConfig#SPARK_DATASOURCE_OPTIONS} so explicit reader options can override.
   *
   * <p>Spark's native Parquet reader honors {@code mergeSchema} on all supported versions. Spark's native ORC
   * reader honors it on Spark 3.0+ (the native ORC impl is the default since Spark 2.4); on older runtimes the
   * option is silently ignored, which is harmless.
   */
  private DataFrameReader applyMergeSchemaOption(DataFrameReader reader, String fileFormat) {
    if (!isParquetOrOrcFileFormat(fileFormat)) {
      return reader;
    }
    if (!getBooleanWithAltKeys(properties, CLOUD_INCREMENTAL_MERGE_SCHEMA)) {
      return reader;
    }
    return reader.option("mergeSchema", "true");
  }

  // Package-private for unit testing — see TestCloudObjectsSelectorCommon.
  static boolean isParquetOrOrcFileFormat(String fileFormat) {
    if (fileFormat == null) {
      return false;
    }
    String f = fileFormat.trim();
    return "parquet".equalsIgnoreCase(f) || "orc".equalsIgnoreCase(f);
  }

  public enum Type {
    S3,
    GCS
  }
}
