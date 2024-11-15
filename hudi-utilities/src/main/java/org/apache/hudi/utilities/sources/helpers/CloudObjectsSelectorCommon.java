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

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
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
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.CollectionUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.ConfigUtils.containsConfigProperty;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.CloudSourceConfig.CLOUD_DATAFILE_EXTENSION;
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

/**
 * Generic helper methods to fetch from Cloud Storage during incremental fetch from cloud storage buckets.
 * NOTE: DO NOT use any implementation specific classes here. This class is supposed to across S3EventsSource,
 * GcsEventsSource etc...so you can't assume the classes for your specific implementation will be available here.
 */
public class CloudObjectsSelectorCommon {

  private static final Logger LOG = LoggerFactory.getLogger(CloudObjectsSelectorCommon.class);

  public static final String S3_OBJECT_KEY = "s3.object.key";
  public static final String S3_OBJECT_SIZE = "s3.object.size";
  public static final String S3_BUCKET_NAME = "s3.bucket.name";
  public static final String GCS_OBJECT_KEY = "name";
  public static final String GCS_OBJECT_SIZE = "size";
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
   * @return
   */
  public static MapPartitionsFunction<Row, CloudObjectMetadata> getCloudObjectMetadataPerPartition(
      String storageUrlSchemePrefix, StorageConfiguration<Configuration> storageConf, boolean checkIfExists) {
    return rows -> {
      List<CloudObjectMetadata> cloudObjectMetadataPerPartition = new ArrayList<>();
      rows.forEachRemaining(row -> {
        Option<String> filePathUrl = getUrlForFile(row, storageUrlSchemePrefix, storageConf, checkIfExists);
        filePathUrl.ifPresent(url -> {
          LOG.info("Adding file: " + url);
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
          cloudObjectMetadataPerPartition.add(new CloudObjectMetadata(url, size));
        });
      });

      return cloudObjectMetadataPerPartition.iterator();
    };
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
    final Configuration configuration = storageConf.unwrapCopy();

    String bucket = row.getString(0);
    String filePath = storageUrlSchemePrefix + bucket + StoragePath.SEPARATOR + row.getString(1);

    try {
      String filePathUrl = URLDecoder.decode(filePath, StandardCharsets.UTF_8.name());
      if (!checkIfExists) {
        return Option.of(filePathUrl);
      }
      boolean exists = checkIfFileExists(storageUrlSchemePrefix, bucket, filePathUrl, configuration);
      return exists ? Option.of(filePathUrl) : Option.empty();
    } catch (Exception exception) {
      LOG.warn(String.format("Failed to generate path to cloud file %s", filePath), exception);
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
      LOG.error(errMsg, ioe);
      throw new HoodieIOException(errMsg, ioe);
    }
  }

  public static String generateFilter(Type type,
                                      TypedProperties props) {
    String fileFormat = CloudDataFetcher.getFileFormat(props);
    Option<String> selectRelativePathPrefix = getPropVal(props, SELECT_RELATIVE_PATH_PREFIX);
    Option<String> ignoreRelativePathPrefix = getPropVal(props, IGNORE_RELATIVE_PATH_PREFIX);
    Option<String> ignoreRelativePathSubStr = getPropVal(props, IGNORE_RELATIVE_PATH_SUBSTR);
    Option<String> selectRelativePathRegex = getPropVal(props, SELECT_RELATIVE_PATH_REGEX);

    String objectKey;
    String objectSizeKey;
    // This is for backwards compatibility of configs for s3.
    if (type.equals(Type.S3)) {
      objectKey = S3_OBJECT_KEY;
      objectSizeKey = S3_OBJECT_SIZE;
      selectRelativePathPrefix = selectRelativePathPrefix.or(() -> getPropVal(props, S3_KEY_PREFIX));
      ignoreRelativePathPrefix = ignoreRelativePathPrefix.or(() -> getPropVal(props, S3_IGNORE_KEY_PREFIX));
      ignoreRelativePathSubStr = ignoreRelativePathSubStr.or(() -> getPropVal(props, S3_IGNORE_KEY_SUBSTRING));
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

    // Match files with a given extension, or use the fileFormat as the default.
    getPropVal(props, CLOUD_DATAFILE_EXTENSION).or(() -> Option.of(fileFormat))
        .map(val -> filter.append(SPACE_DELIMTER).append(String.format("and %s like '%%%s'", objectKey, val)));

    return filter.toString();
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
    if (type == Type.GCS) {
      return cloudObjectMetadataDF
          .select("bucket", "name", "size")
          .distinct()
          .mapPartitions(getCloudObjectMetadataPerPartition(GCS_PREFIX, storageConf, checkIfExists), Encoders.kryo(CloudObjectMetadata.class))
          .collectAsList();
    } else if (type == Type.S3) {
      String s3FS = getStringWithAltKeys(props, S3_FS_PREFIX, true).toLowerCase();
      String s3Prefix = s3FS + "://";
      return cloudObjectMetadataDF
          .select(CloudObjectsSelectorCommon.S3_BUCKET_NAME, CloudObjectsSelectorCommon.S3_OBJECT_KEY, CloudObjectsSelectorCommon.S3_OBJECT_SIZE)
          .distinct()
          .mapPartitions(getCloudObjectMetadataPerPartition(s3Prefix, storageConf, checkIfExists), Encoders.kryo(CloudObjectMetadata.class))
          .collectAsList();
    }
    throw new UnsupportedOperationException("Invalid cloud type " + type);
  }

  public Option<Dataset<Row>> loadAsDataset(SparkSession spark, List<CloudObjectMetadata> cloudObjectMetadata,
                                            String fileFormat, Option<SchemaProvider> schemaProviderOption, int numPartitions) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Extracted distinct files " + cloudObjectMetadata.size()
          + " and some samples " + cloudObjectMetadata.stream().map(CloudObjectMetadata::getPath).limit(10).collect(Collectors.toList()));
    }

    if (isNullOrEmpty(cloudObjectMetadata)) {
      return Option.empty();
    }
    DataFrameReader reader = spark.read().format(fileFormat);
    String datasourceOpts = getStringWithAltKeys(properties, CloudSourceConfig.SPARK_DATASOURCE_OPTIONS, true);

    StructType rowSchema = null;
    if (schemaProviderOption.isPresent()) {
      Schema sourceSchema = schemaProviderOption.get().getSourceSchema();
      if (sourceSchema != null && !sourceSchema.equals(InputBatch.NULL_SCHEMA)) {
        rowSchema = AvroConversionUtils.convertAvroSchemaToStructType(sourceSchema);
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
      LOG.info(String.format("sparkOptions loaded: %s", sparkOptionsMap));
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
      Schema sourceSchema = schemaProviderOption.get().getSourceSchema();
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
        LOG.info(String.format("Adding column %s to dataset", partitionKey));
        dataset = dataset.withColumn(partitionKey, split(split(input_file_name(), partitionPathPattern).getItem(1), StoragePath.SEPARATOR).getItem(0));
      }
    }
    dataset = coalesceOrRepartition(dataset, numPartitions);
    return Option.of(dataset);
  }

  private static boolean isCoalesceRequired(TypedProperties properties, Schema sourceSchema) {
    return getBooleanWithAltKeys(properties, CloudSourceConfig.SPARK_DATASOURCE_READER_COALESCE_ALIAS_COLUMNS)
        && Objects.nonNull(sourceSchema)
        && hasFieldWithAliases(sourceSchema);
  }

  /**
   * Recursively checks if an Avro schema or any of its nested fields contain aliases.
   *
   * @param schema The Avro schema to check.
   * @return True if the schema or any of its fields contain aliases, false otherwise.
   */
  private static boolean hasFieldWithAliases(Schema schema) {
    // If the schema is a record, check its fields recursively
    if (isNestedRecord(schema)) {
      for (Schema.Field field : getRecordFields(schema)) {
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

  private static StructType addAliasesToRowSchema(Schema avroSchema, StructType rowSchema) {
    Map<String, StructField> rowFieldsMap = Arrays.stream(rowSchema.fields())
        .collect(Collectors.toMap(StructField::name, Function.identity()));

    StructField[] modifiedFields = getRecordFields(avroSchema).stream()
        .flatMap(avroField -> generateRowFieldsWithAliases(avroField, rowFieldsMap.get(avroField.name())).stream())
        .toArray(StructField[]::new);

    return new StructType(modifiedFields);
  }

  private static List<Schema.Field> getRecordFields(Schema schema) {
    if (schema.getType() == Schema.Type.RECORD) {
      return schema.getFields();
    }

    if (schema.getType() == Schema.Type.UNION) {
      return schema.getTypes().stream()
          .filter(subSchema -> subSchema.getType() == Schema.Type.RECORD)
          .findFirst()
          .map(Schema::getFields)
          .orElse(Collections.emptyList());
    }

    return Collections.emptyList();
  }

  /**
   * Generates a list of StructFields with aliases applied based on the provided Avro field schema.
   * <p>
   * This method processes a given Avro field and its corresponding Spark SQL StructField, handling
   * nested records and aliases. If the Avro field contains nested records, the method recursively
   * updates the schema for these records and applies any aliases defined in the Avro schema.
   * If the Avro field has aliases, they are added as new fields with nullable set to true and
   * appropriate metadata in the returned list. If no aliases or nesting are present, the original
   * StructField is returned unchanged.
   *
   * @param avroField The Avro field schema to process.
   * @param rowField  The corresponding Spark SQL StructField to map the Avro field to.
   * @return A list of StructFields with aliases applied as per the Avro schema.
   */
  private static List<StructField> generateRowFieldsWithAliases(Schema.Field avroField, StructField rowField) {
    List<StructField> fieldList = new ArrayList<>();

    // Handle nested records
    if (isNestedRecord(avroField.schema())) {
      StructType updatedSchema = addAliasesToRowSchema(avroField.schema(), (StructType) rowField.dataType());

      if (schemaModifiedOrHasAliases(avroField, updatedSchema, rowField)) {
        // Add the original field with the updated schema and add aliases if present
        addFieldWithAliases(fieldList, avroField.name(), updatedSchema, rowField.metadata(), avroField.aliases());
      } else {
        fieldList.add(rowField);
      }
    } else if (!avroField.aliases().isEmpty()) {
      // If the field has aliases, add them to the schema
      addFieldWithAliases(fieldList, avroField.name(), rowField.dataType(), rowField.metadata(), avroField.aliases());
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

  private static Dataset<Row> coalesceAliasFields(Dataset<Row> dataset, Schema sourceSchema) {
    return coalesceNestedAliases(coalesceTopLevelAliases(dataset, sourceSchema), sourceSchema);
  }

  /**
   * Merges top-level fields with their aliases in the dataset.
   * <p>
   * This method goes through the top-level fields in the Avro schema, and for any field that has aliases,
   * it combines them in the dataset using a coalesce operation. This ensures that if a field is null,
   * the value from its alias is used instead.
   *
   * @param dataset      The dataset to process.
   * @param sourceSchema The Avro schema defining the fields and their aliases.
   * @return A dataset with fields merged with their aliases.
   */
  private static Dataset<Row> coalesceTopLevelAliases(Dataset<Row> dataset, Schema sourceSchema) {
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
   * This method iterates through the fields of the provided Avro schema and checks if they represent
   * nested records. For each nested record, it verifies if there are any alias fields present. If
   * aliases are found, the method generates a list of nested fields, coalescing them with their aliases,
   * and creates a new column in the dataset with the merged data.
   *
   * @param dataset      The dataset to process.
   * @param sourceSchema The Avro schema defining the structure and aliases of the data.
   * @return A dataset with nested fields merged with their aliases.
   */
  private static Dataset<Row> coalesceNestedAliases(Dataset<Row> dataset, Schema sourceSchema) {
    for (Schema.Field field : getRecordFields(sourceSchema)) {
      // check if this is a nested record and contains an alias field within
      if (isNestedRecord(field.schema()) && hasFieldWithAliases(field.schema())) {
        dataset = dataset.withColumn(field.name(), functions.struct(getNestedFields("", field, dataset)));
      }
    }
    return dataset;
  }

  private static Column[] getNestedFields(String parentField, Schema.Field field, Dataset<Row> dataset) {
    return getRecordFields(field.schema()).stream()
        .map(avroField -> {
          List<Column> columns = new ArrayList<>();
          String newParentField = getFullName(parentField, field.name());
          if (isNestedRecord(avroField.schema())) {
            // if field is nested, recursively fetch nested column
            columns.add(functions.struct(getNestedFields(newParentField, avroField, dataset)));
          } else {
            columns.add(dataset.col(getFullName(newParentField, avroField.name())));
          }
          avroField.aliases().forEach(alias -> columns.add(dataset.col(getFullName(newParentField, alias))));
          // if avro field contains aliases, coalesce the column with others matching the aliases otherwise return actual column
          return avroField.aliases().isEmpty() ? columns.get(0)
              : functions.coalesce(columns.toArray(new Column[0])).alias(avroField.name());
        }).toArray(Column[]::new);
  }

  private static boolean isNestedRecord(Schema schema) {
    if (schema.getType() == Schema.Type.RECORD) {
      return true;
    }

    if (schema.getType() == Schema.Type.UNION) {
      return schema.getTypes().stream()
          .anyMatch(subSchema -> subSchema.getType() == Schema.Type.RECORD);
    }

    return false;
  }

  private static String getFullName(String namespace, String fieldName) {
    return namespace.isEmpty() ? fieldName : namespace + "." + fieldName;
  }

  private static boolean schemaModifiedOrHasAliases(Schema.Field avroField, StructType modifiedNestedSchema, StructField rowField) {
    return !modifiedNestedSchema.equals(rowField.dataType()) || !avroField.aliases().isEmpty();
  }

  private static Option<String> getPropVal(TypedProperties props, ConfigProperty<String> configProperty) {
    String value = getStringWithAltKeys(props, configProperty, true);
    if (!StringUtils.isNullOrEmpty(value)) {
      return Option.of(value);
    }

    return Option.empty();
  }

  public enum Type {
    S3,
    GCS
  }

  public enum CloudDataColumnInfo {
    S3(
        CloudObjectsSelectorCommon.S3_OBJECT_KEY,
        CloudObjectsSelectorCommon.S3_OBJECT_SIZE
    ),
    GCS(
        CloudObjectsSelectorCommon.GCS_OBJECT_KEY,
        CloudObjectsSelectorCommon.GCS_OBJECT_SIZE
    );

    // both S3 and GCS use COMMIT_TIME_METADATA_FIELD as order column
    private final String orderColumn = HoodieRecord.COMMIT_TIME_METADATA_FIELD;
    private final String keyColumn;
    private final String limitColumn;
    private final List<String> orderByColumns;

    CloudDataColumnInfo(String keyColumn, String limitColumn) {
      this.keyColumn = keyColumn;
      this.limitColumn = limitColumn;
      orderByColumns = Arrays.asList(orderColumn, keyColumn);
    }

    public String getOrderColumn() {
      return orderColumn;
    }

    public String getKeyColumn() {
      return keyColumn;
    }

    public String getLimitColumn() {
      return limitColumn;
    }

    public List<String> getOrderByColumns() {
      return orderByColumns;
    }

    public static CloudDataColumnInfo getCloudDataColumnInfo(CloudObjectsSelectorCommon.Type type) {
      switch (type) {
        case S3:
          return S3;
        case GCS:
          return GCS;
        default:
          throw new IllegalArgumentException("Unsupported cloud data column type: " + type);
      }
    }
  }
}
