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

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utilities used throughout the data source.
 */
public class DataSourceUtils {

  /**
   * Obtain value of the provided field as string, denoted by dot notation. e.g: a.b.c
   */
  public static String getNestedFieldValAsString(GenericRecord record, String fieldName, boolean returnNullIfNotFound) {
    Object obj = getNestedFieldVal(record, fieldName, returnNullIfNotFound);
    return (obj == null) ? null : obj.toString();
  }

  /**
   * Obtain value of the provided field, denoted by dot notation. e.g: a.b.c
   */
  public static Object getNestedFieldVal(GenericRecord record, String fieldName, boolean returnNullIfNotFound) {
    String[] parts = fieldName.split("\\.");
    GenericRecord valueNode = record;
    int i = 0;
    for (; i < parts.length; i++) {
      String part = parts[i];
      Object val = valueNode.get(part);
      if (val == null) {
        break;
      }

      // return, if last part of name
      if (i == parts.length - 1) {
        Schema fieldSchema = valueNode.getSchema().getField(part).schema();
        return convertValueForSpecificDataTypes(fieldSchema, val);
      } else {
        // VC: Need a test here
        if (!(val instanceof GenericRecord)) {
          throw new HoodieException("Cannot find a record at part value :" + part);
        }
        valueNode = (GenericRecord) val;
      }
    }

    if (returnNullIfNotFound) {
      return null;
    } else {
      throw new HoodieException(
        fieldName + "(Part -" + parts[i] + ") field not found in record. Acceptable fields were :"
          + valueNode.getSchema().getFields().stream().map(Field::name).collect(Collectors.toList()));
    }
  }

  /**
   * This method converts values for fields with certain Avro/Parquet data types that require special handling.
   *
   * Logical Date Type is converted to actual Date value instead of Epoch Integer which is how it is
   * represented/stored in parquet.
   *
   * @param fieldSchema avro field schema
   * @param fieldValue avro field value
   * @return field value either converted (for certain data types) or as it is.
   */
  private static Object convertValueForSpecificDataTypes(Schema fieldSchema, Object fieldValue) {
    if (fieldSchema == null) {
      return fieldValue;
    }

    if (isLogicalTypeDate(fieldSchema)) {
      return LocalDate.ofEpochDay(Long.parseLong(fieldValue.toString()));
    }
    return fieldValue;
  }

  /**
   * Given an Avro field schema checks whether the field is of Logical Date Type or not.
   *
   * @param fieldSchema avro field schema
   * @return boolean indicating whether fieldSchema is of Avro's Date Logical Type
   */
  private static boolean isLogicalTypeDate(Schema fieldSchema) {
    if (fieldSchema.getType() == Schema.Type.UNION) {
      return fieldSchema.getTypes().stream().anyMatch(schema -> schema.getLogicalType() == LogicalTypes.date());
    }
    return fieldSchema.getLogicalType() == LogicalTypes.date();
  }

  /**
   * Create a key generator class via reflection, passing in any configs needed.
   * <p>
   * If the class name of key generator is configured through the properties file, i.e., {@code props}, use the
   * corresponding key generator class; otherwise, use the default key generator class specified in {@code
   * DataSourceWriteOptions}.
   */
  public static KeyGenerator createKeyGenerator(TypedProperties props) throws IOException {
    String keyGeneratorClass = props.getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(),
        DataSourceWriteOptions.DEFAULT_KEYGENERATOR_CLASS_OPT_VAL());
    try {
      return (KeyGenerator) ReflectionUtils.loadClass(keyGeneratorClass, props);
    } catch (Throwable e) {
      throw new IOException("Could not load key generator class " + keyGeneratorClass, e);
    }
  }

  /**
   * Create a UserDefinedBulkInsertPartitioner class via reflection,
   * <br>
   * if the class name of UserDefinedBulkInsertPartitioner is configured through the HoodieWriteConfig.
   * @see HoodieWriteConfig#getUserDefinedBulkInsertPartitionerClass()
   */
  private static Option<UserDefinedBulkInsertPartitioner> createUserDefinedBulkInsertPartitioner(HoodieWriteConfig config)
          throws HoodieException {
    String bulkInsertPartitionerClass = config.getUserDefinedBulkInsertPartitionerClass();
    try {
      return StringUtils.isNullOrEmpty(bulkInsertPartitionerClass)
              ? Option.empty() :
              Option.of((UserDefinedBulkInsertPartitioner) ReflectionUtils.loadClass(bulkInsertPartitionerClass));
    } catch (Throwable e) {
      throw new HoodieException("Could not create UserDefinedBulkInsertPartitioner class " + bulkInsertPartitionerClass, e);
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

  public static void checkRequiredProperties(TypedProperties props, List<String> checkPropNames) {
    checkPropNames.forEach(prop -> {
      if (!props.containsKey(prop)) {
        throw new HoodieNotSupportedException("Required property " + prop + " is missing");
      }
    });
  }

  public static HoodieWriteClient createHoodieClient(JavaSparkContext jssc, String schemaStr, String basePath,
                                                     String tblName, Map<String, String> parameters) {

    // inline compaction is on by default for MOR
    boolean inlineCompact = parameters.get(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY())
        .equals(DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL());

    // insert/bulk-insert combining to be true, if filtering for duplicates
    boolean combineInserts = Boolean.parseBoolean(parameters.get(DataSourceWriteOptions.INSERT_DROP_DUPS_OPT_KEY()));

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath).withAutoCommit(false)
        .combineInput(combineInserts, true).withSchema(schemaStr).forTable(tblName)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withPayloadClass(parameters.get(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY()))
            .withInlineCompaction(inlineCompact).build())
        // override above with Hoodie configs specified as options.
        .withProps(parameters).build();

    return new HoodieWriteClient<>(jssc, writeConfig, true);
  }

  public static JavaRDD<WriteStatus> doWriteOperation(HoodieWriteClient client, JavaRDD<HoodieRecord> hoodieRecords,
                                                      String instantTime, String operation) throws HoodieException {
    if (operation.equals(DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL())) {
      Option<UserDefinedBulkInsertPartitioner> userDefinedBulkInsertPartitioner =
          createUserDefinedBulkInsertPartitioner(client.getConfig());
      return client.bulkInsert(hoodieRecords, instantTime, userDefinedBulkInsertPartitioner);
    } else if (operation.equals(DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL())) {
      return client.insert(hoodieRecords, instantTime);
    } else {
      // default is upsert
      return client.upsert(hoodieRecords, instantTime);
    }
  }

  public static JavaRDD<WriteStatus> doDeleteOperation(HoodieWriteClient client, JavaRDD<HoodieKey> hoodieKeys,
                                                       String instantTime) {
    return client.delete(hoodieKeys, instantTime);
  }

  public static HoodieRecord createHoodieRecord(GenericRecord gr, Comparable orderingVal, HoodieKey hKey,
                                                String payloadClass) throws IOException {
    HoodieRecordPayload payload = DataSourceUtils.createPayload(payloadClass, gr, orderingVal);
    return new HoodieRecord<>(hKey, payload);
  }

  /**
   * Drop records already present in the dataset.
   *
   * @param jssc                  JavaSparkContext
   * @param incomingHoodieRecords HoodieRecords to deduplicate
   * @param writeConfig           HoodieWriteConfig
   */
  @SuppressWarnings("unchecked")
  public static JavaRDD<HoodieRecord> dropDuplicates(JavaSparkContext jssc, JavaRDD<HoodieRecord> incomingHoodieRecords,
                                                     HoodieWriteConfig writeConfig) {
    try {
      HoodieReadClient client = new HoodieReadClient<>(jssc, writeConfig);
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

  public static HiveSyncConfig buildHiveSyncConfig(TypedProperties props, String basePath) {
    checkRequiredProperties(props, Collections.singletonList(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY()));
    HiveSyncConfig hiveSyncConfig = new HiveSyncConfig();
    hiveSyncConfig.basePath = basePath;
    hiveSyncConfig.usePreApacheInputFormat =
        props.getBoolean(DataSourceWriteOptions.HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY(),
            Boolean.parseBoolean(DataSourceWriteOptions.DEFAULT_USE_PRE_APACHE_INPUT_FORMAT_OPT_VAL()));
    hiveSyncConfig.databaseName = props.getString(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY(),
        DataSourceWriteOptions.DEFAULT_HIVE_DATABASE_OPT_VAL());
    hiveSyncConfig.tableName = props.getString(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY());
    hiveSyncConfig.hiveUser =
        props.getString(DataSourceWriteOptions.HIVE_USER_OPT_KEY(), DataSourceWriteOptions.DEFAULT_HIVE_USER_OPT_VAL());
    hiveSyncConfig.hivePass =
        props.getString(DataSourceWriteOptions.HIVE_PASS_OPT_KEY(), DataSourceWriteOptions.DEFAULT_HIVE_PASS_OPT_VAL());
    hiveSyncConfig.jdbcUrl =
        props.getString(DataSourceWriteOptions.HIVE_URL_OPT_KEY(), DataSourceWriteOptions.DEFAULT_HIVE_URL_OPT_VAL());
    hiveSyncConfig.partitionFields =
        props.getStringList(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), ",", new ArrayList<>());
    hiveSyncConfig.partitionValueExtractorClass =
        props.getString(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(),
            SlashEncodedDayPartitionValueExtractor.class.getName());
    hiveSyncConfig.useJdbc = Boolean.valueOf(props.getString(DataSourceWriteOptions.HIVE_USE_JDBC_OPT_KEY(),
            DataSourceWriteOptions.DEFAULT_HIVE_USE_JDBC_OPT_VAL()));
    return hiveSyncConfig;
  }
}
