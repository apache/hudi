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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.DatasetNotFoundException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.PartitionValueExtractor;
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor;
import org.apache.hudi.index.HoodieIndex;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Utilities used throughout the data source
 */
public class DataSourceUtils {

  /**
   * Obtain value of the provided field as string, denoted by dot notation. e.g: a.b.c
   */
  public static String getNestedFieldValAsString(GenericRecord record, String fieldName) {
    Object obj = getNestedFieldVal(record, fieldName);
    return (obj == null) ? null : obj.toString();
  }

  /**
   * Obtain value of the provided field, denoted by dot notation. e.g: a.b.c
   */
  public static Object getNestedFieldVal(GenericRecord record, String fieldName) {
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
        return val;
      } else {
        // VC: Need a test here
        if (!(val instanceof GenericRecord)) {
          throw new HoodieException("Cannot find a record at part value :" + part);
        }
        valueNode = (GenericRecord) val;
      }
    }
    throw new HoodieException(
        fieldName + "(Part -" + parts[i] + ") field not found in record. " + "Acceptable fields were :"
            + valueNode.getSchema().getFields().stream().map(Field::name).collect(Collectors.toList()));
  }

  /**
   * Create a key generator class via reflection, passing in any configs needed.
   *
   * If the class name of key generator is configured through the properties file, i.e., {@code
   * props}, use the corresponding key generator class; otherwise, use the default key generator class specified in
   * {@code DataSourceWriteOptions}.
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
   * Create a partition value extractor class via reflection, passing in any configs needed
   */
  public static PartitionValueExtractor createPartitionExtractor(String partitionExtractorClass) {
    try {
      return (PartitionValueExtractor) ReflectionUtils.loadClass(partitionExtractorClass);
    } catch (Throwable e) {
      throw new HoodieException("Could not load partition extractor class " + partitionExtractorClass, e);
    }
  }

  /**
   * Create a payload class via reflection, passing in an ordering/precombine value.
   */
  public static HoodieRecordPayload createPayload(String payloadClass, GenericRecord record, Comparable orderingVal)
      throws IOException {
    try {
      return (HoodieRecordPayload) ReflectionUtils.loadClass(payloadClass,
          new Class<?>[]{GenericRecord.class, Comparable.class}, record, orderingVal);
    } catch (Throwable e) {
      throw new IOException("Could not create payload for class: " + payloadClass, e);
    }
  }

  public static void checkRequiredProperties(TypedProperties props, List<String> checkPropNames) {
    checkPropNames.stream().forEach(prop -> {
      if (!props.containsKey(prop)) {
        throw new HoodieNotSupportedException("Required property " + prop + " is missing");
      }
    });
  }

  public static HoodieWriteClient createHoodieClient(JavaSparkContext jssc, String schemaStr, String basePath,
      String tblName, Map<String, String> parameters) throws Exception {

    // inline compaction is on by default for MOR
    boolean inlineCompact = parameters.get(DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY())
        .equals(DataSourceWriteOptions.MOR_STORAGE_TYPE_OPT_VAL());

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
      String commitTime, String operation) {
    if (operation.equals(DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL())) {
      return client.bulkInsert(hoodieRecords, commitTime);
    } else if (operation.equals(DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL())) {
      return client.insert(hoodieRecords, commitTime);
    } else {
      // default is upsert
      return client.upsert(hoodieRecords, commitTime);
    }
  }

  public static HoodieRecord createHoodieRecord(GenericRecord gr, Comparable orderingVal, HoodieKey hKey,
      String payloadClass) throws IOException {
    HoodieRecordPayload payload = DataSourceUtils.createPayload(payloadClass, gr, orderingVal);
    return new HoodieRecord<>(hKey, payload);
  }

  @SuppressWarnings("unchecked")
  public static JavaRDD<HoodieRecord> dropDuplicates(JavaSparkContext jssc, JavaRDD<HoodieRecord> incomingHoodieRecords,
      HoodieWriteConfig writeConfig, Option<EmbeddedTimelineService> timelineService) throws Exception {
    HoodieReadClient client = null;
    try {
      client = new HoodieReadClient<>(jssc, writeConfig, timelineService);
      return client.tagLocation(incomingHoodieRecords)
          .filter(r -> !((HoodieRecord<HoodieRecordPayload>) r).isCurrentLocationKnown());
    } catch (DatasetNotFoundException e) {
      // this will be executed when there is no hoodie dataset yet
      // so no dups to drop
      return incomingHoodieRecords;
    } finally {
      if (null != client) {
        client.close();
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static JavaRDD<HoodieRecord> dropDuplicates(JavaSparkContext jssc, JavaRDD<HoodieRecord> incomingHoodieRecords,
      Map<String, String> parameters, Option<EmbeddedTimelineService> timelineService) throws Exception {
    HoodieWriteConfig writeConfig =
        HoodieWriteConfig.newBuilder().withPath(parameters.get("path")).withProps(parameters).build();
    return dropDuplicates(jssc, incomingHoodieRecords, writeConfig, timelineService);
  }

  public static HiveSyncConfig buildHiveSyncConfig(TypedProperties props, String basePath) {
    checkRequiredProperties(props, Arrays.asList(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY()));
    HiveSyncConfig hiveSyncConfig = new HiveSyncConfig();
    hiveSyncConfig.basePath = basePath;
    hiveSyncConfig.usePreApacheInputFormat =
        props.getBoolean(DataSourceWriteOptions.HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY(),
            Boolean.valueOf(DataSourceWriteOptions.DEFAULT_USE_PRE_APACHE_INPUT_FORMAT_OPT_VAL()));
    hiveSyncConfig.assumeDatePartitioning =
        props.getBoolean(DataSourceWriteOptions.HIVE_ASSUME_DATE_PARTITION_OPT_KEY(),
            Boolean.valueOf(DataSourceWriteOptions.DEFAULT_HIVE_ASSUME_DATE_PARTITION_OPT_VAL()));
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
    return hiveSyncConfig;
  }
}
