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

package org.apache.hudi.table.catalog;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.sync.common.util.SparkDataSourceTableUtils;
import org.apache.hudi.util.AvroSchemaConverter;

import org.apache.avro.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.hudi.common.table.HoodieTableMetaClient.AUXILIARYFOLDER_NAME;

/**
 * Helper class to read/write flink table options as a map.
 */
public class TableOptionProperties {
  private static final Logger LOG = LoggerFactory.getLogger(TableOptionProperties.class);

  public static final String SPARK_SOURCE_PROVIDER = "spark.sql.sources.provider";
  public static final String SPARK_VERSION = "spark.version";
  public static final String DEFAULT_SPARK_VERSION = "spark2.4.4";
  static final Map<String, String> VALUE_MAPPING = new HashMap<>();
  static final Map<String, String> KEY_MAPPING = new HashMap<>();

  private static final String FILE_NAME = "table_option.properties";

  public static final String PK_CONSTRAINT_NAME = "pk.constraint.name";
  public static final String PK_COLUMNS = "pk.columns";
  public static final String COMMENT = "comment";
  public static final String PARTITION_COLUMNS = "partition.columns";

  public static final List<String> NON_OPTION_KEYS = Arrays.asList(PK_CONSTRAINT_NAME, PK_COLUMNS, COMMENT, PARTITION_COLUMNS);

  static {
    VALUE_MAPPING.put("mor", HoodieTableType.MERGE_ON_READ.name());
    VALUE_MAPPING.put("cow", HoodieTableType.COPY_ON_WRITE.name());

    VALUE_MAPPING.put(HoodieTableType.MERGE_ON_READ.name(), "mor");
    VALUE_MAPPING.put(HoodieTableType.COPY_ON_WRITE.name(), "cow");

    KEY_MAPPING.put("type", FlinkOptions.TABLE_TYPE.key());
    KEY_MAPPING.put("primaryKey", FlinkOptions.RECORD_KEY_FIELD.key());
    KEY_MAPPING.put("preCombineField", FlinkOptions.PRECOMBINE_FIELD.key());
    KEY_MAPPING.put("payloadClass", FlinkOptions.PAYLOAD_CLASS_NAME.key());
    KEY_MAPPING.put(SPARK_SOURCE_PROVIDER, CONNECTOR.key());
    KEY_MAPPING.put(FlinkOptions.KEYGEN_CLASS_NAME.key(), FlinkOptions.KEYGEN_CLASS_NAME.key());
    KEY_MAPPING.put(FlinkOptions.TABLE_TYPE.key(), "type");
    KEY_MAPPING.put(FlinkOptions.RECORD_KEY_FIELD.key(), "primaryKey");
    KEY_MAPPING.put(FlinkOptions.PRECOMBINE_FIELD.key(), "preCombineField");
    KEY_MAPPING.put(FlinkOptions.PAYLOAD_CLASS_NAME.key(), "payloadClass");
  }

  /**
   * Initialize the {@link #FILE_NAME} meta file.
   */
  public static void createProperties(String basePath,
                                      Configuration hadoopConf,
                                      Map<String, String> options) throws IOException {
    Path propertiesFilePath = getPropertiesFilePath(basePath);
    FileSystem fs = FSUtils.getFs(basePath, hadoopConf);
    try (FSDataOutputStream outputStream = fs.create(propertiesFilePath)) {
      Properties properties = new Properties();
      properties.putAll(options);
      properties.store(outputStream,
          "Table option properties saved on " + new Date(System.currentTimeMillis()));
    }
    LOG.info(String.format("Create file %s success.", propertiesFilePath));
  }

  /**
   * Read table options map from the given table base path.
   */
  public static Map<String, String> loadFromProperties(String basePath, Configuration hadoopConf) {
    Path propertiesFilePath = getPropertiesFilePath(basePath);
    Map<String, String> options = new HashMap<>();
    Properties props = new Properties();

    FileSystem fs = FSUtils.getFs(basePath, hadoopConf);
    try (FSDataInputStream inputStream = fs.open(propertiesFilePath)) {
      props.load(inputStream);
      for (final String name : props.stringPropertyNames()) {
        options.put(name, props.getProperty(name));
      }
    } catch (IOException e) {
      throw new HoodieIOException(String.format("Could not load table option properties from %s", propertiesFilePath), e);
    }
    LOG.info(String.format("Loading table option properties from %s success.", propertiesFilePath));
    return options;
  }

  private static Path getPropertiesFilePath(String basePath) {
    String auxPath = basePath + Path.SEPARATOR + AUXILIARYFOLDER_NAME;
    return new Path(auxPath, FILE_NAME);
  }

  public static String getPkConstraintName(Map<String, String> options) {
    return options.get(PK_CONSTRAINT_NAME);
  }

  public static List<String> getPkColumns(Map<String, String> options) {
    if (options.containsKey(PK_COLUMNS)) {
      return Arrays.stream(options.get(PK_COLUMNS).split(",")).collect(Collectors.toList());
    } else {
      return Collections.emptyList();
    }
  }

  public static List<String> getPartitionColumns(Map<String, String> options) {
    if (options.containsKey(PARTITION_COLUMNS)) {
      return Arrays.stream(options.get(PARTITION_COLUMNS).split(",")).collect(Collectors.toList());
    } else {
      return Collections.emptyList();
    }
  }

  public static String getComment(Map<String, String> options) {
    return options.get(COMMENT);
  }

  public static Map<String, String> getTableOptions(Map<String, String> options) {
    Map<String, String> copied = new HashMap<>(options);
    NON_OPTION_KEYS.forEach(copied::remove);
    return copied;
  }

  public static Map<String, String> translateFlinkTableProperties2Spark(
      CatalogTable catalogTable,
      Configuration hadoopConf,
      Map<String, String> properties,
      List<String> partitionKeys) {
    Schema schema = AvroSchemaConverter.convertToSchema(catalogTable.getSchema().toPhysicalRowDataType().getLogicalType());
    MessageType messageType = TableSchemaResolver.convertAvroSchemaToParquet(schema, hadoopConf);
    String sparkVersion = catalogTable.getOptions().getOrDefault(SPARK_VERSION, DEFAULT_SPARK_VERSION);
    Map<String, String> sparkTableProperties = SparkDataSourceTableUtils.getSparkTableProperties(
        partitionKeys,
        sparkVersion,
        4000,
        messageType);
    properties.putAll(sparkTableProperties);
    return properties.entrySet().stream()
        .filter(e -> KEY_MAPPING.containsKey(e.getKey()) && !catalogTable.getOptions().containsKey(KEY_MAPPING.get(e.getKey())))
        .collect(Collectors.toMap(e -> KEY_MAPPING.get(e.getKey()),
            e -> e.getKey().equalsIgnoreCase(FlinkOptions.TABLE_TYPE.key()) ? VALUE_MAPPING.get(e.getValue()) : e.getValue()));
  }

  public static Map<String, String> translateSparkTableProperties2Flink(Map<String, String> options) {
    if (options.containsKey(CONNECTOR.key())) {
      return options;
    }
    return options.entrySet().stream().filter(e -> KEY_MAPPING.containsKey(e.getKey()))
        .collect(Collectors.toMap(e -> KEY_MAPPING.get(e.getKey()),
            e -> e.getKey().equalsIgnoreCase("type") ? VALUE_MAPPING.get(e.getValue()) : e.getValue()));
  }

  public static Map<String, String> translateSparkTableProperties2Flink(Table hiveTable) {
    return translateSparkTableProperties2Flink(hiveTable.getParameters());
  }
}
