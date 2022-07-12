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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.sync.common.util.SparkDataSourceTableUtils;
import org.apache.hudi.util.AvroSchemaConverter;

import org.apache.avro.Schema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.hudi.common.table.HoodieTableMetaClient.AUXILIARYFOLDER_NAME;

/**
 * Helper class to read/write flink table options as a map.
 */
public class TableOptionProperties {
  private static final Logger LOG = LoggerFactory.getLogger(TableOptionProperties.class);

  private static final String FILE_NAME = "table_option.properties";

  public static final String PK_CONSTRAINT_NAME = "pk.constraint.name";
  public static final String PK_COLUMNS = "pk.columns";
  public static final String COMMENT = "comment";
  public static final String PARTITION_COLUMNS = "partition.columns";
  public static final String SPARK_SOURCE_PROVIDER = "spark.sql.sources.provider";
  public static final String SPARK_VERSION = "spark.verison";
  public static final String DEFAULT_SPARK_VERSION = "spark2.4.4";

  public static final List<String> NON_OPTION_KEYS = Arrays.asList(PK_CONSTRAINT_NAME, PK_COLUMNS, COMMENT, PARTITION_COLUMNS);

  private static final Map<String, String> VALUE_MAPPING = new TreeMap<>();
  private static final Map<String, String> KEY_MAPPING = new TreeMap<>();

  static {
    VALUE_MAPPING.put("mor", HoodieTableType.MERGE_ON_READ.name());
    VALUE_MAPPING.put("cow", HoodieTableType.COPY_ON_WRITE.name());

    KEY_MAPPING.put("type", FlinkOptions.TABLE_TYPE.key());
    KEY_MAPPING.put("primaryKey", FlinkOptions.RECORD_KEY_FIELD.key());
    KEY_MAPPING.put("preCombineField", FlinkOptions.PRECOMBINE_FIELD.key());
    KEY_MAPPING.put("payloadClass", FlinkOptions.PAYLOAD_CLASS_NAME.key());
    KEY_MAPPING.put(SPARK_SOURCE_PROVIDER, CONNECTOR.key());
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

  public static Map<String, String> translateFlinkTableProperties2Spark(CatalogTable catalogTable, Configuration hadoopConf) {
    Schema schema = AvroSchemaConverter.convertToSchema(catalogTable.getSchema().toPhysicalRowDataType().getLogicalType());
    MessageType messageType = TableSchemaResolver.convertAvroSchemaToParquet(schema, hadoopConf);
    String sparkVersion = catalogTable.getOptions().getOrDefault(SPARK_VERSION, DEFAULT_SPARK_VERSION);
    return SparkDataSourceTableUtils.getSparkTableProperties(catalogTable.getPartitionKeys(), sparkVersion, 4000, messageType);
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

  /** Get field names from field schemas. */
  public static List<String> getFieldNames(List<FieldSchema> fieldSchemas) {
    List<String> names = new ArrayList<>(fieldSchemas.size());
    for (FieldSchema fs : fieldSchemas) {
      names.add(fs.getName());
    }
    return names;
  }

  public static org.apache.flink.table.api.Schema convertTableSchema(Table hiveTable) {
    List<FieldSchema> allCols = new ArrayList<>(hiveTable.getSd().getCols());
    allCols.addAll(hiveTable.getPartitionKeys());

    String pkConstraintName = hiveTable.getParameters().get(PK_CONSTRAINT_NAME);
    List<String> primaryColNames = StringUtils.isNullOrEmpty(pkConstraintName)
        ? Collections.EMPTY_LIST
        : StringUtils.split(hiveTable.getParameters().get(PK_COLUMNS),",");

    String[] colNames = new String[allCols.size()];
    DataType[] colTypes = new DataType[allCols.size()];

    for (int i = 0; i < allCols.size(); i++) {
      FieldSchema fs = allCols.get(i);

      colNames[i] = fs.getName();
      colTypes[i] =
          toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(fs.getType()));
      if (primaryColNames.contains(colNames[i])) {
        colTypes[i] = colTypes[i].notNull();
      }
    }

    org.apache.flink.table.api.Schema.Builder builder = org.apache.flink.table.api.Schema.newBuilder().fromFields(colNames, colTypes);
    if (!StringUtils.isNullOrEmpty(pkConstraintName)) {
      builder.primaryKeyNamed(pkConstraintName, primaryColNames);
    }

    return builder.build();
  }

  /**
   * Convert Hive data type to a Flink data type.
   *
   * @param hiveType a Hive data type
   * @return the corresponding Flink data type
   */
  public static DataType toFlinkType(TypeInfo hiveType) {
    checkNotNull(hiveType, "hiveType cannot be null");

    switch (hiveType.getCategory()) {
      case PRIMITIVE:
        return toFlinkPrimitiveType((PrimitiveTypeInfo) hiveType);
      case LIST:
        ListTypeInfo listTypeInfo = (ListTypeInfo) hiveType;
        return DataTypes.ARRAY(toFlinkType(listTypeInfo.getListElementTypeInfo()));
      case MAP:
        MapTypeInfo mapTypeInfo = (MapTypeInfo) hiveType;
        return DataTypes.MAP(
            toFlinkType(mapTypeInfo.getMapKeyTypeInfo()),
            toFlinkType(mapTypeInfo.getMapValueTypeInfo()));
      case STRUCT:
        StructTypeInfo structTypeInfo = (StructTypeInfo) hiveType;

        List<String> names = structTypeInfo.getAllStructFieldNames();
        List<TypeInfo> typeInfos = structTypeInfo.getAllStructFieldTypeInfos();

        DataTypes.Field[] fields = new DataTypes.Field[names.size()];

        for (int i = 0; i < fields.length; i++) {
          fields[i] = DataTypes.FIELD(names.get(i), toFlinkType(typeInfos.get(i)));
        }

        return DataTypes.ROW(fields);
      default:
        throw new UnsupportedOperationException(
            String.format("Flink doesn't support Hive data type %s yet.", hiveType));
    }
  }

  private static DataType toFlinkPrimitiveType(PrimitiveTypeInfo hiveType) {
    checkNotNull(hiveType, "hiveType cannot be null");

    switch (hiveType.getPrimitiveCategory()) {
      case CHAR:
        return DataTypes.CHAR(((CharTypeInfo) hiveType).getLength());
      case VARCHAR:
        return DataTypes.VARCHAR(((VarcharTypeInfo) hiveType).getLength());
      case STRING:
        return DataTypes.STRING();
      case BOOLEAN:
        return DataTypes.BOOLEAN();
      case BYTE:
        return DataTypes.TINYINT();
      case SHORT:
        return DataTypes.SMALLINT();
      case INT:
        return DataTypes.INT();
      case LONG:
        return DataTypes.BIGINT();
      case FLOAT:
        return DataTypes.FLOAT();
      case DOUBLE:
        return DataTypes.DOUBLE();
      case DATE:
        return DataTypes.DATE();
      case TIMESTAMP:
        return DataTypes.TIMESTAMP(9);
      case BINARY:
        return DataTypes.BYTES();
      case DECIMAL:
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) hiveType;
        return DataTypes.DECIMAL(
            decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Flink doesn't support Hive primitive type %s yet", hiveType));
    }
  }

  /** Create Hive columns from Flink TableSchema. */
  public static List<FieldSchema> createHiveColumns(TableSchema schema) {
    String[] fieldNames = schema.getFieldNames();
    DataType[] fieldTypes = schema.getFieldDataTypes();

    List<FieldSchema> columns = new ArrayList<>(fieldNames.length);

    for (int i = 0; i < fieldNames.length; i++) {
      columns.add(
          new FieldSchema(
              fieldNames[i],
              toHiveTypeInfo(fieldTypes[i], true).getTypeName(),
              null));
    }

    return columns;
  }

  /**
   * Convert Flink DataType to Hive TypeInfo. For types with a precision parameter, e.g.
   * timestamp, the supported precisions in Hive and Flink can be different. Therefore the
   * conversion will fail for those types if the precision is not supported by Hive and
   * checkPrecision is true.
   *
   * @param dataType a Flink DataType
   * @param checkPrecision whether to fail the conversion if the precision of the DataType is not
   *     supported by Hive
   * @return the corresponding Hive data type
   */
  public static TypeInfo toHiveTypeInfo(DataType dataType, boolean checkPrecision) {
    checkNotNull(dataType, "type cannot be null");
    LogicalType logicalType = dataType.getLogicalType();
    return logicalType.accept(new TypeInfoLogicalTypeVisitor(dataType, checkPrecision));
  }
}
