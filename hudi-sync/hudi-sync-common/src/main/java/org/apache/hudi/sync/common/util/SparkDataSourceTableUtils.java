package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.util.StringUtils;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

public class SparkDataSourceTableUtils {
  /**
   * Get Spark Sql related table properties. This is used for spark datasource table.
   * @param schema  The schema to write to the table.
   * @return A new parameters added the spark's table properties.
   */
  public static Map<String, String> getSparkTableProperties(List<String> partitionNames, String sparkVersion,
                                                            int schemaLengthThreshold, MessageType schema)  {
    // Convert the schema and partition info used by spark sql to hive table properties.
    // The following code refers to the spark code in
    // https://github.com/apache/spark/blob/master/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveExternalCatalog.scala
    GroupType originGroupType = schema.asGroupType();
    List<Type> partitionCols = new ArrayList<>();
    List<Type> dataCols = new ArrayList<>();
    Map<String, Type> column2Field = new HashMap<>();

    for (Type field : originGroupType.getFields()) {
      column2Field.put(field.getName(), field);
    }
    // Get partition columns and data columns.
    for (String partitionName : partitionNames) {
      // Default the unknown partition fields to be String.
      // Keep the same logical with HiveSchemaUtil#getPartitionKeyType.
      partitionCols.add(column2Field.getOrDefault(partitionName,
          new PrimitiveType(Type.Repetition.REQUIRED, BINARY, partitionName, UTF8)));
    }

    for (Type field : originGroupType.getFields()) {
      if (!partitionNames.contains(field.getName())) {
        dataCols.add(field);
      }
    }

    List<Type> reOrderedFields = new ArrayList<>();
    reOrderedFields.addAll(dataCols);
    reOrderedFields.addAll(partitionCols);
    GroupType reOrderedType = new GroupType(originGroupType.getRepetition(), originGroupType.getName(), reOrderedFields);

    Map<String, String> sparkProperties = new HashMap<>();
    sparkProperties.put("spark.sql.sources.provider", "hudi");
    if (!StringUtils.isNullOrEmpty(sparkVersion)) {
      sparkProperties.put("spark.sql.create.version", sparkVersion);
    }
    // Split the schema string to multi-parts according the schemaLengthThreshold size.
    String schemaString = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(reOrderedType);
    int numSchemaPart = (schemaString.length() + schemaLengthThreshold - 1) / schemaLengthThreshold;
    sparkProperties.put("spark.sql.sources.schema.numParts", String.valueOf(numSchemaPart));
    // Add each part of schema string to sparkProperties
    for (int i = 0; i < numSchemaPart; i++) {
      int start = i * schemaLengthThreshold;
      int end = Math.min(start + schemaLengthThreshold, schemaString.length());
      sparkProperties.put("spark.sql.sources.schema.part." + i, schemaString.substring(start, end));
    }
    // Add partition columns
    if (!partitionNames.isEmpty()) {
      sparkProperties.put("spark.sql.sources.schema.numPartCols", String.valueOf(partitionNames.size()));
      for (int i = 0; i < partitionNames.size(); i++) {
        sparkProperties.put("spark.sql.sources.schema.partCol." + i, partitionNames.get(i));
      }
    }
    return sparkProperties;
  }

  public static Map<String, String> getSparkSerdeProperties(boolean readAsOptimized, String basePath) {
    Map<String, String> sparkSerdeProperties = new HashMap<>();
    sparkSerdeProperties.put("path", basePath);
    sparkSerdeProperties.put(ConfigUtils.IS_QUERY_AS_RO_TABLE, String.valueOf(readAsOptimized));
    return sparkSerdeProperties;
  }
}
