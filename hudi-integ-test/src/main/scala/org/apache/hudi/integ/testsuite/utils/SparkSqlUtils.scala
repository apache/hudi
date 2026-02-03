/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.integ.testsuite.utils

import org.apache.hudi.HoodieSchemaConversionUtils
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.util.Option
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config
import org.apache.hudi.integ.testsuite.generator.GenericRecordFullPayloadGenerator
import org.apache.hudi.utilities.schema.RowBasedSchemaProvider

import org.apache.avro.generic.GenericRecord
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.slf4j.Logger

import scala.math.BigDecimal.RoundingMode.RoundingMode

/**
 * Utils for test nodes in Spark SQL
 */
object SparkSqlUtils {

  /**
   * @param sparkSession spark session to use
   * @param tableName    table name
   * @return table schema excluding meta columns in `StructType`
   */
  def getTableSchema(sparkSession: SparkSession, tableName: String): StructType = {
    new StructType(sparkSession.table(tableName).schema.fields
      .filter(field => !HoodieRecord.HOODIE_META_COLUMNS.contains(field.name)))
  }

  /**
   * Converts Avro schema in String to the SQL schema expression, with partition fields at the end
   *
   * For example, given the Avro schema below:
   * """
   * {"type":"record","name":"triprec","fields":[{"name":"timestamp","type":"long"},
   * {"name":"_row_key","type":"string"},{"name":"rider","type":"string"},{"name":"driver","type":"string"},
   * {"name":"begin_lat","type":"double"},{"name":"begin_lon","type":"double"},{"name":"end_lat","type":"double"},
   * {"name":"end_lon","type":"double"},{"name":"fare","type":"double"},
   * {"name":"_hoodie_is_deleted","type":"boolean","default":false}]}
   * """
   * and the partition columns Set("rider"),
   * the SQL schema expression is:
   * """
   * timestamp bigint,
   * _row_key string,
   * driver string,
   * begin_lat double,
   * begin_lon double,
   * end_lat double,
   * end_lon double,
   * fare double,
   * _hoodie_is_deleted boolean,
   * rider string
   * """
   *
   * @param avroSchemaString Avro schema String
   * @param partitionColumns partition columns
   * @return corresponding SQL schema expression
   */
  def convertAvroToSqlSchemaExpression(avroSchemaString: String, partitionColumns: Set[String]): String = {
    val fields: Array[(String, String)] = getFieldNamesAndTypes(avroSchemaString)
    val reorderedFields = fields.filter(field => !partitionColumns.contains(field._1)) ++
      fields.filter(field => partitionColumns.contains(field._1))
    reorderedFields.map(e => e._1 + " " + e._2).mkString(",\n")
  }

  /**
   * Converts Avro schema in String to an array of field names.
   *
   * For example, given the Avro schema below:
   * """
   * {"type":"record","name":"triprec","fields":[{"name":"timestamp","type":"long"},
   * {"name":"_row_key","type":"string"},{"name":"rider","type":"string"},{"name":"driver","type":"string"},
   * {"name":"begin_lat","type":"double"},{"name":"begin_lon","type":"double"},{"name":"end_lat","type":"double"},
   * {"name":"end_lon","type":"double"},{"name":"fare","type":"double"},
   * {"name":"_hoodie_is_deleted","type":"boolean","default":false}]}
   * """
   * the output is
   * ["timestamp", "_row_key", "rider", "driver", "begin_lat", "begin_lon", "end_lat", "end_lon",
   * "fare", "_hoodie_is_deleted"]
   *
   * @param avroSchemaString Avro schema String
   * @return an array of field names.
   */
  def convertAvroToFieldNames(avroSchemaString: String): Array[String] = {
    getFieldNamesAndTypes(avroSchemaString).map(e => e._1)
  }

  /**
   * Gets an array of field names and types from Avro schema String.
   *
   * For example, given the Avro schema below:
   * """
   * {"type":"record","name":"triprec","fields":[{"name":"timestamp","type":"long"},
   * {"name":"_row_key","type":"string"},{"name":"rider","type":"string"},{"name":"driver","type":"string"},
   * {"name":"begin_lat","type":"double"},{"name":"begin_lon","type":"double"},{"name":"end_lat","type":"double"},
   * {"name":"end_lon","type":"double"},{"name":"fare","type":"double"},
   * {"name":"_hoodie_is_deleted","type":"boolean","default":false}]}
   * """
   * the output is
   * [("timestamp", "bigint"),
   * ("_row_key", "string"),
   * ("rider", "string",
   * ("driver", "string"),
   * ("begin_lat", "double"),
   * ("begin_lon", "double"),
   * ("end_lat", "double"),
   * ("end_lon", "double"),
   * ("fare", "double"),
   * ("_hoodie_is_deleted", "boolean")]
   *
   * @param avroSchemaString Avro schema String
   * @return an array of field names and types
   */
  def getFieldNamesAndTypes(avroSchemaString: String): Array[(String, String)] = {
    val schema = HoodieSchema.parse(avroSchemaString)
    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema)
    structType.fields.map(field => (field.name, field.dataType.simpleString))
  }

  /**
   * Logs the Spark SQL query to run.
   *
   * @param log   {@link Logger} instance to use.
   * @param query query String.
   */
  def logQuery(log: Logger, query: String): Unit = {
    log.warn("----- Running the following Spark SQL query -----")
    log.warn(query)
    log.warn("-" * 50)
  }

  /**
   * Constructs the select query.
   *
   * For example, given the Avro schema below:
   * """
   * {"type":"record","name":"triprec","fields":[{"name":"timestamp","type":"long"},
   * {"name":"_row_key","type":"string"},{"name":"rider","type":"string"},{"name":"driver","type":"string"},
   * {"name":"begin_lat","type":"double"},{"name":"begin_lon","type":"double"},{"name":"end_lat","type":"double"},
   * {"name":"end_lon","type":"double"},{"name":"fare","type":"double"},
   * {"name":"_hoodie_is_deleted","type":"boolean","default":false}]}
   * """
   * and the partition columns Set("rider"),
   * the output is
   * """
   * select timestamp, _row_key, driver, begin_lat, begin_lon, end_lat, end_lon, fare,
   * _hoodie_is_deleted, rider from _temp_table
   * """
   *
   * @param inputSchema      input Avro schema String.
   * @param partitionColumns partition columns
   * @param tableName        table name.
   * @return select query String.
   */
  def constructSelectQuery(inputSchema: String, partitionColumns: Set[String], tableName: String): String = {
    val fieldNames: Array[String] = SparkSqlUtils.convertAvroToFieldNames(inputSchema)
    val reorderedFieldNames = fieldNames.filter(name => !partitionColumns.contains(name)) ++
      fieldNames.filter(name => partitionColumns.contains(name))
    constructSelectQuery(reorderedFieldNames, tableName)
  }

  /**
   * Constructs the select query with {@link StructType} columns in the select.
   *
   * @param structType {@link StructType} instance.
   * @param tableName  table name.
   * @return select query String.
   */
  def constructSelectQuery(structType: StructType, tableName: String): String = {
    constructSelectQuery(structType, Set.empty[String], tableName)
  }

  /**
   * Constructs the select query with {@link StructType} columns in the select and the partition
   * columns at the end.
   *
   * @param structType       {@link StructType} instance.
   * @param partitionColumns partition columns in a {@link Set}
   * @param tableName        table name.
   * @return select query String.
   */
  def constructSelectQuery(structType: StructType, partitionColumns: Set[String], tableName: String): String = {
    val fieldNames: Array[String] = structType.fields.map(field => field.name)
    val reorderedFieldNames = fieldNames.filter(name => !partitionColumns.contains(name)) ++
      fieldNames.filter(name => partitionColumns.contains(name))
    constructSelectQuery(reorderedFieldNames, tableName)
  }

  /**
   * Constructs the select query with a {@link Array} of String.
   *
   * @param fieldNames field names in String.
   * @param tableName  table name.
   * @return select query String.
   */
  def constructSelectQuery(fieldNames: Array[String], tableName: String): String = {
    val selectQueryBuilder = new StringBuilder("select ");
    selectQueryBuilder.append(fieldNames.mkString(", "))
    selectQueryBuilder.append(" from ")
    selectQueryBuilder.append(tableName)
    selectQueryBuilder.toString()
  }

  /**
   * Constructs the Spark SQL create table query based on the configs.
   *
   * @param config          DAG node configurations.
   * @param targetTableName target table name.
   * @param targetBasePath  target bash path for external table.
   * @param inputSchema     input Avro schema String.
   * @param inputTableName  name of the table containing input data.
   * @return create table query.
   */
  def constructCreateTableQuery(config: Config, targetTableName: String, targetBasePath: String,
                                inputSchema: String, inputTableName: String): String = {
    // Constructs create table statement
    val createTableQueryBuilder = new StringBuilder("create table ")
    createTableQueryBuilder.append(targetTableName)
    val partitionColumns: Set[String] =
      if (config.getPartitionField.isPresent) Set(config.getPartitionField.get) else Set.empty
    if (!config.shouldUseCtas) {
      // Adds the schema statement if not using CTAS
      createTableQueryBuilder.append(" (")
      createTableQueryBuilder.append(SparkSqlUtils.convertAvroToSqlSchemaExpression(inputSchema, partitionColumns))
      createTableQueryBuilder.append("\n)")
    }
    createTableQueryBuilder.append(" using hudi")
    val tableTypeOption = config.getTableType
    val primaryKeyOption = config.getPrimaryKey
    val preCombineFieldOption = config.getPreCombineField

    // Adds location for external table
    if (config.isTableExternal) {
      createTableQueryBuilder.append("\nlocation '" + targetBasePath + "'")
    }

    // Adds options if set
    var options = Array[String]()
    if (tableTypeOption.isPresent) {
      options :+= ("type = '" + tableTypeOption.get() + "'")
    }
    if (primaryKeyOption.isPresent) {
      options :+= ("primaryKey = '" + primaryKeyOption.get() + "'")
    }
    if (preCombineFieldOption.isPresent) {
      options :+= ("preCombineField = '" + preCombineFieldOption.get() + "'")
    }
    if (options.length > 0) {
      createTableQueryBuilder.append(options.mkString("\noptions ( \n", ",\n", "\n)"))
    }

    // Adds partition fields if set
    val partitionFieldOption = config.getPartitionField
    if (partitionFieldOption.isPresent) {
      createTableQueryBuilder.append("\npartitioned by (" + partitionFieldOption.get() + ")")
    }

    if (config.shouldUseCtas()) {
      // Adds as select query
      createTableQueryBuilder.append("\nas\n");
      createTableQueryBuilder.append(constructSelectQuery(inputSchema, partitionColumns, inputTableName))
    }
    createTableQueryBuilder.toString()
  }

  /**
   * Constructs the Spark SQL insert query based on the configs.
   *
   * @param insertType      the insert type, in one of two types: "into" or "overwrite".
   * @param targetTableName target table name.
   * @param schema          table schema to use
   * @param inputTableName  name of the table containing input data.
   * @return insert query.
   */
  def constructInsertQuery(insertType: String, targetTableName: String, schema: StructType,
                           inputTableName: String): String = {
    // Constructs insert statement
    val insertQueryBuilder = new StringBuilder("insert ")
    insertQueryBuilder.append(insertType)
    insertQueryBuilder.append(" ")
    insertQueryBuilder.append(targetTableName)
    insertQueryBuilder.append(" ")
    insertQueryBuilder.append(constructSelectQuery(schema, inputTableName))
    insertQueryBuilder.toString()
  }

  /**
   * Constructs the Spark SQL merge query based on the configs.
   *
   * @param config          DAG node configurations.
   * @param targetTableName target table name.
   * @param schema          table schema to use
   * @param inputTableName  name of the table containing input data.
   * @return merge query.
   */
  def constructMergeQuery(config: Config, targetTableName: String, schema: StructType,
                          inputTableName: String): String = {
    val mergeQueryBuilder = new StringBuilder("merge into ")
    mergeQueryBuilder.append(targetTableName)
    mergeQueryBuilder.append(" as target using (\n")
    mergeQueryBuilder.append(constructSelectQuery(schema, inputTableName))
    mergeQueryBuilder.append("\n) source\non ")
    mergeQueryBuilder.append(config.getMergeCondition)
    mergeQueryBuilder.append("\nwhen matched then ")
    mergeQueryBuilder.append(config.getMatchedAction)
    mergeQueryBuilder.append("\nwhen not matched then ")
    mergeQueryBuilder.append(config.getNotMatchedAction)
    mergeQueryBuilder.toString()
  }

  /**
   * Constructs the Spark SQL update query based on the configs.
   *
   * @param config          DAG node configurations.
   * @param sparkSession    Spark session.
   * @param targetTableName target table name.
   * @return update query.
   */
  def constructUpdateQuery(config: Config, sparkSession: SparkSession,
                           targetTableName: String): String = {
    val bounds = getLowerUpperBoundsFromPercentiles(config, sparkSession, targetTableName)
    val updateQueryBuilder = new StringBuilder("update ")
    updateQueryBuilder.append(targetTableName)
    updateQueryBuilder.append(" set ")
    updateQueryBuilder.append(config.getUpdateColumn)
    updateQueryBuilder.append(" = ")
    updateQueryBuilder.append(config.getUpdateColumn)
    updateQueryBuilder.append(" * 1.6 ")
    updateQueryBuilder.append(" where ")
    updateQueryBuilder.append(config.getWhereConditionColumn)
    updateQueryBuilder.append(" between ")
    updateQueryBuilder.append(bounds._1)
    updateQueryBuilder.append(" and ")
    updateQueryBuilder.append(bounds._2)
    updateQueryBuilder.toString()
  }

  /**
   * Constructs the Spark SQL delete query based on the configs.
   *
   * @param config          DAG node configurations.
   * @param sparkSession    Spark session.
   * @param targetTableName target table name.
   * @return delete query.
   */
  def constructDeleteQuery(config: Config, sparkSession: SparkSession,
                           targetTableName: String): String = {
    val bounds = getLowerUpperBoundsFromPercentiles(config, sparkSession, targetTableName)
    val deleteQueryBuilder = new StringBuilder("delete from ")
    deleteQueryBuilder.append(targetTableName)
    deleteQueryBuilder.append(" where ")
    deleteQueryBuilder.append(config.getWhereConditionColumn)
    deleteQueryBuilder.append(" between ")
    deleteQueryBuilder.append(bounds._1)
    deleteQueryBuilder.append(" and ")
    deleteQueryBuilder.append(bounds._2)
    deleteQueryBuilder.toString()
  }

  /**
   * Generates the pair of percentile levels based on the ratio in the config.
   *
   * For example, given ratio as 0.4, the output is (0.3, 0.7).
   *
   * @param config DAG node configurations.
   * @return the lower bound and upper bound percentiles.
   */
  def generatePercentiles(config: Config): (Double, Double) = {
    val ratio: Double = config.getRatioRecordsChange
    (Math.max(0.5 - (ratio / 2.0), 0.0), Math.min(0.5 + (ratio / 2.0), 1.0))
  }

  /**
   * @param number input double number
   * @param mode   rounding mode
   * @return rounded double
   */
  def roundDouble(number: Double, mode: RoundingMode): Double = {
    BigDecimal(number).setScale(4, mode).toDouble
  }

  /**
   * @param config          DAG node configurations.
   * @param sparkSession    Spark session.
   * @param targetTableName target table name.
   * @return lower and upper bound values based on the percentiles.
   */
  def getLowerUpperBoundsFromPercentiles(config: Config, sparkSession: SparkSession,
                                         targetTableName: String): (Double, Double) = {
    val percentiles = generatePercentiles(config)
    val result = sparkSession.sql(constructPercentileQuery(config, targetTableName, percentiles)).collect()(0)
    (roundDouble(result.get(0).asInstanceOf[Double], BigDecimal.RoundingMode.HALF_DOWN),
      roundDouble(result.get(1).asInstanceOf[Double], BigDecimal.RoundingMode.HALF_UP))
  }

  /**
   * Constructs the query to get percentiles for the where condition.
   *
   * @param config          DAG node configurations.
   * @param targetTableName target table name.
   * @param percentiles     lower and upper percentiles.
   * @return percentile query in String.
   */
  def constructPercentileQuery(config: Config, targetTableName: String,
                               percentiles: (Double, Double)): String = {
    val percentileQueryBuilder = new StringBuilder("select percentile(")
    percentileQueryBuilder.append(config.getWhereConditionColumn)
    percentileQueryBuilder.append(", ")
    percentileQueryBuilder.append(percentiles._1)
    percentileQueryBuilder.append("), percentile(")
    percentileQueryBuilder.append(config.getWhereConditionColumn)
    percentileQueryBuilder.append(", ")
    percentileQueryBuilder.append(percentiles._2)
    percentileQueryBuilder.append(") from ")
    percentileQueryBuilder.append(targetTableName)
    percentileQueryBuilder.toString()
  }

  /**
   * Constructs the Spark SQL query to get update or delete records.
   *
   * @param config           DAG node configurations.
   * @param targetTableName  target table name.
   * @param avroSchemaString input Avro schema String.
   * @param lowerBound       lower bound value for the where condition.
   * @param upperBound       upper bound value for the where condition.
   * @return delete query.
   */
  def constructChangedRecordQuery(config: Config, targetTableName: String, avroSchemaString: String,
                                  lowerBound: Double, upperBound: Double): String = {
    val recordQueryBuilder = new StringBuilder(constructSelectQuery(avroSchemaString, Set.empty[String], targetTableName))
    recordQueryBuilder.append(" where ")
    recordQueryBuilder.append(config.getWhereConditionColumn)
    recordQueryBuilder.append(" between ")
    recordQueryBuilder.append(lowerBound)
    recordQueryBuilder.append(" and ")
    recordQueryBuilder.append(upperBound)
    recordQueryBuilder.toString()
  }

  /**
   * Generates the exact same records to update based on the SQL derived from the
   * configs for data validation.
   *
   * @param config           DAG node configurations.
   * @param sparkSession     Spark session.
   * @param avroSchemaString input Avro schema String.
   * @param targetTableName  target table name.
   * @param parallelism      parallelism for RDD
   * @return records in {@link JavaRdd[ GenericRecord ]}.
   */
  def generateUpdateRecords(config: Config, sparkSession: SparkSession, avroSchemaString: String,
                            targetTableName: String, parallelism: Int): JavaRDD[GenericRecord] = {
    val bounds = getLowerUpperBoundsFromPercentiles(config, sparkSession, targetTableName)
    val rows = sparkSession.sql(
      constructChangedRecordQuery(config, targetTableName, avroSchemaString, bounds._1, bounds._2))

    val rdd = HoodieSparkUtils
      .createRdd(rows, RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME,
        RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE, reconcileToLatestSchema = false, Option.empty())
      .map(record => {
        record.put(config.getUpdateColumn, record.get(config.getUpdateColumn).toString.toDouble * 1.6)
        record
      })
      .toJavaRDD()
    val repartitionedRdd = rdd.repartition(parallelism)
    repartitionedRdd.persist(StorageLevel.DISK_ONLY)
    repartitionedRdd
  }

  /**
   * Generates the exact same records to delete based on the SQL derived from the
   * configs for data validation.
   *
   * @param config           DAG node configurations.
   * @param sparkSession     Spark session.
   * @param avroSchemaString input Avro schema String.
   * @param targetTableName  target table name.
   * @param parallelism      parallelism for RDD
   * @return records in {@link JavaRdd[ GenericRecord ]}.
   */
  def generateDeleteRecords(config: Config, sparkSession: SparkSession, avroSchemaString: String,
                            targetTableName: String, parallelism: Int): JavaRDD[GenericRecord] = {
    val bounds = getLowerUpperBoundsFromPercentiles(config, sparkSession, targetTableName)
    val rows = sparkSession.sql(
      constructChangedRecordQuery(config, targetTableName, avroSchemaString, bounds._1, bounds._2))

    val rdd = HoodieSparkUtils
      .createRdd(rows, RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME,
        RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE, reconcileToLatestSchema = false, Option.empty())
      .map(record => {
        record.put(GenericRecordFullPayloadGenerator.DEFAULT_HOODIE_IS_DELETED_COL, true)
        record
      })
      .toJavaRDD()
    val repartitionedRdd = rdd.repartition(parallelism)
    repartitionedRdd.persist(StorageLevel.DISK_ONLY)
    repartitionedRdd
  }
}
