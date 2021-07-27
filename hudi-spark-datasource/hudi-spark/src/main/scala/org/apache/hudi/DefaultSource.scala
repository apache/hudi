/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieRecord}
import org.apache.hudi.DataSourceWriteOptions.{BOOTSTRAP_OPERATION_OPT_VAL, OPERATION_OPT_KEY}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.SerializableConfiguration
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieTableType.{COPY_ON_WRITE, MERGE_ON_READ}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.TablePathUtils
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.HoodieROTablePathFilter
import org.apache.hudi.hive.util.ConfigUtils
import org.apache.hudi.metadata.FileSystemBackedTableMetadata
import org.apache.log4j.LogManager
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.execution.datasources.{DataSource, FileStatusCache, HadoopFsRelation}
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.hudi.streaming.HoodieStreamSource
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

import scala.collection.JavaConverters._

/**
  * Hoodie Spark Datasource, for reading and writing hoodie tables
  *
  */
class DefaultSource extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider
  with DataSourceRegister
  with StreamSinkProvider
  with StreamSourceProvider
  with Serializable {

  SparkSession.getActiveSession.foreach { spark =>
    val sparkVersion = spark.version
    if (sparkVersion.startsWith("0.") || sparkVersion.startsWith("1.") || sparkVersion.startsWith("2.")) {
      // Enable "passPartitionByAsOptions" to support "write.partitionBy(...)"
      spark.conf.set("spark.sql.legacy.sources.write.passPartitionByAsOptions", "true")
    }
  }

  private val log = LogManager.getLogger(classOf[DefaultSource])

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext,
                              optParams: Map[String, String],
                              schema: StructType): BaseRelation = {
    // Add default options for unspecified read options keys.
    val originalParameters = DataSourceOptionsHelper.translateConfigurations(optParams)

    val (path, parameters) = parsePath(sqlContext, originalParameters)
    val readPathsStr = parameters.get(DataSourceReadOptions.READ_PATHS_OPT_KEY.key)
    if (path.isEmpty && readPathsStr.isEmpty) {
      throw new HoodieException(s"'path' or '$READ_PATHS_OPT_KEY' or both must be specified.")
    }

    val readPaths = readPathsStr.map(p => p.split(",").toSeq).getOrElse(Seq())
    val allPaths = path.map(p => Seq(p)).getOrElse(Seq()) ++ readPaths

    val fs = FSUtils.getFs(allPaths.head, sqlContext.sparkContext.hadoopConfiguration)
    // Use the HoodieFileIndex only if the 'path' is not globbed.
    // Or else we use the original way to read hoodie table.
    val enableFileIndex = optParams.get(ENABLE_HOODIE_FILE_INDEX.key)
      .map(_.toBoolean).getOrElse(ENABLE_HOODIE_FILE_INDEX.defaultValue)

    val useHoodieFileIndex =
    enableFileIndex && path.isDefined &&
       !path.get.contains("*") &&
      !parameters.contains(DataSourceReadOptions.READ_PATHS_OPT_KEY.key)
    val globPaths = if (useHoodieFileIndex) {
      None
    } else {
      Some(HoodieSparkUtils.checkAndGlobPathIfNecessary(allPaths, fs))
    }
    // Get the table base path
    val tablePath = if (globPaths.isDefined) {
      DataSourceUtils.getTablePath(fs, globPaths.get.toArray)
    } else {
      DataSourceUtils.getTablePath(fs, Array(new Path(path.get)))
    }

    log.info("Obtained hudi table path: " + tablePath)

    val metaClient = HoodieTableMetaClient.builder().setConf(fs.getConf).setBasePath(tablePath).build()
    val isBootstrappedTable = metaClient.getTableConfig.getBootstrapBasePath.isPresent
    val tableType = metaClient.getTableType

    // First check if the ConfigUtils.IS_QUERY_AS_RO_TABLE has set by HiveSyncTool,
    // or else use query type from QUERY_TYPE_OPT_KEY.
    val queryType = parameters.get(ConfigUtils.IS_QUERY_AS_RO_TABLE)
      .map(is => if (is.toBoolean) QUERY_TYPE_READ_OPTIMIZED_OPT_VAL else QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .getOrElse(parameters.getOrElse(QUERY_TYPE_OPT_KEY.key, QUERY_TYPE_OPT_KEY.defaultValue()))

    log.info(s"Is bootstrapped table => $isBootstrappedTable, tableType is: $tableType, queryType is: $queryType")

    (tableType, queryType, isBootstrappedTable) match {
      case (COPY_ON_WRITE, QUERY_TYPE_SNAPSHOT_OPT_VAL, false) |
           (COPY_ON_WRITE, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, false) |
           (MERGE_ON_READ, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, false) =>
        getBaseFileOnlyView(useHoodieFileIndex, sqlContext, parameters, schema, tablePath,
          readPaths, metaClient)

      case (COPY_ON_WRITE, QUERY_TYPE_INCREMENTAL_OPT_VAL, _) =>
        new IncrementalRelation(sqlContext, parameters, schema, metaClient)

      case (MERGE_ON_READ, QUERY_TYPE_SNAPSHOT_OPT_VAL, false) =>
        new MergeOnReadSnapshotRelation(sqlContext, parameters, schema, globPaths, metaClient)

      case (MERGE_ON_READ, QUERY_TYPE_INCREMENTAL_OPT_VAL, _) =>
        new MergeOnReadIncrementalRelation(sqlContext, parameters, schema, metaClient)

      case (_, _, true) =>
        new HoodieBootstrapRelation(sqlContext, schema, globPaths, metaClient, parameters)

      case (_, _, _) =>
        throw new HoodieException(s"Invalid query type : $queryType for tableType: $tableType," +
          s"isBootstrappedTable: $isBootstrappedTable ")
    }
  }

  /**
    * This DataSource API is used for writing the DataFrame at the destination. For now, we are returning a dummy
    * relation here because Spark does not really make use of the relation returned, and just returns an empty
    * dataset at [[org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand.run()]]. This saves us the cost
    * of creating and returning a parquet relation here.
    *
    * TODO: Revisit to return a concrete relation here when we support CREATE TABLE AS for Hudi with DataSource API.
    *       That is the only case where Spark seems to actually need a relation to be returned here
    *       [[DataSource.writeAndRead()]]
    *
    * @param sqlContext Spark SQL Context
    * @param mode Mode for saving the DataFrame at the destination
    * @param optParams Parameters passed as part of the DataFrame write operation
    * @param df Spark DataFrame to be written
    * @return Spark Relation
    */
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              optParams: Map[String, String],
                              df: DataFrame): BaseRelation = {
    val parameters = HoodieWriterUtils.parametersWithWriteDefaults(optParams)
    val translatedOptions = DataSourceWriteOptions.translateSqlOptions(parameters)
    val dfWithoutMetaCols = df.drop(HoodieRecord.HOODIE_META_COLUMNS.asScala:_*)

    if (translatedOptions(OPERATION_OPT_KEY.key).equals(BOOTSTRAP_OPERATION_OPT_VAL)) {
      HoodieSparkSqlWriter.bootstrap(sqlContext, mode, translatedOptions, dfWithoutMetaCols)
    } else {
      HoodieSparkSqlWriter.write(sqlContext, mode, translatedOptions, dfWithoutMetaCols)
    }
    new HoodieEmptyRelation(sqlContext, dfWithoutMetaCols.schema)
  }

  override def createSink(sqlContext: SQLContext,
                          optParams: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    val parameters = HoodieWriterUtils.parametersWithWriteDefaults(optParams)
    val translatedOptions = DataSourceWriteOptions.translateSqlOptions(parameters)
    new HoodieStreamingSink(
      sqlContext,
      translatedOptions,
      partitionColumns,
      outputMode)
  }

  override def shortName(): String = "hudi"

  private def getBaseFileOnlyView(useHoodieFileIndex: Boolean,
                                  sqlContext: SQLContext,
                                  optParams: Map[String, String],
                                  schema: StructType,
                                  tablePath: String,
                                  extraReadPaths: Seq[String],
                                  metaClient: HoodieTableMetaClient): BaseRelation = {
    log.info("Loading Base File Only View  with options :" + optParams)
    val (tableFileFormat, formatClassName) = metaClient.getTableConfig.getBaseFileFormat match {
      case HoodieFileFormat.PARQUET => (new ParquetFileFormat, "parquet")
      case HoodieFileFormat.ORC => (new OrcFileFormat, "orc")
    }

    if (useHoodieFileIndex) {

      val fileIndex = HoodieFileIndex(sqlContext.sparkSession, metaClient,
        if (schema == null) Option.empty[StructType] else Some(schema),
        optParams, FileStatusCache.getOrCreate(sqlContext.sparkSession))

      HadoopFsRelation(
        fileIndex,
        fileIndex.partitionSchema,
        fileIndex.dataSchema,
        bucketSpec = None,
        fileFormat = tableFileFormat,
        optParams)(sqlContext.sparkSession)
    } else {
      // this is just effectively RO view only, where `path` can contain a mix of
      // non-hoodie/hoodie path files. set the path filter up
      sqlContext.sparkContext.hadoopConfiguration.setClass(
        "mapreduce.input.pathFilter.class",
        classOf[HoodieROTablePathFilter],
        classOf[org.apache.hadoop.fs.PathFilter])

      val specifySchema = if (schema == null) {
        // Load the schema from the commit meta data.
        // Here we should specify the schema to the latest commit schema since
        // the table schema evolution.
        val tableSchemaResolver = new TableSchemaResolver(metaClient)
        try {
          Some(SchemaConverters.toSqlType(tableSchemaResolver.getTableAvroSchema)
            .dataType.asInstanceOf[StructType])
        } catch {
          case _: Throwable =>
            None // If there is no commit in the table, we can not get the schema
                 // with tableSchemaResolver, return None here.
        }
      } else {
        Some(schema)
      }
      // simply return as a regular relation
      DataSource.apply(
        sparkSession = sqlContext.sparkSession,
        paths = extraReadPaths,
        userSpecifiedSchema = specifySchema,
        className = formatClassName,
        options = optParams)
        .resolveRelation()
    }
  }

  private def parsePath(sqlContext: SQLContext,
                      originalParameters: Map[String, String]
                     ): (Option[String], Map[String, String]) = {
    val originalPath = originalParameters.get("path")
    // Check if the request is to read all partitions from the table
    // in the case that the user has not used the blob
    // This case occurs when the requested Table Path is the base table path
    // and does not contain the /*/*... blob
    // The actual partition path is automatically inferred
    if (originalPath.nonEmpty && !originalPath.get.contains("*")) {
      val strippedPath = originalPath.get.stripSuffix("/")
      val fs = FSUtils.getFs(strippedPath, sqlContext.sparkContext.hadoopConfiguration)
      val qualifiedPath = new Path(strippedPath).makeQualified(fs.getUri, fs.getWorkingDirectory)
      val tablePath = TablePathUtils.getTablePath(fs, new Path(strippedPath)).get().toString

      if (qualifiedPath.toString.equals(tablePath)) {
        val sparkEngineContext = new HoodieSparkEngineContext(sqlContext.sparkContext)
        val fsBackedTableMetadata =
          new FileSystemBackedTableMetadata(sparkEngineContext, new SerializableConfiguration(fs.getConf), tablePath, false)
        val singlePartitionPath = DataSourceUtils.getFullPartitionPath(tablePath, fsBackedTableMetadata.getAllPartitionPaths)

        val pathFromTable = DataSourceUtils.getDataPath(tablePath, singlePartitionPath)
        val modifiedParameters = originalParameters + ("path" -> pathFromTable)
        log.info("Obtained hudi data path: " + originalPath)
        System.out.println("WNI Obtained hudi data path: " + originalPath
          + " \ntablePath " + tablePath
          + " \nparameters: " + modifiedParameters)
        return (Option(pathFromTable), modifiedParameters)
      }
    }
    (originalPath, originalParameters)
  }

  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    val path = parameters.get("path")
    if (path.isEmpty || path.get == null) {
      throw new HoodieException(s"'path'  must be specified.")
    }
    val metaClient = HoodieTableMetaClient.builder().setConf(
      sqlContext.sparkSession.sessionState.newHadoopConf()).setBasePath(path.get).build()
    val schemaResolver = new TableSchemaResolver(metaClient)
    val sqlSchema =
      try {
        val avroSchema = schemaResolver.getTableAvroSchema
        AvroConversionUtils.convertAvroSchemaToStructType(avroSchema)
      } catch {
        case _: Exception =>
          require(schema.isDefined, "Fail to resolve source schema")
          schema.get
      }
    (shortName(), sqlSchema)
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {
    new HoodieStreamSource(sqlContext, metadataPath, schema, parameters)
  }
}
