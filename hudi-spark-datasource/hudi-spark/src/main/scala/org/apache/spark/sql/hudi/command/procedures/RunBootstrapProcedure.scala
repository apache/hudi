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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.{DataSourceWriteOptions, HoodieCLIUtils}
import org.apache.hudi.cli.BootstrapExecutorUtils
import org.apache.hudi.cli.HDFSParquetImporterUtils.{buildProperties, readConfig}
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.config.{HoodieBootstrapConfig, HoodieWriteConfig}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.storage.StoragePath

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.Locale
import java.util.function.Supplier

import scala.collection.JavaConverters._
class RunBootstrapProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "table_type", DataTypes.StringType),
    ProcedureParameter.required(2, "bootstrap_path", DataTypes.StringType),
    ProcedureParameter.required(3, "base_path", DataTypes.StringType),
    ProcedureParameter.required(4, "rowKey_field", DataTypes.StringType),
    ProcedureParameter.optional(5, "base_file_format", DataTypes.StringType, "PARQUET"),
    ProcedureParameter.optional(6, "partition_path_field", DataTypes.StringType, ""),
    ProcedureParameter.optional(7, "bootstrap_index_class", DataTypes.StringType, "org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex"),
    ProcedureParameter.optional(8, "selector_class", DataTypes.StringType, "org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector"),
    ProcedureParameter.optional(9, "key_generator_class", DataTypes.StringType, "org.apache.hudi.keygen.SimpleKeyGenerator"),
    ProcedureParameter.optional(10, "full_bootstrap_input_provider", DataTypes.StringType, "org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider"),
    ProcedureParameter.optional(11, "schema_provider_class", DataTypes.StringType, ""),
    ProcedureParameter.optional(12, "payload_class", DataTypes.StringType, "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"),
    ProcedureParameter.optional(13, "parallelism", DataTypes.IntegerType, 1500),
    ProcedureParameter.optional(14, "enable_hive_sync", DataTypes.BooleanType, false),
    ProcedureParameter.optional(15, "props_file_path", DataTypes.StringType, ""),
    ProcedureParameter.optional(16, "bootstrap_overwrite", DataTypes.BooleanType, false),
    // params => key=value, key2=value2
    ProcedureParameter.optional(17, "options", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("status", DataTypes.IntegerType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0))
    val tableType = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val bootstrapPath = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]
    val basePath = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[String]
    val rowKeyField = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]
    val baseFileFormat = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[String]
    val partitionPathField = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[String]
    val bootstrapIndexClass = getArgValueOrDefault(args, PARAMETERS(7)).get.asInstanceOf[String]
    val selectorClass = getArgValueOrDefault(args, PARAMETERS(8)).get.asInstanceOf[String]
    val keyGeneratorClass = getArgValueOrDefault(args, PARAMETERS(9)).get.asInstanceOf[String]
    val fullBootstrapInputProvider = getArgValueOrDefault(args, PARAMETERS(10)).get.asInstanceOf[String]
    val schemaProviderClass = getArgValueOrDefault(args, PARAMETERS(11)).get.asInstanceOf[String]
    val payloadClass = getArgValueOrDefault(args, PARAMETERS(12)).get.asInstanceOf[String]
    val parallelism = getArgValueOrDefault(args, PARAMETERS(13)).get.asInstanceOf[Int]
    val enableHiveSync = getArgValueOrDefault(args, PARAMETERS(14)).get.asInstanceOf[Boolean]
    val propsFilePath = getArgValueOrDefault(args, PARAMETERS(15)).get.asInstanceOf[String]
    val bootstrapOverwrite = getArgValueOrDefault(args, PARAMETERS(16)).get.asInstanceOf[Boolean]
    val options = getArgValueOrDefault(args, PARAMETERS(17))

    val (tableName, database) = HoodieCLIUtils.getTableIdentifier(table.get.asInstanceOf[String])
    val configs: util.List[String] = new util.ArrayList[String]

    val properties: TypedProperties = if (propsFilePath == null || propsFilePath.isEmpty) buildProperties(configs)
    else readConfig(jsc.hadoopConfiguration, new StoragePath(propsFilePath), configs).getProps(true)

    properties.setProperty(HoodieBootstrapConfig.BASE_PATH.key, bootstrapPath)

    if (bootstrapPath.equals(basePath)) {
      throw new IllegalArgumentException("bootstrap_path and base_path must be different")
    }

    if (!StringUtils.isNullOrEmpty(keyGeneratorClass) && KeyGeneratorType.getNames.contains(keyGeneratorClass.toUpperCase(Locale.ROOT))) {
      properties.setProperty(HoodieWriteConfig.KEYGENERATOR_TYPE.key, keyGeneratorClass.toUpperCase(Locale.ROOT))
    }
    else {
      properties.setProperty(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key, keyGeneratorClass)
    }

    properties.setProperty(HoodieBootstrapConfig.FULL_BOOTSTRAP_INPUT_PROVIDER_CLASS_NAME.key, fullBootstrapInputProvider)
    properties.setProperty(HoodieBootstrapConfig.PARALLELISM_VALUE.key, parallelism.toString)
    properties.setProperty(HoodieBootstrapConfig.MODE_SELECTOR_CLASS_NAME.key, selectorClass)
    properties.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, rowKeyField)
    properties.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, partitionPathField)

    val fs = HadoopFSUtils.getFs(basePath, jsc.hadoopConfiguration)

    val cfg = new BootstrapExecutorUtils.Config()
    cfg.setTableName(tableName)
    cfg.setDatabase(database.getOrElse(sparkSession.sessionState.catalog.getCurrentDatabase))
    cfg.setTableType(tableType)
    cfg.setBasePath(basePath)
    cfg.setBaseFileFormat(baseFileFormat)
    cfg.setBootstrapIndexClass(bootstrapIndexClass)
    cfg.setSchemaProviderClass(schemaProviderClass)
    cfg.setPayloadClass(payloadClass)
    cfg.setEnableHiveSync(enableHiveSync)
    cfg.setBootstrapOverwrite(bootstrapOverwrite)

    // add session bootstrap conf
    TypedProperties.putAll(properties, spark.sqlContext.conf.getAllConfs.asJava)

    // add conf from procedure, may overwrite session conf
    options match {
      case Some(p) =>
        val paramPairs = HoodieCLIUtils.extractOptions(p.asInstanceOf[String])
        paramPairs.foreach { pair =>
          properties.setProperty(pair._1, pair._2)
        }
      case _ =>
        logInfo("No options")
    }

    new BootstrapExecutorUtils(cfg, jsc, fs, jsc.hadoopConfiguration, properties).execute()
    Seq(Row(0))
  }

  override def build = new RunBootstrapProcedure()
}

object RunBootstrapProcedure {
  val NAME = "run_bootstrap"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new RunBootstrapProcedure
  }
}
