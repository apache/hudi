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

package org.apache.spark.sql.hudi.catalog

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceWriteOptions.RECORDKEY_FIELD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, V1Write, WriteBuilder}
import org.apache.spark.sql.types.StructType

import java.net.URI
import java.util
import scala.jdk.CollectionConverters.{mapAsScalaMapConverter, setAsJavaSetConverter}

case class HoodieStagedTable(ident: Identifier,
                             locUriAndTableType: (URI, CatalogTableType),
                             catalog: HoodieCatalog,
                             override val schema: StructType,
                             partitions: Array[Transform],
                             override val properties: util.Map[String, String],
                             mode: TableCreationMode) extends StagedTable with SupportsWrite {

  private var sourceQuery: Option[DataFrame] = None
  private var writeOptions: Map[String, String] = Map.empty

  override def commitStagedChanges(): Unit = {
    val props = new util.HashMap[String, String]()
    val optionsThroughProperties = properties.asScala.collect {
      case (k, _) if k.startsWith("option.") => k.stripPrefix("option.")
    }.toSet
    val sqlWriteOptions = new util.HashMap[String, String]()
    properties.asScala.foreach { case (k, v) =>
      if (!k.startsWith("option.") && !optionsThroughProperties.contains(k)) {
        props.put(k, v)
      } else if (optionsThroughProperties.contains(k)) {
        sqlWriteOptions.put(k, v)
      }
    }
    if (writeOptions.isEmpty && !sqlWriteOptions.isEmpty) {
      writeOptions = sqlWriteOptions.asScala.toMap
    }
    props.putAll(properties)
    props.put("hoodie.table.name", ident.name())
    props.put(RECORDKEY_FIELD.key, properties.get("primaryKey"))
    catalog.createHoodieTable(
      ident, schema, locUriAndTableType, partitions, props, writeOptions, sourceQuery, mode)
  }

  override def name(): String = ident.name()

  override def abortStagedChanges(): Unit = {
    clearTablePath(locUriAndTableType._1.getPath, catalog.spark.sparkContext.hadoopConfiguration)
  }

  private def clearTablePath(tablePath: String, conf: Configuration): Unit = {
    val path = new Path(tablePath)
    val fs = path.getFileSystem(conf)
    fs.delete(path, true)
  }

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.V1_BATCH_WRITE).asJava

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    writeOptions = info.options.asCaseSensitiveMap().asScala.toMap
    new HoodieV1WriteBuilder
  }

  /*
   * WriteBuilder for creating a Hoodie table.
   */
  private class HoodieV1WriteBuilder extends WriteBuilder {
    override def build(): V1Write = () => {
      (data: DataFrame, overwrite: Boolean) => {
        sourceQuery = Option(data)
      }
    }
  }

}
