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

package org.apache.spark.sql.hudi

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.connector.expressions.{BucketTransform, FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{SparkSession, types}

import java.net.URI
import java.util.Locale
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

object SparkSqlUtils {

  private lazy val metaFields = HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet

  def isMetaField(name: String): Boolean = {
    metaFields.contains(name)
  }

  def removeMetaFields(attrs: Seq[Attribute]): Seq[Attribute] = {
    attrs.filterNot(attr => isMetaField(attr.name))
  }

  def addMetaFields(attrs: Seq[Attribute]): Seq[Attribute] = {
    val metaFields = HoodieRecord.HOODIE_META_COLUMNS.asScala
    // filter the meta field to avoid duplicate field.
    val dataFields = attrs.filterNot(f => metaFields.contains(f.name))
    metaFields.map(AttributeReference(_, StringType)()) ++ dataFields
  }

  def convertTransforms(partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
    val identityCols = new mutable.ArrayBuffer[String]
    var bucketSpec = Option.empty[BucketSpec]

    partitions.map {
      case IdentityTransform(FieldReference(Seq(col))) =>
        identityCols += col


      case BucketTransform(numBuckets, FieldReference(Seq(col))) =>
        bucketSpec = Some(BucketSpec(numBuckets, col :: Nil, Nil))

      case transform =>
        throw new HoodieException(s"Partitioning by expressions")
    }

    (identityCols, bucketSpec)
  }

  def isHoodieTable(properties: Map[String, String]): Boolean = {
    properties.getOrElse("provider", "").toLowerCase(Locale.ROOT) == "hudi"
  }

  def getTableLocation(properties: Map[String, String], identifier: TableIdentifier, sparkSession: SparkSession): String = {
    val location: Option[String] = Some(properties.getOrElse("location", ""))
    val isManaged = location.isEmpty || location.get.isEmpty
    val uri = if (isManaged) {
      Some(sparkSession.sessionState.catalog.defaultTablePath(identifier))
    } else {
      Some(new Path(location.get).toUri)
    }
    val conf = sparkSession.sessionState.newHadoopConf()
    uri.map(makePathQualified(_, conf))
      .map(removePlaceHolder)
      .getOrElse(throw new IllegalArgumentException(s"Missing location for $identifier"))
  }

  def makePathQualified(path: URI, hadoopConf: Configuration): String = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(hadoopConf)
    fs.makeQualified(hadoopPath).toUri.toString
  }

  private def removePlaceHolder(path: String): String = {
    if (path == null || path.isEmpty) {
      path
    } else if (path.endsWith("-__PLACEHOLDER__")) {
      path.substring(0, path.length() - 16)
    } else {
      path
    }
  }
}
