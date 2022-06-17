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

package org.apache.spark.sql.hudi.command

import org.apache.hudi.common.index.HoodieIndex
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.{Row, SparkSession}

case class CreateIndexCommand(
    tableId: TableIdentifier,
    indexName: String,
    indexType: String,
    ignoreIfExists: Boolean,
    columns: Seq[(Attribute, Map[String, String])],
    properties: Map[String, String],
    override val output: Seq[Attribute]) extends IndexBaseCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // The implementation for different index type
    Seq.empty
  }
}

case class DropIndexCommand(
    tableId: TableIdentifier,
    indexName: String,
    ignoreIfNotExists: Boolean,
    override val output: Seq[Attribute]) extends IndexBaseCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // The implementation for different index type
    Seq.empty
  }
}

case class ShowIndexesCommand(
    tableId: TableIdentifier,
    override val output: Seq[Attribute]) extends IndexBaseCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // The implementation for different index type
    Seq.empty
  }
}

case class RefreshIndexCommand(
    tableId: TableIdentifier,
    indexName: String,
    override val output: Seq[Attribute]) extends IndexBaseCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // The implementation for different index type
    Seq.empty
  }
}

abstract class IndexBaseCommand extends HoodieLeafRunnableCommand with Logging {

  /**
   * Check hoodie index exists. In a hoodie table, hoodie index name
   * must be unique, so the index name will be checked firstly,
   *
   * @param secondaryIndexes Current hoodie indexes
   * @param indexName        The index name to be checked
   * @param colNames         The column names to be checked
   * @return true if the index exists
   */
  def indexExists(
      secondaryIndexes: Option[Array[HoodieIndex]],
      indexName: String,
      indexType: Option[String] = None,
      colNames: Option[Array[String]] = None): Boolean = {
    secondaryIndexes.exists(i => {
      i.exists(_.getIndexName.equals(indexName)) ||
          // Index type and column name need to be checked if present
          indexType.exists(t =>
            colNames.exists(c =>
              i.exists(index =>
                index.getIndexType.name().equalsIgnoreCase(t) && index.getColNames.sameElements(c))))
    })
  }
}
