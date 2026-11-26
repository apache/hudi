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

package org.apache.spark.sql.hudi.catalog

import org.apache.hudi.SparkAdapterSupport

import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, DelegatingCatalogExtension, Identifier, Table}
import org.apache.spark.sql.connector.expressions.Transform

import java.util

/**
 * Routes the [[Column]]-based `createTable` overload (Spark 3.4+) to the `StructType`-based one that
 * [[HoodieCatalog]] implements, for Hudi tables.
 *
 * [[DelegatingCatalogExtension]] forwards this overload straight to the session catalog, which registers the
 * table without initializing it as a Hudi table (no `hoodie.properties`). Spark 4.1's
 * `createTable(Identifier, TableInfo)`, used by Spark Declarative Pipelines, ends up in this overload as well.
 */
trait HoodieCatalogColumnsSupport extends DelegatingCatalogExtension with SparkAdapterSupport {

  override def createTable(ident: Identifier,
                           columns: Array[Column],
                           partitions: Array[Transform],
                           properties: util.Map[String, String]): Table = {
    if (sparkAdapter.isHoodieTable(properties)) {
      createTable(ident, CatalogV2Util.v2ColumnsToStructType(columns), partitions, properties)
    } else {
      super.createTable(ident, columns, partitions, properties)
    }
  }
}
