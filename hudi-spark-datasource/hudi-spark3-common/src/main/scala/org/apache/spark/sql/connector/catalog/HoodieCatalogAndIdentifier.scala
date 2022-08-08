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

package org.apache.spark.sql.connector.catalog

import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

object HoodieCatalogAndIdentifier {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper

  private val globalTempDB = SQLConf.get.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)

  def parse(catalogManager: CatalogManager, nameParts: Seq[String])
      : Option[(CatalogPlugin, Identifier)] = {
    assert(nameParts.nonEmpty)
    val currentCatalog = catalogManager.currentCatalog
    if (nameParts.length == 1) {
      Some((currentCatalog, Identifier.of(catalogManager.currentNamespace, nameParts.head)))
    } else if (nameParts.head.equalsIgnoreCase(globalTempDB)) {
      // Conceptually global temp views are in a special reserved catalog. However, the v2 catalog
      // API does not support view yet, and we have to use v1 commands to deal with global temp
      // views. To simplify the implementation, we put global temp views in a special namespace
      // in the session catalog. The special namespace has higher priority during name resolution.
      // For example, if the name of a custom catalog is the same with `GLOBAL_TEMP_DATABASE`,
      // this custom catalog can't be accessed.
      Some((catalogManager.v2SessionCatalog, nameParts.asIdentifier))
    } else {
      try {
        Some((catalogManager.catalog(nameParts.head), nameParts.tail.asIdentifier))
      } catch {
        case _: CatalogNotFoundException =>
          Some((currentCatalog, nameParts.asIdentifier))
      }
    }
  }
}
