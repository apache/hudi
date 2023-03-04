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

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.hudi.avro.AvroSchemaUtils.{isCompatibleProjectionOf, isSchemaCompatible}
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.config.HoodieCommonConfig
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.internal.schema.utils.AvroSchemaEvolutionUtils

import scala.collection.JavaConversions.asScalaBuffer

object SchemaReconcileUtils {

  def reconcileSchema(sourceSchema: Schema, canonicalizedSourceSchema: Schema, internalSchema: InternalSchema, tableSchemaName: String) : Schema = {
    val mergedInternalSchema = AvroSchemaEvolutionUtils.reconcileSchema(canonicalizedSourceSchema, internalSchema)
    val evolvedSchema = AvroInternalSchemaConverter.convert(mergedInternalSchema, tableSchemaName)
    val shouldRemoveMetaDataFromInternalSchema = sourceSchema.getFields().filter(f => f.name().equalsIgnoreCase(HoodieRecord.RECORD_KEY_METADATA_FIELD)).isEmpty
    if (shouldRemoveMetaDataFromInternalSchema) HoodieAvroUtils.removeMetadataFields(evolvedSchema) else evolvedSchema
  }

  def reconcileSchemas(tableSchema: Schema, newSchema: Schema, reconcileStrategy: String): (Schema, Boolean) = {
    if (reconcileStrategy.equalsIgnoreCase(HoodieCommonConfig.LEGACY_RECONCILE_STRATEGY)) {
      // Legacy reconciliation implements following semantic
      //    - In case new-schema is a "compatible" projection of the existing table's one (projection allowing
      //      permitted type promotions), table's schema would be picked as (reconciled) writer's schema;
      //    - Otherwise, we'd fall back to picking new (batch's) schema as a writer's schema;
      //
      // Philosophically, such semantic aims at always choosing a "wider" schema, ie the one containing
      // the other one (schema A contains schema B, if schema B is a projection of A). This enables us,
      // to always "extend" the schema during schema evolution and hence never lose the data (when, for ex
      // existing column is being dropped in a new batch)
      //
      // NOTE: By default Hudi doesn't allow automatic schema evolution to drop the columns from the target
      //       table. However, when schema reconciliation is turned on, we would allow columns to be dropped
      //       in the incoming batch (as these would be reconciled in anyway)
      if (isCompatibleProjectionOf(tableSchema, newSchema)) {
        // Picking table schema as a writer schema we need to validate that we'd be able to
        // rewrite incoming batch's data (written in new schema) into it
        (tableSchema, isSchemaCompatible(newSchema, tableSchema, true))
      } else {
        // Picking new schema as a writer schema we need to validate that we'd be able to
        // rewrite table's data into it
        (newSchema, isSchemaCompatible(tableSchema, newSchema, true))
      }
    } else {
      val mergedInternalSchema = AvroSchemaEvolutionUtils.reconcileSchema(newSchema, AvroInternalSchemaConverter.convert(tableSchema))
      val evolvedSchema = AvroInternalSchemaConverter.convert(mergedInternalSchema, tableSchema.getFullName)
      (evolvedSchema, isSchemaCompatible(evolvedSchema, tableSchema, true))
    }
  }
}
