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

import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.types._

import java.util.Locale

/**
 * Rewrites partial 2-field BLOB struct expressions to the canonical 3-field shape
 * before Spark's [[org.apache.spark.sql.catalyst.analysis.TableOutputResolver]] runs,
 * so SQL inserts of `{type, reference}` (OUT_OF_LINE) are not rejected by its positional
 * struct check. Top-level BLOB columns only; nested partial BLOBs in SQL still need the
 * canonical `named_struct` literal.
 */
object InsertBlobCanonicalizer {

  /**
   * Idempotent. For each top-level column whose target is BLOB-tagged and whose source
   * is a partial 2-field accepted layout, projects a canonical 3-field
   * [[CreateNamedStruct]] onto the query.
   */
  def padPartialBlobsAgainstTarget(
      query: LogicalPlan,
      expectedSchema: StructType): LogicalPlan = {
    val sourceAttrs = query.output
    if (sourceAttrs.length != expectedSchema.length) {
      query
    } else {
      val newProjectList: Seq[NamedExpression] = sourceAttrs.zip(expectedSchema.fields).map {
        case (sourceAttr, targetField) =>
          canonicalizeIfPartialBlob(sourceAttr, targetField) match {
            case Some(canonical) =>
              // Use a fresh ExprId: reshaping changes the column's dataType, and reusing
              // sourceAttr.exprId would put two attributes with the same id but different
              // types in the plan tree (the upstream child still emits the partial struct),
              // which Spark's plan validator rejects (PLAN_VALIDATION_FAILED_RULE_IN_BATCH).
              Alias(canonical, sourceAttr.name)(qualifier = sourceAttr.qualifier)
            case None =>
              sourceAttr
          }
      }

      if (newProjectList.zip(sourceAttrs).forall { case (a, b) => a eq b }) query
      else Project(newProjectList, query)
    }
  }

  private def canonicalizeIfPartialBlob(
      sourceAttr: Attribute,
      targetField: StructField): Option[Expression] = {
    if (!isBlobMetadata(targetField.metadata)) {
      None
    } else {
      (sourceAttr.dataType, targetField.dataType) match {
        case (sourceStruct: StructType, targetStruct: StructType)
            if isPartialBlobLayout(sourceStruct) && isCanonicalBlobLayout(targetStruct) =>
          Some(canonicalizeBlobExpr(sourceAttr, sourceStruct, targetStruct))
        case _ => None
      }
    }
  }

  private def isBlobMetadata(metadata: Metadata): Boolean = {
    metadata.contains(HoodieSchema.TYPE_METADATA_FIELD) &&
      HoodieSchema.parseTypeDescriptor(metadata.getString(HoodieSchema.TYPE_METADATA_FIELD))
        .getType == HoodieSchemaType.BLOB
  }

  private def isCanonicalBlobLayout(st: StructType): Boolean = {
    st.length == 3 &&
      st.fields.exists(_.name.equalsIgnoreCase(HoodieSchema.Blob.TYPE)) &&
      st.fields.exists(_.name.equalsIgnoreCase(HoodieSchema.Blob.INLINE_DATA_FIELD)) &&
      st.fields.exists(_.name.equalsIgnoreCase(HoodieSchema.Blob.EXTERNAL_REFERENCE))
  }

  private def isPartialBlobLayout(st: StructType): Boolean = {
    if (st.length != 2) {
      false
    } else {
      val names = st.fields.map(_.name.toLowerCase(Locale.ROOT)).toSet
      val typeKey = HoodieSchema.Blob.TYPE.toLowerCase(Locale.ROOT)
      val dataKey = HoodieSchema.Blob.INLINE_DATA_FIELD.toLowerCase(Locale.ROOT)
      val refKey = HoodieSchema.Blob.EXTERNAL_REFERENCE.toLowerCase(Locale.ROOT)
      names == Set(typeKey, dataKey) || names == Set(typeKey, refKey)
    }
  }

  private def canonicalizeBlobExpr(
      sourceExpr: Expression,
      sourceStruct: StructType,
      targetStruct: StructType): Expression = {
    val nameToOrdinal = sourceStruct.fields.zipWithIndex.map {
      case (f, i) => f.name.toLowerCase(Locale.ROOT) -> i
    }.toMap

    def lookupOrdinal(name: String): Option[Int] =
      nameToOrdinal.get(name.toLowerCase(Locale.ROOT))

    val typeOrd = lookupOrdinal(HoodieSchema.Blob.TYPE).get
    val typeFieldExpr = GetStructField(sourceExpr, typeOrd, Some(HoodieSchema.Blob.TYPE))

    val dataExpr: Expression = lookupOrdinal(HoodieSchema.Blob.INLINE_DATA_FIELD) match {
      case Some(ord) => GetStructField(sourceExpr, ord, Some(HoodieSchema.Blob.INLINE_DATA_FIELD))
      case None => Literal(null, BinaryType)
    }

    val refTargetType = targetStruct.fields
      .find(_.name.equalsIgnoreCase(HoodieSchema.Blob.EXTERNAL_REFERENCE)).get.dataType
    val refExpr: Expression = lookupOrdinal(HoodieSchema.Blob.EXTERNAL_REFERENCE) match {
      case Some(ord) => GetStructField(sourceExpr, ord, Some(HoodieSchema.Blob.EXTERNAL_REFERENCE))
      case None => Literal(null, refTargetType)
    }

    // Preserve null-struct semantics: a null source struct round-trips as null,
    // not as a non-null struct with all-null fields produced by CreateNamedStruct.
    If(IsNull(sourceExpr),
      Literal(null, targetStruct),
      CreateNamedStruct(Seq(
        Literal(HoodieSchema.Blob.TYPE), typeFieldExpr,
        Literal(HoodieSchema.Blob.INLINE_DATA_FIELD), dataExpr,
        Literal(HoodieSchema.Blob.EXTERNAL_REFERENCE), refExpr
      )))
  }
}
