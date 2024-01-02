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

package org.apache.spark.sql.execution.datasources

import org.apache.hudi.{HoodieBaseRelation, SparkAdapterSupport}
import org.apache.spark.sql.HoodieSpark3CatalystPlanUtils
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, AttributeSet, Expression, NamedExpression, ProjectionOverSchema}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

/**
 * Prunes unnecessary physical columns given a [[PhysicalOperation]] over a data source relation.
 * By "physical column", we mean a column as defined in the data source format like Parquet format
 * or ORC format. For example, in Spark SQL, a root-level Parquet column corresponds to a SQL
 * column, and a nested Parquet column corresponds to a [[StructField]].
 *
 * NOTE: This class is borrowed from Spark 3.2.1, with modifications adapting it to handle [[HoodieBaseRelation]],
 *       instead of [[HadoopFsRelation]]
 */
class Spark30NestedSchemaPruning extends Rule[LogicalPlan] {
  import org.apache.spark.sql.catalyst.expressions.SchemaPruning._
  override def apply(plan: LogicalPlan): LogicalPlan =
    if (SQLConf.get.nestedSchemaPruningEnabled) {
      apply0(plan)
    } else {
      plan
    }

  private def apply0(plan: LogicalPlan): LogicalPlan =
    plan transformDown {
      case op @ PhysicalOperation(projects, filters,
      // NOTE: This is modified to accommodate for Hudi's custom relations, given that original
      //       [[NestedSchemaPruning]] rule is tightly coupled w/ [[HadoopFsRelation]]
      // TODO generalize to any file-based relation
      l @ LogicalRelation(relation: HoodieBaseRelation, _, _, _))
        if relation.canPruneRelationSchema =>

        prunePhysicalColumns(l.output, projects, filters, relation.dataSchema,
          prunedDataSchema => {
            val prunedRelation =
              relation.updatePrunedDataSchema(prunedSchema = prunedDataSchema)
            buildPrunedRelation(l, prunedRelation)
          }).getOrElse(op)
    }

  /**
   * This method returns optional logical plan. `None` is returned if no nested field is required or
   * all nested fields are required.
   */
  private def prunePhysicalColumns(output: Seq[AttributeReference],
                                   projects: Seq[NamedExpression],
                                   filters: Seq[Expression],
                                   dataSchema: StructType,
                                   outputRelationBuilder: StructType => LogicalRelation): Option[LogicalPlan] = {
    val (normalizedProjects, normalizedFilters) =
      normalizeAttributeRefNames(output, projects, filters)
    val requestedRootFields = identifyRootFields(normalizedProjects, normalizedFilters)

    // If requestedRootFields includes a nested field, continue. Otherwise,
    // return op
    if (requestedRootFields.exists { root: RootField => !root.derivedFromAtt }) {
      val prunedDataSchema = pruneDataSchema(dataSchema, requestedRootFields)

      // If the data schema is different from the pruned data schema, continue. Otherwise,
      // return op. We effect this comparison by counting the number of "leaf" fields in
      // each schemata, assuming the fields in prunedDataSchema are a subset of the fields
      // in dataSchema.
      if (countLeaves(dataSchema) > countLeaves(prunedDataSchema)) {
        val planUtils = SparkAdapterSupport.sparkAdapter.getCatalystPlanUtils.asInstanceOf[HoodieSpark3CatalystPlanUtils]

        val prunedRelation = outputRelationBuilder(prunedDataSchema)
        val projectionOverSchema = planUtils.projectOverSchema(prunedDataSchema, AttributeSet(output))

        Some(buildNewProjection(projects, normalizedProjects, normalizedFilters,
          prunedRelation, projectionOverSchema))
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
   * Normalizes the names of the attribute references in the given projects and filters to reflect
   * the names in the given logical relation. This makes it possible to compare attributes and
   * fields by name. Returns a tuple with the normalized projects and filters, respectively.
   */
  private def normalizeAttributeRefNames(output: Seq[AttributeReference],
                                         projects: Seq[NamedExpression],
                                         filters: Seq[Expression]): (Seq[NamedExpression], Seq[Expression]) = {
    val normalizedAttNameMap = output.map(att => (att.exprId, att.name)).toMap
    val normalizedProjects = projects.map(_.transform {
      case att: AttributeReference if normalizedAttNameMap.contains(att.exprId) =>
        att.withName(normalizedAttNameMap(att.exprId))
    }).map { case expr: NamedExpression => expr }
    val normalizedFilters = filters.map(_.transform {
      case att: AttributeReference if normalizedAttNameMap.contains(att.exprId) =>
        att.withName(normalizedAttNameMap(att.exprId))
    })
    (normalizedProjects, normalizedFilters)
  }

  /**
   * Builds the new output [[Project]] Spark SQL operator that has the `leafNode`.
   */
  private def buildNewProjection(projects: Seq[NamedExpression],
                                 normalizedProjects: Seq[NamedExpression],
                                 filters: Seq[Expression],
                                 prunedRelation: LogicalRelation,
                                 projectionOverSchema: ProjectionOverSchema): Project = {
    // Construct a new target for our projection by rewriting and
    // including the original filters where available
    val projectionChild =
      if (filters.nonEmpty) {
        val projectedFilters = filters.map(_.transformDown {
          case projectionOverSchema(expr) => expr
        })
        val newFilterCondition = projectedFilters.reduce(And)
        Filter(newFilterCondition, prunedRelation)
      } else {
        prunedRelation
      }

    // Construct the new projections of our Project by
    // rewriting the original projections
    val newProjects = normalizedProjects.map(_.transformDown {
      case projectionOverSchema(expr) => expr
    }).map { case expr: NamedExpression => expr }

    if (log.isDebugEnabled) {
      logDebug(s"New projects:\n${newProjects.map(_.treeString).mkString("\n")}")
    }

    Project(restoreOriginalOutputNames(newProjects, projects.map(_.name)), projectionChild)
  }

  /**
   * Builds a pruned logical relation from the output of the output relation and the schema of the
   * pruned base relation.
   */
  private def buildPrunedRelation(outputRelation: LogicalRelation,
                                  prunedBaseRelation: BaseRelation): LogicalRelation = {
    val prunedOutput = getPrunedOutput(outputRelation.output, prunedBaseRelation.schema)
    outputRelation.copy(relation = prunedBaseRelation, output = prunedOutput)
  }

  // Prune the given output to make it consistent with `requiredSchema`.
  private def getPrunedOutput(output: Seq[AttributeReference],
                              requiredSchema: StructType): Seq[AttributeReference] = {
    // We need to replace the expression ids of the pruned relation output attributes
    // with the expression ids of the original relation output attributes so that
    // references to the original relation's output are not broken
    val outputIdMap = output.map(att => (att.name, att.exprId)).toMap
    requiredSchema
      .toAttributes
      .map {
        case att if outputIdMap.contains(att.name) =>
          att.withExprId(outputIdMap(att.name))
        case att => att
      }
  }

  /**
   * Counts the "leaf" fields of the given dataType. Informally, this is the
   * number of fields of non-complex data type in the tree representation of
   * [[DataType]].
   */
  private def countLeaves(dataType: DataType): Int = {
    dataType match {
      case array: ArrayType => countLeaves(array.elementType)
      case map: MapType => countLeaves(map.keyType) + countLeaves(map.valueType)
      case struct: StructType =>
        struct.map(field => countLeaves(field.dataType)).sum
      case _ => 1
    }
  }

  private def restoreOriginalOutputNames(
                                  projectList: Seq[NamedExpression],
                                  originalNames: Seq[String]): Seq[NamedExpression] = {
    projectList.zip(originalNames).map {
      case (attr: Attribute, name) => attr.withName(name)
      case (alias: Alias, name) => if (name == alias.name) {
        alias
      } else {
        AttributeReference(name, alias.dataType, alias.nullable, alias.metadata)(alias.exprId, alias.qualifier)
      }
      case (other, _) => other
    }
  }

  // NOTE: `pruneDataSchema` and `sortLeftFieldsByRight` functions are copied from Spark 3.1.2,
  // as these functions in `SchemaPruning` have bugs in Spark 3.0.2 (see SPARK-35096,
  // https://github.com/apache/spark/commit/2bbe0a4151f2af00f1105489d5757be28ff278d6)
  /**
   * Prunes the nested schema by the requested fields. For example, if the schema is:
   * `id int, s struct<a:int, b:int>`, and given requested field "s.a", the inner field "b"
   * is pruned in the returned schema: `id int, s struct<a:int>`.
   * Note that:
   *   1. The schema field ordering at original schema is still preserved in pruned schema.
   *   2. The top-level fields are not pruned here.
   */
  private def pruneDataSchema(
                       dataSchema: StructType,
                       requestedRootFields: Seq[RootField]): StructType = {
    val resolver = SQLConf.get.resolver
    // Merge the requested root fields into a single schema. Note the ordering of the fields
    // in the resulting schema may differ from their ordering in the logical relation's
    // original schema
    val mergedSchema = requestedRootFields
      .map { root: RootField => StructType(Array(root.field)) }
      .reduceLeft(_ merge _)
    val mergedDataSchema =
      StructType(dataSchema.map(d => mergedSchema.find(m => resolver(m.name, d.name)).getOrElse(d)))
    // Sort the fields of mergedDataSchema according to their order in dataSchema,
    // recursively. This makes mergedDataSchema a pruned schema of dataSchema
    sortLeftFieldsByRight(mergedDataSchema, dataSchema).asInstanceOf[StructType]
  }

  /**
   * Sorts the fields and descendant fields of structs in left according to their order in
   * right. This function assumes that the fields of left are a subset of the fields of
   * right, recursively. That is, left is a "subschema" of right, ignoring order of
   * fields.
   */
  private def sortLeftFieldsByRight(left: DataType, right: DataType): DataType =
    (left, right) match {
      case (ArrayType(leftElementType, containsNull), ArrayType(rightElementType, _)) =>
        ArrayType(
          sortLeftFieldsByRight(leftElementType, rightElementType),
          containsNull)
      case (MapType(leftKeyType, leftValueType, containsNull),
      MapType(rightKeyType, rightValueType, _)) =>
        MapType(
          sortLeftFieldsByRight(leftKeyType, rightKeyType),
          sortLeftFieldsByRight(leftValueType, rightValueType),
          containsNull)
      case (leftStruct: StructType, rightStruct: StructType) =>
        val resolver = SQLConf.get.resolver
        val filteredRightFieldNames = rightStruct.fieldNames
          .filter(name => leftStruct.fieldNames.exists(resolver(_, name)))
        val sortedLeftFields = filteredRightFieldNames.map { fieldName =>
          val resolvedLeftStruct = leftStruct.find(p => resolver(p.name, fieldName)).get
          val leftFieldType = resolvedLeftStruct.dataType
          val rightFieldType = rightStruct(fieldName).dataType
          val sortedLeftFieldType = sortLeftFieldsByRight(leftFieldType, rightFieldType)
          StructField(fieldName, sortedLeftFieldType, nullable = resolvedLeftStruct.nullable)
        }
        StructType(sortedLeftFields)
      case _ => left
    }
}
