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

package org.apache.spark.sql.hudi.blob

import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hudi.blob.BatchedByteRangeReader.DATA_COL
import org.apache.spark.sql.hudi.expressions.ResolveBytesExpression
import org.slf4j.LoggerFactory

/**
 * Analyzer rule to resolve blob references efficiently using BatchedByteRangeReader.
 *
 * This rule detects queries containing [[ResolveBytesExpression]] markers (created by
 * the `resolve_bytes()` SQL function) and transforms the logical plan to use batched
 * I/O operations for efficient blob data reading.
 *
 * <h3>Transformation Process:</h3>
 * {{{
 * Before: Project([id, resolve_bytes(file_info) as data], child)
 * After:  Project([id, __temp__data as data], BatchedRead(child))
 * }}}
 *
 * <h3>How It Works:</h3>
 * <ol>
 *   <li>Detects [[Project]] nodes containing [[ResolveBytesExpression]] markers</li>
 *   <li>Applies [[BatchedByteRangeReader.readBatched()]] to the child DataFrame</li>
 *   <li>Replaces [[ResolveBytesExpression]] with references to the `__temp__data` column</li>
 *   <li>Returns transformed plan with efficient batched I/O</li>
 * </ol>
 *
 * <h3>Configuration:</h3>
 * <ul>
 *   <li>`hoodie.blob.batching.max.gap.bytes` (default: 4096) - Max gap between reads to batch</li>
 *   <li>`hoodie.blob.batching.lookahead.size` (default: 50) - Rows to buffer for batch detection</li>
 * </ul>
 *
 * <h3>Performance:</h3>
 * <ul>
 *   <li>2-5x speedup for sorted data (by reference.file, reference.position)</li>
 *   <li>Reduces file seeks by merging consecutive reads</li>
 *   <li>Configurable batching parameters for different workloads</li>
 * </ul>
 *
 * @param spark SparkSession for accessing configuration
 */
case class ResolveBlobReferencesRule(spark: SparkSession) extends Rule[LogicalPlan] {

  private val logger = LoggerFactory.getLogger(classOf[ResolveBlobReferencesRule])

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    case _ @ Project(projectList, child) if containsResolveBytesExpression(projectList) =>
      // Only process if we have resolve_bytes expressions
      transformProjectWithBlobResolution(projectList, child)

    case _ @ Aggregate(groupingExpressions, aggregateExpressions, child)
        if containsResolveBytesExpression(aggregateExpressions) ||
           containsResolveBytesExpression(groupingExpressions) =>
      transformAggregateWithBlobResolution(groupingExpressions, aggregateExpressions, child)
  }

  /**
   * Check if any expression in the project list contains a ResolveBytesExpression.
   */
  private def containsResolveBytesExpression(projectList: Seq[Expression]): Boolean = {
    projectList.exists(expr => containsResolveBytesInExpression(expr))
  }

  private def containsResolveBytesInExpression(expr: Expression): Boolean = {
    expr match {
      case _: ResolveBytesExpression => true
      case other => other.children.exists(containsResolveBytesInExpression)
    }
  }

  /**
   * Transform the project to use BatchedByteRangeReader.
   *
   * This method:
   * <ol>
   *   <li>Applies BatchedByteRangeReader to child plan</li>
   *   <li>Transforms project list expressions</li>
   *   <li>Returns new Project with resolved plan</li>
   * </ol>
   */
  private def transformProjectWithBlobResolution(
      projectList: Seq[NamedExpression],
      child: LogicalPlan): LogicalPlan = {

    // Apply batched reader and get resolved plan with mappings
    val (resolvedPlan, dataAttr, attributeMap) = applyBatchedReader(child, projectList, "Project")

    // Transform project list expressions
    val newProjectList = transformNamedExpressions(projectList, dataAttr, attributeMap)

    // Create new project on top of resolved DataFrame's logical plan
    Project(newProjectList, resolvedPlan)
  }

  /**
   * Transform the aggregate to use BatchedByteRangeReader.
   *
   * Similar to Project transformation, but handles both grouping and aggregate expressions.
   */
  private def transformAggregateWithBlobResolution(
      groupingExpressions: Seq[Expression],
      aggregateExpressions: Seq[NamedExpression],
      child: LogicalPlan): LogicalPlan = {

    // Apply batched reader and get resolved plan with mappings
    val (resolvedPlan, dataAttr, attributeMap) =
      applyBatchedReader(child, aggregateExpressions ++ groupingExpressions, "Aggregate")

    // Transform grouping expressions
    val newGroupingExpressions = groupingExpressions.map { expr =>
      val transformed = replaceResolveBytesExpression(expr, dataAttr)
      remapAttributes(transformed, attributeMap)
    }

    // Transform aggregate expressions
    val newAggregateExpressions = transformNamedExpressions(aggregateExpressions, dataAttr, attributeMap)

    // Create new Aggregate on top of resolved plan
    Aggregate(newGroupingExpressions, newAggregateExpressions, resolvedPlan)
  }

  /**
   * Apply BatchedByteRangeReader to child plan and return resolved plan with mappings.
   *
   * This shared method handles the common logic for both Project and Aggregate transformations:
   * <ol>
   *   <li>Extract blob column name from expressions</li>
   *   <li>Create DataFrame from child plan</li>
   *   <li>Apply BatchedByteRangeReader with configured parameters</li>
   *   <li>Extract data attribute and create attribute mapping</li>
   * </ol>
   *
   * @param child Child logical plan to transform
   * @param expressions Expressions to search for blob column name
   * @param nodeType Node type for logging ("Project" or "Aggregate")
   * @return Tuple of (resolvedPlan, dataAttribute, attributeMap)
   */
  private def applyBatchedReader(
      child: LogicalPlan,
      expressions: Seq[Expression],
      nodeType: String): (LogicalPlan, Attribute, Map[String, Attribute]) = {

    // Extract blob column name from expressions
    val blobColumnName = extractBlobColumnNameFromExpressions(expressions)

    // Apply BatchedByteRangeReader to the child DataFrame
    val childDF = org.apache.spark.sql.Dataset.ofRows(spark, child)
    val storageConf = new HadoopStorageConfiguration(spark.sparkContext.hadoopConfiguration)

    // Get configuration parameters
    val maxGapBytes = spark.conf.get("hoodie.blob.batching.max.gap.bytes", "4096").toInt
    val lookaheadSize = spark.conf.get("hoodie.blob.batching.lookahead.size", "50").toInt

    logger.debug(s"Resolving blob references in $nodeType with batched reader " +
      s"(maxGapBytes=$maxGapBytes, lookaheadSize=$lookaheadSize)" +
      blobColumnName.map(name => s", columnName=$name").getOrElse(""))

    val resolvedDF = BatchedByteRangeReader.readBatched(
      childDF, storageConf, maxGapBytes, lookaheadSize, blobColumnName, keepTempColumn = true)

    // Get the data attribute
    val dataAttr = resolvedDF.queryExecution.analyzed.output.find(_.name == DATA_COL).getOrElse {
      throw new RuntimeException(s"Expected data column '$DATA_COL' not found in resolved DataFrame")
    }

    // Create attribute mapping
    val resolvedPlan = resolvedDF.queryExecution.analyzed
    val attributeMap = createAttributeMapping(child.output, resolvedPlan.output)

    (resolvedPlan, dataAttr, attributeMap)
  }

  /**
   * Transform a sequence of NamedExpressions by replacing ResolveBytesExpression and remapping attributes.
   *
   * This shared method handles the common expression transformation logic used by both
   * Project and Aggregate nodes. It preserves expression IDs and metadata.
   *
   * @param expressions Expressions to transform
   * @param dataAttr Data attribute to replace ResolveBytesExpression with
   * @param attributeMap Map for remapping attribute references
   * @return Transformed expressions
   */
  private def transformNamedExpressions(
      expressions: Seq[NamedExpression],
      dataAttr: Attribute,
      attributeMap: Map[String, Attribute]): Seq[NamedExpression] = {
    expressions.map {
      case alias @ Alias(childExpr, name) =>
        val transformed = replaceResolveBytesExpression(childExpr, dataAttr)
        val remapped = remapAttributes(transformed, attributeMap)
        Alias(remapped, name)(alias.exprId, alias.qualifier, alias.explicitMetadata)
      case attr: AttributeReference =>
        val remapped = remapAttributes(attr, attributeMap)
        Alias(remapped, attr.name)(attr.exprId)
      case expr =>
        val transformed = replaceResolveBytesExpression(expr, dataAttr)
        remapAttributes(transformed, attributeMap).asInstanceOf[NamedExpression]
    }
  }

  /**
   * Replace ResolveBytesExpression with reference to resolved data column.
   *
   * Recursively traverses expression tree and replaces all [[ResolveBytesExpression]]
   * nodes with references to the data column produced by [[BatchedByteRangeReader]].
   */
  private def replaceResolveBytesExpression(
      expr: Expression,
      dataAttr: Attribute): Expression = expr match {

    case ResolveBytesExpression(_) =>
      // Replace with reference to the data column
      dataAttr

    case other =>
      // Recursively process children
      other.mapChildren(child => replaceResolveBytesExpression(child, dataAttr))
  }

  /**
   * Extract blob column name from expressions (handles nesting in aggregate functions).
   */
  private def extractBlobColumnNameFromExpressions(expressions: Seq[Expression]): Option[String] = {
    expressions.collectFirst {
      case expr if containsResolveBytesInExpression(expr) =>
        findBlobColumnName(expr)
    }.flatten
  }

  /**
   * Recursively search for ResolveBytesExpression and extract column name.
   */
  private def findBlobColumnName(expr: Expression): Option[String] = expr match {
    case ResolveBytesExpression(child) =>
      child match {
        case attr: AttributeReference => Some(attr.name)
        case _ => None
      }
    case other =>
      other.children.flatMap(findBlobColumnName).headOption
  }

  /**
   * Create a mapping from old attribute references to new ones.
   *
   * When we replace the child with resolvedDF, the attribute IDs change.
   * This method creates a map from old attribute names to new AttributeReferences
   * so we can update references in the project list.
   *
   * @param oldAttrs Attributes from the original child plan
   * @param newAttrs Attributes from the resolved DataFrame
   * @return Map from attribute name to new Attribute
   */
  private def createAttributeMapping(
      oldAttrs: Seq[Attribute],
      newAttrs: Seq[Attribute]): Map[String, Attribute] = {
    newAttrs.map(attr => attr.name -> attr).toMap
  }

  /**
   * Recursively remap attribute references to use new attribute IDs.
   *
   * This is necessary because when we replace the child with resolvedDF,
   * all attribute IDs change, but the project list still references the old IDs.
   * We traverse the expression tree and replace old AttributeReferences with
   * new ones from the attributeMap.
   *
   * @param expr Expression to remap
   * @param attributeMap Map from attribute name to new Attribute
   * @return Expression with remapped attribute references
   */
  private def remapAttributes(
      expr: Expression,
      attributeMap: Map[String, Attribute]): Expression = expr match {

    case attr: AttributeReference =>
      // Replace with new attribute from map, or keep original if not in map
      attributeMap.getOrElse(attr.name, attr)

    case other =>
      // Recursively process children
      other.mapChildren(child => remapAttributes(child, attributeMap))
  }
}
