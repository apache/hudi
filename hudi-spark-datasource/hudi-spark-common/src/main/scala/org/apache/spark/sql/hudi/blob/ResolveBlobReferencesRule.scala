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
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
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
 *   <li>2-5x speedup for sorted data (by file_path, offset)</li>
 *   <li>Reduces file seeks by merging consecutive reads</li>
 *   <li>Configurable batching parameters for different workloads</li>
 * </ul>
 *
 * @param spark SparkSession for accessing configuration
 */
case class ResolveBlobReferencesRule(spark: SparkSession) extends Rule[LogicalPlan] {

  private val logger = LoggerFactory.getLogger(classOf[ResolveBlobReferencesRule])
  private val DATA_COL = "__temp__data"

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    case p @ Project(projectList, child) if containsResolveBytesExpression(projectList) =>
      // Only process if we have resolve_bytes expressions
      transformProjectWithBlobResolution(p, projectList, child)
  }

  /**
   * Check if any expression in the project list contains a ResolveBytesExpression.
   */
  private def containsResolveBytesExpression(projectList: Seq[NamedExpression]): Boolean = {
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
   *   <li>Converts child plan to DataFrame</li>
   *   <li>Extracts column name from ResolveBytesExpression</li>
   *   <li>Applies BatchedByteRangeReader with configured parameters</li>
   *   <li>Replaces ResolveBytesExpression markers with data column references</li>
   *   <li>Returns new Project with resolved plan</li>
   * </ol>
   */
  private def transformProjectWithBlobResolution(
      project: Project,
      projectList: Seq[NamedExpression],
      child: LogicalPlan): LogicalPlan = {

    // Extract the blob column name from ResolveBytesExpression
    val blobColumnName = extractBlobColumnName(projectList)

    // Apply BatchedByteRangeReader to the child DataFrame
    val childDF = org.apache.spark.sql.Dataset.ofRows(spark, child)

    val storageConf = new HadoopStorageConfiguration(spark.sparkContext.hadoopConfiguration)

    // Get configuration parameters
    val maxGapBytes = spark.conf.get("hoodie.blob.batching.max.gap.bytes", "4096").toInt
    val lookaheadSize = spark.conf.get("hoodie.blob.batching.lookahead.size", "50").toInt

    logger.info(s"Resolving blob references with batched reader " +
      s"(maxGapBytes=$maxGapBytes, lookaheadSize=$lookaheadSize)" +
      blobColumnName.map(name => s", columnName=$name").getOrElse(""))

    // Apply batched reading with explicit column name
    val resolvedDF = BatchedByteRangeReader.readBatched(
      childDF,
      storageConf,
      maxGapBytes,
      lookaheadSize,
      blobColumnName,
      keepTempColumn = true  // Keep __temp__data for expression replacement
    )

    // Transform the project list:
    // Replace ResolveBytesExpression with reference to __temp__data column
    val dataAttr = resolvedDF.queryExecution.analyzed.output.find(_.name == DATA_COL).getOrElse {
      throw new RuntimeException(s"Expected data column '$DATA_COL' not found in resolved DataFrame")
    }

    // Create attribute mapping from old child to new resolved child
    val resolvedPlan = resolvedDF.queryExecution.analyzed
    val attributeMap = createAttributeMapping(child.output, resolvedPlan.output)

    val newProjectList = projectList.map {
      case alias @ Alias(childExpr, name) =>
        val transformed = replaceResolveBytesExpression(childExpr, dataAttr)
        val remapped = remapAttributes(transformed, attributeMap)
        Alias(remapped, name)(
          alias.exprId, alias.qualifier, alias.explicitMetadata)
      case attr: AttributeReference =>
        // For simple attributes: remap to new child, wrap in Alias to preserve output ID
        val remapped = remapAttributes(attr, attributeMap)
        Alias(remapped, attr.name)(attr.exprId)
      case expr =>
        // For other named expressions: transform and remap
        val transformed = replaceResolveBytesExpression(expr, dataAttr)
        remapAttributes(transformed, attributeMap).asInstanceOf[NamedExpression]
    }

    // Create new project on top of resolved DataFrame's logical plan
    Project(newProjectList, resolvedPlan)
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
   * Extract the blob column name from ResolveBytesExpression.
   *
   * Finds the first ResolveBytesExpression and extracts the column name
   * from its child expression (which should be an AttributeReference).
   *
   * @param projectList The project list to search
   * @return Optional column name if found
   */
  private def extractBlobColumnName(projectList: Seq[NamedExpression]): Option[String] = {
    projectList.collectFirst {
      case Alias(ResolveBytesExpression(child), _) =>
        child match {
          case attr: AttributeReference => attr.name
          case _ => null
        }
    }.filter(_ != null)
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
