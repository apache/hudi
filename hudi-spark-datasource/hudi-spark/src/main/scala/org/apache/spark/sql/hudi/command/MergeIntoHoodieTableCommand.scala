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

package org.apache.spark.sql.hudi.command

import org.apache.avro.Schema
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.hudi.hive.HiveSyncConfigHolder
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.hudi.{AvroConversionUtils, DataSourceWriteOptions, HoodieSparkSqlWriter, SparkAdapterSupport}
import org.apache.spark.sql.HoodieCatalystExpressionUtils.MatchCast
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BoundReference, Cast, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.hudi.HoodieSqlUtils.getMergeIntoTargetTableId
import org.apache.spark.sql.hudi.command.MergeIntoHoodieTableCommand.CoercedAttributeReference
import org.apache.spark.sql.hudi.command.payload.ExpressionPayload
import org.apache.spark.sql.hudi.command.payload.ExpressionPayload._
import org.apache.spark.sql.hudi.{ProvidesHoodieConfig, SerDeUtils}
import org.apache.spark.sql.types.{BooleanType, StructType}

import java.util.Base64


/**
 * The Command for hoodie MergeIntoTable.
 * The match on condition must contain the row key fields currently, so that we can use Hoodie
 * Index to speed up the performance.
 *
 * The main algorithm:
 *
 * We pushed down all the matched and not matched (condition, assignment) expression pairs to the
 * ExpressionPayload. And the matched (condition, assignment) expression pairs will execute in the
 * ExpressionPayload#combineAndGetUpdateValue to compute the result record, while the not matched
 * expression pairs will execute in the ExpressionPayload#getInsertValue.
 *
 * For Mor table, it is a litter complex than this. The matched record also goes through the getInsertValue
 * and write append to the log. So the update actions & insert actions should process by the same
 * way. We pushed all the update actions & insert actions together to the
 * ExpressionPayload#getInsertValue.
 *
 */
case class MergeIntoHoodieTableCommand(mergeInto: MergeIntoTable) extends HoodieLeafRunnableCommand
  with SparkAdapterSupport with ProvidesHoodieConfig {

  private var sparkSession: SparkSession = _

  /**
    * The target table identify.
    */
  private lazy val targetTableIdentify: TableIdentifier = getMergeIntoTargetTableId(mergeInto)

  /**
   * The target table schema without hoodie meta fields.
   */
  private var sourceDFOutput = mergeInto.sourceTable.output.filter(attr => !isMetaField(attr.name))

  /**
   * The target table schema without hoodie meta fields.
   */
  private lazy val targetTableSchemaWithoutMetaFields =
    removeMetaFields(mergeInto.targetTable.schema).fields

  private lazy val hoodieCatalogTable = HoodieCatalogTable(sparkSession, targetTableIdentify)

  private lazy val targetTableType = hoodieCatalogTable.tableTypeName

  /**
   *
   * Return a map of target key to the source expression from the Merge-On Condition.
   * e.g. merge on t.id = s.s_id AND t.name = s.s_name, we return
   * Map("id" -> "s_id", "name" ->"s_name")
   * TODO Currently Non-equivalent conditions are not supported.
   */
  private lazy val targetKey2SourceExpression: Map[String, Expression] = {
    val resolver = sparkSession.sessionState.conf.resolver
    val conditions = splitByAnd(mergeInto.mergeCondition)
    val allEqs = conditions.forall(p => p.isInstanceOf[EqualTo])
    if (!allEqs) {
      throw new IllegalArgumentException("Non-Equal condition is not support for Merge " +
        s"Into Statement: ${mergeInto.mergeCondition.sql}")
    }
    val targetAttrs = mergeInto.targetTable.output

    val cleanedConditions = conditions.map(_.asInstanceOf[EqualTo]).map {
      // Here we're unraveling superfluous casting of expressions on both sides of the matched-on condition,
      // in case both of them are casted to the same type (which might be result of either explicit casting
      // from the user, or auto-casting performed by Spark for type coercion), which has potential
      // potential of rendering the whole operation as invalid (check out HUDI-4861 for more details)
      case EqualTo(MatchCast(leftExpr, leftCastTargetType, _, _), MatchCast(rightExpr, rightCastTargetType, _, _))
        if leftCastTargetType.sameType(rightCastTargetType) => EqualTo(leftExpr, rightExpr)

      case c => c
    }

    val exprUtils = sparkAdapter.getCatalystExpressionUtils
    // Expressions of the following forms are supported:
    //    `target.id = <expr>` (or `<expr> = target.id`)
    //    `cast(target.id, ...) = <expr>` (or `<expr> = cast(target.id, ...)`)
    //
    // In the latter case, there are further restrictions: since cast will be dropped on the
    // target table side (since we're gonna be matching against primary-key column as is) expression
    // on the opposite side of the comparison should be cast-able to the primary-key column's data-type
    // t/h "up-cast" (ie w/o any loss in precision)
    val target2Source = cleanedConditions.map {
      case EqualTo(CoercedAttributeReference(attr), expr)
        if targetAttrs.exists(f => attributeEqual(f, attr, resolver)) =>
          if (exprUtils.canUpCast(expr.dataType, attr.dataType)) {
            targetAttrs.find(f => resolver(f.name, attr.name)).get.name ->
              castIfNeeded(expr, attr.dataType, sparkSession.sqlContext.conf)
          } else {
            throw new AnalysisException(s"Invalid MERGE INTO matching condition: ${expr.sql}: "
              + s"can't cast ${expr.sql} (of ${expr.dataType}) to ${attr.dataType}")
          }

      case EqualTo(expr, CoercedAttributeReference(attr))
        if targetAttrs.exists(f => attributeEqual(f, attr, resolver)) =>
          if (exprUtils.canUpCast(expr.dataType, attr.dataType)) {
            targetAttrs.find(f => resolver(f.name, attr.name)).get.name ->
              castIfNeeded(expr, attr.dataType, sparkSession.sqlContext.conf)
          } else {
            throw new AnalysisException(s"Invalid MERGE INTO matching condition: ${expr.sql}: "
              + s"can't cast ${expr.sql} (of ${expr.dataType}) to ${attr.dataType}")
          }

      case expr =>
        throw new AnalysisException(s"Invalid MERGE INTO matching condition: `${expr.sql}`: "
          + "expected condition should be 'target.id = <source-column-expr>', e.g. "
          + "`t.id = s.id` or `t.id = cast(s.id, ...)`")
    }.toMap

    target2Source
  }

  /**
   * Get the mapping of target preCombineField to the source expression.
   */
  private lazy val target2SourcePreCombineFiled: Option[(String, Expression)] = {
    val updateActions = mergeInto.matchedActions.collect { case u: UpdateAction => u }
    assert(updateActions.size <= 1, s"Only support one updateAction currently, current update action count is: ${updateActions.size}")

    val updateAction = updateActions.headOption
    hoodieCatalogTable.preCombineKey.map(preCombineField => {
      val sourcePreCombineField =
        updateAction.map(u => u.assignments.filter {
            case Assignment(key: AttributeReference, _) => key.name.equalsIgnoreCase(preCombineField)
            case _=> false
          }.head.value
        ).getOrElse {
          // If there is no update action, mapping the target column to the source by order.
          val target2Source = mergeInto.targetTable.output
            .filter(attr => !isMetaField(attr.name))
            .map(_.name)
            .zip(mergeInto.sourceTable.output.filter(attr => !isMetaField(attr.name)))
            .toMap
          target2Source.getOrElse(preCombineField, null)
        }
      (preCombineField, sourcePreCombineField)
    }).filter(p => p._2 != null)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    this.sparkSession = sparkSession

    // Create the write parameters
    val parameters = buildMergeIntoConfig(hoodieCatalogTable)

    if (mergeInto.matchedActions.nonEmpty) { // Do the upsert
      executeUpsert(sourceDF, parameters)
    } else { // If there is no match actions in the statement, execute insert operation only.
      val targetDF = Dataset.ofRows(sparkSession, mergeInto.targetTable)
      val primaryKeys = hoodieCatalogTable.tableConfig.getRecordKeyFieldProp.split(",")
      // Only records that are not included in the target table can be inserted
      val insertSourceDF = sourceDF.join(targetDF, primaryKeys,"leftanti")

      // column order changed after left anti join , we should keep column order of source dataframe
      val cols = removeMetaFields(sourceDF).columns
      executeInsertOnly(insertSourceDF.select(cols.head, cols.tail:_*), parameters)
    }
    sparkSession.catalog.refreshTable(targetTableIdentify.unquotedString)
    Seq.empty[Row]
  }

  /**
   * Build the sourceDF. We will append the source primary key expressions and
   * preCombine field expression to the sourceDF.
   * e.g.
   * <p>
   * merge into h0
   * using (select 1 as id, 'a1' as name, 1000 as ts) s0
   * on h0.id = s0.id + 1
   * when matched then update set id = s0.id, name = s0.name, ts = s0.ts + 1
   * </p>
   * "ts" is the pre-combine field of h0.
   *
   * The targetKey2SourceExpression is: ("id", "s0.id + 1").
   * The target2SourcePreCombineFiled is:("ts", "s0.ts + 1").
   * We will append the "s0.id + 1 as id" and "s0.ts + 1 as ts" to the sourceDF to compute the
   * row key and pre-combine field.
   *
   */
  private lazy val sourceDF: DataFrame = {
    var sourceDF = Dataset.ofRows(sparkSession, mergeInto.sourceTable)
    targetKey2SourceExpression.foreach {
      case (targetColumn, sourceExpression)
        if !containsPrimaryKeyFieldReference(targetColumn, sourceExpression) =>
          sourceDF = sourceDF.withColumn(targetColumn, new Column(sourceExpression))
          sourceDFOutput = sourceDFOutput :+ AttributeReference(targetColumn, sourceExpression.dataType)()
      case _=>
    }
    target2SourcePreCombineFiled.foreach {
      case (targetPreCombineField, sourceExpression)
        if !containsPreCombineFieldReference(targetPreCombineField, sourceExpression) =>
          sourceDF = sourceDF.withColumn(targetPreCombineField, new Column(sourceExpression))
          sourceDFOutput = sourceDFOutput :+ AttributeReference(targetPreCombineField, sourceExpression.dataType)()
      case _=>
    }
    sourceDF
  }

  /**
   * Check whether the source expression has the same column name with target column.
   *
   * Merge condition cases that return true:
   * 1) merge into .. on h0.id = s0.id ..
   * 2) merge into .. on h0.id = cast(s0.id as int) ..
   * "id" is primaryKey field of h0.
   */
  private def containsPrimaryKeyFieldReference(targetColumnName: String, sourceExpression: Expression): Boolean = {
    val sourceColumnNames = sourceDFOutput.map(_.name)
    val resolver = sparkSession.sessionState.conf.resolver

    sourceExpression match {
      case attr: AttributeReference if sourceColumnNames.find(resolver(_, attr.name)).get.equals(targetColumnName) => true
      // SPARK-35857: the definition of Cast has been changed in Spark3.2.
      // Match the class type instead of call the `unapply` method.
      case cast: Cast =>
        cast.child match {
          case attr: AttributeReference if sourceColumnNames.find(resolver(_, attr.name)).get.equals(targetColumnName) => true
          case _ => false
        }
      case _=> false
    }
  }

  /**
   * Check whether the source expression on preCombine field contains the same column name with target column.
   *
   * Merge expression cases that return true:
   * 1) merge into .. on .. update set ts = s0.ts
   * 2) merge into .. on .. update set ts = cast(s0.ts as int)
   * 3) merge into .. on .. update set ts = s0.ts+1 (expressions like this whose sub node has the same column name with target)
   * "ts" is preCombine field of h0.
   */
  private def containsPreCombineFieldReference(targetColumnName: String, sourceExpression: Expression): Boolean = {
    val sourceColumnNames = sourceDFOutput.map(_.name)
    val resolver = sparkSession.sessionState.conf.resolver

    // sub node of the expression may have same column name with target column name
    sourceExpression.find {
      case attr: AttributeReference => sourceColumnNames.find(resolver(_, attr.name)).get.equals(targetColumnName)
      case _ => false
    }.isDefined
  }

  /**
   * Compare a [[Attribute]] to another, return true if they have the same column name(by resolver) and exprId
   */
  private def attributeEqual(
      attr: Attribute, other: Attribute, resolver: Resolver): Boolean = {
    resolver(attr.name, other.name) && attr.exprId == other.exprId
  }

  /**
   * Execute the update and delete action. All the matched and not-matched actions will
   * execute in one upsert write operation. We pushed down the matched condition and assignment
   * expressions to the ExpressionPayload#combineAndGetUpdateValue and the not matched
   * expressions to the ExpressionPayload#getInsertValue.
   */
  private def executeUpsert(sourceDF: DataFrame, parameters: Map[String, String]): Unit = {
    val updateActions = mergeInto.matchedActions.filter(_.isInstanceOf[UpdateAction])
      .map(_.asInstanceOf[UpdateAction])
    // Check for the update actions
    checkUpdateAssignments(updateActions)

    val deleteActions = mergeInto.matchedActions.filter(_.isInstanceOf[DeleteAction])
      .map(_.asInstanceOf[DeleteAction])
    assert(deleteActions.size <= 1, "Should be only one delete action in the merge into statement.")
    val deleteAction = deleteActions.headOption

    val insertActions =
      mergeInto.notMatchedActions.map(_.asInstanceOf[InsertAction])

    // Check for the insert actions
    checkInsertAssignments(insertActions)

    // Append the table schema to the parameters. In the case of merge into, the schema of sourceDF
    // may be different from the target table, because the are transform logical in the update or
    // insert actions.
    val operation = if (StringUtils.isNullOrEmpty(parameters.getOrElse(PRECOMBINE_FIELD.key, ""))) {
      INSERT_OPERATION_OPT_VAL
    } else {
      UPSERT_OPERATION_OPT_VAL
    }
    var writeParams = parameters +
      (OPERATION.key -> operation) +
      (HoodieWriteConfig.WRITE_SCHEMA.key -> getTableSchema.toString) +
      (DataSourceWriteOptions.TABLE_TYPE.key -> targetTableType)

    // Map of Condition -> Assignments
    val updateConditionToAssignments =
      updateActions.map(update => {
        val rewriteCondition = update.condition.map(replaceAttributeInExpression)
          .getOrElse(Literal.create(true, BooleanType))
        val formatAssignments = rewriteAndReOrderAssignments(update.assignments)
        rewriteCondition -> formatAssignments
      }).toMap
    // Serialize the Map[UpdateCondition, UpdateAssignments] to base64 string
    val serializedUpdateConditionAndExpressions = Base64.getEncoder
      .encodeToString(SerDeUtils.toBytes(updateConditionToAssignments))
    writeParams += (PAYLOAD_UPDATE_CONDITION_AND_ASSIGNMENTS ->
      serializedUpdateConditionAndExpressions)

    if (deleteAction.isDefined) {
      val deleteCondition = deleteAction.get.condition
        .map(replaceAttributeInExpression)
        .getOrElse(Literal.create(true, BooleanType))
      // Serialize the Map[DeleteCondition, empty] to base64 string
      val serializedDeleteCondition = Base64.getEncoder
        .encodeToString(SerDeUtils.toBytes(Map(deleteCondition -> Seq.empty[Assignment])))
      writeParams += (PAYLOAD_DELETE_CONDITION -> serializedDeleteCondition)
    }

    // Serialize the Map[InsertCondition, InsertAssignments] to base64 string
    writeParams += (PAYLOAD_INSERT_CONDITION_AND_ASSIGNMENTS ->
      serializedInsertConditionAndExpressions(insertActions))

    // Remove the meta fields from the sourceDF as we do not need these when writing.
    val sourceDFWithoutMetaFields = removeMetaFields(sourceDF)
    HoodieSparkSqlWriter.write(sparkSession.sqlContext, SaveMode.Append, writeParams, sourceDFWithoutMetaFields)
  }

  /**
   * If there are not matched actions, we only execute the insert operation.
   * @param sourceDF
   * @param parameters
   */
  private def executeInsertOnly(sourceDF: DataFrame, parameters: Map[String, String]): Unit = {
    val insertActions = mergeInto.notMatchedActions.map(_.asInstanceOf[InsertAction])
    checkInsertAssignments(insertActions)

    var writeParams = parameters +
      (OPERATION.key -> INSERT_OPERATION_OPT_VAL) +
      (HoodieWriteConfig.WRITE_SCHEMA.key -> getTableSchema.toString)

    writeParams += (PAYLOAD_INSERT_CONDITION_AND_ASSIGNMENTS ->
      serializedInsertConditionAndExpressions(insertActions))

    // Remove the meta fields from the sourceDF as we do not need these when writing.
    val sourceDFWithoutMetaFields = removeMetaFields(sourceDF)
    HoodieSparkSqlWriter.write(sparkSession.sqlContext, SaveMode.Append, writeParams, sourceDFWithoutMetaFields)
  }

  private def checkUpdateAssignments(updateActions: Seq[UpdateAction]): Unit = {
    updateActions.foreach(update =>
      assert(update.assignments.length == targetTableSchemaWithoutMetaFields.length,
        s"The number of update assignments[${update.assignments.length}] must equal to the " +
          s"targetTable field size[${targetTableSchemaWithoutMetaFields.length}]"))
    // For MOR table, the target table field cannot be the right-value in the update action.
    if (targetTableType == MOR_TABLE_TYPE_OPT_VAL) {
      updateActions.foreach(update => {
        val targetAttrs = update.assignments.flatMap(a => a.value.collect {
          case attr: AttributeReference if mergeInto.targetTable.outputSet.contains(attr) => attr
        })
        assert(targetAttrs.isEmpty,
          s"Target table's field(${targetAttrs.map(_.name).mkString(",")}) cannot be the right-value of the update clause for MOR table.")
      })
    }
  }

  private def checkInsertAssignments(insertActions: Seq[InsertAction]): Unit = {
    insertActions.foreach(insert =>
      assert(insert.assignments.length == targetTableSchemaWithoutMetaFields.length,
        s"The number of insert assignments[${insert.assignments.length}] must equal to the " +
          s"targetTable field size[${targetTableSchemaWithoutMetaFields.length}]"))

  }

  private def getTableSchema: Schema = {
    val (structName, nameSpace) = AvroConversionUtils
      .getAvroRecordNameAndNamespace(targetTableIdentify.identifier)
    AvroConversionUtils.convertStructTypeToAvroSchema(
      new StructType(targetTableSchemaWithoutMetaFields), structName, nameSpace)
  }

  /**
   * Serialize the Map[InsertCondition, InsertAssignments] to base64 string.
   * @param insertActions
   * @return
   */
  private def serializedInsertConditionAndExpressions(insertActions: Seq[InsertAction]): String = {
    val insertConditionAndAssignments =
      insertActions.map(insert => {
        val rewriteCondition = insert.condition.map(replaceAttributeInExpression)
          .getOrElse(Literal.create(true, BooleanType))
        val formatAssignments = rewriteAndReOrderAssignments(insert.assignments)
        // Do the check for the insert assignments
        checkInsertExpression(formatAssignments)

        rewriteCondition -> formatAssignments
      }).toMap
    Base64.getEncoder.encodeToString(
      SerDeUtils.toBytes(insertConditionAndAssignments))
  }

  /**
   * Rewrite and ReOrder the assignments.
   * The Rewrite is to replace the AttributeReference to BoundReference.
   * The ReOrder is to make the assignments's order same with the target table.
   * @param assignments
   * @return
   */
  private def rewriteAndReOrderAssignments(assignments: Seq[Expression]): Seq[Expression] = {
    val attr2Assignment = assignments.map {
      case Assignment(attr: AttributeReference, value) => {
        val rewriteValue = replaceAttributeInExpression(value)
        attr -> Alias(rewriteValue, attr.name)()
      }
      case assignment => throw new IllegalArgumentException(s"Illegal Assignment: ${assignment.sql}")
    }.toMap[Attribute, Expression]
   // reorder the assignments by the target table field
    mergeInto.targetTable.output
      .filterNot(attr => isMetaField(attr.name))
      .map(attr => {
        val assignment = attr2Assignment.find(f => attributeEqual(f._1, attr, sparkSession.sessionState.conf.resolver))
          .getOrElse(throw new IllegalArgumentException(s"Cannot find related assignment for field: ${attr.name}"))
        castIfNeeded(assignment._2, attr.dataType, sparkSession.sqlContext.conf)
      })
  }

  /**
   * Replace the AttributeReference to BoundReference. This is for the convenience of CodeGen
   * in ExpressionCodeGen which use the field index to generate the code. So we must replace
   * the AttributeReference to BoundReference here.
   * @param exp
   * @return
   */
  private def replaceAttributeInExpression(exp: Expression): Expression = {
    val sourceJoinTargetFields = sourceDFOutput ++
      mergeInto.targetTable.output.filterNot(attr => isMetaField(attr.name))

    exp transform {
      case attr: AttributeReference =>
        val index = sourceJoinTargetFields.indexWhere(p => p.semanticEquals(attr))
        if (index == -1) {
            throw new IllegalArgumentException(s"cannot find ${attr.qualifiedName} in source or " +
              s"target at the merge into statement")
          }
          BoundReference(index, attr.dataType, attr.nullable)
      case other => other
    }
  }

  /**
   * Check the insert action expression.
   * The insert expression should not contain target table field.
   */
  private def checkInsertExpression(expressions: Seq[Expression]): Unit = {
    expressions.foreach(exp => {
      val references = exp.collect {
        case reference: BoundReference => reference
      }
      references.foreach(ref => {
        if (ref.ordinal >= sourceDFOutput.size) {
          val targetColumn = targetTableSchemaWithoutMetaFields(ref.ordinal - sourceDFOutput.size)
          throw new IllegalArgumentException(s"Insert clause cannot contain target table's field: ${targetColumn.name}" +
            s" in ${exp.sql}")
        }
      })
    })
  }

  /**
   * Create the config for hoodie writer.
   */
  private def buildMergeIntoConfig(hoodieCatalogTable: HoodieCatalogTable): Map[String, String] = {

    val targetTableDb = targetTableIdentify.database.getOrElse("default")
    val targetTableName = targetTableIdentify.identifier
    val path = hoodieCatalogTable.tableLocation
    val catalogProperties = hoodieCatalogTable.catalogProperties
    val tableConfig = hoodieCatalogTable.tableConfig
    val tableSchema = hoodieCatalogTable.tableSchema
    val partitionColumns = tableConfig.getPartitionFieldProp.split(",").map(_.toLowerCase)
    val partitionSchema = StructType(tableSchema.filter(f => partitionColumns.contains(f.name)))

    // NOTE: Here we fallback to "" to make sure that null value is not overridden with
    // default value ("ts")
    // TODO(HUDI-3456) clean up
    val preCombineField = hoodieCatalogTable.preCombineKey.getOrElse("")

    val hoodieProps = getHoodieProps(catalogProperties, tableConfig, sparkSession.sqlContext.conf)
    val hiveSyncConfig = buildHiveSyncConfig(hoodieProps, hoodieCatalogTable)

    withSparkConf(sparkSession, hoodieCatalogTable.catalogProperties) {
      Map(
        "path" -> path,
        RECORDKEY_FIELD.key -> tableConfig.getRecordKeyFieldProp,
        PRECOMBINE_FIELD.key -> preCombineField,
        TBL_NAME.key -> hoodieCatalogTable.tableName,
        PARTITIONPATH_FIELD.key -> tableConfig.getPartitionFieldProp,
        PAYLOAD_CLASS_NAME.key -> classOf[ExpressionPayload].getCanonicalName,
        HIVE_STYLE_PARTITIONING.key -> tableConfig.getHiveStylePartitioningEnable,
        URL_ENCODE_PARTITIONING.key -> tableConfig.getUrlEncodePartitioning,
        KEYGENERATOR_CLASS_NAME.key -> classOf[SqlKeyGenerator].getCanonicalName,
        SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME -> tableConfig.getKeyGeneratorClassName,
        HoodieSyncConfig.META_SYNC_ENABLED.key -> hiveSyncConfig.getString(HoodieSyncConfig.META_SYNC_ENABLED.key),
        HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key -> hiveSyncConfig.getString(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key),
        HiveSyncConfigHolder.HIVE_SYNC_MODE.key -> hiveSyncConfig.getString(HiveSyncConfigHolder.HIVE_SYNC_MODE),
        HoodieSyncConfig.META_SYNC_DATABASE_NAME.key -> targetTableDb,
        HoodieSyncConfig.META_SYNC_TABLE_NAME.key -> targetTableName,
        HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE.key -> hiveSyncConfig.getBoolean(HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE).toString,
        HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key -> tableConfig.getPartitionFieldProp,
        HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key -> hiveSyncConfig.getString(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS),
        HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key -> hoodieProps.getString(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "200"), // set the default parallelism to 200 for sql
        HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key -> hoodieProps.getString(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "200"),
        HoodieWriteConfig.DELETE_PARALLELISM_VALUE.key -> hoodieProps.getString(HoodieWriteConfig.DELETE_PARALLELISM_VALUE.key, "200"),
        SqlKeyGenerator.PARTITION_SCHEMA -> partitionSchema.toDDL
      )
        .filter { case (_, v) => v != null }
    }
  }
}

object MergeIntoHoodieTableCommand {

  object CoercedAttributeReference {
    def unapply(expr: Expression): Option[AttributeReference] = {
      expr match {
        case attr: AttributeReference => Some(attr)
        case MatchCast(attr: AttributeReference, _, _, _) => Some(attr)

        case _ => None
      }
    }
  }

}
