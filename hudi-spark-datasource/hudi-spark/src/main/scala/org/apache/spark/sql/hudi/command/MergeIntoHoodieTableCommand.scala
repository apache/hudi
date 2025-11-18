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
import org.apache.hudi.AvroConversionUtils.convertStructTypeToAvroSchema
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieSparkSqlWriter.CANONICALIZE_SCHEMA
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.model.HoodieAvroRecordMerger
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig.{AVRO_SCHEMA_VALIDATE_ENABLE, SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP, TBL_NAME}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hive.HiveSyncConfigHolder
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.hudi.util.JFunction.scalaFunction1Noop
import org.apache.hudi.{AvroConversionUtils, DataSourceWriteOptions, HoodieSparkSqlWriter, HoodieSparkUtils, SparkAdapterSupport}
import org.apache.spark.sql.HoodieCatalystExpressionUtils.{MatchCast, attributeEquals}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BoundReference, EqualTo, Expression, Literal, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.ProvidesHoodieConfig.combineOptions
import org.apache.spark.sql.hudi.analysis.HoodieAnalysis.failAnalysis
import org.apache.spark.sql.hudi.command.MergeIntoHoodieTableCommand.{CoercedAttributeReference, encodeAsBase64String, stripCasting, toStructType}
import org.apache.spark.sql.hudi.command.PartialAssignmentMode.PartialAssignmentMode
import org.apache.spark.sql.hudi.command.payload.ExpressionPayload
import org.apache.spark.sql.hudi.command.payload.ExpressionPayload._
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

import java.util.Base64

/**
 * Hudi's implementation of the {@code MERGE INTO} (MIT) Spark SQL statement.
 *
 * NOTE: That this implementation is restricted in a some aspects to accommodate for Hudi's crucial
 *       constraint (of requiring every record to bear unique primary-key): merging condition ([[mergeCondition]])
 *       is currently can only (and must) reference target table's primary-key columns (this is necessary to
 *       leverage Hudi's upserting capabilities including Indexes)
 *
 * Following algorithm is applied:
 *
 * <ol>
 *   <li>Incoming batch ([[sourceTable]]) is reshaped such that it bears correspondingly:
 *   a) (required) "primary-key" column as well as b) (optional) "pre-combine" column; this is
 *   required since MIT statements does not restrict [[sourceTable]]s schema to be aligned w/ the
 *   [[targetTable]]s one, while Hudi's upserting flow expects such columns to be present</li>
 *
 *   <li>After reshaping we're writing [[sourceTable]] as a normal batch using Hudi's upserting
 *   sequence, where special [[ExpressionPayload]] implementation of the [[HoodieRecordPayload]]
 *   is used allowing us to execute updating, deleting and inserting clauses like following:</li>
 *
 *     <ol>
 *       <li>All the matched {@code WHEN MATCHED AND ... THEN (DELETE|UPDATE ...)} conditional clauses
 *       will produce [[(condition, expression)]] tuples that will be executed w/in the
 *       [[ExpressionPayload#combineAndGetUpdateValue]] against existing (from [[targetTable]]) and
 *       incoming (from [[sourceTable]]) records producing the updated one;</li>
 *
 *       <li>Not matched {@code WHEN NOT MATCHED AND ... THEN INSERT ...} conditional clauses
 *       will produce [[(condition, expression)]] tuples that will be executed w/in [[ExpressionPayload#getInsertValue]]
 *       against incoming records producing ones to be inserted into target table;</li>
 *     </ol>
 * </ol>
 *
 * TODO explain workflow for MOR tables
 */
case class MergeIntoHoodieTableCommand(mergeInto: MergeIntoTable) extends HoodieLeafRunnableCommand
  with SparkAdapterSupport
  with ProvidesHoodieConfig
  with PredicateHelper {

  private var sparkSession: SparkSession = _

  /**
   * The target table schema without hoodie meta fields.
   */
  private lazy val targetTableSchema =
    removeMetaFields(mergeInto.targetTable.schema).fields

  private lazy val hoodieCatalogTable = sparkAdapter.resolveHoodieTable(mergeInto.targetTable) match {
    case Some(catalogTable) => HoodieCatalogTable(sparkSession, catalogTable)
    case _ =>
      failAnalysis(s"Failed to resolve MERGE INTO statement into the Hudi table. Got instead: ${mergeInto.targetTable}")
  }

  private lazy val targetTableType = hoodieCatalogTable.tableTypeName

  /**
   * Mapping of the Merge-Into-Table (MIT) command's [[targetTable]] attribute into
   * corresponding expression (involving reference from the [[sourceTable]]) from the MIT
   * [[mergeCondition]] condition. For ex,
   * <pre>MERGE INTO ... ON t.id = s.s_id AND t.name = lowercase(s.s_name)</pre>
   * will produce
   * <pre>Map("id" -> "s_id", "name" -> lowercase("s_name")</pre>
   *
   * Such mapping is used to be able to properly merge the record in the incoming batch against
   * existing table. Let's take following merge statement as an example:
   *
   * <pre>
   * MERGE INTO ... AS target USING ... AS source
   * ON target.id = lowercase(source.id) ...
   * </pre>
   *
   * To be able to leverage Hudi's engine to merge an incoming dataset against the existing table
   * we will have to make sure that both [[source]] and [[target]] tables have the *same*
   * "primary-key" and "pre-combine" columns. Since actual MIT condition might be leveraging an arbitrary
   * expression involving [[source]] column(s), we will have to add "phony" column matching the
   * primary-key one of the target table.
   */
  private lazy val recordKeyAttributeToConditionExpression: Seq[(Attribute, Expression)] = {
    val primaryKeyFields = hoodieCatalogTable.tableConfig.getRecordKeyFields
    val conditions = splitConjunctivePredicates(mergeInto.mergeCondition)
    if (primaryKeyFields.isPresent) {
      //pkless tables can have more complex conditions
      if (!conditions.forall(p => p.isInstanceOf[EqualTo])) {
        throw new AnalysisException(s"Currently only equality predicates are supported in MERGE INTO statement on primary key table" +
          s"(provided ${mergeInto.mergeCondition.sql}")
      }
    }
    val resolver = sparkSession.sessionState.analyzer.resolver
    val partitionPathFields = hoodieCatalogTable.tableConfig.getPartitionFields
    //ensure all primary key fields are part of the merge condition
    //allow partition path to be part of the merge condition but not required
    val targetAttr2ConditionExpressions = doCasting(conditions, primaryKeyFields.isPresent)
    val expressionSet = scala.collection.mutable.Set[(Attribute, Expression)](targetAttr2ConditionExpressions:_*)
    var partitionAndKeyFields: Seq[(String,String)] = Seq.empty
    if (primaryKeyFields.isPresent) {
     partitionAndKeyFields = partitionAndKeyFields ++ primaryKeyFields.get().map(pk => ("primaryKey", pk)).toSeq
    }
    if (partitionPathFields.isPresent) {
      partitionAndKeyFields = partitionAndKeyFields ++ partitionPathFields.get().map(pp => ("partitionPath", pp)).toSeq
    }
    val resolvedCols = partitionAndKeyFields.map(rk => {
      val resolving = expressionSet.collectFirst {
        case (attr, expr) if resolver(attr.name, rk._2) =>
          // NOTE: Here we validate that condition expression involving record-key column(s) is a simple
          //       attribute-reference expression (possibly wrapped into a cast). This is necessary to disallow
          //       statements like following
          //
          //         MERGE INTO ... AS t USING (
          //            SELECT ... FROM ... AS s
          //         )
          //            ON t.id = s.id + 1
          //            WHEN MATCHED THEN UPDATE *
          //
          //       Which (in the current design) could result in a primary key of the record being modified,
          //       which is not allowed.
          if (!resolvesToSourceAttribute(expr)) {
            throw new AnalysisException("Only simple conditions of the form `t.id = s.id` are allowed on the " +
              s"primary-key and partition path column. Found `${attr.sql} = ${expr.sql}`")
          }
          expressionSet.remove((attr, expr))
          (attr, expr)
      }
      if (resolving.isEmpty && rk._1.equals("primaryKey")
        && sparkSession.sqlContext.conf.getConfString(SPARK_SQL_OPTIMIZED_WRITES.key(), "false") == "true") {
        throw new AnalysisException(s"Hudi tables with primary key are required to match on all primary key colums. Column: '${rk._2}' not found")
      }
      resolving
    }).filter(_.nonEmpty).map(_.get)
    resolvedCols
  }


  private def doCasting(conditions: Seq[Expression], pkTable: Boolean): Seq[(Attribute, Expression)] = {
    val targetAttrs = mergeInto.targetTable.outputSet
    val exprUtils = sparkAdapter.getCatalystExpressionUtils
    // Here we're unraveling superfluous casting of expressions on both sides of the matched-on condition,
    // in case both of them are casted to the same type (which might be result of either explicit casting
    // from the user, or auto-casting performed by Spark for type coercion), which has potential
    // of rendering the whole operation as invalid. This is the case b/c we're leveraging Hudi's internal
    // flow of matching records and therefore will be matching source and target table's primary-key values
    // as they are w/o the ability of transforming them w/ custom expressions (unlike in vanilla Spark flow).
    //
    // Check out HUDI-4861 for more details
    val cleanedConditions = conditions.map(stripCasting)

    // Expressions of the following forms are supported:
    //    `target.id = <expr>` (or `<expr> = target.id`)
    //    `cast(target.id, ...) = <expr>` (or `<expr> = cast(target.id, ...)`)
    //
    // In the latter case, there are further restrictions: since cast will be dropped on the
    // target table side (since we're gonna be matching against primary-key column as is) expression
    // on the opposite side of the comparison should be cast-able to the primary-key column's data-type
    // t/h "up-cast" (ie w/o any loss in precision)
    cleanedConditions.collect {
      case EqualTo(CoercedAttributeReference(attr), expr) if targetAttrs.exists(f => attributeEquals(f, attr)) =>
        if (exprUtils.canUpCast(expr.dataType, attr.dataType)) {
          // NOTE: It's critical we reference output attribute here and not the one from condition
          val targetAttr = targetAttrs.find(f => attributeEquals(f, attr)).get
          targetAttr -> castIfNeeded(expr, attr.dataType)
        } else {
          throw new AnalysisException(s"Invalid MERGE INTO matching condition: ${expr.sql}: "
            + s"can't cast ${expr.sql} (of ${expr.dataType}) to ${attr.dataType}")
        }

      case EqualTo(expr, CoercedAttributeReference(attr)) if targetAttrs.exists(f => attributeEquals(f, attr)) =>
        if (exprUtils.canUpCast(expr.dataType, attr.dataType)) {
          // NOTE: It's critical we reference output attribute here and not the one from condition
          val targetAttr = targetAttrs.find(f => attributeEquals(f, attr)).get
          targetAttr -> castIfNeeded(expr, attr.dataType)
        } else {
          throw new AnalysisException(s"Invalid MERGE INTO matching condition: ${expr.sql}: "
            + s"can't cast ${expr.sql} (of ${expr.dataType}) to ${attr.dataType}")
        }

      case expr if pkTable =>
        throw new AnalysisException(s"Invalid MERGE INTO matching condition: `${expr.sql}`: "
          + "expected condition should be 'target.id = <source-column-expr>', e.g. "
          + "`t.id = s.id` or `t.id = cast(s.id, ...)")
    }
  }

  /**
   * Please check description for [[primaryKeyAttributeToConditionExpression]]
   */
  private lazy val preCombineAttributeAssociatedExpression: Option[(Attribute, Expression)] = {
    val resolver = sparkSession.sessionState.analyzer.resolver
    hoodieCatalogTable.preCombineKey.map { preCombineField =>
      val targetPreCombineAttribute =
        mergeInto.targetTable.output
          .find { attr => resolver(attr.name, preCombineField) }
          .get

      // To find corresponding "pre-combine" attribute w/in the [[sourceTable]] we do
      //    - Check if we can resolve the attribute w/in the source table as is; if unsuccessful, then
      //    - Check if in any of the update actions, right-hand side of the assignment actually resolves
      //    to it, in which case we will determine left-hand side expression as the value of "pre-combine"
      //    attribute w/in the [[sourceTable]]
      val sourceExpr = {
        mergeInto.sourceTable.output.find(attr => resolver(attr.name, preCombineField)) match {
          case Some(attr) => attr
          case None =>
            updatingActions.flatMap(_.assignments).collectFirst {
              case Assignment(attr: AttributeReference, expr)
                if resolver(attr.name, preCombineField) && resolvesToSourceAttribute(expr) => expr
            } getOrElse {
              throw new AnalysisException(s"Failed to resolve pre-combine field `${preCombineField}` w/in the source-table output")
            }

        }
      }

      (targetPreCombineAttribute, sourceExpr)
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    this.sparkSession = sparkSession
    // TODO move to analysis phase
    validate

    if (HoodieSparkUtils.isSpark2) {
      //already enabled by default for spark 3+
      sparkSession.conf.set("spark.sql.crossJoin.enabled","true")
    }

    val processedInputDf: DataFrame = getProcessedInputDf
    // Create the write parameters
    val props = buildMergeIntoConfig(hoodieCatalogTable)
    // Do the upsert
    executeUpsert(processedInputDf, props)
    // Refresh the table in the catalog
    sparkSession.catalog.refreshTable(hoodieCatalogTable.table.qualifiedName)

    Seq.empty[Row]
  }

  private val updatingActions: Seq[UpdateAction] = mergeInto.matchedActions.collect { case u: UpdateAction => u}
  private val insertingActions: Seq[InsertAction] = mergeInto.notMatchedActions.collect { case u: InsertAction => u}
  private val deletingActions: Seq[DeleteAction] = mergeInto.matchedActions.collect { case u: DeleteAction => u}

  private def hasPrimaryKey(): Boolean = {
    hoodieCatalogTable.tableConfig.getRecordKeyFields.isPresent
  }

  /**
   * Here we're adjusting incoming (source) dataset in case its schema is divergent from
   * the target table, to make sure it (at a bare minimum)
   *
   * <ol>
   *   <li>Contains "primary-key" column (as defined by target table's config)</li>
   *   <li>Contains "pre-combine" column (as defined by target table's config, if any)</li>
   * </ol>
   *
   * In cases when [[sourceTable]] doesn't contain aforementioned columns, following heuristic
   * will be applied:
   *
   * <ul>
   * <li>Expression for the "primary-key" column is extracted from the merge-on condition of the
   * MIT statement: Hudi's implementation of the statement restricts kind of merge-on condition
   * permitted to only such referencing primary-key column(s) of the target table; as such we're
   * leveraging matching side of such conditional expression (containing [[sourceTable]] attribute)
   * interpreting it as a primary-key column in the [[sourceTable]]</li>
   *
   * <li>Expression for the "pre-combine" column (optional) is extracted from the matching update
   * clause ({@code WHEN MATCHED ... THEN UPDATE ...}) as right-hand side of the expression referencing
   * pre-combine attribute of the target column</li>
   * <ul>
   *
   * For example, w/ the following statement (primary-key column is [[id]], while pre-combine column is [[ts]])
   * <pre>
   * MERGE INTO target
   * USING (SELECT 1 AS sid, 'A1' AS sname, 1000 AS sts) source
   * ON target.id = source.sid
   * WHEN MATCHED THEN UPDATE SET id = source.sid, name = source.sname, ts = source.sts
   * </pre>
   *
   * We will append following columns to the source dataset:
   * <ul>
   * <li>{@code id = source.sid}</li>
   * <li>{@code ts = source.sts}</li>
   * </ul>
   */
  private def getProcessedInputDf: DataFrame = {
    val resolver = sparkSession.sessionState.analyzer.resolver

    // For pkless table, we need to project the meta columns by joining with the target table;
    // for a Hudi table with record key, we use the source table and rely on Hudi's tagging
    // to identify inserts, updates, and deletes to avoid the join
    val inputPlan = if (!hasPrimaryKey()) {
      // We want to join the source and target tables.
      // Then we want to project the output so that we have the meta columns from the target table
      // followed by the data columns of the source table
      val tableMetaCols = mergeInto.targetTable.output.filter(a => isMetaField(a.name))
      val joinData = sparkAdapter.getCatalystPlanUtils.createMITJoin(mergeInto.sourceTable, mergeInto.targetTable, LeftOuter, Some(mergeInto.mergeCondition), "NONE")
      val incomingDataCols = joinData.output.filterNot(mergeInto.targetTable.outputSet.contains)
      Project(tableMetaCols ++ incomingDataCols, joinData)
    } else {
      mergeInto.sourceTable
    }

    val inputPlanAttributes = inputPlan.output

    val requiredAttributesMap = recordKeyAttributeToConditionExpression ++ preCombineAttributeAssociatedExpression

    val (existingAttributesMap, missingAttributesMap) = requiredAttributesMap.partition {
      case (keyAttr, _) => inputPlanAttributes.exists(attr => resolver(keyAttr.name, attr.name))
    }

    // This is to handle the situation where condition is something like "s0.s_id = t0.id" so In the source table
    // we add an additional column that is an alias of "s0.s_id" named "id"
    // NOTE: Primary key attribute (required) as well as Pre-combine one (optional) defined
    //       in the [[targetTable]] schema has to be present in the incoming [[sourceTable]] dataset.
    //       In cases when [[sourceTable]] doesn't bear such attributes (which, for ex, could happen
    //       in case of it having different schema), we will be adding additional columns (while setting
    //       them according to aforementioned heuristic) to meet Hudi's requirements
    val additionalColumns: Seq[NamedExpression] =
      missingAttributesMap.flatMap {
        case (keyAttr, sourceExpression) if !inputPlanAttributes.exists(attr => resolver(attr.name, keyAttr.name)) =>
          Seq(Alias(sourceExpression, keyAttr.name)())

        case _ => Seq()
      }

    // In case when we're not adding new columns we need to make sure that the casing of the key attributes'
    // matches to that one of the target table. This is necessary b/c unlike Spark, Avro is case-sensitive
    // and therefore would fail downstream if case of corresponding columns don't match
    val existingAttributes = existingAttributesMap.map(_._1)
    val adjustedSourceTableOutput = inputPlanAttributes.map { attr =>
      existingAttributes.find(keyAttr => resolver(keyAttr.name, attr.name)) match {
        // To align the casing we just rename the attribute to match that one of the
        // target table
        case Some(keyAttr) => attr.withName(keyAttr.name)
        case _ => attr
      }
    }

    val amendedPlan = Project(adjustedSourceTableOutput ++ additionalColumns, inputPlan)

    Dataset.ofRows(sparkSession, amendedPlan)
  }

  /**
   * Execute the update and delete action. All the matched and not-matched actions will
   * execute in one upsert write operation. We pushed down the matched condition and assignment
   * expressions to the ExpressionPayload#combineAndGetUpdateValue and the not matched
   * expressions to the ExpressionPayload#getInsertValue.
   */
  private def executeUpsert(sourceDF: DataFrame, parameters: Map[String, String]): Unit = {
    val operation = if (StringUtils.isNullOrEmpty(parameters.getOrElse(PRECOMBINE_FIELD.key, "")) && updatingActions.isEmpty) {
      INSERT_OPERATION_OPT_VAL
    } else {
      UPSERT_OPERATION_OPT_VAL
    }

    // Append the table schema to the parameters. In the case of merge into, the schema of projectedJoinedDF
    // may be different from the target table, because the are transform logical in the update or
    // insert actions.
    var writeParams = parameters +
      (OPERATION.key -> operation) +
      (HoodieWriteConfig.WRITE_SCHEMA_OVERRIDE.key -> getTableSchema.toString) +
      (DataSourceWriteOptions.TABLE_TYPE.key -> targetTableType)

    writeParams ++= Seq(
      // Append (encoded) updating actions
      PAYLOAD_UPDATE_CONDITION_AND_ASSIGNMENTS ->
        // NOTE: For updating clause we allow partial assignments, where only some of the fields of the target
        //       table's records are updated (w/ the missing ones keeping their existing values)
        serializeConditionalAssignments(updatingActions.map(a => (a.condition, a.assignments)),
          partialAssigmentMode = Some(PartialAssignmentMode.ORIGINAL_VALUE)),
      // Append (encoded) inserting actions
      PAYLOAD_INSERT_CONDITION_AND_ASSIGNMENTS ->
        serializeConditionalAssignments(insertingActions.map(a => (a.condition, a.assignments)),
          partialAssigmentMode = Some(PartialAssignmentMode.NULL_VALUE),
          validator = validateInsertingAssignmentExpression)
    )

    // Append (encoded) deleting actions
    writeParams ++= deletingActions.headOption.map {
      case DeleteAction(condition) =>
        PAYLOAD_DELETE_CONDITION -> serializeConditionalAssignments(Seq(condition -> Seq.empty))
    }.toSeq

    // Append
    //  - Original [[sourceTable]] (Avro) schema
    //  - Schema of the expected "joined" output of the [[sourceTable]] and [[targetTable]]
    writeParams ++= Seq(
      PAYLOAD_RECORD_AVRO_SCHEMA ->
        HoodieAvroUtils.removeMetadataFields(convertStructTypeToAvroSchema(sourceDF.schema, "record", "")).toString,
      PAYLOAD_EXPECTED_COMBINED_SCHEMA -> encodeAsBase64String(toStructType(joinedExpectedOutput))
    )

    val (success, _, _, _, _, _) = HoodieSparkSqlWriter.write(sparkSession.sqlContext, SaveMode.Append, writeParams, sourceDF)
    if (!success) {
      throw new HoodieException("Merge into Hoodie table command failed")
    }
  }

  private def getTableSchema: Schema = {
    val (structName, nameSpace) = AvroConversionUtils
      .getAvroRecordNameAndNamespace(hoodieCatalogTable.tableName)
    AvroConversionUtils.convertStructTypeToAvroSchema(
      new StructType(targetTableSchema), structName, nameSpace)
  }

  /**
   * Binds and serializes sequence of [[(Expression, Seq[Expression])]] where
   * <ul>
   *   <li>First [[Expression]] designates condition (in update/insert clause)</li>
   *   <li>Second [[Seq[Expression] ]] designates individual column assignments (in update/insert clause)</li>
   * </ul>
   *
   * Such that
   * <ol>
   *   <li>All expressions are bound against expected payload layout (and ready to be code-gen'd)</li>
   *   <li>Serialized into Base64 string to be subsequently passed to [[ExpressionPayload]]</li>
   * </ol>
   */
  private def serializeConditionalAssignments(conditionalAssignments: Seq[(Option[Expression], Seq[Assignment])],
                                              partialAssigmentMode: Option[PartialAssignmentMode] = None,
                                              validator: Expression => Unit = scalaFunction1Noop): String = {
    val boundConditionalAssignments =
      conditionalAssignments.map {
        case (condition, assignments) =>
          val boundCondition = condition.map(bindReferences).getOrElse(Literal.create(true, BooleanType))
          // NOTE: For deleting actions there's no assignments provided and no re-ordering is required.
          //       All other actions are expected to provide assignments correspondent to every field
          //       of the [[targetTable]] being assigned
          val reorderedAssignments = if (assignments.nonEmpty) {
            alignAssignments(assignments, partialAssigmentMode)
          } else {
            Seq.empty
          }
          // NOTE: We need to re-order assignments to follow the ordering of the attributes
          //       of the target table, such that the resulting output produced after execution
          //       of these expressions could be inserted into the target table as is
          val boundAssignmentExprs = reorderedAssignments.map {
            case Assignment(attr: Attribute, value) =>
              val boundExpr = bindReferences(value)
              validator(boundExpr)
              // Alias resulting expression w/ target table's expected column name, as well as
              // do casting if necessary
              Alias(castIfNeeded(boundExpr, attr.dataType), attr.name)()
            }

          boundCondition -> boundAssignmentExprs
      }.toMap

    encodeAsBase64String(boundConditionalAssignments)
  }

  /**
   * Re-orders assignment expressions to adhere to the ordering of that of [[targetTable]]
   */
  private def alignAssignments(
              assignments: Seq[Assignment],
              partialAssigmentMode: Option[PartialAssignmentMode]): Seq[Assignment] = {
    val attr2Assignments = assignments.map {
      case assign @ Assignment(attr: Attribute, _) => attr -> assign
      case a =>
        throw new AnalysisException(s"Only assignments of the form `t.field = ...` are supported at the moment (provided: `${a.sql}`)")
    }

    // Reorder the assignments to follow the ordering of the target table
    mergeInto.targetTable.output
      .filterNot(attr => isMetaField(attr.name))
      .map { attr =>
        attr2Assignments.find(tuple => attributeEquals(tuple._1, attr)) match {
          case Some((_, assignment)) => assignment
          case None =>
            // In case partial assignments are allowed and there's no corresponding conditional assignment,
            // create a self-assignment for the target table's attribute
            partialAssigmentMode match {
              case Some(mode) =>
                mode match {
                  case PartialAssignmentMode.NULL_VALUE =>
                    Assignment(attr, Literal(null))
                  case PartialAssignmentMode.ORIGINAL_VALUE =>
                    Assignment(attr, attr)
                  case PartialAssignmentMode.DEFAULT_VALUE =>
                    Assignment(attr, Literal.default(attr.dataType))
                }
              case _ =>
                throw new AnalysisException(s"Assignment expressions have to assign every attribute of target table " +
                  s"(provided: `${assignments.map(_.sql).mkString(",")}`)")
            }
        }
      }
  }

  /**
   * Binds existing [[AttributeReference]]s (converting them into [[BoundReference]]s) against
   * expected combined payload of
   *
   * <ol>
   *   <li>Source table record, joined w/</li>
   *   <li>Target table record</li>
   * </ol>
   *
   * NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
   *       This has to be in sync w/ [[ExpressionPayload]] that is actually performing comnbining of the
   *       records producing final payload being persisted.
   *
   * Joining is necessary to handle the case of the records being _updated_ (when record is present in
   * both target and the source tables), since MIT statement allows resulting record to be
   * an amalgamation of both existing and incoming records (for ex, partially updated).
   *
   * For newly inserted records, since no prior record exist in the target table, we're only going to
   * use source payload to produce the resulting record -- hence, source dataset output is the left
   * prefix of this join.
   *
   * Binding is necessary for [[ExpressionPayload]] to use the code-gen to effectively perform
   * handling of the records (combining updated records, as well as producing new records to be inserted)
   */
  private def bindReferences(expr: Expression): Expression = {
    // NOTE: Since original source dataset could be augmented w/ additional columns (please
    //       check its corresponding java-doc for more details) we have to get up-to-date list
    //       of its output attributes
    val joinedExpectedOutputAttributes = joinedExpectedOutput

    bindReference(expr, joinedExpectedOutputAttributes, allowFailures = false)
  }

  /**
   * Output of the expected (left) join of the a) [[sourceTable]] dataset (potentially amended w/ primary-key,
   * pre-combine columns) with b) existing [[targetTable]]
   */
  private def joinedExpectedOutput: Seq[Attribute] = {
    // NOTE: We're relying on [[sourceDataset]] here instead of [[mergeInto.sourceTable]],
    //       as it could be amended to add missing primary-key and/or pre-combine columns.
    //       Please check [[sourceDataset]] scala-doc for more details
    (getProcessedInputDf.queryExecution.analyzed.output ++ mergeInto.targetTable.output).filterNot(a => isMetaField(a.name))
  }

  private def resolvesToSourceAttribute(expr: Expression): Boolean = {
    val sourceTableOutputSet = mergeInto.sourceTable.outputSet
    expr match {
      case attr: AttributeReference => sourceTableOutputSet.contains(attr)
      case MatchCast(attr: AttributeReference, _, _, _) => sourceTableOutputSet.contains(attr)

      case _ => false
    }
  }

  private def validateInsertingAssignmentExpression(expr: Expression): Unit = {
    val sourceTableOutput = mergeInto.sourceTable.output
    expr.collect { case br: BoundReference => br }
      .foreach(br => {
        if (br.ordinal >= sourceTableOutput.length) {
          throw new AnalysisException(s"Expressions in insert clause of the MERGE INTO statement can only reference " +
            s"source table attributes (ordinal ${br.ordinal}, total attributes in the source table ${sourceTableOutput.length})")
        }
      })
  }

  /**
   * Create the config for hoodie writer.
   */
  private def buildMergeIntoConfig(hoodieCatalogTable: HoodieCatalogTable): Map[String, String] = {
    val tableId = hoodieCatalogTable.table.identifier
    val targetTableDb = tableId.database.getOrElse("default")
    val targetTableName = tableId.identifier
    val path = hoodieCatalogTable.tableLocation
    val tableConfig = hoodieCatalogTable.tableConfig
    val tableSchema = hoodieCatalogTable.tableSchema
    val partitionColumns = tableConfig.getPartitionFieldProp.split(",").map(_.toLowerCase)
    val partitionSchema = StructType(tableSchema.filter(f => partitionColumns.contains(f.name)))

    // NOTE: Here we fallback to "" to make sure that null value is not overridden with
    // default value ("ts")
    // TODO(HUDI-3456) clean up
    val preCombineField = hoodieCatalogTable.preCombineKey.getOrElse("")
    val hiveSyncConfig = buildHiveSyncConfig(sparkSession, hoodieCatalogTable, tableConfig)
    // for pkless tables, we need to enable optimized merge
    val isPrimaryKeylessTable = !hasPrimaryKey()
    val keyGeneratorClassName = if (isPrimaryKeylessTable) {
      classOf[MergeIntoKeyGenerator].getCanonicalName
    } else {
      classOf[SqlKeyGenerator].getCanonicalName
    }

    val overridingOpts = Map(
      "path" -> path,
      RECORDKEY_FIELD.key -> tableConfig.getRawRecordKeyFieldProp,
      PRECOMBINE_FIELD.key -> preCombineField,
      TBL_NAME.key -> hoodieCatalogTable.tableName,
      PARTITIONPATH_FIELD.key -> tableConfig.getPartitionFieldProp,
      HIVE_STYLE_PARTITIONING.key -> tableConfig.getHiveStylePartitioningEnable,
      URL_ENCODE_PARTITIONING.key -> tableConfig.getUrlEncodePartitioning,
      KEYGENERATOR_CLASS_NAME.key -> keyGeneratorClassName,
      SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME -> tableConfig.getKeyGeneratorClassName,
      HoodieSyncConfig.META_SYNC_ENABLED.key -> hiveSyncConfig.getString(HoodieSyncConfig.META_SYNC_ENABLED.key),
      HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key -> hiveSyncConfig.getString(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key),
      HiveSyncConfigHolder.HIVE_SYNC_MODE.key -> hiveSyncConfig.getString(HiveSyncConfigHolder.HIVE_SYNC_MODE),
      HoodieSyncConfig.META_SYNC_DATABASE_NAME.key -> targetTableDb,
      HoodieSyncConfig.META_SYNC_TABLE_NAME.key -> targetTableName,
      HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE.key -> hiveSyncConfig.getBoolean(HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE).toString,
      HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key -> tableConfig.getPartitionFieldProp,
      HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key -> hiveSyncConfig.getString(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS),
      SqlKeyGenerator.PARTITION_SCHEMA -> partitionSchema.toDDL,
      PAYLOAD_CLASS_NAME.key -> classOf[ExpressionPayload].getCanonicalName,
      RECORD_MERGER_IMPLS.key -> classOf[HoodieAvroRecordMerger].getName,

      // NOTE: We have to explicitly override following configs to make sure no schema validation is performed
      //       as schema of the incoming dataset might be diverging from the table's schema (full schemas'
      //       compatibility b/w table's schema and incoming one is not necessary in this case since we can
      //       be cherry-picking only selected columns from the incoming dataset to be inserted/updated in the
      //       target table, ie partially updating)
      AVRO_SCHEMA_VALIDATE_ENABLE.key -> "false",
      RECONCILE_SCHEMA.key -> "false",
      CANONICALIZE_SCHEMA.key -> "false",
      SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP.key -> "true",
      HoodieSparkSqlWriter.SQL_MERGE_INTO_WRITES.key -> "true",
      HoodieWriteConfig.SPARK_SQL_MERGE_INTO_PREPPED_KEY -> isPrimaryKeylessTable.toString,
      HoodieWriteConfig.COMBINE_BEFORE_UPSERT.key() -> (!StringUtils.isNullOrEmpty(preCombineField)).toString
    )

    combineOptions(hoodieCatalogTable, tableConfig, sparkSession.sqlContext.conf,
      defaultOpts = Map.empty, overridingOpts = overridingOpts)
  }


  def validate(): Unit = {
    checkUpdatingActions(updatingActions)
    checkInsertingActions(insertingActions)
    checkDeletingActions(deletingActions)
  }

  private def checkDeletingActions(deletingActions: Seq[DeleteAction]): Unit = {
    if (deletingActions.length > 1) {
      throw new AnalysisException(s"Only one deleting action is supported in MERGE INTO statement (provided ${deletingActions.length})")
    }
  }

  private def checkInsertingActions(insertActions: Seq[InsertAction]): Unit = {
    insertActions.foreach(insert =>
      assert(insert.assignments.length <= targetTableSchema.length,
        s"The number of insert assignments[${insert.assignments.length}] must be less than or equal to the " +
          s"targetTable field size[${targetTableSchema.length}]"))

  }

  private def checkUpdatingActions(updateActions: Seq[UpdateAction]): Unit = {
    if (hoodieCatalogTable.preCombineKey.isEmpty && updateActions.nonEmpty) {
      logWarning(s"Updates without precombine can have nondeterministic behavior")
    }
    updateActions.foreach(update =>
      assert(update.assignments.length <= targetTableSchema.length,
        s"The number of update assignments[${update.assignments.length}] must be less than or equalequal to the " +
          s"targetTable field size[${targetTableSchema.length}]"))

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

  def stripCasting(expr: Expression): Expression = expr match {
    case EqualTo(MatchCast(leftExpr, leftTargetType, _, _), MatchCast(rightExpr, rightTargetType, _, _))
      if leftTargetType.sameType(rightTargetType) => EqualTo(leftExpr, rightExpr)
    case _ => expr
  }

  def toStructType(attrs: Seq[Attribute]): StructType =
    StructType(attrs.map(a => StructField(a.qualifiedName.replace('.', '_'), a.dataType, a.nullable, a.metadata)))

  def encodeAsBase64String(any: Any): String =
    Base64.getEncoder.encodeToString(Serializer.toBytes(any))
}

object PartialAssignmentMode extends Enumeration {
  type PartialAssignmentMode = Value
  val ORIGINAL_VALUE, DEFAULT_VALUE, NULL_VALUE = Value
}
