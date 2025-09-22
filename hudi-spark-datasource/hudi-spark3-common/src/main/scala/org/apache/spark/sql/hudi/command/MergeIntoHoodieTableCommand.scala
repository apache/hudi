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

import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, DataSourceWriteOptions, HoodieSparkSqlWriter, SparkAdapterSupport}
import org.apache.hudi.AvroConversionUtils.convertStructTypeToAvroSchema
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieSparkSqlWriter.CANONICALIZE_SCHEMA
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.config.RecordMergeMode
import org.apache.hudi.common.model.{HoodieAvroRecordMerger, HoodieRecordMerger}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableVersion, PartialUpdateMode}
import org.apache.hudi.common.util.{ConfigUtils, StringUtils}
import org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.config.HoodieWriteConfig.{AVRO_SCHEMA_VALIDATE_ENABLE, SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP, TBL_NAME, WRITE_PARTIAL_UPDATE_SCHEMA}
import org.apache.hudi.exception.{HoodieException, HoodieNotSupportedException}
import org.apache.hudi.hive.HiveSyncConfigHolder
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.hudi.util.JFunction.scalaFunction1Noop

import org.apache.avro.Schema
import org.apache.spark.sql._
import org.apache.spark.sql.HoodieCatalystExpressionUtils.{attributeEquals, MatchCast}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BoundReference, EqualTo, Expression, Literal, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.plans.{LeftOuter, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.ProvidesHoodieConfig.{combineOptions, getPartitionPathFieldWriteConfig}
import org.apache.spark.sql.hudi.command.HoodieCommandMetrics.updateCommitMetrics
import org.apache.spark.sql.hudi.command.MergeIntoHoodieTableCommand._
import org.apache.spark.sql.hudi.command.PartialAssignmentMode.PartialAssignmentMode
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.hudi.command.payload.ExpressionPayload
import org.apache.spark.sql.hudi.command.payload.ExpressionPayload._
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

import java.util.Base64

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Exception thrown when field resolution fails during MERGE INTO validation
 */
class MergeIntoFieldResolutionException(message: String)
  extends AnalysisException(s"MERGE INTO field resolution error: $message")

/**
 * Exception thrown when field type does not match between source and target table
 * during MERGE INTO validation
 */
class MergeIntoFieldTypeMismatchException(message: String)
  extends AnalysisException(s"MERGE INTO field type mismatch error: $message")

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
 *   a) (required) "primary-key" column as well as b) (optional) "ordering" columns; this is
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
case class MergeIntoHoodieTableCommand(mergeInto: MergeIntoTable,
                                       hoodieCatalogTable: HoodieCatalogTable,
                                       sparkSession: SparkSession,
                                       query: LogicalPlan) extends DataWritingCommand
  with SparkAdapterSupport
  with ProvidesHoodieConfig
  with PredicateHelper {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def outputColumnNames: Seq[String] = {
    query.output.map(_.name)
  }

  override lazy val metrics: Map[String, SQLMetric] = HoodieCommandMetrics.metrics

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(query = newChild)

  /**
   * The target table schema without hoodie meta fields.
   */
  private lazy val targetTableSchema =
    removeMetaFields(mergeInto.targetTable.schema).fields

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
   * "primary-key" and "ordering" columns. Since actual MIT condition might be leveraging an arbitrary
   * expression involving [[source]] column(s), we will have to add "phony" column matching the
   * primary-key one of the target table.
   */
  private lazy val recordKeyAttributeToConditionExpression: Seq[(Attribute, Expression)] = {
    val primaryKeyFields = hoodieCatalogTable.tableConfig.getRecordKeyFields
    val conditions = splitConjunctivePredicates(mergeInto.mergeCondition)
    if (primaryKeyFields.isPresent) {
      //pkless tables can have more complex conditions
      if (!conditions.forall(p => p.isInstanceOf[EqualTo])) {
        throw new HoodieAnalysisException(s"Currently only equality predicates are supported in MERGE INTO statement on record key table" +
          s"(provided ${mergeInto.mergeCondition.sql}")
      }
    }
    val resolver = sparkSession.sessionState.analyzer.resolver
    val partitionPathFields = hoodieCatalogTable.tableConfig.getPartitionFields
    //ensure all record key fields are part of the merge condition
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
          //       Which (in the current design) could result in a record key of the record being modified,
          //       which is not allowed.
          if (!resolvesToSourceAttribute(mergeInto.sourceTable, expr)) {
            throw new HoodieAnalysisException("Only simple conditions of the form `t.id = s.id` are allowed on the " +
              s"primary-key and partition path column. Found `${attr.sql} = ${expr.sql}`")
          }
          expressionSet.remove((attr, expr))
          (attr, expr)
      }
      if (resolving.isEmpty && rk._1.equals("primaryKey")
        && sparkSession.sessionState.conf.getConfString(SPARK_SQL_OPTIMIZED_WRITES.key(), "false") == "true") {
        throw new HoodieAnalysisException(s"Hudi tables with record key are required to match on all record key columns. Column: '${rk._2}' not found")
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
          throw new HoodieAnalysisException(s"Invalid MERGE INTO matching condition: ${expr.sql}: "
            + s"can't cast ${expr.sql} (of ${expr.dataType}) to ${attr.dataType}")
        }

      case EqualTo(expr, CoercedAttributeReference(attr)) if targetAttrs.exists(f => attributeEquals(f, attr)) =>
        if (exprUtils.canUpCast(expr.dataType, attr.dataType)) {
          // NOTE: It's critical we reference output attribute here and not the one from condition
          val targetAttr = targetAttrs.find(f => attributeEquals(f, attr)).get
          targetAttr -> castIfNeeded(expr, attr.dataType)
        } else {
          throw new HoodieAnalysisException(s"Invalid MERGE INTO matching condition: ${expr.sql}: "
            + s"can't cast ${expr.sql} (of ${expr.dataType}) to ${attr.dataType}")
        }

      case expr if pkTable =>
        throw new HoodieAnalysisException(s"Invalid MERGE INTO matching condition: `${expr.sql}`: "
          + "expected condition should be 'target.id = <source-column-expr>', e.g. "
          + "`t.id = s.id` or `t.id = cast(s.id, ...)")
    }
  }

  /**
   * Please check description for [[primaryKeyAttributeToConditionExpression]]
   */
  private lazy val orderingFieldsAssociatedExpressions: Seq[(Attribute, Expression)] =
    resolveFieldAssociationsBetweenSourceAndTarget(
      sparkSession.sessionState.conf.resolver,
      mergeInto.targetTable,
      mergeInto.sourceTable,
      hoodieCatalogTable.orderingFields.asScala.toSeq,
      "ordering fields",
      updatingActions.flatMap(_.assignments))

  override def run(sparkSession: SparkSession, inputPlan: SparkPlan): Seq[Row] = {
    // TODO move to analysis phase
    // Create the write parameters
    val props = buildMergeIntoConfig(hoodieCatalogTable)
    validate(props)

    val processedInputDf: DataFrame = sparkSession.internalCreateDataFrame(inputPlan.execute(), inputPlan.schema)
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
   * Here we're processing the logical plan of the source table and optionally the target
   * table to get it prepared for writing the data into the Hudi table:
   * <ul>
   * <li> For a target table with record key(s) configure, the source table
   * [[mergeInto.sourceTable]] is used.
   * <li> For a primary keyless target table, the source table [[mergeInto.sourceTable]]
   * and target table [[mergeInto.targetTable]] are left-outer joined based the on the
   * merge condition so that the record key stored in the record key meta column
   * (`_hoodie_record_key`) are attached to the input records if they are updates.
   * </ul>
   *
   * After getting the initial logical plan to precess as above, we're adjusting incoming
   * (source) dataset in case its schema is divergent from the target table, to make sure
   * it contains all the required columns for MERGE INTO (at a bare minimum)
   *
   * <ol>
   *   <li>Contains "primary-key" column (as defined by target table's config)</li>
   *   <li>Contains "ordering" columns (as defined by target table's config, if any)</li>
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
   * <li>Expression for the "ordering" columns (optional) is extracted from the matching update
   * clause ({@code WHEN MATCHED ... THEN UPDATE ...}) as right-hand side of the expression referencing
   * ordering attribute of the target column</li>
   * <ul>
   *
   * For example, w/ the following statement (primary-key column is [[id]], while ordering column is [[ts]])
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
  def getProcessedInputPlan: LogicalPlan = {
    val resolver = sparkSession.sessionState.analyzer.resolver

    // For pkless table, we need to project the meta columns by joining with the target table;
    // for a Hudi table with record key, we use the source table and rely on Hudi's tagging
    // to identify inserts, updates, and deletes to avoid the join
    val inputPlan = if (!hasPrimaryKey()) {
      // For a primary keyless target table, join the source and target tables.
      // Then we want to project the output so that we have the meta columns from the target table
      // followed by the data columns of the source table
      val tableMetaCols = mergeInto.targetTable.output.filter(a => isMetaField(a.name))
      val joinData = sparkAdapter.getCatalystPlanUtils.createMITJoin(mergeInto.sourceTable, mergeInto.targetTable, LeftOuter, Some(mergeInto.mergeCondition), "NONE")
      val incomingDataCols = joinData.output.filterNot(mergeInto.targetTable.outputSet.contains)
      Project(tableMetaCols ++ incomingDataCols, joinData)
    } else {
      // For a target table with record key(s) configure, the source table is used
      mergeInto.sourceTable
    }

    val inputPlanAttributes = inputPlan.output

    val requiredAttributesMap = recordKeyAttributeToConditionExpression ++ orderingFieldsAssociatedExpressions

    val (existingAttributesMap, missingAttributesMap) = requiredAttributesMap.partition {
      case (keyAttr, _) => inputPlanAttributes.exists(attr => resolver(keyAttr.name, attr.name))
    }

    // This is to handle the situation where condition is something like "s0.s_id = t0.id" so In the source table
    // we add an additional column that is an alias of "s0.s_id" named "id"
    // NOTE: Record key attribute (required) as well as ordering attributes (optional) defined
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
    Project(adjustedSourceTableOutput ++ additionalColumns, inputPlan)
  }

  /**
   * Execute the update and delete action. All the matched and not-matched actions will
   * execute in one upsert write operation. We pushed down the matched condition and assignment
   * expressions to the ExpressionPayload#combineAndGetUpdateValue and the not matched
   * expressions to the ExpressionPayload#getInsertValue.
   */
  private def executeUpsert(sourceDF: DataFrame, parameters: Map[String, String]): Unit = {
    val operation: String = getOperationType(parameters)
    // Append the table schema to the parameters. In the case of merge into, the schema of projectedJoinedDF
    // may be different from the target table, because the are transform logical in the update or
    // insert actions.
    val fullSchema = getTableSchema
    var writeParams = parameters +
      (OPERATION.key -> operation) +
      (HoodieWriteConfig.WRITE_SCHEMA_OVERRIDE.key -> fullSchema.toString) +
      (DataSourceWriteOptions.TABLE_TYPE.key -> targetTableType)

    // Only enable writing partial updates to data blocks for upserts to MOR tables,
    // when ENABLE_MERGE_INTO_PARTIAL_UPDATES is set to true,
    // and not all fields are updated
    val writePartialUpdates = if (isPartialUpdateActionForMOR(parameters)) {
      val updatedFieldSet = getUpdatedFields(updatingActions.map(a => a.assignments))
      // Only enable partial updates if not all fields are updated
      if (!areAllFieldsUpdated(updatedFieldSet)) {
        val orderedUpdatedFieldSeq = getOrderedUpdatedFields(updatedFieldSet)
        writeParams ++= Seq(
          WRITE_PARTIAL_UPDATE_SCHEMA.key ->
            HoodieAvroUtils.generateProjectionSchema(fullSchema, orderedUpdatedFieldSeq.asJava).toString
        )
        true
      } else {
        false
      }
    } else {
      false
    }

    // validate that we can support partial updates
    if (writePartialUpdates) {
      if (hoodieCatalogTable.tableConfig.getBootstrapBasePath.isPresent) {
        throw new HoodieNotSupportedException(
          "Partial updates are not supported for bootstrap tables. " + userGuideString)
      }

      if (!hoodieCatalogTable.tableConfig.populateMetaFields()) {
        throw new HoodieNotSupportedException(
          "Partial updates are not supported for virtual key tables. " + userGuideString)
      }

      if (parameters.getOrElse(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key(),
        DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.defaultValue().toString).toBoolean) {
        throw new HoodieNotSupportedException(
          "Partial updates are not supported for tables with schema on read evolution. " + userGuideString)
      }
    }

    writeParams ++= Seq(
      // Append (encoded) updating actions
      PAYLOAD_UPDATE_CONDITION_AND_ASSIGNMENTS ->
        // NOTE: For updating clause we allow partial assignments, where only some of the fields of the target
        //       table's records are updated (w/ the missing ones keeping their existing values)
        serializeConditionalAssignments(updatingActions.map(a => (a.condition, a.assignments)),
          partialAssignmentMode = Some(PartialAssignmentMode.ORIGINAL_VALUE),
          keepUpdatedFieldsOnly = writePartialUpdates),
      // Append (encoded) inserting actions
      PAYLOAD_INSERT_CONDITION_AND_ASSIGNMENTS ->
        serializeConditionalAssignments(insertingActions.map(a => (a.condition, a.assignments)),
          partialAssignmentMode = Some(PartialAssignmentMode.NULL_VALUE),
          keepUpdatedFieldsOnly = false,
          validator = validateInsertingAssignmentExpression)
    )

    // Append (encoded) deleting actions
    writeParams ++= deletingActions.headOption.map {
      case DeleteAction(condition) =>
        PAYLOAD_DELETE_CONDITION -> serializeConditionalAssignments(Seq(condition -> Seq.empty),
          keepUpdatedFieldsOnly = false)
    }.toSeq

    // Append
    //  - Original [[sourceTable]] (Avro) schema
    //  - Schema of the expected "joined" output of the [[sourceTable]] and [[targetTable]]
    writeParams ++= Seq(
      PAYLOAD_RECORD_AVRO_SCHEMA ->
        HoodieAvroUtils.removeMetadataFields(convertStructTypeToAvroSchema(sourceDF.schema, "record", "")).toString,
      PAYLOAD_EXPECTED_COMBINED_SCHEMA -> encodeAsBase64String(toStructType(joinedExpectedOutput))
    )

    // Append original payload class
    writeParams ++= Seq(
      PAYLOAD_ORIGINAL_AVRO_PAYLOAD -> hoodieCatalogTable.tableConfig.getPayloadClass
    )

    val (success, commitInstantTime, _, _, _, _) = HoodieSparkSqlWriter.write(sparkSession.sqlContext, SaveMode.Append, writeParams, sourceDF)
    if (!success) {
      throw new HoodieException("Merge into Hoodie table command failed")
    }
    if (commitInstantTime.isPresent) {
      updateCommitMetrics(metrics, hoodieCatalogTable.metaClient, commitInstantTime.get())
      DataWritingCommand.propogateMetrics(sparkSession.sparkContext, this, metrics)
    }
  }

  private def isPartialUpdateActionForMOR(parameters: Map[String, String]) = {
    (targetTableType == MOR_TABLE_TYPE_OPT_VAL
      && UPSERT_OPERATION_OPT_VAL == getOperationType(parameters)
      && parameters.getOrElse(
      ENABLE_MERGE_INTO_PARTIAL_UPDATES.key,
      ENABLE_MERGE_INTO_PARTIAL_UPDATES.defaultValue.toString).toBoolean
      && updatingActions.nonEmpty
      // Partial update is enabled only for table version >= 8
      && (parameters.getOrElse(HoodieWriteConfig.WRITE_TABLE_VERSION.key, HoodieTableVersion.current().versionCode().toString).toInt
      >= HoodieTableVersion.EIGHT.versionCode())
      // Partial update is disabled when global index is used.
      // After HUDI-9257 is done, we can remove this limitation.
      && !useGlobalIndex(parameters)
      // Partial update is disabled when custom merge mode is set.
      && !useCustomMergeMode(parameters)
      // Partial update is disabled when partialUpdateMode is set.
      && getPartialUpdateMode(parameters).isEmpty)
  }

  private def getOperationType(parameters: Map[String, String]) = {
    if (StringUtils.isNullOrEmpty(ConfigUtils.getOrderingFieldsStrDuringWrite(parameters.asJava)) && updatingActions.isEmpty) {
      INSERT_OPERATION_OPT_VAL
    } else {
      UPSERT_OPERATION_OPT_VAL
    }
  }

  private def getTableSchema: Schema = {
    val (structName, nameSpace) = AvroConversionUtils
      .getAvroRecordNameAndNamespace(hoodieCatalogTable.tableName)
    AvroConversionUtils.convertStructTypeToAvroSchema(
      new StructType(targetTableSchema), structName, nameSpace)
  }

  /**
   * @param conditionalAssignments Conditional assignments.
   * @return Updated fields based on the conditional assignments in the MERGE INTO statement.
   */
  private def getUpdatedFields(conditionalAssignments: Seq[Seq[Assignment]]): Set[Attribute] = {
    {
      conditionalAssignments.flatMap {
        case assignments =>
          // Extract all fields that are updated through the assignments
          if (assignments.nonEmpty) {
            assignments.map {
              case Assignment(attr: Attribute, _) => attr
              case a =>
                throw new HoodieAnalysisException(s"Only assignments of the form `t.field = ...` are supported at the moment (provided: `${a.sql}`)")
            }
          } else {
            Seq.empty
          }
      }
    }.toSet
  }

  /**
   * @param updatedFieldSet Updated fields based on the conditional assignments in the MERGE INTO statement.
   * @return {@code true} if the updated fields covers all fields in the table schema;
   *         {@code false} otherwise.
   */
  private def areAllFieldsUpdated(updatedFieldSet: Set[Attribute]): Boolean = {
    !mergeInto.targetTable.output
      .filterNot(attr => isMetaField(attr.name)).exists { tableAttr =>
      !updatedFieldSet.exists(attr => attributeEquals(attr, tableAttr))
    }
  }

  /**
   * @param updatedFieldSet Set of fields updated.
   * @return Ordered updated fields based on the target table schema.
   */
  private def getOrderedUpdatedFields(updatedFieldSet: Set[Attribute]): Seq[String] = {
    // Reorder the assignments to follow the ordering of the target table
    mergeInto.targetTable.output
      .filterNot(attr => isMetaField(attr.name))
      .filter { tableAttr =>
        updatedFieldSet.exists(attr => attributeEquals(attr, tableAttr))
      }
      .map(attr => attr.name)
  }

  /**
   * Binds and serializes sequence of [[(Expression, Seq[Expression])]] where
   * <ul>
   * <li>First [[Expression]] designates condition (in update/insert clause)</li>
   * <li>Second [[Seq[Expression] ]] designates individual column assignments (in update/insert clause)</li>
   * </ul>
   *
   * Such that
   * <ol>
   * <li>All expressions are bound against expected payload layout (and ready to be code-gen'd)</li>
   * <li>Serialized into Base64 string to be subsequently passed to [[ExpressionPayload]]</li>
   * </ol>
   *
   * When [[keepUpdatedFieldsOnly]] is false, all fields in the target table schema have
   * corresponding assignments from the generation; When [[keepUpdatedFieldsOnly]] is true,
   * i.e., for partial updates, only the fields as the assignees of the assignments have
   * corresponding assignments, so that the generated records for updates only contain
   * updated fields, to be written to the log files in a MOR table.
   */
  private def serializeConditionalAssignments(conditionalAssignments: Seq[(Option[Expression], Seq[Assignment])],
                                              partialAssignmentMode: Option[PartialAssignmentMode] = None,
                                              keepUpdatedFieldsOnly: Boolean,
                                              validator: Expression => Unit = scalaFunction1Noop): String = {
    val boundConditionalAssignments =
      conditionalAssignments.map {
        case (condition, assignments) =>
          val boundCondition = condition.map(bindReferences).getOrElse(Literal.create(true, BooleanType))
          // NOTE: For deleting actions there's no assignments provided and no re-ordering is required.
          //       All other actions are expected to provide assignments correspondent to every field
          //       of the [[targetTable]] being assigned
          val reorderedAssignments = if (assignments.nonEmpty) {
            alignAssignments(assignments, partialAssignmentMode, keepUpdatedFieldsOnly)
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
  private def alignAssignments(assignments: Seq[Assignment],
                               partialAssignmentMode: Option[PartialAssignmentMode],
                               keepUpdatedFieldsOnly: Boolean): Seq[Assignment] = {
    val attr2Assignments = assignments.map {
      case assign@Assignment(attr: Attribute, _) => attr -> assign
      case a =>
        throw new HoodieAnalysisException(s"Only assignments of the form `t.field = ...` are supported at the moment (provided: `${a.sql}`)")
    }

    // Reorder the assignments to follow the ordering of the target table
    if (keepUpdatedFieldsOnly) {
      mergeInto.targetTable.output
        .map(attr =>
          attr2Assignments.find(tuple => attributeEquals(tuple._1, attr))
        )
        .filter(e => e.nonEmpty)
        .map(e => e.get._2)
    } else {
      mergeInto.targetTable.output
        .filterNot(attr => isMetaField(attr.name))
        .map { attr =>
          attr2Assignments.find(tuple => attributeEquals(tuple._1, attr)) match {
            case Some((_, assignment)) => assignment
            case None =>
              // In case partial assignments are allowed and there's no corresponding conditional assignment,
              // create a self-assignment for the target table's attribute
              partialAssignmentMode match {
                case Some(mode) =>
                  mode match {
                    case PartialAssignmentMode.NULL_VALUE =>
                      Assignment(attr, Literal(null))
                    case PartialAssignmentMode.ORIGINAL_VALUE =>
                      if (targetTableType == MOR_TABLE_TYPE_OPT_VAL) {
                        Assignment(attr, Literal(null))
                      } else {
                        Assignment(attr, attr)
                      }
                    case PartialAssignmentMode.DEFAULT_VALUE =>
                      Assignment(attr, Literal.default(attr.dataType))
                  }
                case _ =>
                  throw new HoodieAnalysisException(s"Assignment expressions have to assign every attribute of target table " +
                    s"(provided: `${assignments.map(_.sql).mkString(",")}`)")
              }
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
   * ordering columns) with b) existing [[targetTable]]
   */
  private def joinedExpectedOutput: Seq[Attribute] = {
    // NOTE: We're relying on [[sourceDataset]] here instead of [[mergeInto.sourceTable]],
    //       as it could be amended to add missing primary-key and/or ordering columns.
    //       Please check [[sourceDataset]] scala-doc for more details
    (query.output ++ mergeInto.targetTable.output).filterNot(a => isMetaField(a.name))
  }

  private def validateInsertingAssignmentExpression(expr: Expression): Unit = {
    val sourceTableOutput = mergeInto.sourceTable.output
    expr.collect { case br: BoundReference => br }
      .foreach(br => {
        if (br.ordinal >= sourceTableOutput.length) {
          throw new HoodieAnalysisException(s"Expressions in insert clause of the MERGE INTO statement can only reference " +
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
    val orderingFieldsAsString = String.join(",", hoodieCatalogTable.orderingFields)
    val hiveSyncConfig = buildHiveSyncConfig(sparkSession, hoodieCatalogTable, tableConfig)
    // for pkless tables, we need to enable optimized merge
    val isPrimaryKeylessTable = !hasPrimaryKey()
    val keyGeneratorClassName = if (isPrimaryKeylessTable) {
      classOf[MergeIntoKeyGenerator].getCanonicalName
    } else {
      classOf[SqlKeyGenerator].getCanonicalName
    }

    val mergeMode = if (tableConfig.getTableVersion.lesserThan(HoodieTableVersion.EIGHT)) {
      val inferredMergeConfigs = HoodieTableConfig.inferMergingConfigsForWrites(
        tableConfig.getRecordMergeMode,
        tableConfig.getPayloadClass,
        tableConfig.getRecordMergeStrategyId,
        tableConfig.getOrderingFieldsStr.orElse(null),
        tableConfig.getTableVersion)
      inferredMergeConfigs.getLeft.name()
    } else {
      tableConfig.getRecordMergeMode.name()
    }
    val overridingOpts = Map(
      "path" -> path,
      RECORDKEY_FIELD.key -> tableConfig.getRawRecordKeyFieldProp,
      HoodieTableConfig.ORDERING_FIELDS.key -> orderingFieldsAsString,
      TBL_NAME.key -> hoodieCatalogTable.tableName,
      PARTITIONPATH_FIELD.key -> getPartitionPathFieldWriteConfig(
        tableConfig.getKeyGeneratorClassName, tableConfig.getPartitionFieldProp, hoodieCatalogTable),
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
      RECORD_MERGE_IMPL_CLASSES.key -> classOf[HoodieAvroRecordMerger].getName,
      HoodieWriteConfig.RECORD_MERGE_MODE.key() -> mergeMode,
      RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID,

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
      // Only primary keyless table requires prepped keys and upsert
      HoodieWriteConfig.SPARK_SQL_MERGE_INTO_PREPPED_KEY -> isPrimaryKeylessTable.toString,
      HoodieWriteConfig.COMBINE_BEFORE_UPSERT.key() -> (!StringUtils.isNullOrEmpty(orderingFieldsAsString)).toString
    )

    combineOptions(hoodieCatalogTable, tableConfig, sparkSession.sessionState.conf,
      defaultOpts = Map.empty, overridingOpts = overridingOpts)
  }

  def validate(props: Map[String, String]): Unit = {
    checkUpdatingActions(updatingActions, props)
    checkInsertingActions(insertingActions, props)
    checkDeletingActions(deletingActions)
  }

  private def checkDeletingActions(deletingActions: Seq[DeleteAction]): Unit = {
    if (deletingActions.length > 1) {
      throw new HoodieAnalysisException(s"Only one deleting action is supported in MERGE INTO statement (provided ${deletingActions.length})")
    }
  }

  private def checkInsertingActions(insertActions: Seq[InsertAction], props: Map[String, String]): Unit = {
    insertActions.foreach(insert =>
      assert(insert.assignments.length <= targetTableSchema.length,
        s"The number of insert assignments[${insert.assignments.length}] must be less than or equal to the " +
          s"targetTable field size[${targetTableSchema.length}]"))
    // Ordering field and record key field must be present in the assignment clause of all insert actions for event time ordering mode.
    // Check has no effect if we don't have such fields in target table or we don't have insert actions
    // Please note we are relying on merge mode in the table config as writer merge mode is always "CUSTOM" for MIT.
    if (isEventTimeOrdering(props)) {
      insertActions.foreach(action =>
        validateTargetTableAttrExistsInAssignments(
          sparkSession.sessionState.conf.resolver,
          mergeInto.targetTable,
          hoodieCatalogTable.orderingFields.asScala.toSeq,
          "ordering field",
          action.assignments)
      )
    }
    insertActions.foreach(action =>
      validateTargetTableAttrExistsInAssignments(
        sparkSession.sessionState.conf.resolver,
        mergeInto.targetTable,
        hoodieCatalogTable.tableConfig.getRecordKeyFields.orElse(Array.empty),
        "record key field",
        action.assignments))

    val insertAssignments = insertActions.flatMap(_.assignments)
    checkSchemaMergeIntoCompatibility(insertAssignments, props)
  }

  private def isEventTimeOrdering(props: Map[String, String]) = {
    RecordMergeMode.EVENT_TIME_ORDERING.name()
      .equals(getStringWithAltKeys(props.asJava.asInstanceOf[java.util.Map[String, Object]],
        HoodieTableConfig.RECORD_MERGE_MODE))
  }

  /**
    * Check the merge into schema compatibility between the target table and the source table.
    * The merge into schema compatibility requires data type matching for the following fields:
    * 1. Partition key
    * 2. Primary key
    * 3. Ordering Fields
    *
    * @param assignments the assignment clause of the insert/update statement for figuring out
    *                    the mapping between the target table and the source table.
    */
  private def checkSchemaMergeIntoCompatibility(assignments: Seq[Assignment], props: Map[String, String]): Unit = {
    if (assignments.nonEmpty) {
      // Assert data type matching for partition key
      hoodieCatalogTable.partitionFields.foreach {
        partitionField => {
          try {
            val association = resolveFieldAssociationsBetweenSourceAndTarget(
              sparkSession.sessionState.conf.resolver,
              mergeInto.targetTable,
              mergeInto.sourceTable,
              Seq(partitionField),
              "partition key",
              assignments).head
            validateDataTypes(association._1, association._2, "Partition key")
          } catch {
            // Only catch AnalysisException from resolveFieldAssociationsBetweenSourceAndTarget
            case _: MergeIntoFieldResolutionException =>
          }
        }
      }
      val primaryAttributeAssociatedExpression: Array[(Attribute, Expression)] =
        resolveFieldAssociationsBetweenSourceAndTarget(
          sparkSession.sessionState.conf.resolver,
          mergeInto.targetTable,
          mergeInto.sourceTable,
          hoodieCatalogTable.primaryKeys,
          "primary key",
          assignments).toArray
      primaryAttributeAssociatedExpression.foreach { case (attr, expr) =>
        validateDataTypes(attr, expr, "Primary key")
      }
      if (isEventTimeOrdering(props)) {
        try {
          val associations = resolveFieldAssociationsBetweenSourceAndTarget(
            sparkSession.sessionState.conf.resolver,
            mergeInto.targetTable,
            mergeInto.sourceTable,
            hoodieCatalogTable.orderingFields.asScala.toSeq,
            "ordering field",
            assignments)
          associations.foreach(association => validateDataTypes(association._1, association._2, "Ordering field"))
        } catch {
          // Only catch AnalysisException from resolveFieldAssociationsBetweenSourceAndTarget
          case _: MergeIntoFieldResolutionException =>
        }
      }
    }
  }

  private def checkUpdatingActions(updateActions: Seq[UpdateAction], props: Map[String, String]): Unit = {
    if (hoodieCatalogTable.orderingFields.isEmpty && updateActions.nonEmpty) {
      logWarning(s"Updates without precombine can have nondeterministic behavior")
    }
    updateActions.foreach(update =>
      assert(update.assignments.length <= targetTableSchema.length,
        s"The number of update assignments[${update.assignments.length}] must be less than or equal to the " +
          s"targetTable field size[${targetTableSchema.length}]"))

    val updateAssignments = updateActions.flatMap(_.assignments)
    checkSchemaMergeIntoCompatibility(updateAssignments, props)

    if (targetTableType == MOR_TABLE_TYPE_OPT_VAL) {
      // For MOR table, the target table field cannot be the right-value in the update action.
      updateActions.foreach(update => {
        val targetAttrs = update.assignments.flatMap(a => a.value.collect {
          case attr: AttributeReference if mergeInto.targetTable.outputSet.contains(attr) => attr
        })
        assert(targetAttrs.isEmpty,
          s"Target table's field(${targetAttrs.map(_.name).mkString(",")}) cannot be the right-value of the update clause for MOR table.")
      })
      // Only when the partial update is enabled that record key assignment is not mandatory in update actions for MOR tables.
      if (!isPartialUpdateActionForMOR(props)) {
        // For MOR table, update assignment clause must have record key field being set explicitly even if it does not
        // change. The check has no effect if there is no updateActions or we don't have record key
        updateActions.foreach(action => validateTargetTableAttrExistsInAssignments(
          sparkSession.sessionState.conf.resolver,
          mergeInto.targetTable,
          hoodieCatalogTable.tableConfig.getRecordKeyFields.orElse(Array.empty),
          "record key field",
          action.assignments))
      }
    }
  }
}

object MergeIntoHoodieTableCommand {

  val userGuideString: String = "To use the MERGE INTO statement on this MOR table, " +
    "please specify `UPDATE SET *` as the update statement to update all columns, " +
    "and set `" + DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key + "=false`."

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

  /**
   * Generic method to validate target table attributes in the assignments clause of the merge into
   * statement.
   *
   * @param resolver The resolver to use
   * @param targetTable The target table of the merge
   * @param fields The fields from the target table which should have an assignment clause
   * @param fieldType String describing the type of field (for error messages)
   * @param assignments The assignments clause of the merge into
   *
   * @throws AnalysisException if the target field from the target table is not found in the assignments.
   */
  def validateTargetTableAttrExistsInAssignments(resolver: Resolver,
                                                 targetTable: LogicalPlan,
                                                 fields: Seq[String],
                                                 fieldType: String,
                                                 assignments: Seq[Assignment]): Unit = {
    fields.foreach { field =>
      targetTable.output
        .find(attr => resolver(attr.name, field))
        .getOrElse(throw new MergeIntoFieldResolutionException(s"Failed to resolve $fieldType `$field` in target table"))

      if (!assignments.exists {
        case Assignment(attr: AttributeReference, _) if resolver(attr.name, field) => true
        case _ => false
      }) {
        throw new MergeIntoFieldResolutionException(s"No matching assignment found for target table $fieldType `$field`")
      }
    }
  }

  /**
   * Generic method to resolve field associations between target and source tables
   *
   * @param resolver The resolver to use
   * @param targetTable The target table of the merge
   * @param sourceTable The source table of the merge
   * @param fields The fields from the target table whose association with the source to be resolved
   * @param fieldType String describing the type of field (for error messages)
   * @param assignments The assignments clause of the merge into used for resolving the association
   * @return Sequence of resolved (target table attribute, source table expression)
   * mapping for target [[fields]].
   *
   * @throws AnalysisException if a field cannot be resolved
   */
  def resolveFieldAssociationsBetweenSourceAndTarget(resolver: Resolver,
                                                     targetTable: LogicalPlan,
                                                     sourceTable: LogicalPlan,
                                                     fields: Seq[String],
                                                     fieldType: String,
                                                     assignments: Seq[Assignment]
                             ): Seq[(Attribute, Expression)] = {
    fields.map { field =>
      val targetAttribute = targetTable.output
        .find(attr => resolver(attr.name, field))
        .getOrElse(throw new MergeIntoFieldResolutionException(
          s"Failed to resolve $fieldType `$field` in target table"))

      val sourceExpr = sourceTable.output
        .find(attr => resolver(attr.name, field))
        .getOrElse {
          assignments.collectFirst {
            case Assignment(attr: AttributeReference, expr)
              if resolver(attr.name, field) && resolvesToSourceAttribute(sourceTable, expr) => expr
          }.getOrElse {
            throw new MergeIntoFieldResolutionException(
              s"Failed to resolve $fieldType `$field` w/in the source-table output")
          }
        }

      (targetAttribute, sourceExpr)
    }
  }

  def resolvesToSourceAttribute(sourceTable: LogicalPlan, expr: Expression): Boolean = {
    val sourceTableOutputSet = sourceTable.outputSet
    expr match {
      case attr: AttributeReference => sourceTableOutputSet.contains(attr)
      case MatchCast(attr: AttributeReference, _, _, _) => sourceTableOutputSet.contains(attr)

      case _ => false
    }
  }

  def validateDataTypes(attr: Attribute, expr: Expression, columnType: String): Unit = {
    if (attr.dataType != expr.dataType) {
      throw new MergeIntoFieldTypeMismatchException(
        s"$columnType data type mismatch between source table and target table. " +
          s"Target table uses ${attr.dataType} for column '${attr.name}', " +
          s"source table uses ${expr.dataType} for '${expr.sql}'"
      )
    }
  }

  // Check if global index, e.g., GLOBAL_BLOOM, is set.
  def useGlobalIndex(parameters: Map[String, String]): Boolean = {
    parameters.get(HoodieIndexConfig.INDEX_TYPE.key).exists { indexType =>
      isGlobalIndexEnabled(indexType, parameters)
    }
  }

  // Check if goal index is enabled for specific indexes.
  def isGlobalIndexEnabled(indexType: String, parameters: Map[String, String]): Boolean = {
    Seq(
      HoodieIndex.IndexType.GLOBAL_SIMPLE -> HoodieIndexConfig.SIMPLE_INDEX_UPDATE_PARTITION_PATH_ENABLE,
      HoodieIndex.IndexType.GLOBAL_BLOOM -> HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE,
      HoodieIndex.IndexType.RECORD_INDEX -> HoodieIndexConfig.RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE
    ).collectFirst {
      case (hoodieIndex, config) if indexType == hoodieIndex.name =>
        parameters.getOrElse(config.key, config.defaultValue().toString).toBoolean
    }.getOrElse(false)
  }

  def useCustomMergeMode(parameters: Map[String, String]): Boolean = {
    val mergeModeOpt = parameters.get(DataSourceWriteOptions.RECORD_MERGE_MODE.key)
    // For table version >= 8, mergeMode should exist.
    if (mergeModeOpt.isEmpty) {
      throw new HoodieException("Merge mode cannot be null here")
    }
    mergeModeOpt.get.equals(RecordMergeMode.CUSTOM.name)
  }

  def getPartialUpdateMode(parameters: Map[String, String]): Option[PartialUpdateMode] = {
    parameters.get(HoodieTableConfig.PARTIAL_UPDATE_MODE.key).flatMap { value =>
      Try(PartialUpdateMode.valueOf(value)).toOption
    }
  }
}

object PartialAssignmentMode extends Enumeration {
  type PartialAssignmentMode = Value
  val ORIGINAL_VALUE, DEFAULT_VALUE, NULL_VALUE = Value
}
