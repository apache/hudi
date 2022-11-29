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
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.common.model.HoodieAvroRecordMerger
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig.{AVRO_SCHEMA_VALIDATE_ENABLE, TBL_NAME}
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.hudi.config.HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hive.HiveSyncConfigHolder
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.hudi.{AvroConversionUtils, DataSourceWriteOptions, HoodieSparkSqlWriter, SparkAdapterSupport}
import org.apache.spark.sql.HoodieCatalystExpressionUtils.{MatchCast, attributeEquals}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BoundReference, Cast, EqualTo, Expression, Literal, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.hudi.analysis.HoodieAnalysis.failAnalysis
import org.apache.spark.sql.hudi.command.MergeIntoHoodieTableCommand.{encodeAsBase64String, toStructType}
import org.apache.spark.sql.hudi.command.payload.ExpressionPayload
import org.apache.spark.sql.hudi.command.payload.ExpressionPayload._
import org.apache.spark.sql.hudi.{ProvidesHoodieConfig, SerDeUtils}
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
  private lazy val primaryKeyAttributeToConditionExpression: Seq[(Attribute, Expression)] = {
    val conditions = splitConjunctivePredicates(mergeInto.mergeCondition)
    if (!conditions.forall(p => p.isInstanceOf[EqualTo])) {
      throw new IllegalArgumentException(s"Currently only equality predicates are supported in MERGE INTO statement " +
        s"(provided ${mergeInto.mergeCondition.sql}")
    }

    val resolver = sparkSession.sessionState.analyzer.resolver
    val primaryKeyField = hoodieCatalogTable.tableConfig.getRecordKeyFieldProp

    val targetAttrs = mergeInto.targetTable.outputSet

    val targetAttr2ConditionExpressions = conditions.map {
      case EqualTo(attr: Attribute, rightExpr) if targetAttrs.contains(attr) =>
        attr -> rightExpr
      case EqualTo(leftExpr, attr: Attribute) if targetAttrs.contains(attr) =>
        attr -> leftExpr

      case e =>
        throw new AnalysisException(s"Unsupported predicate w/in MERGE INTO statement: ${e.sql}. " +
          "Currently, only equality predicates one side of which is the receiving (target) table attribute are supported " +
          "(e.g. `t.id = s.id`)")
    }

    targetAttr2ConditionExpressions.collect {
      case (attr, expr) if resolver(attr.name, primaryKeyField) =>
        // NOTE: Here we validate that condition expression involving primary-key column(s) is a simple
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
        if (!resolvesToSourceAttributeReferenceExpr(expr)) {
          throw new AnalysisException("Only simple conditions of the form `t.id = s.id` are allowed on the " +
            s"primary-key column. Found `${attr.sql} = ${expr.sql}`")
        }

        (attr, expr)
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
                if resolver(attr.name, preCombineField) && resolvesToSourceAttributeReferenceExpr(expr) => expr
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
    validate(mergeInto)

    val sourceDF: DataFrame = sourceDataset
    // Create the write parameters
    val parameters = buildMergeIntoConfig(hoodieCatalogTable)
    // TODO Remove it when we implement ExpressionPayload for SparkRecord
    val parametersWithAvroRecordMerger = parameters ++ Map(HoodieWriteConfig.MERGER_IMPLS.key -> classOf[HoodieAvroRecordMerger].getName)
    if (mergeInto.matchedActions.nonEmpty) { // Do the upsert
      executeUpsert(sourceDF, parametersWithAvroRecordMerger)
    } else { // If there is no match actions in the statement, execute insert operation only.
      val targetDF = Dataset.ofRows(sparkSession, mergeInto.targetTable)
      val primaryKeys = hoodieCatalogTable.tableConfig.getRecordKeyFieldProp.split(",")
      // Only records that are not included in the target table can be inserted
      val insertSourceDF = sourceDF.join(targetDF, primaryKeys,"left_anti")

      // column order changed after left anti join , we should keep column order of source dataframe
      val cols = sourceDF.columns
      executeInsertOnly(insertSourceDF.select(cols.head, cols.tail:_*), parameters)
    }

    sparkSession.catalog.refreshTable(hoodieCatalogTable.table.qualifiedName)

    Seq.empty[Row]
  }

  private val updatingActions: Seq[UpdateAction] = mergeInto.matchedActions.collect { case u: UpdateAction => u}
  private val insertingActions: Seq[InsertAction] = mergeInto.notMatchedActions.collect { case u: InsertAction => u}
  private val deletingActions: Seq[DeleteAction] = mergeInto.matchedActions.collect { case u: DeleteAction => u}

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
   *   <li>Expression for the "primary-key" column is extracted from the merge-on condition of the
   *   MIT statement: Hudi's implementation of the statement restricts kind of merge-on condition
   *   permitted to only such referencing primary-key column(s) of the target table; as such we're
   *   leveraging matching side of such conditional expression (containing [[sourceTable]] attrobute)
   *   interpreting it as a primary-key column in the [[sourceTable]]</li>
   *
   *   <li>Expression for the "pre-combine" column (optional) is extracted from the matching update
   *   clause ({@code WHEN MATCHED ... THEN UPDATE ...}) as right-hand side of the expression referencing
   *   pre-combine attribute of the target column</li>
   * <ul>
   *
   * For example, w/ the following statement (primary-key column is [[id]], while pre-combine column is [[ts]])
   * <pre>
   *    MERGE INTO target
   *    USING (SELECT 1 AS sid, 'A1' AS sname, 1000 AS sts) source
   *    ON target.id = source.sid
   *    WHEN MATCHED THEN UPDATE SET id = source.sid, name = source.sname, ts = source.sts
   * </pre>
   *
   * We will append following columns to the source dataset:
   * <ul>
   *   <li>{@code id = source.sid}</li>
   *   <li>{@code ts = source.sts}</li>
   * </ul>
   */
  def sourceDataset: DataFrame = {
    val resolver = sparkSession.sessionState.analyzer.resolver
    var sourceDF = Dataset.ofRows(sparkSession, mergeInto.sourceTable)
    val sourceTableOutput = mergeInto.sourceTable.output

    (primaryKeyAttributeToConditionExpression ++ preCombineAttributeAssociatedExpression).foreach {
      // NOTE: Primary-key attribute (required) as well as Pre-combine one (optional) defined
      //       in the [[targetTable]] schema has to be present in the incoming [[sourceTable]] dataset.
      //       In cases when [[sourceTable]] doesn't bear such attributes (which, for ex, could happen
      //       in case of it having different schema), we will be adding additional columns (while setting
      //       them according to aforementioned heuristic) to meet Hudi's requirements
      case (targetAttr, sourceExpression)
        if !sourceTableOutput.exists(attr => resolver(attr.name, targetAttr.name)) =>
          sourceDF = sourceDF.withColumn(targetAttr.name, new Column(sourceExpression))

      case _ => // no-op
    }

    sourceDF
  }

  /**
   * Execute the update and delete action. All the matched and not-matched actions will
   * execute in one upsert write operation. We pushed down the matched condition and assignment
   * expressions to the ExpressionPayload#combineAndGetUpdateValue and the not matched
   * expressions to the ExpressionPayload#getInsertValue.
   */
  private def executeUpsert(sourceDF: DataFrame, parameters: Map[String, String]): Unit = {
    val operation = if (StringUtils.isNullOrEmpty(parameters.getOrElse(PRECOMBINE_FIELD.key, ""))) {
      INSERT_OPERATION_OPT_VAL
    } else {
      UPSERT_OPERATION_OPT_VAL
    }

    // Append the table schema to the parameters. In the case of merge into, the schema of sourceDF
    // may be different from the target table, because the are transform logical in the update or
    // insert actions.
    var writeParams = parameters +
      (OPERATION.key -> operation) +
      (HoodieWriteConfig.WRITE_SCHEMA_OVERRIDE.key -> getTableSchema.toString) +
      (DataSourceWriteOptions.TABLE_TYPE.key -> targetTableType)

    // Append (encoded) updating actions
    writeParams += PAYLOAD_UPDATE_CONDITION_AND_ASSIGNMENTS ->
      serializeConditionalAssignments(updatingActions.map(a => (a.condition, a.assignments)))
    // Append (encoded) deleting actions
    deletingActions.headOption match {
      case Some(DeleteAction(condition)) =>
        writeParams += PAYLOAD_DELETE_CONDITION -> serializeConditionalAssignments(Seq(condition -> Seq.empty))
      case _ => // no-op
    }
    // Append (encoded) inserting actions
    writeParams += PAYLOAD_INSERT_CONDITION_AND_ASSIGNMENTS ->
      serializeConditionalAssignments(insertingActions.map(a => (a.condition, a.assignments)), validateInsertingAssignmentExpression)
    // Add a schema of the expected "joined" output of the [[sourceTable]] and [[targetTable]]
    writeParams += PAYLOAD_EXPECTED_COMBINED_SCHEMA -> encodeAsBase64String(toStructType(joinedExpectedOutput))
    // Remove the meta fields from the sourceDF as we do not need these when writing.
    val trimmedSourceDF = removeMetaFields(sourceDF)

    // Supply original record's Avro schema to provided to [[ExpressionPayload]]
    writeParams += (PAYLOAD_RECORD_AVRO_SCHEMA ->
      convertStructTypeToAvroSchema(trimmedSourceDF.schema, "record", "").toString)

    val (success, _, _, _, _, _) = HoodieSparkSqlWriter.write(sparkSession.sqlContext, SaveMode.Append, writeParams, trimmedSourceDF)
    if (!success) {
      throw new HoodieException("Merge into Hoodie table command failed")
    }
  }

  /**
   * If there are not matched actions, we only execute the insert operation.
   *
   * TODO unify w/ executeUpsert
   */
  private def executeInsertOnly(sourceDF: DataFrame, parameters: Map[String, String]): Unit = {
    var writeParams = parameters +
      (OPERATION.key -> INSERT_OPERATION_OPT_VAL) +
      (HoodieWriteConfig.WRITE_SCHEMA_OVERRIDE.key -> getTableSchema.toString)

    writeParams += (PAYLOAD_INSERT_CONDITION_AND_ASSIGNMENTS ->
      serializeConditionalAssignments(insertingActions.map(a => (a.condition, a.assignments))))

    // Remove the meta fields from the sourceDF as we do not need these when writing.
    val sourceDFWithoutMetaFields = removeMetaFields(sourceDF)
    HoodieSparkSqlWriter.write(sparkSession.sqlContext, SaveMode.Append, writeParams, sourceDFWithoutMetaFields)
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
                                              validateAssignmentExpression: Function[Expression, Unit] = _ => {}): String = {
    val boundConditionalAssignments =
      conditionalAssignments.map {
        case (condition, assignments) =>
          val boundCondition = condition.map(bindReferences).getOrElse(Literal.create(true, BooleanType))
          // NOTE: For deleting actions there's no assignments provided and no re-ordering is required.
          //       All other actions are expected to provide assignments correspondent to every field
          //       of the [[targetTable]] being assigned
          val reorderedAssignments = if (assignments.nonEmpty) {
            reorderAssignments(assignments)
          } else {
            Seq.empty
          }
          // NOTE: We need to re-order assignments to follow the ordering of the attributes
          //       of the target table, such that the resulting output produced after execution
          //       of these expressions could be inserted into the target table as is
          val boundAssignmentExprs = reorderedAssignments.map {
            case Assignment(attr: Attribute, value) =>
              val boundExpr = bindReferences(value)
              validateAssignmentExpression(boundExpr)
              // Alias resulting expression w/ target table's expected column name, as well as
              // do casting if necessary
              Alias(castIfNeeded(boundExpr, attr.dataType, sparkSession.sqlContext.conf), attr.name)()
            }

          boundCondition -> boundAssignmentExprs
      }.toMap

    encodeAsBase64String(boundConditionalAssignments)
  }

  /**
   * Re-orders assignment expressions to adhere to the ordering of that of [[targetTable]]
   */
  private def reorderAssignments(assignments: Seq[Assignment]): Seq[Assignment] = {
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
            throw new AnalysisException(s"Assignment expressions have to assign every attribute of target table " +
              s"(provided: `${assignments.map(_.sql).mkString(",")}`")
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

    expr transform {
      case attr: AttributeReference =>
        val index = joinedExpectedOutputAttributes.indexWhere(attributeEquals(_, attr))
        if (index == -1) {
            throw new AnalysisException(s"Can't find `${attr.qualifiedName}` attribute in either source or the target " +
              s"tables of the MERGE INTO statement (${joinedExpectedOutputAttributes.map(_.qualifiedName)})");
          }
          BoundReference(index, attr.dataType, attr.nullable)
      case other => other
    }
  }

  /**
   * Output of the expected (left) join of the a) [[sourceTable]] dataset (potentially amended w/ primary-key,
   * pre-combine columns) with b) existing [[targetTable]]
   */
  private def joinedExpectedOutput: Seq[Attribute] = {
    // NOTE: We're relying on [[sourceDataset]] here instead of [[mergeInto.sourceTable]],
    //       as it could be amended to add missing primary-key and/or pre-combine columns.
    //       Please check [[sourceDataset]] scala-doc for more details
    sourceDataset.queryExecution.analyzed.output ++ mergeInto.targetTable.output
  }

  private def resolvesToSourceAttributeReferenceExpr(expr: Expression): Boolean = {
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

    // Enable the hive sync by default if spark have enable the hive metastore.
    val enableHive = isUsingHiveCatalog(sparkSession)
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
        HoodieSyncConfig.META_SYNC_ENABLED.key -> enableHive.toString,
        HiveSyncConfigHolder.HIVE_SYNC_MODE.key -> hiveSyncConfig.getString(HiveSyncConfigHolder.HIVE_SYNC_MODE),
        HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key -> enableHive.toString,
        HoodieSyncConfig.META_SYNC_DATABASE_NAME.key -> targetTableDb,
        HoodieSyncConfig.META_SYNC_TABLE_NAME.key -> targetTableName,
        HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE.key -> hiveSyncConfig.getBoolean(HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE).toString,
        HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key -> tableConfig.getPartitionFieldProp,
        HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key -> hiveSyncConfig.getString(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS),
        HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key -> hoodieProps.getString(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "200"), // set the default parallelism to 200 for sql
        HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key -> hoodieProps.getString(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "200"),
        HoodieWriteConfig.DELETE_PARALLELISM_VALUE.key -> hoodieProps.getString(HoodieWriteConfig.DELETE_PARALLELISM_VALUE.key, "200"),
        SqlKeyGenerator.PARTITION_SCHEMA -> partitionSchema.toDDL,
        // NOTE: We have to explicitly override following configs to make sure no schema validation is performed
        //       as schema of the incoming dataset might be diverging from the table's schema (full schemas'
        //       compatibility b/w table's schema and incoming one is not necessary in this case since we can
        //       be cherry-picking only selected columns from the incoming dataset to be inserted/updated in the
        //       target table, ie partially updating)
        AVRO_SCHEMA_VALIDATE_ENABLE.key -> "false",
        RECONCILE_SCHEMA.key -> "false",
        "hoodie.datasource.write.schema.canonicalize" -> "false"
      )
        .filter { case (_, v) => v != null }
    }
  }

  def validate(mit: MergeIntoTable): Unit = {
    // TODO validate MIT adheres to Hudi's constraints
    //       - Merge-on condition can only ref primary-key columns
    //       - Source table has to contain column mapping into pre-combine one (if defined for the target table)
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
      assert(insert.assignments.length == targetTableSchema.length,
        s"The number of insert assignments[${insert.assignments.length}] must equal to the " +
          s"targetTable field size[${targetTableSchema.length}]"))

  }

  private def checkUpdatingActions(updateActions: Seq[UpdateAction]): Unit = {
    if (updateActions.length > 1) {
      throw new AnalysisException(s"Only one updating action is supported in MERGE INTO statement (provided ${updateActions.length})")
    }

    updateActions.foreach(update =>
      assert(update.assignments.length == targetTableSchema.length,
        s"The number of update assignments[${update.assignments.length}] must equal to the " +
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

  def toStructType(attrs: Seq[Attribute]): StructType =
    StructType(attrs.map(a => StructField(a.qualifiedName.replace('.', '_'), a.dataType, a.nullable, a.metadata)))

  def encodeAsBase64String(any: Any): String =
    Base64.getEncoder.encodeToString(SerDeUtils.toBytes(any))

}
