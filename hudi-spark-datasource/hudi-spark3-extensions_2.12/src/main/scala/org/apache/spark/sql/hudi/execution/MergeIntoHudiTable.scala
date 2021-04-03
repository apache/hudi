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

package org.apache.spark.sql.hudi.execution

import org.apache.hudi.DataSourceWriteOptions.{DEFAULT_PAYLOAD_OPT_VAL, PAYLOAD_CLASS_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.common.model.{OverwriteNonDefaultsWithLatestAvroPayload, WriteOperationType}
import org.apache.hudi.execution.HudiSQLUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.merge._
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BasePredicate, Expression, Literal, PredicateHelper, UnsafeProjection}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.catalyst.merge.{HudiMergeClause, HudiMergeInsertClause}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * merge command, execute merge operation for hudi
  */
case class MergeIntoHudiTable(
    target: LogicalPlan,
    source: LogicalPlan,
    joinCondition: Expression,
    matchedClauses: Seq[HudiMergeClause],
    noMatchedClause: Seq[HudiMergeInsertClause],
    finalSchema: StructType,
    trimmedSchema: StructType) extends RunnableCommand with Logging with PredicateHelper {

  var tableMeta: Map[String, String] = null

  lazy val isRecordKeyJoin = {
    val recordKeyFields = tableMeta.get(RECORDKEY_FIELD_OPT_KEY).get.split(",").map(_.trim).filter(!_.isEmpty)
    val intersect = joinCondition.references.intersect(target.outputSet).toSeq
    if (recordKeyFields.size == 1 && intersect.size ==1 && intersect(0).name.equalsIgnoreCase(recordKeyFields(0))) {
      true
    } else {
      false
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    tableMeta = HudiSQLUtils.getHoodiePropsFromRelation(target, sparkSession)
    val enableHive = "hive" == sparkSession.sessionState.conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION)
    if (isRecordKeyJoin) {
      val rawDataFrame = Dataset.ofRows(sparkSession, source)

      val updateClause = matchedClauses.collect { case x: HudiMergeUpdateClause => x }
      val deleteClause = matchedClauses.collect { case x: HudiMergeDeleteClause => x }
      val canUpdateDirectly = (updateClause.size == 1 && updateClause(0).condition.isEmpty && updateClause(0).isStarAction) || updateClause.isEmpty
      val canInsertDirectly = noMatchedClause.size == 1 && noMatchedClause(0).condition.isEmpty && noMatchedClause(0).isStarAction || noMatchedClause.isEmpty
      if (deleteClause.isEmpty && canUpdateDirectly && canInsertDirectly) {
        HudiSQLUtils.update(rawDataFrame, HudiSQLUtils.buildDefaultParameter(tableMeta, enableHive), sparkSession, enableHive = enableHive)
      } else if (deleteClause.size == 1 && deleteClause(0).condition.isEmpty &&  updateClause.isEmpty && noMatchedClause.isEmpty) {
        // only delete
        HudiSQLUtils.update(rawDataFrame, HudiSQLUtils.buildDefaultParameter(tableMeta, enableHive),
          sparkSession, WriteOperationType.DELETE, enableHive)
      } else {
        doMergeNormal(sparkSession, enableHive)
      }
    } else {
      doMergeNormal(sparkSession, enableHive)
    }
    sparkSession.catalog.refreshTable(tableMeta.get("currentTable").get)
    Seq.empty[Row]
  }

  /**
    * trim updateClause, which will reduce the data size in memory
    * when trimming condition is satisfied
    * we no need unnecessary column for update, just set them null,
    * hudi will deal with null value itself
    */
  private def doTrimMergeUpdateClause(): Seq[HudiMergeClause] = {
    val unWantedTargetColumns = target.schema.filter(p => trimmedSchema.find(t => conf.resolver(p.name, t.name)).isDefined)
    val trimmedMatchedClauses = matchedClauses.map {
      case u: HudiMergeUpdateClause =>
        val trimActions = u.resolvedActions.map { act =>
          if (unWantedTargetColumns.exists(p => conf.resolver(p.name, act.targetColNameParts.head))) {
            act.copy(act.targetColNameParts, Literal(null, act.expr.dataType))
          } else {
            act
          }
        }
        u.copy(u.condition, trimActions)
      case other => other
    }
    trimmedMatchedClauses
  }

  /**
    * normal deleteClause output, which will reduce the data size in memory
    * we just keep necessary column, otherwise set them to null
    */
  private def doNormalMergeDeleteClause(): Seq[Expression] = {

    finalSchema.map { f =>
      target.resolve(Seq(f.name), conf.resolver).get
    }
  }

  /**
    * now only OverwriteNonDefaultsWithLatestAvroPayload allow trim
    */
  private def canTrim(): Boolean = {
    val tablePath = tableMeta.get("path").get
    val payload = HudiSQLUtils.getPropertiesFromTableConfigCache(tablePath).getProperty(PAYLOAD_CLASS_OPT_KEY, DEFAULT_PAYLOAD_OPT_VAL)

    if (payload.equals(classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName)) {
      true
    } else {
      false
    }
  }

  /**
    * do merge Normal
    * step1: select join type
    * step2: merge all clause condition
    * step3: build joinPlan and add _hoodie_is_delete field to mark delete rows
    * step4: build mapPartitition function to get final result for merging
    */
  private def doMergeNormal(sparkSession: SparkSession, enableHive: Boolean): Unit = {
    val doTrim = if (canTrim() && trimmedSchema != null) true else false

    val joinTye: String = "right_outer"


    val trimmedMatchedClauses = if (doTrim) doTrimMergeUpdateClause() else matchedClauses

    val joinDF = buildJoinDataFrame(sparkSession, joinTye, doTrim)

    logInfo(
      s"""
         |build join with:
         |targetPlan: ${target.toString()}
         |sourcePlan: ${source.toString()}
         | joinType: ${joinTye}
         | doTrimTarget: ${doTrim}
       """.stripMargin)

    val joinPlan = joinDF.queryExecution.analyzed
    val fSchema = finalSchema.add(HudiSQLUtils.MERGE_MARKER, StringType, true)

    def resolveOnJoinPlan(exprSeq: Seq[Expression]): Seq[Expression] = {
      exprSeq.map { expr => HudiMergeIntoUtils.tryResolveReferences(sparkSession)(expr, joinPlan)}
    }

    def createClauseOutput(clause: HudiMergeClause): Seq[Expression] = {
      val resolveExprSeq = clause match {
        case u: HudiMergeUpdateClause =>
          u.resolvedActions.map(_.expr) :+ joinPlan.resolve(Seq(HudiSQLUtils.MERGE_MARKER), conf.resolver).get
        case i: HudiMergeInsertClause =>
          i.resolvedActions.map(_.expr) :+ joinPlan.resolve(Seq(HudiSQLUtils.MERGE_MARKER), conf.resolver).get
        case _: HudiMergeDeleteClause =>
          doNormalMergeDeleteClause() :+ Literal.create("D", StringType)
      }
      resolveOnJoinPlan(resolveExprSeq)
    }

    def createCondition(clause: HudiMergeClause): Expression = {
      // if condition is None, then expression always true
      val condExpr = clause.condition.getOrElse(Literal(true))
      resolveOnJoinPlan(Seq(condExpr)).head
    }

    val updateClause = trimmedMatchedClauses.collect { case x: HudiMergeUpdateClause => x }
    val deleteClause = trimmedMatchedClauses.collect { case x: HudiMergeDeleteClause => x }

    val insertClause = noMatchedClause
    val joinedRowEncoder = RowEncoder(joinPlan.schema)
    val finalOutputEncoder = RowEncoder(fSchema).resolveAndBind()
    val uidProcess = new MergeIntoCommand.UIDProcess(
      updateClause.map(createCondition(_)),
      updateClause.map(createClauseOutput(_)),
      deleteClause.map(createCondition(_)),
      deleteClause.map(createClauseOutput(_)),
      insertClause.map(createCondition(_)),
      insertClause.map(createClauseOutput(_)),
      joinPlan.output,
      joinedRowEncoder,
      finalOutputEncoder)

    val rawDataFrame = Dataset.ofRows(sparkSession, joinPlan).mapPartitions(uidProcess.processUID(_))(finalOutputEncoder)

    logInfo(s"construct merge data: ${rawDataFrame.schema.toString()} for hudi finished, start merge .....")

    if (finalSchema.fields.exists(p => p.name.equalsIgnoreCase("_hoodie_record_key"))) {
      HudiSQLUtils.merge(rawDataFrame, sparkSession, tableMeta, true, enableHive)
    } else {
      HudiSQLUtils.merge(rawDataFrame, sparkSession, tableMeta, false, enableHive)
    }

  }

  /**
    * buildJoinDataFrame
    * add "exsit_on_target" column to mark the row belong to target
    * add "exsit_on_source" column to mark the row belong to source
    */
  private def buildJoinDataFrame(sparkSession: SparkSession, joinType: String, doTrim: Boolean = false): DataFrame = {
    val trimmedTarget = if (doTrim) {
      Project(target.output.filter { col => trimmedSchema.exists(p => conf.resolver(p.name, col.name))}, target)
    } else {
      target
    }
    // try to push some join filter
    val targetPredicates = splitConjunctivePredicates(joinCondition)
      .find(p => p.references.subsetOf(trimmedTarget.outputSet)).reduceLeftOption(And)

    var targetDF = Dataset.ofRows(sparkSession, trimmedTarget).withColumn("exist_on_target", lit(true))
    val sourceDF = Dataset.ofRows(sparkSession, source).withColumn("exist_on_source", lit(true))

    targetPredicates.map(f => targetDF.filter(Column(f))).getOrElse(targetDF)
      .join(sourceDF, new Column(joinCondition), joinType)
      .withColumn(HudiSQLUtils.MERGE_MARKER, combineClauseConditions())
  }

  /**
    * combine all clause conditions
    * build clause condition "_hoodie_is_deleted"
    * case 1 = 1
    * when isnotnull('exsit_on_target') && isnotnull('exsit_on_source') && updateCondition then 'U'
    * when isnotnull('exsit_on_target') && isnotnull('exsit_on_source') && deleteCondition then 'D'
    * when isnull('exsit_on_target') && isnotnull('exsit_on_source') && insertCondtition then 'I'
    * no need to consider when isnotnull('exsit_on_target') && isnull('exsit_on_source') && insertCondtition then 'O' since hudi has pk
    * else 'O'
    * end
    */
  private def combineClauseConditions(): Column = {
    val columnList = new ArrayBuffer[(Column, String)]()

    val matchedColumn = col("exist_on_target").isNotNull.and(col("exist_on_source").isNotNull)
    columnList.append((matchedColumn, "U"))

    val noMatchedColumn = col("exist_on_target").isNull.and(col("exist_on_source").isNotNull)
    columnList.append((noMatchedColumn, "I"))

    var mergeCondition: Column = null
    columnList.foreach { case (column, index) =>
      if (mergeCondition == null) {
        mergeCondition = when(column, lit(index))
      } else {
        mergeCondition = mergeCondition.when(column, lit(index))
      }
    }
    mergeCondition.otherwise(lit("O"))
  }
}

object MergeIntoCommand {
  class UIDProcess(
      updateConditions: Seq[Expression],
      updateOutput: Seq[Seq[Expression]],
      deleteConditions: Seq[Expression],
      deleteOutput: Seq[Seq[Expression]],
      insertConditions: Seq[Expression],
      insertOutput: Seq[Seq[Expression]],
      joinAttributes: Seq[Attribute],
      joinedRowEncoder: ExpressionEncoder[Row],
      finalOutputRowEncoder: ExpressionEncoder[Row]) extends Serializable {

    private def matchProjections(
        actions: Seq[(BasePredicate, UnsafeProjection)],
        inputRow: InternalRow): InternalRow = {
      // find the first one that satisfies the predicate, then do unsafeProject
      val p = actions.find {
        case (predicate, _) => predicate.eval(inputRow)
      }

      // apply unsafeProject to produce an output row, if no unsafeProject found just return null
      p match {
        case Some((_, projection)) =>
          projection.apply(inputRow)
        case _ =>
          null
      }

    }

    def processUID(rowIterator: Iterator[Row]): Iterator[Row] = {
      val toRow = joinedRowEncoder.createSerializer()
      val fromRow = finalOutputRowEncoder.createDeserializer()
      val updatePreds = updateConditions.map(GeneratePredicate.generate(_, joinAttributes))
      val updateProject = updateOutput.map(UnsafeProjection.create(_, joinAttributes))

      val deletePreds = deleteConditions.map(GeneratePredicate.generate(_, joinAttributes))
      val deleteProject = deleteOutput.map(UnsafeProjection.create(_, joinAttributes))

      val insertPreds = insertConditions.map(GeneratePredicate.generate(_, joinAttributes))
      val insertProject = insertOutput.map(UnsafeProjection.create(_, joinAttributes))

      rowIterator.map(toRow).filterNot { irow =>
        irow.getString(joinAttributes.length - 1) == "O"
      }.map { irow =>
        irow.getString(joinAttributes.length - 1) match {
          case "U" =>
            val zipPredsAndProjects = (updatePreds ++ deletePreds) zip (updateProject ++ deleteProject)
            matchProjections(zipPredsAndProjects, irow)
          case "I" =>
            val zipInsertPredsAndProjects = insertPreds zip  insertProject
            matchProjections(zipInsertPredsAndProjects, irow)
        }
      }.filter(row => row != null).map { irow =>
        val finalProject = UnsafeProjection.create(finalOutputRowEncoder.schema)
        fromRow(finalProject(irow))
      }
    }
  }
}
