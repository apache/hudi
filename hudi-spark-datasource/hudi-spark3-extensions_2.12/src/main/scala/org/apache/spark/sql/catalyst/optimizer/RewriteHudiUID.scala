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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.execution.HudiSQLUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Alias, AnsiCast, AttributeReference, Cast, CreateNamedStruct, Expression, GetStructField, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.merge._
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, LogicalPlan, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hudi.execution.{DeleteFromHudiTable, MergeIntoHudiTable, UpdateFromHudiTable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.mutable

/**
  * convert IUD operation to IUD comand for execution
  */
object RewriteHudiUID extends Rule[LogicalPlan] {

  val conf: SQLConf = SparkSession.active.sessionState.conf

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case DeleteFromTable(table: LogicalPlan, cond) if (HudiSQLUtils.isHudiRelation(table)) =>
      DeleteFromHudiTable(table, cond)
    case update @ UpdateTable(table: LogicalPlan, _, _) if HudiSQLUtils.isHudiRelation(table) =>
      convertToUpdateCommand(update)
    case merge: HudiMergeIntoTable =>
      convertToMergeCommand(merge)
  }

  /**
    * convert logical updateTable to command
    */
  def convertToUpdateCommand(update: UpdateTable): UpdateFromHudiTable = {
    val UpdateTable(table, assignments, condition) = update
    val (cols, expressions) = assignments.map(a => a.key.asInstanceOf[Expression] -> a.value).unzip
    val targetColNameParts = cols.map(HudiMergeIntoUtils.getTargetColNameParts(_))
    val alignedUpdateExprs = generateUpdateExpressions(table.output, targetColNameParts, expressions, conf.resolver)
    UpdateFromHudiTable(table, alignedUpdateExprs, condition)
  }

  /**
    * add special hoodie fields to trim
    * we add hudi MetaCols since those fields can build taggedRdd directly which will skip index find and save time
    * however if target out size is not big enough, it is better to drop those special column
    */
  private def addHoodieSpecialColumn2Trim(trimmed: mutable.HashSet[StructField], target: LogicalPlan): Unit = {
    val tablePath = HudiSQLUtils.getTablePathFromRelation(target, SparkSession.active)
    val (preCombineKey, recordKeyAndPartitionKey) = HudiSQLUtils.getRequredFieldsFromTableConf(tablePath)
    if (target.schema.length > HoodieRecord.HOODIE_META_COLUMNS.size * 2
      && target.schema.exists(_.name.equals(HoodieRecord.COMMIT_TIME_METADATA_FIELD))) {
      trimmed.add(target.schema.find(p => p.name == HoodieRecord.COMMIT_TIME_METADATA_FIELD).get)
      trimmed.add(target.schema.find(p => p.name == HoodieRecord.RECORD_KEY_METADATA_FIELD).get)
      trimmed.add(target.schema.find(p => p.name == HoodieRecord.PARTITION_PATH_METADATA_FIELD).get)
      trimmed.add(target.schema.find(p => p.name == HoodieRecord.FILENAME_METADATA_FIELD).get)
    } else {
      recordKeyAndPartitionKey.foreach { key =>
        trimmed.add(target.schema.find(p => p.name == key).get)
      }
    }
    trimmed.add(target.schema.find(p => p.name == preCombineKey).get)

  }

  /**
    * convert logical MergeTable to command
    */
  def convertToMergeCommand(merge: HudiMergeIntoTable): MergeIntoHudiTable = {
    val HudiMergeIntoTable(target, source, joinCondition, matchedClauses, noMatchedClauses, finalSchema) = merge
    val trimmedStructFields = mutable.HashSet[StructField]()
    addHoodieSpecialColumn2Trim(trimmedStructFields, target)

    val processedMatched = matchedClauses.map {
      case m: HudiMergeUpdateClause =>
        // get any new columns which are in the insert clause, but not in the target output or this update clause
        val existingColumns = m.resolvedActions.map(_.targetColNameParts.head) ++ target.output.map(_.name)

        val newColsFromInsert = noMatchedClauses.toSeq.flatMap {
          _.resolvedActions.filterNot { insertAct =>
            existingColumns.exists(colName => conf.resolver(insertAct.targetColNameParts.head, colName))
          }
        }.map { action =>
          AttributeReference(action.targetColNameParts.head, action.dataType)()
        }
        // Add construct operations for columns that the insert clause will add
        val newOpsFromInset = newColsFromInsert.map { col =>
          UpdateOperation(Seq(col.name), Literal(null, col.dataType))
        }
        // Get the operations for columns that already exist...
        val existingUpdateOps = m.resolvedActions.map { col =>
          UpdateOperation(col.targetColNameParts, col.expr)
        }

        existingUpdateOps.foreach { updateOp =>
          trimmedStructFields.add(target.schema.find(f => conf.resolver(updateOp.targetColNameParts.head, f.name)).get)
        }

        //
        val newUpdateOpsFromTargetSchema = target.output.filterNot { col =>
          m.resolvedActions.exists { act =>
            conf.resolver(act.targetColNameParts.head, col.name)
          }
        }.map(col => UpdateOperation(Seq(col.name), col))

        val finalSchemaExprs = finalSchema.map { field =>
          target.resolve(Seq(field.name), conf.resolver).getOrElse {
            AttributeReference(field.name, field.dataType)()
          }
        }

        val alignedExprs = generateUpdateExpressions(
          finalSchemaExprs,
          existingUpdateOps ++ newUpdateOpsFromTargetSchema ++ newOpsFromInset,
          conf.resolver)
        val alignedActions = alignedExprs.zip(finalSchemaExprs).map {
          case (expr, attr) =>
            HudiMergeAction(Seq(attr.name), expr)
        }
        m.copy(m.condition, alignedActions)
      case d: HudiMergeDeleteClause => d
    }

    val processedNotMatched = noMatchedClauses.map { m =>

      val newActionsFromTargetSchema = target.output.filterNot { col =>
        m.resolvedActions.exists { act =>
          conf.resolver(col.name, act.targetColNameParts.head)
        }
      }.map(col => HudiMergeAction(Seq(col.name), Literal(null, col.dataType)))

      val newActionsFromUpdate = matchedClauses.flatMap {
        _.resolvedActions.filterNot { updateAct =>
          m.resolvedActions.exists { act =>
            conf.resolver(updateAct.targetColNameParts.head, act.targetColNameParts.head)
          }
        }
      }.map { updateAct =>
        HudiMergeAction(updateAct.targetColNameParts, Literal(null, updateAct.dataType))
      }

      if (!m.isStarAction) {
        m.resolvedActions.foreach { act =>
          val f = target.schema.find(p => conf.resolver(act.targetColNameParts.head, p.name))
          if (f.isDefined) trimmedStructFields.add(f.get)
        }
      }

      val alignedActions: Seq[HudiMergeAction] = finalSchema.map { finalAttribute =>
        (m.resolvedActions ++ newActionsFromTargetSchema ++ newActionsFromUpdate).find { a =>
          conf.resolver(finalAttribute.name, a.targetColNameParts.head)
        }.map { a =>
          HudiMergeAction(Seq(finalAttribute.name), castIfNeeded(target.resolve(Seq(finalAttribute.name), conf.resolver).getOrElse{
            AttributeReference(finalAttribute.name, finalAttribute.dataType)()
          }, a.expr, conf.resolver))
        }.getOrElse {
          throw new SparkException(s"unable to find column: ${finalAttribute.name}")
        }
      }
      //
      (Seq(joinCondition) ++ ( (matchedClauses ++ noMatchedClauses).filter(_.condition.nonEmpty)  )).foreach { cond =>
        cond.references.foreach {
          case a: AttributeReference if target.outputSet.contains(a) =>
            val f = target.schema.find(p => conf.resolver(a.name, p.name))
            if (f.isDefined) trimmedStructFields.add(f.get)
          case _ =>
        }
      }
      //
      m.copy(m.condition, alignedActions)
    }
    MergeIntoHudiTable(target, source, joinCondition, processedMatched,
      processedNotMatched, finalSchema, StructType(trimmedStructFields.toList))
  }

  /**
   * Specifies on operation that updates a target column with the given expression.
   * The target column may or may no be a nested field and it is specified as a full quoted name
   */
  case class UpdateOperation(targetColNameParts: Seq[String], updateExpr: Expression)

  /**
   * cast function which modified checkField from spark TableOutputResolver.scala
   */
  protected def castIfNeeded(originAttr: NamedExpression, expr: Expression, resolver: Resolver): Expression = {
    val storeAssignmentPolicy = conf.storeAssignmentPolicy

    lazy val outputField = {
      // do cast
      storeAssignmentPolicy match {
        case StoreAssignmentPolicy.ANSI =>
          AnsiCast(expr, originAttr.dataType, Option(conf.sessionLocalTimeZone))
        case _ =>
          Cast(expr, originAttr.dataType, Option(conf.sessionLocalTimeZone))
      }
    }

    // if hudi schema filed is non-null , the follow check  will throw exception. maybe we need a parameter to control this behavior
    storeAssignmentPolicy match {
      case _ if originAttr.dataType.sameType(expr.dataType) =>
        expr
      case StoreAssignmentPolicy.STRICT | StoreAssignmentPolicy.ANSI =>
        val errors = new mutable.ArrayBuffer[String]()
        val canWrite = DataType.canWrite(expr.dataType,
          originAttr.dataType, byName = true, resolver, originAttr.name, storeAssignmentPolicy, err => errors += err)
        if (expr.nullable && !originAttr.nullable) {
          errors += s"Cannot write nullable value to non-null column ${originAttr.name} for HudiTable"
        }
        if (errors.isEmpty && canWrite) {
          outputField
        } else {
          throw new SparkException(errors.mkString("/n"))
        }
    }
  }

  /**
    * generate update expressions, support structType update
    */
  protected def generateUpdateExpressions(
       targetCols: Seq[NamedExpression],
       updateOps: Seq[UpdateOperation],
       resolver: Resolver,
       pathPrefix: Seq[String] = Nil): Seq[Expression] = {
    // check that the head of nameParts in each update operation can match a target col. this avoids
    // silently ignoring invalid column names specified in update operation
    updateOps.foreach { u =>
      if (!targetCols.exists(f => resolver(f.name, u.targetColNameParts.head))) {
        throw new SparkException(s"update set column not found:  " +
          s"SET column ${(pathPrefix :+ u.targetColNameParts.head).mkString(".")} not found given columns:" +
          s" ${targetCols.map(col => (pathPrefix :+ col.name).mkString("."))}")
      }
    }

    // Transform each targetCol to a possible updated expression
    targetCols.map { targetCol =>
      // The prefix of a update path matches the current targetCol path.
      val prefixMatchedOps =
        updateOps.filter(u => resolver(u.targetColNameParts.head, targetCol.name))
      // No prefix matched this target column, return its original expression
      if (prefixMatchedOps.isEmpty) {
        targetCol
      } else {
        //The update operation whose path exactly matched the current targetCol path.
        val fullMatchedOp = prefixMatchedOps.find(_.targetColNameParts.size == 1)
        if (fullMatchedOp.isDefined) {
          // if a full match is found, then it should be the ONLY prefix match. Any other match
          // would be a conflict, whether it is a full match or prefix-only. For examples,
          // when users are updating a nested column a.b, the can't simultaneously update a
          // descendant of a.b such as a.b.c
          if (prefixMatchedOps.size > 1) {
            throw new SparkException(s"There is aconflick from these SET columns:" +
              s" ${prefixMatchedOps.map(op => (pathPrefix ++ op.targetColNameParts).mkString("."))}")
          }
          castIfNeeded(targetCol, fullMatchedOp.get.updateExpr, conf.resolver)
        } else {
          // So there are prefix-matched update operations, but none of them is full match, Then
          // that means targetCol is a complex data type, so we recursively pass along the update
          // operation to its children
          targetCol.dataType match {
            case StructType(fields) =>
              val fieldExpr = targetCol
              val childExprs = fields.zipWithIndex.map { case (field, ordinal) =>
                Alias(GetStructField(fieldExpr, ordinal, Some(field.name)), field.name)()
              }
              // Recursively apply update operations to the children
              val updatedChildExprs = generateUpdateExpressions(
                childExprs,
                prefixMatchedOps.map(u => u.copy(targetColNameParts = u.targetColNameParts.tail)),
                resolver,
                pathPrefix :+ targetCol.name)
              // Reconstruct the expression for targetCOl using its possibley updated children
              val namedStructExprs = fields.zip(updatedChildExprs).flatMap { case (field, expr) => Seq(Literal(field.name), expr)}
              CreateNamedStruct(namedStructExprs)
            case otherType =>
              throw new SparkException(s"update nested fields is only suppory for StructType, but you are trying to update" +
                s"a field of ${(pathPrefix :+ targetCol.name).mkString(".")}, which is of type ${otherType}")
          }
        }
      }
    }
  }

  private def generateUpdateExpressions(
      targetCols: Seq[NamedExpression],
      nameParts: Seq[Seq[String]],
      updateExprs: Seq[Expression], resolver: Resolver): Seq[Expression] = {
    val updateOps = nameParts.zip(updateExprs).map {
      case (namePart, expr) => UpdateOperation(namePart, expr)
    }
    generateUpdateExpressions(targetCols, updateOps, resolver)
  }

}
