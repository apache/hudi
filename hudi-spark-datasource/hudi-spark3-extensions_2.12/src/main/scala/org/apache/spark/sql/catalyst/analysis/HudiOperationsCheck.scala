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

package org.apache.spark.sql.catalyst.analysis

import org.apache.hudi.execution.HudiSQLUtils
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, InSubquery, Literal, Not}
import org.apache.spark.sql.catalyst.merge.HudiMergeIntoTable
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, LogicalPlan}
import org.apache.spark.sql.execution.command._

/**
  * check rule which can intercept unSupportOperation for hudi
  */
class HudiOperationsCheck(spark: SparkSession) extends (LogicalPlan => Unit) {
  val catalog = spark.sessionState.catalog

  override def apply(plan: LogicalPlan): Unit = {
    plan foreach {
      case AlterTableAddPartitionCommand(tableName, _, _) if (checkHudiTable(tableName)) =>
        throw new AnalysisException("AlterTableAddPartitionCommand are not currently supported in HUDI")

      case a: AlterTableDropPartitionCommand if (checkHudiTable(a.tableName)) =>
        throw new AnalysisException("AlterTableDropPartitionCommand are not currently supported in HUDI")

      case AlterTableChangeColumnCommand(tableName, _, _) if (checkHudiTable(tableName)) =>
        throw new AnalysisException("AlterTableChangeColumnCommand are not currently supported in HUDI")

      case AlterTableRenamePartitionCommand(tableName, _, _) if (checkHudiTable(tableName)) =>
        throw new AnalysisException("AlterTableRenamePartitionCommand are not currently supported in HUDI")

      case AlterTableRecoverPartitionsCommand(tableName, _) if (checkHudiTable(tableName)) =>
        throw new AnalysisException("AlterTableRecoverPartitionsCommand are not currently supported in HUDI")

      case AlterTableSetLocationCommand(tableName, _, _) if (checkHudiTable(tableName)) =>
        throw new AnalysisException("AlterTableSetLocationCommand are not currently supported in HUDI")

      case DeleteFromTable(target, Some(condition)) if (hasNullAwarePredicateWithNot(condition) && checkHudiTable(target)) =>
        throw new AnalysisException("Null-aware predicate sub-squeries a are not currently supported for DELETE")

      case HudiMergeIntoTable(target, _, _, _, noMatchedClauses, _) =>
        noMatchedClauses.map { m =>
          m.resolvedActions.foreach { action =>
            if (action.targetColNameParts.size > 1) {
              throw new AnalysisException(s"cannot insert nested values: ${action.targetColNameParts.mkString("<") }")
            }
          }

          val targetCol = m.resolvedActions.map(_.targetColNameParts.head)
          if (targetCol.distinct.size < targetCol.size) {
            throw new AnalysisException(s"find duplcate cols for insert")
          }

          val tablePath = HudiSQLUtils.getTablePathFromRelation(target, spark)
          val (preCombieKey, recordAndPartitionKey) = HudiSQLUtils.getRequredFieldsFromTableConf(tablePath)
          val checkKeys = Seq(preCombieKey) ++ recordAndPartitionKey
          checkKeys.foreach { key =>
            m.resolvedActions.find(a => a.targetColNameParts.head.equals(key)).getOrElse {
              throw new AnalysisException(s"insert clause must contains record key, combineKey, and partitionKey: ${checkKeys.mkString(",")}" +
                s" + now ${key} is missing, pls check it ")
            }.expr match {
              case Literal(null, _) =>
                throw new AnalysisException(s"insert clause must contains record key, combineKey, and partitionKey: ${checkKeys.mkString(",")}" +
                  s" + now ${key} is missing, pls check it ")
              case _ =>
            }
          }

        }
      case _ =>
    }
  }

  /**
    * spark do not support not in Subquery
    */
  private def hasNullAwarePredicateWithNot(cond: Expression): Boolean = {
    cond.find {
      case Not(expr) if expr.find(_.isInstanceOf[InSubquery]).isDefined => true
      case _ => false
    }.isDefined
  }

  /**
    * Check is hudi table or not
    */
  private def checkHudiTable(tableName: TableIdentifier): Boolean = {
    catalog.getTableMetadata(tableName).storage.inputFormat.getOrElse("").contains("Hoodie")
  }

  private def checkHudiTable(target: LogicalPlan): Boolean = {
    val tablePath = HudiSQLUtils.getTablePathFromRelation(target, spark)
    if (tablePath.isEmpty) {
      false
    } else {
      val tableConfig = HudiSQLUtils.getPropertiesFromTableConfigCache(tablePath)
      val tableInputFormat = HudiSQLUtils.getHoodiePropsFromRelation(target, spark).getOrElse("inputformat", "")

      if (tableConfig.getProperty("hoodie.table.type", "COPY_ON_WRITE").equals("MERGE_ON_READ")
        && tableInputFormat.contains("HoodieParquetInputFormat")) {
        throw new AnalysisException("for hudi mor table, IUD operations need mor_rt table but find mor_ro table")
      } else {
        true
      }
    }
  }

}
