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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.util

import scala.collection.JavaConverters._

object HoodieProcedureUtils {

  // Timeline type constants
  val TIMELINE_TYPE_ACTIVE: String = "ACTIVE"
  val TIMELINE_TYPE_ARCHIVED: String = "ARCHIVED"

  // Status constants
  val STATUS_SUCCESS: String = "SUCCESS"
  val STATUS_FAILED: String = "FAILED"

  /**
   * Build named procedure arguments from given args' map
   *
   * @param args The arguments map
   * @return Named procedure arguments
   */
  def buildProcedureArgs(args: Map[String, Any]): ProcedureArgs = {
    val values: Array[Any] = new Array[Any](args.size)
    val map = new util.LinkedHashMap[String, Int]()

    args.zipWithIndex.foreach {
      case ((key, value), index) =>
        values(index) = value
        map.put(key, index)
    }

    ProcedureArgs(isNamedArgs = true, map, new GenericInternalRow(values))
  }

  sealed trait Operation {
    def value: String

    def isSchedule: Boolean

    def isExecute: Boolean
  }

  /**
   * schedule: schedule a new plan
   */
  case object Schedule extends Operation {
    override def value: String = "schedule"

    override def isSchedule: Boolean = true

    override def isExecute: Boolean = false
  }

  /**
   * execute: if specific instants exist, execute them, otherwise execute all pending plans
   */
  case object Execute extends Operation {
    override def value: String = "execute"

    override def isSchedule: Boolean = false

    override def isExecute: Boolean = true
  }

  /**
   * scheduleAndExecute: schedule a new plan and then execute it, if no plan is generated during
   * schedule, execute all pending plans
   */
  case object ScheduleAndExecute extends Operation {
    override def value: String = "scheduleandexecute"

    override def isSchedule: Boolean = true

    override def isExecute: Boolean = true
  }

  object Operation {
    private val ValueToEnumMap: Map[String, Operation with Product with Serializable] = Seq(Schedule, Execute, ScheduleAndExecute)
      .map(enum => enum.value -> enum).toMap

    def fromValue(value: String): Operation = {
      ValueToEnumMap.getOrElse(value, throw new HoodieException(s"Invalid value ($value)"))
    }
  }

  def filterPendingInstantsAndGetOperation(pendingInstants: Seq[String], specificInstants: Option[String],
                                           op: Option[String], limit: Option[Int] = None): (Seq[String], Operation) = {
    specificInstants match {
      case Some(inst) =>
        if (op.exists(o => !Execute.value.equalsIgnoreCase(o))) {
          throw new HoodieException("specific instants only can be used in 'execute' op or not specific op")
        }
        // No op specified, set it as 'execute' with instants specified
        (HoodieProcedureUtils.checkAndFilterPendingInstants(pendingInstants, inst), Execute)
      case _ =>
        // No op specified, set it as 'scheduleAndExecute' default
        if (limit.isDefined) {
          (pendingInstants.take(limit.get), op.map(o => Operation.fromValue(o.toLowerCase)).getOrElse(ScheduleAndExecute))
        } else {
          (pendingInstants, op.map(o => Operation.fromValue(o.toLowerCase)).getOrElse(ScheduleAndExecute))
        }
    }
  }

  def checkAndFilterPendingInstants(pendingInstants: Seq[String], instantStr: String): Seq[String] = {
    val instants = StringUtils.split(instantStr, ",").asScala
    val pendingSet = pendingInstants.toSet
    val noneInstants = instants.filter(ins => !pendingSet.contains(ins))
    if (noneInstants.nonEmpty) {
      throw new HoodieException (s"specific ${noneInstants.mkString(",")} instants is not exist")
    }
    instants.sortBy(f => f).toSeq
  }

  def normalizeTimeFormat(timeInput: String, isEndTime: Boolean = false): String = {
    if (timeInput.isEmpty) {
      timeInput
    }
    else if (timeInput.matches("^\\d{17}$")) {
      timeInput
    } else if (timeInput.matches("^\\d{14}$")) {
      timeInput + "000"
    } else if (timeInput.matches("^\\d{4}-\\d{2}-\\d{2}$")) {
      val date = LocalDate.parse(timeInput, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
      val datePrefix = date.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
      if (isEndTime) datePrefix + "235959999" else datePrefix + "000000000"
    } else if (timeInput.matches("^\\d{4}/\\d{2}/\\d{2}$")) {
      val date = LocalDate.parse(timeInput, DateTimeFormatter.ofPattern("yyyy/MM/dd"))
      val datePrefix = date.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
      if (isEndTime) datePrefix + "235959999" else datePrefix + "000000000"
    } else if (timeInput.matches("^\\d{8}$")) {
      if (isEndTime) timeInput + "235959999" else timeInput + "000000000"
    } else {
      throw new DateTimeParseException(s"Unsupported time format: $timeInput", timeInput, 0)
    }
  }
}
