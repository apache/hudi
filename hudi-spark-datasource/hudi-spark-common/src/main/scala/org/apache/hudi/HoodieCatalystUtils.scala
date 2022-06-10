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

package org.apache.hudi

import org.apache.hudi.common.data.HoodieData
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

object HoodieCatalystUtils {

  /**
   * Executes provided function while keeping provided [[Dataset]] instance persisted for the
   * duration of the execution
   *
   * @param df target [[Dataset]] to be persisted
   * @param level desired [[StorageLevel]] of the persistence
   * @param f target function to be executed while [[Dataset]] is kept persisted
   * @tparam T return value of the target function
   * @return execution outcome of the [[f]] function
   */
  def withPersistedDataset[T](df: Dataset[_], level: StorageLevel = MEMORY_AND_DISK)(f: => T): T = {
    df.persist(level)
    try {
      f
    } finally {
      df.unpersist()
    }
  }

  /**
   * Executes provided function while keeping provided [[HoodieData]] instance persisted for the
   * duration of the execution
   *
   * @param data target [[Dataset]] to be persisted
   * @param level desired [[StorageLevel]] of the persistence
   * @param f target function to be executed while [[Dataset]] is kept persisted
   * @tparam T return value of the target function
   * @return execution outcome of the [[f]] function
   */
  def withPersistedData[T](data: HoodieData[_], level: StorageLevel = MEMORY_AND_DISK)(f: => T): T = {
    data.persist(levelAsString(level))
    try {
      f
    } finally {
      data.unpersist()
    }
  }

  /**
   * Converts instance of [[StorageLevel]] to a corresponding string
   */
  def levelAsString(level: StorageLevel): String = level match {
    case NONE => "NONE"
    case DISK_ONLY => "DISK_ONLY"
    case DISK_ONLY_2 => "DISK_ONLY_2"
    case DISK_ONLY_3 => "DISK_ONLY_3"
    case MEMORY_ONLY => "MEMORY_ONLY"
    case MEMORY_ONLY_2 => "MEMORY_ONLY_2"
    case MEMORY_ONLY_SER => "MEMORY_ONLY_SER"
    case MEMORY_ONLY_SER_2 => "MEMORY_ONLY_SER_2"
    case MEMORY_AND_DISK => "MEMORY_AND_DISK"
    case MEMORY_AND_DISK_2 => "MEMORY_AND_DISK_2"
    case MEMORY_AND_DISK_SER => "MEMORY_AND_DISK_SER"
    case MEMORY_AND_DISK_SER_2 => "MEMORY_AND_DISK_SER_2"
    case OFF_HEAP => "OFF_HEAP"
    case _ => throw new IllegalArgumentException(s"Invalid StorageLevel: $level")
  }
}
