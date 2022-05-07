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

import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

object HoodieDatasetUtils {

  /**
   * Executes provided function while keeping provided [[DataFrame]] instance persisted for the
   * duration of the execution
   *
   * @param df target [[DataFrame]] to be persisted
   * @param level desired [[StorageLevel]] of the persistence
   * @param f target function to be executed while [[DataFrame]] is kept persisted
   * @tparam T return value of the target function
   * @return execution outcome of the [[f]] function
   */
  def withPersistence[T](df: DataFrame, level: StorageLevel = MEMORY_AND_DISK)(f: => T): T = {
    df.persist(level)
    try {
      f
    } finally {
      df.unpersist()
    }
  }
}
