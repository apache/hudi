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

package org.apache.hudi.functional

import org.apache.hudi.execution.bulkinsert.PreCombineRow
import org.apache.spark.sql.Row

/**
 * Simple Precombine Row used in tests.
 */
class SimplePreCombineRow extends PreCombineRow {
  /**
   * Pre combines two Rows. Chooses the one based on impl. Will be used in deduping.
   *
   * @param v1 first value to be combined.
   * @param v2 second value to be combined.
   * @return the combined value.
   */
  override def combineTwoRows(v1: Row, v2: Row): Row = {
    val tsV1 : Long = v1.getAs("ts")
    val tsV2 : Long = v2.getAs("ts")
    if (tsV1 >= tsV2) v1 else v2
  }
}
