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

import org.apache.spark.sql.hudi.SparkAdapter

/**
 * Use the SparkAdapterSupport trait to get the SparkAdapter when we
 * need to adapt the difference between spark2 and spark3.
 */
trait SparkAdapterSupport {

  lazy val sparkAdapter: SparkAdapter = {
    val adapterClass = if (HoodieSparkUtils.isSpark3_2) {
      "org.apache.spark.sql.adapter.Spark3_2Adapter"
    } else if (HoodieSparkUtils.isSpark3_0 || HoodieSparkUtils.isSpark3_1) {
      "org.apache.spark.sql.adapter.Spark3_1Adapter"
    } else {
      "org.apache.spark.sql.adapter.Spark2Adapter"
    }
    getClass.getClassLoader.loadClass(adapterClass)
      .newInstance().asInstanceOf[SparkAdapter]
  }
}
