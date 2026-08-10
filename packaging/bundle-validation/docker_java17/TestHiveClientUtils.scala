/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.sql.hive.HiveClientUtils
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

/*
 * Converted from hudi-spark-datasource/hudi-spark-common/src/test/scala/org/apache/spark/sql/hive/TestHiveClientUtils.scala
 * as original test couldn't run on Java 17
 */

assert(spark.conf.get(CATALOG_IMPLEMENTATION.key) == "hive")
val hiveClient1 = HiveClientUtils.getSingletonClientForMetadata(spark)
val hiveClient2 = HiveClientUtils.getSingletonClientForMetadata(spark)
assert(hiveClient1 == hiveClient2)
