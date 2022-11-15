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

val hudiDf = spark.read.format("hudi").load("/tmp/hudi-utilities-test/")
val inputDf = spark.read.format("json").load("/opt/bundle-validation/data/stocks/data")
val hudiCount = hudiDf.select("date", "key").distinct.count
val srcCount = inputDf.select("date", "key").distinct.count
if (hudiCount == srcCount) System.exit(0)
println(s"Counts don't match hudiCount: $hudiCount, srcCount: $srcCount")
System.exit(1)
