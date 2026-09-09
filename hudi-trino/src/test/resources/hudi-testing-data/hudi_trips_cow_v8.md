<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

## Create script

Structure of table:
- COW table in table version 8 with MDT and column stats enabled
- Using Hudi 1.0.2 release
- Non-partitioned table
- One large parquet file for testing projection and reader

```scala
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.table.HoodieTableConfig._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.keygen.constant.KeyGeneratorOptions._
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.QuickstartUtils._
import spark.implicits._

val tableName = "hudi_trips_cow_v8"
val basePath = "file:///tmp/hudi_trips_cow_v8"

val dataGen = new DataGenerator
val inserts = convertToStringList(dataGen.generateInserts(40000))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 1))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "").
  option(TABLE_NAME, tableName).
  mode(Overwrite).
  save(basePath)
```
