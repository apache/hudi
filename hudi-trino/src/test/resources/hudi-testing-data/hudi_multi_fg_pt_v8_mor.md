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
- MOR table with MDT enabled
- Revision: eb212c9dca876824b6c570665951777a772bc463
- Record Level Index is enabled with a recordKey of id,name
- Secondary index created on the column `price`
- 2 Partitions [US, SG]
- 2 Filegroups per partition

```scala
test("Create table multi filegroup partitioned mor") {
    withTempDir { tmp =>
        val tableName = "hudi_multi_fg_pt_mor"
        spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long,
               |  country string
               |) using hudi
               | location '${tmp.getCanonicalPath}'
               | tblproperties (
               |  primaryKey ='id,name',
               |  type = 'mor',
               |  preCombineField = 'ts'
               | ) partitioned by (country)
       """.stripMargin)
        // directly write to new parquet file
        spark.sql(s"set hoodie.parquet.small.file.limit=0")
        spark.sql(s"set hoodie.metadata.compact.max.delta.commits=1")
        // partition stats index is enabled together with column stats index
        spark.sql(s"set hoodie.metadata.index.column.stats.enable=true")
        spark.sql(s"set hoodie.metadata.record.index.enable=true")
        spark.sql(s"set hoodie.metadata.index.secondary.enable=true")
        spark.sql(s"set hoodie.metadata.index.column.stats.column.list=_hoodie_commit_time,_hoodie_partition_path,_hoodie_record_key,id,name,price,ts,country")
        // 2 filegroups per partition
        spark.sql(s"insert into $tableName values(1, 'a1', 100, 1000, 'SG'),(2, 'a2', 200, 1000, 'US')")
        spark.sql(s"insert into $tableName values(3, 'a3', 101, 1001, 'SG'),(4, 'a3', 201, 1001, 'US')")
        // create secondary index
        spark.sql(s"create index idx_price on $tableName (price)")
        // generate logs through updates
        spark.sql(s"update $tableName set price=price+1")
    }
}
```

# When to use this table?
- For test cases that require multiple filegroups in a partition
- For test cases that require filegroups that have a log file
- For test cases that require column stats index
- For test cases that require partition stats index
- For test cases that require record level index
- For test cases that require secondary index
