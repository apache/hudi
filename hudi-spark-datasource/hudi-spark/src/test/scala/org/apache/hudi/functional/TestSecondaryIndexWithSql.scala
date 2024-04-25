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

package org.apache.hudi.functional

import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Tag

/**
 * Test cases for secondary index
 */
@Tag("functional")
class TestSecondaryIndexWithSql extends HoodieSparkSqlTestBase {

  test("Test Create Secondary Index") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      withTempDir { tmp =>
        Seq("mor").foreach { tableType =>
          //create a new table
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"

          val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city", "state")
          val data = Seq(
            (1695159649087L, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 19.10, "san_francisco", "california"),
            (1695091554787L, "e96c4396-3fad-413a-a942-4cb36106d720", "rider-B", "driver-M", 27.70, "sao_paulo", "texas"),
            (1695091554788L, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-K", 27.70, "san_francisco", "california"),
            (1695046462179L, "9909a8b1-2d15-4d3d-8ec9-efc48c536a00", "rider-D", "driver-L", 33.90, "san_francisco", "california"),
            (1695516137016L, "e3cf430c-889d-4015-bc98-59bdce1e530c", "rider-E", "driver-P", 34.15, "sao_paulo", "texas"),
            (1695046462179L, "9909a8b1-2d15-4d3d-8ec9-efc48c536a01", "rider-D", "driver-L", 33.90, "los-angeles", "california"),
            (1695516137016L, "e3cf430c-889d-4015-bc98-59bdce1e530b", "rider-E", "driver-P", 34.15, "bengaluru", "karnataka"),
            (1695115999911L, "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa", "rider-F", "driver-T", 17.85, "chennai", "tamil-nadu"))

          spark.sql(
            s"""
               |create table $tableName (
               |  ts bigint,
               |  id string,
               |  rider string,
               |  driver string,
               |  fare int,
               |  city string,
               |  state string
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = '$tableType',
               |  preCombineField = 'ts',
               |  hoodie.metadata.enable = 'true',
               |  hoodie.metadata.record.index.enable = 'true',
               |  hoodie.metadata.index.secondary.enable = 'true',
               |  hoodie.datasource.write.recordkey.field = 'id'
               | )
               | partitioned by(state)
               | location '$basePath'
       """.stripMargin)
          spark.sql(
            s"""
               | insert into $tableName
               | values
               | (1695159649087, '334e26e9-8355-45cc-97c6-c31daf0df330', 'rider-A', 'driver-K', 19, 'san_francisco', 'california'),
               | (1695091554787, 'e96c4396-3fad-413a-a942-4cb36106d720', 'rider-B', 'driver-M', 27, 'austin', 'texas')
               | """.stripMargin
          )

          // validate record_index created successfully
          val metadataDF = spark.sql(s"select key from hudi_metadata('$basePath') where type=5")
          assert(metadataDF.count() == 2)

          var metaClient = HoodieTableMetaClient.builder()
            .setBasePath(basePath)
            .setConf(HoodieTestUtils.getDefaultStorageConf)
            .build()
          assert(metaClient.getTableConfig.getMetadataPartitions.contains("record_index"))
          val createIndexSql = s"create index idx_city on $tableName using secondary_index(city)"
          spark.sql(createIndexSql)
          metaClient = HoodieTableMetaClient.builder()
            .setBasePath(basePath)
            .setConf(HoodieTestUtils.getDefaultStorageConf)
            .build()
          assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_city"))
          assert(metaClient.getTableConfig.getMetadataPartitions.contains("record_index"))

          checkAnswer(s"select key, SecondaryIndexMetadata.recordKey from hudi_metadata('$basePath') where type=7")(
            Seq("austin", "e96c4396-3fad-413a-a942-4cb36106d720"),
            Seq("san_francisco", "334e26e9-8355-45cc-97c6-c31daf0df330")
          )
          metadataDF.show(false)
        }
      }
    }
  }
}
