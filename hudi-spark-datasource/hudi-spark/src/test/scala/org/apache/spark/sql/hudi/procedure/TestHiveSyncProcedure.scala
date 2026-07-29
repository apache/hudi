/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.hive.{HiveSyncConfig, HiveSyncConfigHolder}
import org.apache.hudi.sync.common.HoodieSyncConfig

import org.apache.hadoop.hive.conf.HiveConf
import org.junit.jupiter.api.Assertions.{assertNotNull, assertTrue}

class TestHiveSyncProcedure extends HoodieSparkProcedureTestBase {

  // Session-conf keys the procedure writes from its optional arguments; reset after the call so
  // they do not leak into sibling tests sharing the session.
  private val sessionSyncConfKeys = Seq(
    HiveSyncConfig.HIVE_USER.key, HiveSyncConfig.HIVE_PASS.key, HiveSyncConfig.HIVE_USE_JDBC.key,
    HiveSyncConfigHolder.HIVE_SYNC_MODE.key, HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key,
    HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key,
    HiveSyncConfigHolder.HIVE_SYNC_TABLE_STRATEGY.key, HoodieSyncConfig.META_SYNC_INCREMENTAL.key)

  private def createHudiTable(tableName: String, basePath: String): Unit = {
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         |) using hudi
         | options (
         |  primaryKey = 'id',
         |  type = 'cow',
         |  preCombineField = 'ts'
         | )
         | location '$basePath'
       """.stripMargin)
    spark.sql(s"insert into $tableName values(1, 'a1', 10.0, 1000)")
  }

  test("Test call hive_sync procedure surfaces a sync failure as HoodieException") {
    // The procedure constructs a HiveConf; Hive 2.3.7 is compiled with Java 1.8 and its class
    // loader throws when the Hive APIs are exercised on Java 17, so the test is skipped there.
    if (HoodieTestUtils.getJavaVersion < 17) {
      withTempDir { tmp =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        createHudiTable(tableName, basePath)

        try {
          // Every optional argument is supplied so each argument branch of the procedure runs. The
          // unresolvable partition_extractor_class makes the HiveSyncTool constructor throw while
          // loading the extractor (in the HoodieSyncClient constructor, before any metastore
          // connection is attempted).
          var thrown: Throwable = null
          try {
            spark.sql(
              s"call hive_sync(table => '$tableName', metastore_uri => 'thrift://localhost:9083'," +
                s" username => 'hive', password => 'hive', use_jdbc => 'false', mode => 'hms'," +
                s" partition_fields => 'name', strategy => 'ALL', sync_incremental => 'false'," +
                s" partition_extractor_class => 'org.apache.hudi.hive.MissingPartitionExtractor')")
          } catch {
            case e: Throwable => thrown = e
          }
          assertNotNull(thrown, "hive_sync should fail when the partition extractor cannot be loaded")

          // The procedure must wrap the failure as HoodieException("hive sync failed"), and the
          // cause must be the unresolvable extractor supplied as an argument. Asserting both pins
          // the wrapping contract and that the argument is applied and fails before the metastore.
          val chain = Iterator.iterate(thrown)(_.getCause).takeWhile(_ != null)
            .flatMap(t => Option(t.getMessage)).mkString(" | ")
          assertTrue(chain.contains("hive sync failed"),
            s"expected the procedure to wrap the failure as HoodieException, but got: $chain")
          assertTrue(chain.contains("org.apache.hudi.hive.MissingPartitionExtractor"),
            s"expected the failure to originate from loading the supplied extractor, but got: $chain")
        } finally {
          spark.sparkContext.hadoopConfiguration.unset(HiveConf.ConfVars.METASTOREURIS.varname)
          sessionSyncConfKeys.foreach(spark.sessionState.conf.unsetConf)
        }
      }
    }
  }
}
