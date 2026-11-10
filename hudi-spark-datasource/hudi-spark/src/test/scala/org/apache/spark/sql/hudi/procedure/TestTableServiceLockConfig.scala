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

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider
import org.apache.hudi.common.config.LockConfiguration
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieLockConfig
import org.apache.hudi.exception.HoodieLockException

import java.util.concurrent.TimeUnit

class TestTableServiceLockConfig extends HoodieSparkProcedureTestBase {
  Seq("run_clustering" -> "cow", "run_compaction" -> "mor").foreach { case (procedure, tableType) =>
    test(s"$procedure acquires the derived metadata table lock") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val basePath = tmp.getCanonicalPath
        spark.sql(
          s"""create table $tableName (id int, ts long) using hudi
             |location '$basePath'
             |tblproperties (primaryKey = 'id', preCombineField = 'ts', type = '$tableType',
             |  'hoodie.metadata.enable' = 'true')""".stripMargin)
        spark.sql(s"insert into $tableName values (1, 1)")
        assert(HoodieTestUtils.createMetaClient(basePath).getTableConfig.isMetadataTableAvailable)

        val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, Some(tableName))
        try {
          assert(client.getConfig.getLockProviderClass != classOf[FileSystemBasedLockProvider].getName)
        } finally {
          client.close()
        }

        val lock = new FileSystemBasedLockProvider(
          new LockConfiguration(FileSystemBasedLockProvider.getLockConfig(basePath)),
          HoodieTestUtils.getDefaultStorageConf)
        try {
          assert(lock.tryLock(1, TimeUnit.SECONDS))
          val options = Seq(
            HoodieLockConfig.LOCK_ACQUIRE_WAIT_TIMEOUT_MS.key -> "1",
            HoodieLockConfig.LOCK_ACQUIRE_CLIENT_NUM_RETRIES.key -> "0"
          ).map { case (key, value) => s"$key=$value" }.mkString(",")
          val error = intercept[Exception] {
            spark.sql(s"call $procedure(op => 'schedule', table => '$tableName', options => '$options')").collect()
          }
          assert(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
            .exists(_.isInstanceOf[HoodieLockException]))
        } finally {
          lock.unlock()
          lock.close()
        }
        spark.sql(s"call $procedure(op => 'schedule', table => '$tableName')").collect()
      }
    }
  }
}
