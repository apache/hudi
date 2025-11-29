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

package org.apache.spark.sql.hudi.dml.insert

import org.apache.hudi.sync.common.HoodieSyncTool

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

import java.io.File
import java.util.Properties

class TestInsertTableWithHiveSupport extends TestInsertTable {

  val metastoreDerbyLocation = "/tmp/hive_metastore_db"

  override lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.sql.warehouse.dir", sparkWareHouse.getCanonicalPath)
    .config("spark.sql.session.timeZone", "UTC")
    .config("hoodie.insert.shuffle.parallelism", "4")
    .config("hoodie.upsert.shuffle.parallelism", "4")
    .config("hoodie.delete.shuffle.parallelism", "4")
    .config("hoodie.datasource.hive_sync.enable", "false")
    .config("hoodie.datasource.meta.sync.enable", "false")
    .config("hoodie.meta.sync.client.tool.class", classOf[DummySyncTool].getName)
    .config("spark.hadoop.javax.jdo.option.ConnectionURL", s"jdbc:derby:$metastoreDerbyLocation;create=true")
    .config(sparkConf())
    .enableHiveSupport()
    .getOrCreate()

  override def afterAll(): Unit = {
    // Clean up metastore derby location
    Utils.deleteRecursively(new File(metastoreDerbyLocation))
    super.afterAll()
  }

}

class DummySyncTool(props: Properties, hadoopConf: Configuration) extends HoodieSyncTool(props, hadoopConf) {
  override def syncHoodieTable(): Unit = {
    // do nothing here
  }
}
