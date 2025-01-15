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

package org.apache.hudi.table.action.commit

import org.apache.hudi.client.WriteStatus
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.data.HoodieData
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.model.{HoodieKey, HoodieRecord, WriteOperationType}
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.table.HoodieTable
import org.apache.hudi.table.action.HoodieWriteMetadata

import java.util

class SparkDataFrameInsertCommitActionExecutor[T](context: HoodieEngineContext,
                                                  config: HoodieWriteConfig,
                                                  table: HoodieTable[T, HoodieData[HoodieRecord[T]], HoodieData[HoodieKey], HoodieData[WriteStatus]],
                                                  instantTime: String,
                                                  records: HoodieData[HoodieRecord[T]],
                                                  operationType: WriteOperationType = WriteOperationType.INSERT,
                                                  extraMetadata: Option[util.Map[String, String]] = Option.empty())
  extends BaseSparkDataFrameCommitActionExecutor[T](context, config, table, instantTime, operationType, extraMetadata) {


  def execute(): HoodieWriteMetadata[HoodieData[WriteStatus]] = {
    val data = records
    val x = "temp"
    HoodieDataFrameWriteHelper.newInstance().asInstanceOf[HoodieDataFrameWriteHelper[T, HoodieWriteMetadata[HoodieData[WriteStatus]]]].write(
      instantTime,
      records,
      context, table, config.shouldCombineBeforeInsert(), config.getInsertShuffleParallelism,
      this.asInstanceOf[BaseCommitActionExecutor
        [T, HoodieData[HoodieRecord[T]], HoodieData[HoodieKey], HoodieData[WriteStatus], HoodieWriteMetadata[HoodieData[WriteStatus]]]], operationType)
  }

  override protected def commit(result: HoodieWriteMetadata[HoodieData[WriteStatus]]): Unit = ???
}