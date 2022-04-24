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

package org.apache.spark.hudi.benchmark

import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer
import org.apache.hudi.execution.HoodieLazyInsertIterable
import org.apache.hudi.testutils.HoodieExecutorTestUtils

object HoodieExecutorBenchmark extends HoodieBenchmarkBase {

  val dataGen = new HoodieTestDataGenerator

  val utils = new HoodieExecutorTestUtils
  val recordsNumber = 1000000

  private def cowTableDisruptorExecutorBenchmark(tableName: String = "executorBenchmark"): Unit = {
    val benchmark = new HoodieBenchmark("COW Ingestion", recordsNumber)
    benchmark.addCase("Disruptor Executor") { _ =>
      val con: BoundedInMemoryQueueConsumer[HoodieLazyInsertIterable.HoodieInsertValueGenResult[HoodieRecord[_]], Integer] = new BoundedInMemoryQueueConsumer[HoodieLazyInsertIterable.HoodieInsertValueGenResult[HoodieRecord[_]], Integer]() {

        var count = 0
        /**
         * Consumer One record.
         */
        override protected def consumeOneRecord(record: HoodieLazyInsertIterable.HoodieInsertValueGenResult[HoodieRecord[_]]): Unit = {
          count = count + 1
        }

        /**
         * Notifies implementation that we have exhausted consuming records from queue.
         */
        override protected def finish(): Unit = {
        }

        /**
         * Return result of consuming records so far.
         */
        override def getResult: Integer = {
           count
        }
      }
      val instantTime = HoodieActiveTimeline.createNewInstantTime
      val hoodieRecords = dataGen.generateInserts(instantTime, recordsNumber)
      val disruptorExecutor = utils.getDisruptorExecutor(hoodieRecords, con)
      disruptorExecutor.execute()
      disruptorExecutor.shutdownNow()
    }

    benchmark.addCase("BoundInMemory Executor") { _ =>
      val con: BoundedInMemoryQueueConsumer[HoodieLazyInsertIterable.HoodieInsertValueGenResult[HoodieRecord[_]], Integer] = new BoundedInMemoryQueueConsumer[HoodieLazyInsertIterable.HoodieInsertValueGenResult[HoodieRecord[_]], Integer]() {

        var count = 0
        /**
         * Consumer One record.
         */
        override protected def consumeOneRecord(record: HoodieLazyInsertIterable.HoodieInsertValueGenResult[HoodieRecord[_]]): Unit = {
          count = count + 1
        }

        /**
         * Notifies implementation that we have exhausted consuming records from queue.
         */
        override protected def finish(): Unit = {
        }

        /**
         * Return result of consuming records so far.
         */
        override def getResult: Integer = {
          count
        }
      }
      val instantTime = HoodieActiveTimeline.createNewInstantTime
      val hoodieRecords = dataGen.generateInserts(instantTime, recordsNumber)
      val boundInMemoryExecutor = utils.getBoundedInMemoryExecutor(hoodieRecords, con)
      boundInMemoryExecutor.execute()
      boundInMemoryExecutor.shutdownNow()
    }

    benchmark.run()
  }

  override def afterAll(): Unit = {
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    cowTableDisruptorExecutorBenchmark()
  }
}
