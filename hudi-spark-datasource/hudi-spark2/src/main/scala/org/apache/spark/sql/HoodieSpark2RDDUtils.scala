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

package org.apache.spark.sql
import org.apache.spark.TaskContext
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

object HoodieSpark2RDDUtils extends HoodieRDDUtils {

  override def insertInto[K, V, C](ctx: TaskContext,
                                   records: Iterator[Product2[K, V]],
                                   sorter: ExternalSorter[K, V, C]): Iterator[Product2[K, C]] = {
    sorter.insertAll(records)

    ctx.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
    ctx.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
    ctx.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
    // Use completion callback to stop sorter if task was finished/cancelled.
    ctx.addTaskCompletionListener[Unit](_ => sorter.stop())

    CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
  }

}
