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

package org.apache.hudi.util

import org.apache.hudi.common.util.collection.ClosableIterator

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.io.Closeable

/**
 * A [[ClosableIterator]] returning [[InternalRow]] by iterating through the entries returned
 * by a Spark reader.
 *
 * @param iterator the input iterator that can either contain [[InternalRow]] (non-vectorized)
 *                 or [[ColumnarBatch]] (vectorized), as returned by the Spark reader.
 */
class CloseableInternalRowIterator(iterator: Iterator[_]) extends ClosableIterator[InternalRow] {
  private var entryTypeKnown = false
  private var isColumnarBatch = false
  private var nextBatch: ColumnarBatch = _
  private var seqInBatch: Int = -1

  override def close(): Unit = {
    iterator match {
      case iterator: Iterator[_] with Closeable => iterator.close()
      case _ =>
    }
  }

  override def hasNext: Boolean = {
    seqInBatch >= 0 || iterator.hasNext
  }

  override def next: InternalRow = {
    if (!entryTypeKnown) {
      entryTypeKnown = true
      // First entry
      val nextVal = iterator.next
      seqInBatch = 0
      nextVal match {
        case _: ColumnarBatch =>
          isColumnarBatch = true
          nextBatch = nextVal.asInstanceOf[ColumnarBatch]
          val result = nextBatch.getRow(seqInBatch)
          seqInBatch += 1
          if (seqInBatch >= nextBatch.numRows()) {
            seqInBatch = -1
          }
          result
        case _ =>
          seqInBatch = -1
          nextVal.asInstanceOf[InternalRow]
      }
    } else {
      if (isColumnarBatch) {
        val result = nextBatch.getRow(seqInBatch)
        seqInBatch += 1
        if (seqInBatch >= nextBatch.numRows()) {
          seqInBatch = -1
        }
        result
      } else {
        iterator.next.asInstanceOf[InternalRow]
      }
    }
  }
}
