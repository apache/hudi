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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.common.schema.HoodieSchema

import org.apache.spark.sql.catalyst.plans.logical.HoodieVectorSearchTableValuedFunction.DistanceMetric

import java.nio.{ByteBuffer, ByteOrder}

/**
 * Exact-distance scorer shared by every vector rerank fetch path (positional base-file fetch and
 * merged log-resident fetch). Both paths MUST compute distance identically or exact-vs-brute-force
 * equality breaks; keeping the math here is the single source of truth. Serializable so it can ride
 * into a Spark closure unchanged.
 */
private[analysis] final class VectorExactScorer(
    vectorSchema: HoodieSchema.Vector,
    queryVector: Array[Double],
    metric: DistanceMetric.Value) extends Serializable {

  private val queryNorm = math.sqrt(queryVector.iterator.map(v => v * v).sum)

  /** Decode the packed embedding bytes and score them against the query vector. */
  def scoreBytes(bytes: Array[Byte]): Double = {
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    vectorSchema.getVectorElementType match {
      case HoodieSchema.Vector.VectorElementType.FLOAT =>
        score(vectorSchema.getDimension, i => buffer.getFloat(i * java.lang.Float.BYTES).toDouble)
      case HoodieSchema.Vector.VectorElementType.DOUBLE =>
        score(vectorSchema.getDimension, i => buffer.getDouble(i * java.lang.Double.BYTES))
      case HoodieSchema.Vector.VectorElementType.INT8 =>
        score(vectorSchema.getDimension, i => buffer.get(i).toDouble)
      case other =>
        throw new UnsupportedOperationException(s"Unsupported vector element type for exact fetch: $other")
    }
  }

  private def score(dim: Int, valueAt: Int => Double): Double = {
    metric match {
      case DistanceMetric.L2 =>
        var sum = 0.0d
        var i = 0
        while (i < dim) {
          val diff = valueAt(i) - queryVector(i)
          sum += diff * diff
          i += 1
        }
        math.sqrt(sum)
      case DistanceMetric.COSINE =>
        var dot = 0.0d
        var norm = 0.0d
        var i = 0
        while (i < dim) {
          val value = valueAt(i)
          dot += value * queryVector(i)
          norm += value * value
          i += 1
        }
        val denom = math.sqrt(norm) * queryNorm
        if (denom == 0.0d) 1.0d else math.min(2.0d, math.max(0.0d, 1.0d - dot / denom))
      case DistanceMetric.DOT_PRODUCT =>
        var dot = 0.0d
        var i = 0
        while (i < dim) {
          dot += valueAt(i) * queryVector(i)
          i += 1
        }
        -dot
    }
  }
}
