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

import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.catalyst.plans.logical.HoodieVectorSearchTableValuedFunction.DistanceMetric
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.types.{ByteType, DataType, DoubleType, FloatType}

/**
 * Vector distance utilities: raw distance functions and Spark UDF factories.
 *
 * UDF factories produce typed Spark UDFs for Float, Double, and Byte corpus columns.
 */
object VectorDistanceUtils {
  /**
   * Creates a Spark UDF for the given distance metric and corpus element type.
   * Both arguments (corpus vector and query vector) are evaluated per row.
   * Prefer [[createSingleQueryDistanceUdf]] when the query vector is constant.
   * Supports Float, Double, and Byte element types.
   */
  def createDistanceUdf(metric: DistanceMetric.Value, elementType: DataType): UserDefinedFunction =
    elementType match {
      case FloatType  => createFloatDistanceUdf(metric)
      case DoubleType => createDoubleDistanceUdf(metric)
      case ByteType   => createByteDistanceUdf(metric)
      case _ => throw new HoodieAnalysisException(
        s"Unsupported vector element type for distance computation: $elementType")
    }

  /**
   * Creates a Spark UDF optimized for single-query mode: the query vector's
   * [[DenseVector]] is pre-computed once and closed over, so only the corpus
   * vector is converted per row.
   */
  def createSingleQueryDistanceUdf(
      metric: DistanceMetric.Value,
      elementType: DataType,
      queryVector: Array[Double]): UserDefinedFunction = {
    val queryDv = new DenseVector(queryVector)
    val queryNorm = Vectors.norm(queryDv, 2.0)
    val distFn = resolveDistanceFn(metric)

    // Array[Double] allocation per row is required: DenseVector needs a backing Array[Double],
    // and Spark UDFs are stateless so a reusable buffer is not safe across rows.
    elementType match {
      case FloatType => udf((corpus: Seq[Float]) => {
        requireSameLength(corpus.length, queryVector.length)
        distFn(new DenseVector(corpus.iterator.map(_.toDouble).toArray), queryDv, queryNorm)
      })
      case DoubleType => udf((corpus: Seq[Double]) => {
        requireSameLength(corpus.length, queryVector.length)
        distFn(new DenseVector(corpus.toArray), queryDv, queryNorm)
      })
      case ByteType => udf((corpus: Seq[Byte]) => {
        requireSameLength(corpus.length, queryVector.length)
        distFn(new DenseVector(corpus.iterator.map(_.toDouble).toArray), queryDv, queryNorm)
      })
      case _ => throw new HoodieAnalysisException(
        s"Unsupported vector element type for distance computation: $elementType")
    }
  }

  /**
   * Returns a distance function that takes (corpusDenseVector, queryDenseVector, queryNorm)
   * and returns a Double distance. The queryNorm parameter avoids recomputing
   * the query vector's norm on every row for cosine distance.
   */
  private def resolveDistanceFn(
      metric: DistanceMetric.Value): (DenseVector, DenseVector, Double) => Double =
    metric match {
      case DistanceMetric.COSINE => (a, b, bNorm) =>
        val aNorm = Vectors.norm(a, 2.0)
        val denom = aNorm * bNorm
        if (denom == 0.0) 1.0 else math.min(2.0, math.max(0.0, 1.0 - (a.dot(b) / denom)))
      case DistanceMetric.L2 => (a, b, _) =>
        math.sqrt(Vectors.sqdist(a, b))
      case DistanceMetric.DOT_PRODUCT => (a, b, _) =>
        -(a.dot(b))
    }

  private def createFloatDistanceUdf(metric: DistanceMetric.Value): UserDefinedFunction = {
    val distFn = resolveDistanceFn(metric)
    udf((a: Seq[Float], b: Seq[Float]) => {
      requireSameLength(a.length, b.length)
      val dvA = new DenseVector(a.iterator.map(_.toDouble).toArray)
      val dvB = new DenseVector(b.iterator.map(_.toDouble).toArray)
      distFn(dvA, dvB, Vectors.norm(dvB, 2.0))
    })
  }

  private def createDoubleDistanceUdf(metric: DistanceMetric.Value): UserDefinedFunction = {
    val distFn = resolveDistanceFn(metric)
    udf((a: Seq[Double], b: Seq[Double]) => {
      requireSameLength(a.length, b.length)
      val dvA = new DenseVector(a.toArray)
      val dvB = new DenseVector(b.toArray)
      distFn(dvA, dvB, Vectors.norm(dvB, 2.0))
    })
  }

  private def createByteDistanceUdf(metric: DistanceMetric.Value): UserDefinedFunction = {
    val distFn = resolveDistanceFn(metric)
    udf((a: Seq[Byte], b: Seq[Byte]) => {
      requireSameLength(a.length, b.length)
      val dvA = new DenseVector(a.iterator.map(_.toDouble).toArray)
      val dvB = new DenseVector(b.iterator.map(_.toDouble).toArray)
      distFn(dvA, dvB, Vectors.norm(dvB, 2.0))
    })
  }

  private def requireSameLength(aLen: Int, bLen: Int): Unit = {
    if (aLen != bLen) {
      throw new IllegalArgumentException(
        s"Hudi vector search: vector dimension mismatch ($aLen vs $bLen). " +
          "Ensure all embeddings have the same number of dimensions as the query vector.")
    }
  }
}
