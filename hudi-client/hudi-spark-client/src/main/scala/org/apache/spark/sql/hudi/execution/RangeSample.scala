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

package org.apache.spark.sql.hudi.execution

import org.apache.hudi.config.HoodieClusteringConfig
import org.apache.hudi.optimize.{HilbertCurveUtils, ZOrderingUtil}
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BoundReference, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.MutablePair
import org.apache.spark.util.random.SamplingUtils
import org.davidmoten.hilbert.HilbertCurve

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}
import scala.util.hashing.byteswap32

class RangeSample[K: ClassTag, V](
                                   zEncodeNum: Int,
                                   rdd: RDD[_ <: Product2[K, V]],
                                   private var ascend: Boolean = true,
                                   val samplePointsPerPartitionHint: Int = 20) extends Serializable {

  // We allow zEncodeNum = 0, which happens when sorting an empty RDD under the default settings.
  require(zEncodeNum >= 0, s"Number of zEncodeNum cannot be negative but found $zEncodeNum.")
  require(samplePointsPerPartitionHint > 0,
    s"Sample points per partition must be greater than 0 but found $samplePointsPerPartitionHint")

  def getRangeBounds(): ArrayBuffer[(K, Float)] = {
    if (zEncodeNum <= 1) {
      ArrayBuffer.empty[(K, Float)]
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      // Cast to double to avoid overflowing ints or longs
      val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * zEncodeNum, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        ArrayBuffer.empty[(K, Float)]
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]

        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }

        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        candidates
      }
    }
  }

  /**
    * Determines the bounds for range partitioning from candidates with weights indicating how many
    * items each represents. Usually this is 1 over the probability used to sample this candidate.
    *
    * @param candidates unordered candidates with weights
    * @param partitions number of partitions
    * @return selected bounds
    */
  def determineBound[K : Ordering : ClassTag](
      candidates: ArrayBuffer[(K, Float)],
      partitions: Int, ordering: Ordering[K]): Array[K] = {
    val ordered = candidates.sortBy(_._1)(ordering)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight >= target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }

  def determineRowBounds[K : Ordering : ClassTag](
      candidates: ArrayBuffer[(K, Float)],
      partitions: Int, orderings: Seq[Ordering[K]],
      attributes: Seq[Attribute]): Array[Array[UnsafeRow]] = {

    orderings.zipWithIndex.map { case (ordering, index) =>
      val ordered = candidates.sortBy(_._1)(ordering)
      val numCandidates = ordered.size
      val sumWeights = ordered.map(_._2.toDouble).sum
      val step = sumWeights / partitions
      var cumWeight = 0.0
      var target = step
      val bounds = ArrayBuffer.empty[K]
      var i = 0
      var j = 0
      var previousBound = Option.empty[K]
      while ((i < numCandidates) && (j < partitions - 1)) {
        val (key, weight) = ordered(i)
        cumWeight += weight
        if (cumWeight >= target) {
          // Skip duplicate values.
          if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
            bounds += key
            target += step
            j += 1
            previousBound = Some(key)
          }
        }
        i += 1
      }
      // build project
      val project = UnsafeProjection.create(Seq(attributes(index)), attributes)
      bounds.map { bound =>
        val row = bound.asInstanceOf[UnsafeRow]
        project(row).copy()
      }.toArray
    }.toArray
  }

  /**
    * Sketches the input RDD via reservoir sampling on each partition.
    *
    * @param rdd the input RDD to sketch
    * @param sampleSizePerPartition max sample size per partition
    * @return (total number of items, an array of (partitionId, number of items, sample))
    */
  def sketch[K: ClassTag](
      rdd: RDD[K],
      sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
    val shift = rdd.id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
  }
}

class RawDecisionBound[K : Ordering : ClassTag](ordering: Ordering[K]) extends Serializable {

  private var binarySearch: ((Array[K], K) => Int) = {
    // For primitive keys, we can use the natural ordering. Otherwise, use the Ordering comparator.
    classTag[K] match {
      case ClassTag.Float =>
        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[Float]], x.asInstanceOf[Float])
      case ClassTag.Double =>
        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[Double]], x.asInstanceOf[Double])
      case ClassTag.Byte =>
        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[Byte]], x.asInstanceOf[Byte])
      case ClassTag.Char =>
        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[Char]], x.asInstanceOf[Char])
      case ClassTag.Short =>
        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[Short]], x.asInstanceOf[Short])
      case ClassTag.Int =>
        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[Int]], x.asInstanceOf[Int])
      case ClassTag.Long =>
        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[Long]], x.asInstanceOf[Long])
      case _ =>
        val comparator = ordering.asInstanceOf[java.util.Comparator[Any]]
        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[AnyRef]], x, comparator)
    }
  }

  def getBound(key: Any, candidateBounds: Array[K]): Int = {
    val k = key.asInstanceOf[K]
    var bound = 0
    if (candidateBounds.length <= 128) {
      while(bound < candidateBounds.length && ordering.gt(k, candidateBounds(bound))) {
        bound += 1
      }
    } else {
      bound = binarySearch(candidateBounds, k)
      if (bound < 0 ) {
        bound = -bound - 1
      }
      if (bound > candidateBounds.length) {
        bound = candidateBounds.length
      }
    }
    bound
  }
}

case class ZorderingBinarySort(b: Array[Byte]) extends Ordered[ZorderingBinarySort] with Serializable {
  override def compare(that: ZorderingBinarySort): Int = {
    val len = this.b.length
    ZOrderingUtil.compareTo(this.b, 0, len, that.b, 0, len)
  }
}

object RangeSampleSort {

  /**
    * create z-order DataFrame by sample
    * support all col types
    */
  def sortDataFrameBySampleSupportAllTypes(df: DataFrame, zCols: Seq[String], fileNum: Int): DataFrame = {
    val spark = df.sparkSession
    val internalRdd = df.queryExecution.toRdd
    val schema = df.schema
    val outputAttributes = df.queryExecution.analyzed.output
    val sortingExpressions = outputAttributes.filter(p => zCols.contains(p.name))
    if (sortingExpressions.length == 0 || sortingExpressions.length != zCols.size) {
      df
    } else {
      val zOrderBounds = df.sparkSession.sessionState.conf.getConfString(
        HoodieClusteringConfig.LAYOUT_OPTIMIZE_BUILD_CURVE_SAMPLE_SIZE.key,
        HoodieClusteringConfig.LAYOUT_OPTIMIZE_BUILD_CURVE_SAMPLE_SIZE.defaultValue.toString).toInt

      val sampleRdd = internalRdd.mapPartitionsInternal { iter =>
        val projection = UnsafeProjection.create(sortingExpressions, outputAttributes)
        val mutablePair = new MutablePair[InternalRow, Null]()
        // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
        // partition bounds. To get accurate samples, we need to copy the mutable keys.
        iter.map(row => mutablePair.update(projection(row).copy(), null))
      }

      val orderings = sortingExpressions.map(SortOrder(_, Ascending)).zipWithIndex.map { case (ord, i) =>
        ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
      }

      val lazyGeneratedOrderings = orderings.map(ord => new LazilyGeneratedOrdering(Seq(ord)))

      val sample = new RangeSample(zOrderBounds, sampleRdd)

      val rangeBounds = sample.getRangeBounds()

      implicit val ordering1 = lazyGeneratedOrderings(0)

      val sampleBounds = sample.determineRowBounds(rangeBounds, math.min(zOrderBounds, rangeBounds.length), lazyGeneratedOrderings, sortingExpressions)

      val origin_orderings = sortingExpressions.map(SortOrder(_, Ascending)).map { ord =>
        ord.copy(child = BoundReference(0, ord.dataType, ord.nullable))
      }

      val origin_lazyGeneratedOrderings = origin_orderings.map(ord => new LazilyGeneratedOrdering(Seq(ord)))

      // expand bounds.
      // maybe it's better to use the value of "spark.zorder.bounds.number" as maxLength,
      // however this will lead to extra time costs when all zorder cols distinct count values are less then "spark.zorder.bounds.number"
      val maxLength = sampleBounds.map(_.length).max
      val expandSampleBoundsWithFactor = sampleBounds.map { bound =>
        val fillFactor = maxLength / bound.size.toDouble
        (bound, fillFactor)
      }

      val boundBroadCast = spark.sparkContext.broadcast(expandSampleBoundsWithFactor)

      val indexRdd = internalRdd.mapPartitionsInternal { iter =>
        val boundsWithFactor = boundBroadCast.value
        import java.util.concurrent.ThreadLocalRandom
        val threadLocalRandom = ThreadLocalRandom.current
        val maxBoundNum = boundsWithFactor.map(_._1.length).max
        val origin_Projections = sortingExpressions.map { se =>
          UnsafeProjection.create(Seq(se), outputAttributes)
        }

        iter.map { unsafeRow =>
          val interleaveValues = origin_Projections.zip(origin_lazyGeneratedOrderings).zipWithIndex.map { case ((rowProject, lazyOrdering), index) =>
            val row = rowProject(unsafeRow)
            val decisionBound = new RawDecisionBound(lazyOrdering)
            if (row.isNullAt(0)) {
              maxBoundNum + 1
            } else {
              val (bound, factor) = boundsWithFactor(index)
              if (factor > 1) {
                val currentRank = decisionBound.getBound(row, bound.asInstanceOf[Array[InternalRow]])
                currentRank*factor.toInt + threadLocalRandom.nextInt(factor.toInt)
              } else {
                decisionBound.getBound(row, bound.asInstanceOf[Array[InternalRow]])
              }
            }
          }.toArray.map(ZOrderingUtil.intTo8Byte(_))
          val zValues = ZOrderingUtil.interleaving(interleaveValues, 8)
          val mutablePair = new MutablePair[InternalRow, Array[Byte]]()

          mutablePair.update(unsafeRow, zValues)
        }
      }.sortBy(x => ZorderingBinarySort(x._2), numPartitions = fileNum).map(_._1)
      spark.internalCreateDataFrame(indexRdd, schema)
    }
  }

  /**
    * create optimize DataFrame by sample
    * first, sample origin data to get order-cols bounds, then apply sort to produce DataFrame
    * support all type data.
    * this method need more resource and cost more time than createOptimizedDataFrameByMapValue
    */
  def sortDataFrameBySample(df: DataFrame, zCols: Seq[String], fileNum: Int, sortMode: String): DataFrame = {
    val spark = df.sparkSession
    val columnsMap = df.schema.fields.map(item => (item.name, item)).toMap
    val fieldNum = df.schema.fields.length
    val checkCols = zCols.filter(col => columnsMap(col) != null)
    val useHilbert = sortMode match {
      case "hilbert" => true
      case "z-order" => false
      case other => throw new IllegalArgumentException(s"new only support z-order/hilbert optimize but find: ${other}")
    }

    if (zCols.isEmpty || checkCols.isEmpty) {
      df
    } else {
      val zFields = zCols.map { col =>
        val newCol = columnsMap(col)
        if (newCol == null) {
          (-1, null)
        } else {
          newCol.dataType match {
            case LongType | DoubleType | FloatType | StringType | IntegerType | DateType | TimestampType | ShortType | ByteType =>
              (df.schema.fields.indexOf(newCol), newCol)
            case d: DecimalType =>
              (df.schema.fields.indexOf(newCol), newCol)
            case _ =>
              (-1, null)
          }
        }
      }.filter(_._1 != -1)
      // Complex type found, use createZIndexedDataFrameByRange
      if (zFields.length != zCols.length) {
        return sortDataFrameBySampleSupportAllTypes(df, zCols, fileNum)
      }

      val rawRdd = df.rdd
      val sampleRdd = rawRdd.map { row =>
        val values = zFields.map { case (index, field) =>
          field.dataType match {
            case LongType =>
              if (row.isNullAt(index)) Long.MaxValue else row.getLong(index)
            case DoubleType =>
              if (row.isNullAt(index)) Long.MaxValue else java.lang.Double.doubleToLongBits(row.getDouble(index))
            case IntegerType =>
              if (row.isNullAt(index)) Long.MaxValue else row.getInt(index).toLong
            case FloatType =>
              if (row.isNullAt(index)) Long.MaxValue else java.lang.Double.doubleToLongBits(row.getFloat(index).toDouble)
            case StringType =>
              if (row.isNullAt(index)) "" else row.getString(index)
            case DateType =>
              if (row.isNullAt(index)) Long.MaxValue else row.getDate(index).getTime
            case TimestampType =>
              if (row.isNullAt(index)) Long.MaxValue else row.getTimestamp(index).getTime
            case ByteType =>
              if (row.isNullAt(index)) Long.MaxValue else row.getByte(index).toLong
            case ShortType =>
              if (row.isNullAt(index)) Long.MaxValue else row.getShort(index).toLong
            case d: DecimalType =>
              if (row.isNullAt(index)) Long.MaxValue else row.getDecimal(index).longValue()
            case _ =>
              null
          }
        }.filter(v => v != null).toArray
        (values, null)
      }
      val zOrderBounds = df.sparkSession.sessionState.conf.getConfString(
        HoodieClusteringConfig.LAYOUT_OPTIMIZE_BUILD_CURVE_SAMPLE_SIZE.key,
        HoodieClusteringConfig.LAYOUT_OPTIMIZE_BUILD_CURVE_SAMPLE_SIZE.defaultValue.toString).toInt
      val sample = new RangeSample(zOrderBounds, sampleRdd)
      val rangeBounds = sample.getRangeBounds()
      val sampleBounds = {
        val candidateColNumber = rangeBounds.head._1.length
        (0 to candidateColNumber - 1).map { i =>
          val colRangeBound = rangeBounds.map(x => (x._1(i), x._2))

          if (colRangeBound.head._1.isInstanceOf[String]) {
            sample.determineBound(colRangeBound.asInstanceOf[ArrayBuffer[(String, Float)]], math.min(zOrderBounds, rangeBounds.length), Ordering[String])
          } else {
            sample.determineBound(colRangeBound.asInstanceOf[ArrayBuffer[(Long, Float)]], math.min(zOrderBounds, rangeBounds.length), Ordering[Long])
          }
        }
      }

      // expand bounds.
      // maybe it's better to use the value of "spark.zorder.bounds.number" as maxLength,
      // however this will lead to extra time costs when all zorder cols distinct count values are less then "spark.zorder.bounds.number"
      val maxLength = sampleBounds.map(_.length).max
      val expandSampleBoundsWithFactor = sampleBounds.map { bound =>
        val fillFactor = maxLength / bound.size
        val newBound = new Array[Double](bound.length * fillFactor)
        if (bound.isInstanceOf[Array[Long]] && fillFactor > 1) {
          val longBound = bound.asInstanceOf[Array[Long]]
          for (i <- 0 to bound.length - 1) {
            for (j <- 0 to fillFactor - 1) {
              // sample factor shoud not be too large, so it's ok to use 1 / fillfactor as slice
              newBound(j + i*(fillFactor)) = longBound(i) + (j + 1) * (1 / fillFactor.toDouble)
            }
          }
          (newBound, fillFactor)
        } else {
          (bound, 0)
        }
      }

      val boundBroadCast = spark.sparkContext.broadcast(expandSampleBoundsWithFactor)

      val indexRdd = rawRdd.mapPartitions { iter =>
        val hilbertCurve = if (useHilbert) Some(HilbertCurve.bits(32).dimensions(zFields.length)) else None
        val expandBoundsWithFactor = boundBroadCast.value
        val maxBoundNum = expandBoundsWithFactor.map(_._1.length).max
        val longDecisionBound = new RawDecisionBound(Ordering[Long])
        val doubleDecisionBound = new RawDecisionBound(Ordering[Double])
        val stringDecisionBound = new RawDecisionBound(Ordering[String])
        import java.util.concurrent.ThreadLocalRandom
        val threadLocalRandom = ThreadLocalRandom.current

        def getRank(rawIndex: Int, value: Long, isNull: Boolean): Int = {
          val (expandBound, factor) = expandBoundsWithFactor(rawIndex)
          if (isNull) {
            expandBound.length + 1
          } else {
            if (factor > 1) {
              doubleDecisionBound.getBound(value + (threadLocalRandom.nextInt(factor) + 1)*(1 / factor.toDouble), expandBound.asInstanceOf[Array[Double]])
            } else {
              longDecisionBound.getBound(value, expandBound.asInstanceOf[Array[Long]])
            }
          }
        }

        iter.map { row =>
          val values = zFields.zipWithIndex.map { case ((index, field), rawIndex) =>
            field.dataType match {
              case LongType =>
                val isNull = row.isNullAt(index)
                getRank(rawIndex, if (isNull) 0 else row.getLong(index), isNull)
              case DoubleType =>
                val isNull = row.isNullAt(index)
                getRank(rawIndex, if (isNull) 0 else java.lang.Double.doubleToLongBits(row.getDouble(index)), isNull)
              case IntegerType =>
                val isNull = row.isNullAt(index)
                getRank(rawIndex, if (isNull) 0 else row.getInt(index).toLong, isNull)
              case FloatType =>
                val isNull = row.isNullAt(index)
                getRank(rawIndex, if (isNull) 0 else java.lang.Double.doubleToLongBits(row.getFloat(index).toDouble), isNull)
              case StringType =>
                val factor = maxBoundNum.toDouble / expandBoundsWithFactor(rawIndex)._1.length
                if (row.isNullAt(index)) {
                  maxBoundNum + 1
                } else {
                  val currentRank = stringDecisionBound.getBound(row.getString(index), expandBoundsWithFactor(rawIndex)._1.asInstanceOf[Array[String]])
                  if (factor > 1) {
                    (currentRank*factor).toInt + threadLocalRandom.nextInt(factor.toInt)
                  } else {
                    currentRank
                  }
                }
              case DateType =>
                val isNull = row.isNullAt(index)
                getRank(rawIndex, if (isNull) 0 else row.getDate(index).getTime, isNull)
              case TimestampType =>
                val isNull = row.isNullAt(index)
                getRank(rawIndex, if (isNull) 0 else row.getTimestamp(index).getTime, isNull)
              case ByteType =>
                val isNull = row.isNullAt(index)
                getRank(rawIndex, if (isNull) 0 else row.getByte(index).toLong, isNull)
              case ShortType =>
                val isNull = row.isNullAt(index)
                getRank(rawIndex, if (isNull) 0 else row.getShort(index).toLong, isNull)
              case d: DecimalType =>
                val isNull = row.isNullAt(index)
                getRank(rawIndex, if (isNull) 0 else row.getDecimal(index).longValue(), isNull)
              case _ =>
                -1
            }
          }.filter(v => v != -1)
          val mapValues = if (hilbertCurve.isDefined) {
            HilbertCurveUtils.indexBytes(hilbertCurve.get, values.map(_.toLong).toArray, 32)
          } else {
            ZOrderingUtil.interleaving(values.map(ZOrderingUtil.intTo8Byte(_)).toArray, 8)
          }
          Row.fromSeq(row.toSeq ++ Seq(mapValues))
        }
      }.sortBy(x => ZorderingBinarySort(x.getAs[Array[Byte]](fieldNum)), numPartitions = fileNum)
      val newDF = df.sparkSession.createDataFrame(indexRdd, StructType(
        df.schema.fields ++ Seq(
          StructField(s"index",
            BinaryType, false))
      ))
      newDF.drop("index")
    }
  }
}

