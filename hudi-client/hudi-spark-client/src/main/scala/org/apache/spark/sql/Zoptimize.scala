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

package org.apache.spark.sql

import java.sql.Date
import java.util.concurrent.{Executors, ThreadPoolExecutor}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.config.HoodieClusteringConfig
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, Attribute, AttributeReference, BoundReference, EqualNullSafe, EqualTo, Expression, ExtractValue, GetStructField, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not, Or, SortOrder, StartsWith, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions._
import org.apache.hudi.optimize.ZOrderingUtil
import org.apache.spark.sql.hudi.execution._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{MutablePair, SerializableConfiguration}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object Zoptimize {

  case class FileStats(val minVal: String, val maxVal: String, val num_nulls: Int = 0)
  case class ColumnFileStats(val fileName: String, val colName: String, val minVal: String, val maxVal: String, val num_nulls: Int = 0)

  def createZIndexedDataFrameBySample(df: DataFrame, zCols: String, fileNum: Int): DataFrame = {
    if (zCols == null || zCols.isEmpty) {
      df
    } else {
      createZIndexedDataFrameBySample(df, zCols.split(",").map(_.trim), fileNum)
    }
  }

  /**
    * create z-order DataFrame by sample
    * first, sample origin data to get z-cols bounds, then create z-order DataFrame
    * support all type data.
    * this method need more resource and cost more time than createZIndexedDataFrameByMapValue
    */
  def createZIndexedDataFrameBySample(df: DataFrame, zCols: Seq[String], fileNum: Int): DataFrame = {
    val spark = df.sparkSession
    val columnsMap = df.schema.fields.map(item => (item.name, item)).toMap
    val fieldNum = df.schema.fields.length
    val checkCols = zCols.filter(col => columnsMap(col) != null)

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
        return createZIndexedDataFrameByRange(df, zCols, fieldNum)
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
        HoodieClusteringConfig.DATA_OPTIMIZE_BUILD_CURVE_SAMPLE_NUMBER.key,
        HoodieClusteringConfig.DATA_OPTIMIZE_BUILD_CURVE_SAMPLE_NUMBER.defaultValue.toString).toInt
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
          }.filter(v => v != -1).map(ZOrderingUtil.intTo8Byte(_)).toArray
          val zValues = ZOrderingUtil.interleaving(values, 8)
          Row.fromSeq(row.toSeq ++ Seq(zValues))
        }
      }.sortBy(x => ZorderingBinarySort(x.getAs[Array[Byte]](fieldNum)), numPartitions = fileNum)
      val newDF = df.sparkSession.createDataFrame(indexRdd, StructType(
        df.schema.fields ++ Seq(
          StructField(s"zindex",
            BinaryType, false))
      ))
      newDF.drop("zindex")
    }
  }

  /**
    * create z-order DataFrame by sample
    * support all col types
    */
  def createZIndexedDataFrameByRange(df: DataFrame, zCols: Seq[String], fileNum: Int): DataFrame = {
    val spark = df.sparkSession
    val internalRdd = df.queryExecution.toRdd
    val schema = df.schema
    val outputAttributes = df.queryExecution.analyzed.output
    val sortingExpressions = outputAttributes.filter(p => zCols.contains(p.name))
    if (sortingExpressions.length == 0 || sortingExpressions.length != zCols.size) {
      df
    } else {
      val zOrderBounds = df.sparkSession.sessionState.conf.getConfString(
        HoodieClusteringConfig.DATA_OPTIMIZE_BUILD_CURVE_SAMPLE_NUMBER.key,
        HoodieClusteringConfig.DATA_OPTIMIZE_BUILD_CURVE_SAMPLE_NUMBER.defaultValue.toString).toInt

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

  def getMinMaxValueSpark(df: DataFrame, cols: Seq[String]): DataFrame = {
    val sqlContext = df.sparkSession.sqlContext
    import sqlContext.implicits._

    val values = cols.flatMap(c => Seq( min(col(c)).as(c + "_minValue"), max(col(c)).as(c + "_maxValue"), count(c).as(c + "_noNullCount")))
    val valueCounts = count("*").as("totalNum")
    val projectValues = Seq(col("file")) ++ cols.flatMap(c =>
      Seq(col(c + "_minValue"), col(c + "_maxValue"), expr(s"totalNum - ${c + "_noNullCount"}").as(c + "_num_nulls")))

    val result = df.select(input_file_name() as "file", col("*"))
      .groupBy($"file")
      .agg(valueCounts,  values: _*).select(projectValues:_*)
    result
  }

  def getMinMaxValue(df: DataFrame, zCols: String): DataFrame = {

    val rawCols = zCols.split(",").map(_.trim)

    val columnsMap = df.schema.fields.map(item => (item.name, item)).toMap

    val cols = rawCols.filter { col =>
      if (columnsMap.contains(col)) {
        columnsMap(col).dataType match {
          case IntegerType | DoubleType | StringType | DateType | LongType | FloatType | ShortType =>
            true
          case a: DecimalType =>
            true
          case other =>
            false
        }
      } else {
        false
      }
    }

    if (cols.size != rawCols.size) return getMinMaxValueSpark(df, rawCols)

    val inputFiles = df.inputFiles
    val conf = df.sparkSession.sparkContext.hadoopConfiguration

    val startTime = System.nanoTime()

    val allMetaData: Array[ColumnFileStats] = if (inputFiles.length < 10) {

      val listParallelism = math.min(Runtime.getRuntime.availableProcessors()/2 + 1, inputFiles.length)

      val slicedInputFiles = inputFiles.grouped(listParallelism)

      val threadPool = {
        val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("columnStatics" + "-%d").build()
        Executors.newFixedThreadPool(listParallelism, threadFactory).asInstanceOf[ThreadPoolExecutor]
      }

      try {
        implicit val executionContext = ExecutionContext.fromExecutor(threadPool)
        val staticTasks = slicedInputFiles.map { paths =>
          Future {
            paths.map(new Path(_)).flatMap { filePath =>
              val blocks = ParquetFileReader.readFooter(conf, filePath).getBlocks().asScala
              blocks.flatMap(b => b.getColumns().asScala.
                map(col => (col.getPath().toDotString(),
                  FileStats(col.getStatistics().minAsString(), col.getStatistics().maxAsString(), col.getStatistics.getNumNulls.toInt))))
                .groupBy(x => x._1).mapValues(v => v.map(vv => vv._2)).
                mapValues(value => FileStats(value.map(_.minVal).min, value.map(_.maxVal).max, value.map(_.num_nulls).max)).toSeq.
                map(x => ColumnFileStats(filePath.getName(), x._1, x._2.minVal, x._2.maxVal, x._2.num_nulls))
            }.filter(p => cols.contains(p.colName))
          }
        }

        val futureResult = try {
          val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
          Future.sequence(staticTasks).result(Duration.Inf)(awaitPermission)
        } catch {
          case e: Throwable =>
            throw e
        }
        futureResult.flatMap(x => x).toArray
      } finally {
        threadPool.shutdown()
      }
    } else {
      val sc = df.sparkSession.sparkContext
      val serializableConfiguration = new SerializableConfiguration(conf)
      val numParallelism = inputFiles.size/3
      val previousJobDescription = sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION)
      try {
        val description = s"Listing parquet column statistics"
        sc.setJobDescription(description)
        sc.parallelize(inputFiles, numParallelism).mapPartitions { paths =>
          val hadoopConf = serializableConfiguration.value
          paths.map(new Path(_)).flatMap { filePath =>
            val blocks = ParquetFileReader.readFooter(hadoopConf, filePath).getBlocks().asScala
            blocks.flatMap(b => b.getColumns().asScala.
              map(col => (col.getPath().toDotString(),
                FileStats(col.getStatistics().minAsString(), col.getStatistics().maxAsString(), col.getStatistics.getNumNulls.toInt))))
              .groupBy(x => x._1).mapValues(v => v.map(vv => vv._2)).
              mapValues(value => FileStats(value.map(_.minVal).min, value.map(_.maxVal).max, value.map(_.num_nulls).max)).toSeq.
              map(x => ColumnFileStats(filePath.getName(), x._1, x._2.minVal, x._2.maxVal, x._2.num_nulls))
          }.filter(p => cols.contains(p.colName))
        }.collect()
      } finally {
        sc.setJobDescription(previousJobDescription)
      }
    }

    val allMetaDataRDD = df.sparkSession.sparkContext.parallelize(allMetaData.groupBy(x => x.fileName).mapValues { css =>
      val size = css.length
      if (size == 0) {
        null
      } else {
        val rows = new ArrayBuffer[Any]()
        rows.append(css.head.fileName)
        cols.foreach { col =>
          val cs = css.find(p => p.colName.equals(col)).get
          columnsMap(cs.colName).dataType match {
            case IntegerType =>
              rows.append(cs.minVal.toInt)
              rows.append(cs.maxVal.toInt)
            case DoubleType =>
              rows.append(cs.minVal.toDouble)
              rows.append(cs.maxVal.toDouble)
            case StringType =>
              rows.append(cs.minVal)
              rows.append(cs.maxVal)
            case a: DecimalType =>
              rows.append(BigDecimal(cs.minVal))
              rows.append(BigDecimal(cs.maxVal))
            case DateType =>
              rows.append(Date.valueOf(cs.minVal))
              rows.append(Date.valueOf(cs.maxVal))
            case LongType =>
              rows.append(cs.minVal.toLong)
              rows.append(cs.maxVal.toLong)
            case ShortType =>
              rows.append(cs.minVal.toShort)
              rows.append(cs.maxVal.toShort)
            case FloatType =>
              rows.append(cs.minVal.toFloat)
              rows.append(cs.maxVal.toFloat)
          }
          rows.append(cs.num_nulls)
        }
        Row.fromSeq(rows)
      }
    }.map(_._2).filter(x => x != null).toSeq, 1)

    val allMetaDataSchema = {
      val neededFields = mutable.ListBuffer[StructField]()
      neededFields.append(new StructField("file", StringType, false))
      cols.foreach { col =>
        neededFields.append(columnsMap(col).copy(name = col + "_minValue"))
        neededFields.append(columnsMap(col).copy(name = col + "_maxValue"))
        neededFields.append(new StructField( col + "_num_nulls", IntegerType, true))
      }
      StructType(neededFields)
    }

    val metaDF = df.sparkSession.createDataFrame(allMetaDataRDD, allMetaDataSchema)
    metaDF
  }

  def createZIndexedDataFrameByMapValue(df: DataFrame, zCols: String, fileNum: Int): DataFrame = {
    if (zCols == null || zCols.isEmpty) {
      df
    } else {
      createZIndexedDataFrameByMapValue(df, zCols.split(",").map(_.trim), fileNum)
    }
  }

  /**
   * create z-order DataFrame directly
   * first, map all base type data to byte[8], then create z-order DataFrame
   * only support base type data. long,int,short,double,float,string,timestamp,decimal,date,byte
   * this method is more effective than createZIndexDataFrameBySample
   */
  def createZIndexedDataFrameByMapValue(df: DataFrame, zCols: Seq[String], fileNum: Int): DataFrame = {
    val columnsMap = df.schema.fields.map(item => (item.name, item)).toMap
    val fieldNum = df.schema.fields.length

    val checkCols = zCols.filter( col => columnsMap(col) != null)

    if (zCols.length ==0 && checkCols.size != zCols.size) {
      df
    } else {
      val zFields = zCols.map { col =>
        val newCol = columnsMap(col)
        (df.schema.fields.indexOf(newCol), newCol)
      }

      val newRDD = df.rdd.map { row =>
        val values = zFields.map { case (index, field) =>
          field.dataType match {
            case LongType =>
              ZOrderingUtil.longTo8Byte(if (row.isNullAt(index)) Long.MaxValue else row.getLong(index))
            case DoubleType =>
              ZOrderingUtil.doubleTo8Byte(if (row.isNullAt(index)) Double.MaxValue else row.getDouble(index))
            case IntegerType =>
              ZOrderingUtil.intTo8Byte(if (row.isNullAt(index)) Int.MaxValue else row.getInt(index))
            case FloatType =>
              ZOrderingUtil.doubleTo8Byte(if (row.isNullAt(index)) Float.MaxValue else row.getFloat(index).toDouble)
            case StringType =>
              ZOrderingUtil.utf8To8Byte(if (row.isNullAt(index)) "" else row.getString(index))
            case DateType =>
              ZOrderingUtil.longTo8Byte(if (row.isNullAt(index)) Long.MaxValue else row.getDate(index).getTime)
            case TimestampType =>
              ZOrderingUtil.longTo8Byte(if (row.isNullAt(index)) Long.MaxValue else row.getTimestamp(index).getTime)
            case ByteType =>
              ZOrderingUtil.byteTo8Byte(if (row.isNullAt(index)) Byte.MaxValue else row.getByte(index))
            case ShortType =>
              ZOrderingUtil.intTo8Byte(if (row.isNullAt(index)) Short.MaxValue else row.getShort(index).toInt)
            case d: DecimalType =>
              ZOrderingUtil.longTo8Byte(if (row.isNullAt(index)) Long.MaxValue else row.getDecimal(index).longValue())
            case _ =>
              null
          }
        }.filter(v => v != null).toArray
        val zValues = ZOrderingUtil.interleaving(values, 8)
        Row.fromSeq(row.toSeq ++ Seq(zValues))
      }.sortBy(x => ZorderingBinarySort(x.getAs[Array[Byte]](fieldNum)), numPartitions = fileNum)

      val newDF = df.sparkSession.createDataFrame(newRDD, StructType(
        df.schema.fields ++ Seq(
          StructField(s"zindex",
            BinaryType, false))
      ))
      newDF.drop("zindex")
    }
  }

  /**
   * create z_index filter and push those filters to index table to filter all candidate scan files.
   * @param condition  origin filter from query.
   * @param indexSchema schema from index table.
   * @return filters for index table.
   */
  def createZindexFilter(condition: Expression, indexSchema: StructType): Expression = {
    def buildExpressionInternal(colName: Seq[String], statisticValue: String): Expression = {
      val appendColName = UnresolvedAttribute(colName).name + statisticValue
      col(appendColName).expr
    }

    def reWriteCondition(colName: Seq[String], conditionExpress: Expression): Expression = {
      val appendColName = UnresolvedAttribute(colName).name + "_minValue"
      if (indexSchema.exists(p => p.name == appendColName)) {
        conditionExpress
      } else {
        Literal.TrueLiteral
      }
    }

    val minValue = (colName: Seq[String]) => buildExpressionInternal(colName, "_minValue")
    val maxValue = (colName: Seq[String]) => buildExpressionInternal(colName, "_maxValue")
    val num_nulls = (colName: Seq[String]) => buildExpressionInternal(colName, "_num_nulls")

    condition match {
      // query filter "colA = b"  convert it to "colA_minValue <= b and colA_maxValue >= b" for index table
      case EqualTo(attribute: AttributeReference, value: Literal) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, And(LessThanOrEqual(minValue(colName), value), GreaterThanOrEqual(maxValue(colName), value)))
      // query filter "b = colA"  convert it to "colA_minValue <= b and colA_maxValue >= b" for index table
      case EqualTo(value: Literal, attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, And(LessThanOrEqual(minValue(colName), value), GreaterThanOrEqual(maxValue(colName), value)))
      // query filter "colA = null"  convert it to "colA_num_nulls = null" for index table
      case equalNullSafe @ EqualNullSafe(_: AttributeReference, _ @ Literal(null, _)) =>
        val colName = getTargetColNameParts(equalNullSafe.left)
        reWriteCondition(colName, EqualTo(num_nulls(colName), equalNullSafe.right))
      // query filter "colA < b"  convert it to "colA_minValue < b" for index table
      case LessThan(attribute: AttributeReference, value: Literal) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName,LessThan(minValue(colName), value))
      // query filter "b < colA"  convert it to "colA_maxValue > b" for index table
      case LessThan(value: Literal, attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, GreaterThan(maxValue(colName), value))
      // query filter "colA > b"  convert it to "colA_maxValue > b" for index table
      case GreaterThan(attribute: AttributeReference, value: Literal) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, GreaterThan(maxValue(colName), value))
      // query filter "b > colA"  convert it to "colA_minValue < b" for index table
      case GreaterThan(value: Literal, attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, LessThan(minValue(colName), value))
      // query filter "colA <= b"  convert it to "colA_minValue <= b" for index table
      case LessThanOrEqual(attribute: AttributeReference, value: Literal) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, LessThanOrEqual(minValue(colName), value))
      // query filter "b <= colA"  convert it to "colA_maxValue >= b" for index table
      case LessThanOrEqual(value: Literal, attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, GreaterThanOrEqual(maxValue(colName), value))
      // query filter "colA >= b"   convert it to "colA_maxValue >= b" for index table
      case GreaterThanOrEqual(attribute: AttributeReference, right: Literal) =>
        val colName = getTargetColNameParts(attribute)
        GreaterThanOrEqual(maxValue(colName), right)
      // query filter "b >= colA"   convert it to "colA_minValue <= b" for index table
      case GreaterThanOrEqual(value: Literal, attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, LessThanOrEqual(minValue(colName), value))
      // query filter "colA is null"   convert it to "colA_num_nulls > 0" for index table
      case IsNull(attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, GreaterThan(num_nulls(colName), Literal(0)))
      // query filter "colA is not null"   convert it to "colA_num_nulls = 0" for index table
      case IsNotNull(attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, EqualTo(num_nulls(colName), Literal(0)))
      // query filter "colA in (a,b)"   convert it to " (colA_minValue <= a and colA_maxValue >= a) or (colA_minValue <= b and colA_maxValue >= b) " for index table
      case In(attribute: AttributeReference, list: Seq[Literal]) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, list.map { lit =>
          And(LessThanOrEqual(minValue(colName), lit), GreaterThanOrEqual(maxValue(colName), lit))
        }.reduce(Or))
      // query filter "colA like xxx"   convert it to "  (colA_minValue <= xxx and colA_maxValue >= xxx) or (colA_min start with xxx or colA_max start with xxx)  " for index table
      case StartsWith(attribute, v @ Literal(_: UTF8String, _)) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, Or(And(LessThanOrEqual(minValue(colName), v), GreaterThanOrEqual(maxValue(colName), v)) ,
          Or(StartsWith(minValue(colName), v), StartsWith(maxValue(colName), v))))
      // query filter "colA not in (a, b)"   convert it to " (not( colA_minValue = a and colA_maxValue = a)) and (not( colA_minValue = b and colA_maxValue = b)) " for index table
      case Not(In(attribute: AttributeReference, list: Seq[Literal])) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, list.map { lit =>
          Not(And(EqualTo(minValue(colName), lit), EqualTo(maxValue(colName), lit)))
        }.reduce(And))
      // query filter "colA != b"   convert it to "not ( colA_minValue = b and colA_maxValue = b )" for index table
      case Not(EqualTo(attribute: AttributeReference, value: Literal)) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, Not(And(EqualTo(minValue(colName), value), EqualTo(maxValue(colName), value))))
      // query filter "b != colA"   convert it to "not ( colA_minValue = b and colA_maxValue = b )" for index table
      case Not(EqualTo(value: Literal, attribute: AttributeReference)) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, Not(And(EqualTo(minValue(colName), value), EqualTo(maxValue(colName), value))))
      // query filter "colA not like xxxx"   convert it to "not ( colA_minValue startWith xxx and colA_maxValue startWith xxx)" for index table
      case Not(StartsWith(attribute, value @ Literal(_: UTF8String, _))) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, Not(And(StartsWith(minValue(colName), value), StartsWith(maxValue(colName), value))))
      case or: Or =>
        val resLeft = createZindexFilter(or.left, indexSchema)
        val resRight = createZindexFilter(or.right, indexSchema)
        Or(resLeft, resRight)

      case and: And =>
        val resLeft = createZindexFilter(and.left, indexSchema)
        val resRight = createZindexFilter(and.right, indexSchema)
        And(resLeft, resRight)

      case expr: Expression =>
        Literal.TrueLiteral
    }
  }

  /**
    * Extracts name from a resolved expression referring to a nested or non-nested column.
    */
  def getTargetColNameParts(resolvedTargetCol: Expression): Seq[String] = {
    resolvedTargetCol match {
      case attr: Attribute => Seq(attr.name)

      case Alias(c, _) => getTargetColNameParts(c)

      case GetStructField(c, _, Some(name)) => getTargetColNameParts(c) :+ name

      case ex: ExtractValue =>
        throw new AnalysisException(s"convert reference to name failed, Updating nested fields is only supported for StructType: ${ex}.")

      case other =>
        throw new AnalysisException(s"convert reference to name failed,  Found unsupported expression ${other}")
    }
  }

  def createDataFrameInternal(
      spark: SparkSession,
      catalystRows: RDD[InternalRow],
      schema: StructType,
      isStreaming: Boolean = false): DataFrame = {
    spark.internalCreateDataFrame(catalystRows, schema, isStreaming)
  }

  def getIndexFiles(conf: Configuration, indexPath: String): Seq[FileStatus] = {
    val basePath = new Path(indexPath)
    basePath.getFileSystem(conf)
      .listStatus(basePath).filterNot(f => shouldFilterOutPathName(f.getPath.getName))
  }

  /**
    * read parquet files concurrently by local.
    * this method is mush faster than spark
    */
  def readParquetFile(spark: SparkSession, indexFiles: Seq[FileStatus], filters: Seq[Filter] = Nil, schemaOpts: Option[StructType] = None): Set[String] = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val partitionedFiles = indexFiles.map(f => PartitionedFile(InternalRow.empty, f.getPath.toString, 0, f.getLen))

    val requiredSchema = new StructType().add("file", StringType, true)
    val schema = schemaOpts.getOrElse(requiredSchema)
    val parquetReader = new ParquetFileFormat().buildReaderWithPartitionValues(spark
      , schema , StructType(Nil), requiredSchema, filters, Map.empty, hadoopConf)
    val results = new Array[Iterator[String]](partitionedFiles.size)
    partitionedFiles.zipWithIndex.par.foreach { case (pf, index) =>
      val fileIterator = parquetReader(pf).asInstanceOf[Iterator[Any]]
      val rows = fileIterator.flatMap(_ match {
        case r: InternalRow => Seq(r)
        case b: ColumnarBatch => b.rowIterator().asScala
      }).map(r => r.getString(0))
      results(index) = rows
    }
    results.flatMap(f => f).toSet
  }

  def shouldFilterOutPathName(pathName: String): Boolean = {
    // We filter follow paths:
    // 1. everything that starts with _ and ., except _common_metadata and metadata
    // because Parquet needs to find those metadata files from leaf files returned by this method.
    // We should refactor this logic to not mix metadata files with data files.
    // 2. everything that ends with ._COPYING_, because this is a intermediate state of file. we
    // should skip this file in case of double reading.
    val exclude = (pathName.startsWith("") && !pathName.contains("=")) ||
      pathName.startsWith(".") || pathName.endsWith(".COPYING")
    val include = pathName.startsWith("_common_metadata") || pathName.startsWith("_metadata")
    exclude && !include
  }

  /**
    * update statistics info.
    * this method will update old index table by full out join,
    * and save the updated table into a new index table based on commitTime.
    * old index table will be cleaned also.
    */
  def saveStatisticsInfo(
      df: DataFrame,
      cols: String,
      indexPath: String,
      commitTime: String,
      validateCommits: Seq[String]): Unit = {
    val savePath = new Path(indexPath, commitTime)
    val spark = df.sparkSession
    val fs = savePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val statisticsDF = getMinMaxValue(df, cols)
    // try to find last validate index table from index path
    if (fs.exists(new Path(indexPath))) {
      // find all the indexTable from .hoodie/.index
      val allIndexTables = fs.listStatus(new Path(indexPath)).filter(_.isDirectory)
        .map(_.getPath.getName)
      val candidateIndexTables = allIndexTables.filter(f => validateCommits.contains(f)).sortBy(x => x).toList
      val residualTables = allIndexTables.filter(f => !validateCommits.contains(f))

      val optIndexDf = if (candidateIndexTables.isEmpty) {
        None
      } else {
        try {
          Some(spark.read.load(new Path(indexPath, candidateIndexTables.last).toString))
        } catch {
          case _: Throwable =>
            None
        }
      }
      // clean old index table, keep at most 1 index table
      candidateIndexTables.dropRight(1).foreach(f =>  fs.delete(new Path(indexPath, f)))
      // clean residualTables
      // retried cluster operations at the same instant time is also considered,
      // the residual files produced by retried are cleaned up before save statistics
      // save statistics info to index table which named commitTime
      residualTables.foreach(f => fs.delete(new Path(indexPath, f)))
      if (optIndexDf.isDefined && optIndexDf.get.schema.equals(statisticsDF.schema)) {
        val originalTable = "indexTable_" + java.util.UUID.randomUUID().toString.replace("-", "")
        val updateTable = "updateTable_" + java.util.UUID.randomUUID().toString.replace("-", "")
        optIndexDf.get.registerTempTable(originalTable)
        statisticsDF.registerTempTable(updateTable)
        // update table by full out join
        val cols = optIndexDf.get.schema.map(_.name)
        spark.sql(createSql(originalTable, updateTable, cols)).repartition(1).write.save(savePath.toString)
      } else {
        statisticsDF.repartition(1).write.mode("overwrite").save(savePath.toString)
      }
    } else {
      statisticsDF.repartition(1).write.mode("overwrite").save(savePath.toString)
    }

  }

  private def createSql(leftTable: String, rightTable: String, cols: Seq[String]): String = {
    var selectsql = ""
    for (i <- (0 to cols.size-1)) {
      selectsql = selectsql + s" if (${leftTable}.${cols(0)} is null, ${rightTable}.${cols(i)}, ${leftTable}.${cols(i)}) as ${cols(i)} ,"
    }
    "select " + selectsql.dropRight(1) + s" from ${leftTable} full join ${rightTable} on ${leftTable}.${cols(0)} = ${rightTable}.${cols(0)}"
  }
}

