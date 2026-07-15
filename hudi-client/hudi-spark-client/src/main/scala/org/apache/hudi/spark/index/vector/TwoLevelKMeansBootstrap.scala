/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.spark.index.vector

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util.Random
import scala.collection.mutable.ArrayBuffer

/**
 * Two-level k-means training + assignment for the Hudi vector index bootstrap.
 *
 * Level 1 (coarse): MLlib KMeans over the training sample, k1 = sqrt(numClusters)-ish.
 * Level 2 (leaves): per-coarse-cell LOCAL Lloyd's inside executor tasks -- deliberately
 * NOT 256 separate MLlib jobs (per-job scheduling overhead x k1 would dwarf the math).
 *
 * Output leaf centroids are the flat cluster set the index/LIRE maintains; the coarse
 * level is retained only as a routing accelerator for assignment (rebuildable at any
 * centroid epoch).
 *
 * All leaf-level math is float32; MLlib's double Vectors are confined to the coarse fit.
 */
object TwoLevelKMeansBootstrap {

  private val LOG = LoggerFactory.getLogger(getClass)

  /** Trained structure: flat leaves + coarse routing. Broadcast for assignment. */
  final case class TwoLevelModel(
      coarseCentroids: Array[Array[Float]],
      leafCentroids: Array[Array[Float]],
      leafOffsets: Array[Int],
      leafNorms: Array[Float],
      coarseNorms: Array[Float]) extends Serializable {

    def numLeaves: Int = leafCentroids.length

    def leavesOf(coarseCell: Int): Range = leafOffsets(coarseCell) until leafOffsets(coarseCell + 1)
  }

  final case class TrainConfig(
      numClusters: Int,
      coarseCells: Int = 0,
      coarseMaxIter: Int = 10,
      subMaxIter: Int = 8,
      subConvergenceTol: Double = 1e-3,
      neighborExpandRatio: Float = 1.1f,
      maxClusterShareGate: Double = 0.05,
      seed: Long = 42L)

  def trainLeafCentroids(
      spark: SparkSession,
      sample: RDD[Array[Float]],
      numClusters: Int,
      coarseMaxIter: Int,
      seed: Long): Array[Array[Float]] = {
    train(
      spark,
      sample,
      TrainConfig(
        numClusters = numClusters,
        coarseMaxIter = math.max(1, coarseMaxIter),
        seed = seed))
      .leafCentroids
  }

  def trainModelForJava(
      spark: SparkSession,
      sample: RDD[Array[Float]],
      numClusters: Int,
      coarseMaxIter: Int,
      seed: Long): AnyRef = {
    train(
      spark,
      sample,
      TrainConfig(
        numClusters = numClusters,
        coarseMaxIter = math.max(1, coarseMaxIter),
        seed = seed))
  }

  def leafCentroidsForJava(model: AnyRef): Array[Array[Float]] =
    model.asInstanceOf[TwoLevelModel].leafCentroids

  def assignOneForJava(model: AnyRef, v: Array[Float], expandRatio: Float): Int =
    assignOne(model.asInstanceOf[TwoLevelModel], v, expandRatio)

  // ---------------------------------------------------------------------------
  // Training
  // ---------------------------------------------------------------------------

  def train(spark: SparkSession, sample: RDD[Array[Float]], cfg: TrainConfig): TwoLevelModel = {
    val dim = sample.first().length
    val k1 = if (cfg.coarseCells > 0) cfg.coarseCells else math.max(16, math.round(math.sqrt(cfg.numClusters)).toInt)
    val leavesPerCell = math.max(1, cfg.numClusters / k1)
    LOG.info("[vector_bootstrap][train] numClusters={} k1={} leavesPerCell={} dim={}",
      Int.box(cfg.numClusters), Int.box(k1), Int.box(leavesPerCell), Int.box(dim))

    // ---- Level 1: MLlib coarse fit (double Vectors only here) ----
    val t0 = System.nanoTime()
    val coarseInput = sample.map(v => Vectors.dense(v.map(_.toDouble))).cache()
    val coarseModel: KMeansModel = new MLlibKMeans()
      .setK(k1)
      .setMaxIterations(cfg.coarseMaxIter)
      .setInitializationMode(MLlibKMeans.K_MEANS_PARALLEL)
      .setSeed(cfg.seed)
      .run(coarseInput)
    coarseInput.unpersist(blocking = false)
    val coarse: Array[Array[Float]] = coarseModel.clusterCenters.map(_.toArray.map(_.toFloat))
    LOG.info("[vector_bootstrap][train] coarse fit done in {} ms", Long.box((System.nanoTime() - t0) / 1000000))

    // ---- Level 2: partition sample by coarse cell, local Lloyd's per cell ----
    val t1 = System.nanoTime()
    val bcCoarse = spark.sparkContext.broadcast(coarse)
    val bcCoarseNorms = spark.sparkContext.broadcast(coarse.map(sqNorm))

    val perCellLeaves: Array[(Int, Array[Array[Float]])] = sample
      .map { v => (nearest(bcCoarse.value, bcCoarseNorms.value, v), v) }
      .groupByKey(k1)
      .map { case (cell, it) =>
        val pts = it.toArray
        val leaves = localLloyds(pts, leavesPerCell, cfg.subMaxIter, cfg.subConvergenceTol,
          new Random(cfg.seed ^ cell))
        (cell, leaves)
      }
      .collect()
      .sortBy(_._1)
    bcCoarse.destroy()
    bcCoarseNorms.destroy()
    LOG.info("[vector_bootstrap][train] sub-level done in {} ms", Long.box((System.nanoTime() - t1) / 1000000))

    // ---- Flatten: dense leaf ids ordered by (coarseCell, localLeaf) ----
    val offsets = new Array[Int](k1 + 1)
    val flat = new ArrayBuffer[Array[Float]](cfg.numClusters)
    var cellIdx = 0
    perCellLeaves.foreach { case (cell, leaves) =>
      while (cellIdx < cell) { offsets(cellIdx + 1) = flat.length; cellIdx += 1 }
      flat ++= leaves
      offsets(cell + 1) = flat.length
      cellIdx = cell + 1
    }
    while (cellIdx < k1) { offsets(cellIdx + 1) = flat.length; cellIdx += 1 }

    val leafCentroids = flat.toArray
    val model = TwoLevelModel(coarse, leafCentroids, offsets, leafCentroids.map(sqNorm), coarse.map(sqNorm))
    LOG.info(s"[vector_bootstrap][train] leaves=${model.numLeaves} (requested ${cfg.numClusters})")
    model
  }

  private[vector] def localLloyds(
      points: Array[Array[Float]],
      k: Int,
      maxIter: Int,
      tol: Double,
      rnd: Random): Array[Array[Float]] = {
    val n = points.length
    if (n == 0) return Array.empty
    val effectiveK = math.min(k, n)
    val dim = points(0).length

    val initPool = if (n > 50 * effectiveK) sampleArray(points, 50 * effectiveK, rnd) else points
    var centroids = kmeansPlusPlusInit(initPool, effectiveK, rnd)

    val assign = new Array[Int](n)
    val counts = new Array[Long](effectiveK)
    val sums = Array.ofDim[Double](effectiveK, dim)
    var iter = 0
    var shift = Double.MaxValue
    while (iter < maxIter && shift > tol) {
      java.util.Arrays.fill(counts, 0L)
      sums.foreach(java.util.Arrays.fill(_, 0.0))
      val cNorms = centroids.map(sqNorm)
      var i = 0
      while (i < n) {
        val a = nearest(centroids, cNorms, points(i))
        assign(i) = a
        counts(a) += 1
        val s = sums(a)
        val p = points(i)
        var d = 0
        while (d < dim) { s(d) += p(d); d += 1 }
        i += 1
      }
      shift = 0.0
      var c = 0
      while (c < effectiveK) {
        if (counts(c) > 0) {
          val nc = new Array[Float](dim)
          var d = 0
          var delta = 0.0
          while (d < dim) {
            nc(d) = (sums(c)(d) / counts(c)).toFloat
            val dv = nc(d) - centroids(c)(d)
            delta += dv * dv
            d += 1
          }
          shift = math.max(shift, math.sqrt(delta) / math.max(1e-12, math.sqrt(sqNorm(centroids(c)))))
          centroids(c) = nc
        } else {
          centroids(c) = points(rnd.nextInt(n)).clone()
          shift = Double.MaxValue
        }
        c += 1
      }
      iter += 1
    }
    centroids
  }

  private def kmeansPlusPlusInit(points: Array[Array[Float]], k: Int, rnd: Random): Array[Array[Float]] = {
    val out = new Array[Array[Float]](k)
    out(0) = points(rnd.nextInt(points.length)).clone()
    val d2 = Array.fill(points.length)(Double.MaxValue)
    var chosen = 1
    while (chosen < k) {
      var sum = 0.0
      var i = 0
      while (i < points.length) {
        val d = l2Sq(points(i), out(chosen - 1))
        if (d < d2(i)) d2(i) = d
        sum += d2(i)
        i += 1
      }
      val target = rnd.nextDouble() * sum
      var pick = 0
      i = 0
      var acc = 0.0
      var selected = false
      while (i < points.length && !selected) {
        acc += d2(i)
        if (acc >= target) {
          pick = i
          selected = true
        }
        i += 1
      }
      out(chosen) = points(pick).clone()
      chosen += 1
    }
    out
  }

  // ---------------------------------------------------------------------------
  // Assignment (the 1B-row pass)
  // ---------------------------------------------------------------------------

  def assignAll[T](
      rows: RDD[(T, Array[Float])],
      bcModel: Broadcast[TwoLevelModel],
      expandRatio: Float): RDD[(T, Int)] = {
    rows.mapPartitions { it =>
      val m = bcModel.value
      it.map { case (key, v) =>
        (key, assignOne(m, v, expandRatio))
      }
    }
  }

  private[vector] def assignOne(m: TwoLevelModel, v: Array[Float], expandRatio: Float): Int = {
    val k1 = m.coarseCentroids.length
    val queryNorm = sqNorm(v).toDouble
    var bestD = Double.MaxValue
    val cellD = new Array[Double](k1)
    var c = 0
    while (c < k1) {
      val d = math.max(0.0, l2SqWithNorm(v, m.coarseCentroids(c), m.coarseNorms(c)) + queryNorm)
      cellD(c) = d
      if (d < bestD) {
        bestD = d
      }
      c += 1
    }
    val limit = bestD * expandRatio * expandRatio
    var bestLeaf = -1
    var bestLeafD = Double.MaxValue
    c = 0
    while (c < k1) {
      if (cellD(c) <= limit) {
        var l = m.leafOffsets(c)
        val end = m.leafOffsets(c + 1)
        while (l < end) {
          val d = l2SqWithNorm(v, m.leafCentroids(l), m.leafNorms(l))
          if (d < bestLeafD) { bestLeafD = d; bestLeaf = l }
          l += 1
        }
      }
      c += 1
    }
    if (bestLeaf < 0) {
      bestLeaf = bruteForceNearest(m.leafCentroids, m.leafNorms, v)
    }
    bestLeaf
  }

  final case class TrainGates(relativeQuantError: Double, maxClusterShare: Double, assignmentMismatchPct: Double)

  def evaluateGates(
      sample: RDD[Array[Float]],
      bcModel: Broadcast[TwoLevelModel],
      expandRatio: Float,
      bruteForceCheckFraction: Double = 0.001): TrainGates = {
    val stats = sample.mapPartitions { it =>
      val m = bcModel.value
      val rnd = new Random(7)
      var qErr = 0.0
      var norm = 0.0
      var n = 0L
      var checked = 0L
      var mismatched = 0L
      val counts = new java.util.HashMap[Integer, Long]()
      it.foreach { v =>
        val leaf = assignOne(m, v, expandRatio)
        qErr += l2Sq(v, m.leafCentroids(leaf))
        norm += sqNorm(v).toDouble
        n += 1
        counts.merge(leaf, 1L, new java.util.function.BiFunction[Long, Long, Long] {
          override def apply(a: Long, b: Long): Long = a + b
        })
        if (rnd.nextDouble() < bruteForceCheckFraction) {
          checked += 1
          if (bruteForceNearest(m.leafCentroids, m.leafNorms, v) != leaf) mismatched += 1
        }
      }
      Iterator.single((qErr, norm, n, checked, mismatched, counts))
    }.reduce { (a, b) =>
      b._6.forEach(new java.util.function.BiConsumer[Integer, Long] {
        override def accept(k: Integer, v: Long): Unit = a._6.merge(k, v, new java.util.function.BiFunction[Long, Long, Long] {
          override def apply(x: Long, y: Long): Long = x + y
        })
      })
      (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6)
    }
    var maxCount = 0L
    stats._6.forEach(new java.util.function.BiConsumer[Integer, Long] {
      override def accept(k: Integer, v: Long): Unit = if (v > maxCount) maxCount = v
    })
    TrainGates(
      relativeQuantError = stats._1 / math.max(1e-12, stats._2),
      maxClusterShare = maxCount.toDouble / math.max(1L, stats._3),
      assignmentMismatchPct = if (stats._4 == 0) 0.0 else 100.0 * stats._5 / stats._4)
  }

  private def bruteForceNearest(cs: Array[Array[Float]], norms: Array[Float], v: Array[Float]): Int = {
    var best = 0
    var bd = Double.MaxValue
    var i = 0
    while (i < cs.length) {
      val d = l2SqWithNorm(v, cs(i), norms(i))
      if (d < bd) { bd = d; best = i }
      i += 1
    }
    best
  }

  // ---------------------------------------------------------------------------
  // Float math helpers
  // ---------------------------------------------------------------------------

  private def sqNorm(v: Array[Float]): Float = {
    var s = 0.0
    var i = 0
    while (i < v.length) { s += v(i).toDouble * v(i); i += 1 }
    s.toFloat
  }

  private def l2Sq(a: Array[Float], b: Array[Float]): Double = {
    var s = 0.0
    var i = 0
    while (i < a.length) { val d = a(i).toDouble - b(i); s += d * d; i += 1 }
    s
  }

  private def l2SqWithNorm(v: Array[Float], c: Array[Float], cNormSq: Float): Double = {
    var dot = 0.0
    var i = 0
    while (i < v.length) { dot += v(i).toDouble * c(i); i += 1 }
    cNormSq - 2.0 * dot
  }

  private def nearest(cs: Array[Array[Float]], norms: Array[Float], v: Array[Float]): Int = {
    var best = 0
    var bd = Double.MaxValue
    var i = 0
    while (i < cs.length) {
      val d = l2SqWithNorm(v, cs(i), norms(i))
      if (d < bd) { bd = d; best = i }
      i += 1
    }
    best
  }

  private def sampleArray(pts: Array[Array[Float]], n: Int, rnd: Random): Array[Array[Float]] = {
    val out = new Array[Array[Float]](n)
    var i = 0
    while (i < n) { out(i) = pts(rnd.nextInt(pts.length)); i += 1 }
    out
  }
}
