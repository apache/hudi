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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.internal.SQLConf

import java.io.Closeable
import java.lang.ref.SoftReference
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Reuses the [[UnsafeProjection]]s that readers generate per file across the files a thread reads.
 *
 * Generating the projection of a wide nested schema costs milliseconds: Spark caches the compiled class, but it
 * generates and splits the source again for every projection before it looks the class up. A reader generates one
 * projection per file, although the projection only depends on the schemas and casts it is generated for, which are
 * the same for most files of a scan. So each thread keeps the projections its finished iterators used, keyed by those
 * inputs, and hands them to later iterators with the same key, in the same task or in later tasks.
 *
 * An UnsafeProjection writes every row into the same buffer, so it serves one iterator at a time:
 *  - A [[Lease]] takes its projection out of the pool, so iterators whose rows are alive at the same time, even in
 *    one task, never share a projection.
 *  - The projection goes back to the pool only once its iterator has no more rows. A caller may use a row of a scan
 *    only until it calls hasNext or next on that iterator again (the vectorized reader reloads its batch in hasNext),
 *    so no row the projection wrote is still in use when a later iterator takes it. An iterator that is not read to
 *    the end never gives its projection back.
 *  - Each thread has its own pool and only that thread reads or changes it, so a projection is never used by two
 *    threads at once.
 *
 * A key must hold everything the generated projection depends on, with value equality. Expressions other than column
 * references, such as casts and map builders, read the SQL conf when they are built, so the keys of such projections
 * include [[sqlConf]]. The key is built, and the projection generated, on the thread that projects the first row.
 *
 * The pool keeps a projection's buffer, which grows to twice the largest row it wrote and never shrinks. A projection
 * that wrote a row larger than [[MAX_RETAINED_ROW_BYTES]] is dropped instead of kept, and idle projections are held
 * through soft references, so that the garbage collector can clear them under memory pressure.
 */
object UnsafeProjectionPool {

  /** The most idle projections a thread keeps, one per key. The least recently returned one goes first. */
  val MAX_IDLE_PER_THREAD = 16

  /** A projection that wrote a larger row is not kept idle: its buffer is at least twice that size. */
  val MAX_RETAINED_ROW_BYTES: Int = 1024 * 1024

  private val idle: ThreadLocal[util.LinkedHashMap[AnyRef, SoftReference[UnsafeProjection]]] =
    new ThreadLocal[util.LinkedHashMap[AnyRef, SoftReference[UnsafeProjection]]] {
      override def initialValue(): util.LinkedHashMap[AnyRef, SoftReference[UnsafeProjection]] =
        new util.LinkedHashMap[AnyRef, SoftReference[UnsafeProjection]](MAX_IDLE_PER_THREAD, 0.75f, true) {
          override def removeEldestEntry(eldest: util.Map.Entry[AnyRef, SoftReference[UnsafeProjection]]): Boolean =
            size() > MAX_IDLE_PER_THREAD
        }
    }

  /** Generations per key class, for tests. */
  private val generationCounts = new ConcurrentHashMap[Class[_], AtomicLong]()

  /** When false, every lease generates its projection and none is kept, as before the pool. For tests. */
  @volatile private[sql] var poolingEnabled: Boolean = true

  /**
   * Leases the projection of `key` for one iterator. The key is built, and the projection taken from the calling
   * thread's pool or generated with `generate`, when the first row is projected, so an iterator that returns no rows
   * needs neither.
   *
   * @param closeOnFailure closed when `generate` fails, such as the reader whose rows the projection was for, since
   *                       the failure surfaces while the caller reads rows rather than while it opens the reader
   */
  def lease(key: => AnyRef, generate: => UnsafeProjection, closeOnFailure: Closeable = null): Lease =
    new Lease(() => key, () => generate, closeOnFailure)

  /**
   * The SQL configs the calling task runs with (the session's on the driver), for keys of projections whose
   * expressions read them when they are built. Only registered SQL configs are kept: a task also sees the local
   * properties of its job, such as spark.sql.execution.id, which change with every query and would keep the key from
   * ever matching again.
   */
  def sqlConf: Map[String, String] =
    SQLConf.get.getAllConfs.filter { case (key, _) => SQLConf.containsConfigKey(key) }

  /** The number of idle projections the calling thread holds. */
  private[sql] def idleCount: Int = {
    val iterator = idle.get().values().iterator()
    var count = 0
    while (iterator.hasNext) {
      if (iterator.next().get() != null) {
        count += 1
      }
    }
    count
  }

  /** Drops the idle projections of the calling thread. */
  private[sql] def clear(): Unit = idle.get().clear()

  /** The number of projections generated so far for keys of class `keyClass`, on all threads. */
  private[sql] def generations(keyClass: Class[_]): Long = {
    val count = generationCounts.get(keyClass)
    if (count == null) 0L else count.get()
  }

  private def take(key: AnyRef): UnsafeProjection = {
    val pooled = idle.get().remove(key)
    if (pooled == null) null else pooled.get()
  }

  private def giveBack(key: AnyRef, projection: UnsafeProjection): Unit = {
    val pool = idle.get()
    val existing = pool.get(key)
    if (existing == null || existing.get() == null) {
      pool.put(key, new SoftReference(projection))
    }
  }

  private def countGeneration(key: AnyRef): Unit = {
    var count = generationCounts.get(key.getClass)
    if (count == null) {
      generationCounts.putIfAbsent(key.getClass, new AtomicLong())
      count = generationCounts.get(key.getClass)
    }
    count.incrementAndGet()
  }

  /**
   * The projection of one iterator. Not thread-safe, like the projection itself: an iterator is read by one thread
   * at a time.
   */
  final class Lease private[UnsafeProjectionPool](keyOf: () => AnyRef,
                                                  generate: () => UnsafeProjection,
                                                  closeOnFailure: Closeable) {
    private var key: AnyRef = _
    private var leased: UnsafeProjection = _
    private var largestRow = 0
    private var released = false

    /** Projects `row` into the projection's buffer, overwriting the row it returned last. */
    def apply(row: InternalRow): UnsafeRow = {
      if (leased == null) {
        leased = acquire()
      }
      val projected = leased(row)
      val size = projected.getSizeInBytes
      if (size > largestRow) {
        largestRow = size
      }
      projected
    }

    /** This lease as an UnsafeProjection, for callers that take one. It leases nothing until it projects a row. */
    def asProjection: UnsafeProjection = new UnsafeProjection {
      override def apply(row: InternalRow): UnsafeRow = Lease.this.apply(row)
    }

    /**
     * Returns the projection to the calling thread's pool, once no row it wrote is used any more, unless it wrote a
     * row larger than [[MAX_RETAINED_ROW_BYTES]]. Later calls do nothing.
     */
    def release(): Unit = {
      if (!released) {
        released = true
        if (leased != null) {
          if (poolingEnabled && largestRow <= MAX_RETAINED_ROW_BYTES) {
            giveBack(key, leased)
          }
          leased = null
        }
      }
    }

    /** Wraps `rows`, whose rows this lease projects, to release it once `rows` has no more rows. */
    def releaseWhenExhausted[T](rows: Iterator[T]): Iterator[T] = new Iterator[T] {
      override def hasNext: Boolean = checkHasNext(rows.hasNext)

      override def next(): T = rows.next()
    }

    /** As [[releaseWhenExhausted]], for iterators that the caller may close before they are exhausted. */
    def releaseWhenExhaustedCloseable[T](rows: Iterator[T] with Closeable): Iterator[T] with Closeable =
      new Iterator[T] with Closeable {
        override def hasNext: Boolean = checkHasNext(rows.hasNext)

        override def next(): T = rows.next()

        override def close(): Unit = rows.close()
      }

    /** Releases the lease when `hasNext` is false, and returns `hasNext`. */
    def checkHasNext(hasNext: Boolean): Boolean = {
      if (!hasNext) {
        release()
      }
      hasNext
    }

    private def acquire(): UnsafeProjection = {
      if (key == null) {
        key = keyOf()
      }
      val pooled = if (poolingEnabled) take(key) else null
      if (pooled != null) {
        pooled
      } else {
        val generated = try {
          generate()
        } catch {
          case failure: Throwable =>
            if (closeOnFailure != null) {
              try {
                closeOnFailure.close()
              } catch {
                case closeFailure: Throwable => failure.addSuppressed(closeFailure)
              }
            }
            throw failure
        }
        countGeneration(key)
        generated
      }
    }
  }
}
