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

package org.apache.spark.sql.hudi.blob

import org.apache.hudi.io.SeekableDataInputStream
import org.apache.hudi.storage.{HoodieStorage, StorageConfiguration, StoragePath, StoragePathFilter, StoragePathInfo}

import java.io.{InputStream, OutputStream}
import java.net.URI
import java.util.{List => JList}
import java.util.concurrent.atomic.AtomicInteger

/**
 * Test-only {@link HoodieStorage} decorator that counts I/O operations performed by
 * batched blob reads. Delegates every method to an underlying storage, but tracks:
 *
 *  - openSeekableCount — number of times openSeekable was called
 *  - seekCount         — number of times seek was called on a returned SeekableDataInputStream
 *
 * Use this wrapper in tests to assert that batching reduces the number of physical
 * I/O operations versus a non-batching configuration. Counters are reset via
 * {@link #reset()}.
 */
class CountingHoodieStorage(delegate: HoodieStorage) extends HoodieStorage(delegate.getConf) {

  val openSeekableCount = new AtomicInteger(0)
  val seekCount = new AtomicInteger(0)

  def reset(): Unit = {
    openSeekableCount.set(0)
    seekCount.set(0)
  }

  override def openSeekable(path: StoragePath, bufferSize: Int, wrapStream: Boolean): SeekableDataInputStream = {
    openSeekableCount.incrementAndGet()
    val inner = delegate.openSeekable(path, bufferSize, wrapStream)
    new CountingSeekableDataInputStream(inner, seekCount)
  }

  // --- pass-through delegation -------------------------------------------------

  override def newInstance(path: StoragePath, storageConf: StorageConfiguration[_]): HoodieStorage =
    new CountingHoodieStorage(delegate.newInstance(path, storageConf))

  override def getScheme: String = delegate.getScheme
  override def getDefaultBlockSize(path: StoragePath): Int = delegate.getDefaultBlockSize(path)
  override def getDefaultBufferSize: Int = delegate.getDefaultBufferSize
  override def getDefaultReplication(path: StoragePath): Short = delegate.getDefaultReplication(path)
  override def getUri: URI = delegate.getUri

  override def create(path: StoragePath, overwrite: Boolean): OutputStream =
    delegate.create(path, overwrite)
  override def create(path: StoragePath, overwrite: Boolean, bufferSize: Integer, replication: java.lang.Short, sizeThreshold: java.lang.Long): OutputStream =
    delegate.create(path, overwrite, bufferSize, replication, sizeThreshold)

  override def open(path: StoragePath): InputStream = delegate.open(path)
  override def append(path: StoragePath): OutputStream = delegate.append(path)

  override def exists(path: StoragePath): Boolean = delegate.exists(path)
  override def getPathInfo(path: StoragePath): StoragePathInfo = delegate.getPathInfo(path)
  override def createDirectory(path: StoragePath): Boolean = delegate.createDirectory(path)

  override def listDirectEntries(path: StoragePath): JList[StoragePathInfo] = delegate.listDirectEntries(path)
  override def listFiles(path: StoragePath): JList[StoragePathInfo] = delegate.listFiles(path)
  override def listDirectEntries(path: StoragePath, filter: StoragePathFilter): JList[StoragePathInfo] =
    delegate.listDirectEntries(path, filter)

  override def setModificationTime(path: StoragePath, modificationTimeInMillisEpoch: Long): Unit =
    delegate.setModificationTime(path, modificationTimeInMillisEpoch)

  override def globEntries(pathPattern: StoragePath, filter: StoragePathFilter): JList[StoragePathInfo] =
    delegate.globEntries(pathPattern, filter)

  override def rename(oldPath: StoragePath, newPath: StoragePath): Boolean =
    delegate.rename(oldPath, newPath)

  override def deleteDirectory(path: StoragePath): Boolean = delegate.deleteDirectory(path)
  override def deleteFile(path: StoragePath): Boolean = delegate.deleteFile(path)

  override def getFileSystem: AnyRef = delegate.getFileSystem
  override def getRawStorage: HoodieStorage = delegate.getRawStorage

  override def close(): Unit = delegate.close()
}

/**
 * Counting wrapper around a {@link SeekableDataInputStream}. Counts seeks; final
 * readFully methods on DataInputStream delegate to the underlying stream's
 * read(...) which the wrapped stream services directly.
 */
private class CountingSeekableDataInputStream(
    delegate: SeekableDataInputStream,
    seekCount: AtomicInteger)
    extends SeekableDataInputStream(delegate) {

  override def seek(pos: Long): Unit = {
    seekCount.incrementAndGet()
    delegate.seek(pos)
  }

  override def getPos: Long = delegate.getPos

  override def close(): Unit = delegate.close()
}
