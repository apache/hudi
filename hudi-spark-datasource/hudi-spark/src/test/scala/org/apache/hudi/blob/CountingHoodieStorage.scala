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

package org.apache.hudi.blob

import org.apache.hudi.storage.{StorageConfiguration, StoragePath}
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage

import java.util.concurrent.atomic.AtomicInteger

/**
 * A hoodie.storage.class that counts construction and close, so a test can assert every storage a
 * reader resolves is also closed. HoodieHadoopStorage.close is deliberately a no-op because it does
 * not own the cached Hadoop filesystem, which is exactly why a no-op cannot stand in for proof that
 * close is called.
 *
 * The counters are static and the tests using them run Spark in-process, so executor-side
 * construction is visible to the assertions.
 */
class CountingHoodieStorage(path: StoragePath, conf: StorageConfiguration[_])
  extends HoodieHadoopStorage(path, conf) {

  CountingHoodieStorage.constructed.incrementAndGet()

  override def close(): Unit = {
    CountingHoodieStorage.closed.incrementAndGet()
    super.close()
  }
}

object CountingHoodieStorage {
  val constructed = new AtomicInteger(0)
  val closed = new AtomicInteger(0)

  def reset(): Unit = {
    constructed.set(0)
    closed.set(0)
  }
}
