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

package org.apache.hudi.util

/**
 * Extension of the [[Iterator]] allowing for caching of the underlying record produced
 * during iteration to provide for the idempotency of the [[hasNext]] invocation:
 * meaning, that invoking [[hasNext]] multiple times consequently (w/o invoking [[next]]
 * in between) will only make iterator step over a single element
 *
 * NOTE: [[hasNext]] and [[next]] are purposefully marked as final, requiring iteration
 *       semantic to be implemented t/h overriding of a single [[doHasNext]] method
 */
trait CachingIterator[T >: Null] extends Iterator[T] {

  protected var nextRecord: T = _

  protected def doHasNext: Boolean

  override final def hasNext: Boolean = nextRecord != null || doHasNext

  override final def next: T = {
    val record = nextRecord
    nextRecord = null
    record
  }

}
