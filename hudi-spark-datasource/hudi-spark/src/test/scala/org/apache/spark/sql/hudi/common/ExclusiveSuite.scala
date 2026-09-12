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

package org.apache.spark.sql.hudi.common

import org.scalatest.{Args, Status}

/**
 * Marks a suite that must not run at the same time as any other suite in the JVM because it
 * mutates JVM-wide state (the shared Hadoop conf, persisted RDDs of the shared context, a
 * static metrics registry). Takes the write side of [[HoodieSparkSqlTestBase.suiteLock]] for
 * its whole run; every other suite holds the read side for its whole run. An exclusive suite
 * therefore waits for the suites already running to finish, not just for their current test. No
 * effect until suites run concurrently.
 */
trait ExclusiveSuite extends HoodieSparkSqlTestBase {

  abstract override def run(testName: Option[String], args: Args): Status = {
    val lock = HoodieSparkSqlTestBase.suiteLock.writeLock()
    lock.lock()
    try {
      super.run(testName, args)
    } finally {
      lock.unlock()
    }
  }
}
