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

import org.apache.spark.sql.HoodieSparkCatalogUtils.MatchBucketTransform
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue, fail}
import org.junit.jupiter.api.Test

class TestHoodieSparkCatalogUtils {

  @Test
  def testMatchBucketTransformExtractsBucketTransform(): Unit = {
    val transform: Transform = Expressions.bucket(8, "id")
    MatchBucketTransform.unapply(transform) match {
      case Some((numBuckets, refs, sortedRefs)) =>
        assertEquals(8, numBuckets)
        assertEquals(Seq(Seq("id")), refs.map(_.fieldNames().toSeq))
        assertTrue(sortedRefs.isEmpty)
      case None => fail("expected a bucket transform to match")
    }
  }

  @Test
  def testMatchBucketTransformIgnoresOtherTransforms(): Unit = {
    assertTrue(MatchBucketTransform.unapply(Expressions.identity("id")).isEmpty)
  }
}
