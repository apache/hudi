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

package org.apache.hudi

import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.{ComplexKeyGenerator, SimpleKeyGenerator}

import org.apache.spark.sql.hudi.command.SqlKeyGenerator

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class TestHoodieSparkSqlWriter2 {

  @Test
  def testGetOriginKeyGenerator(): Unit = {
    // for dataframe write
    val m1 = Map(
      HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key -> classOf[ComplexKeyGenerator].getName
    )
    val kg1 = HoodieWriterUtils.getOriginKeyGenerator(m1)
    assertTrue(kg1 == classOf[ComplexKeyGenerator].getName)

    // for sql write
    val m2 = Map(
      HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key -> classOf[SqlKeyGenerator].getName,
      SqlKeyGenerator.ORIGIN_KEYGEN_CLASS_NAME -> classOf[SimpleKeyGenerator].getName
    )
    val kg2 = HoodieWriterUtils.getOriginKeyGenerator(m2)
    assertTrue(kg2 == classOf[SimpleKeyGenerator].getName)
  }
}
