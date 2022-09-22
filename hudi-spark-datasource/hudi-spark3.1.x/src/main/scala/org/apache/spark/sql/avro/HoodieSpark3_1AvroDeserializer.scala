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

package org.apache.spark.sql.avro

import org.apache.avro.Schema
import org.apache.spark.sql.catalyst.NoopFilters
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types.DataType

class HoodieSpark3_1AvroDeserializer(rootAvroType: Schema, rootCatalystType: DataType)
  extends HoodieAvroDeserializer {

  private val avroDeserializer = {
    val avroRebaseModeInRead = LegacyBehaviorPolicy.withName(SQLConf.get.getConf(SQLConf.LEGACY_AVRO_REBASE_MODE_IN_READ))
    new AvroDeserializer(rootAvroType, rootCatalystType, avroRebaseModeInRead, new NoopFilters)
  }


  def deserialize(data: Any): Option[Any] = avroDeserializer.deserialize(data)
}
