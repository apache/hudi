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

package org.apache.spark.sql.hudi.command

import java.util.UUID
import org.apache.avro.generic.GenericRecord
import org.apache.hudi.common.config.TypedProperties
import org.apache.spark.sql.Row

/**
 * A KeyGenerator which use the uuid as the record key.
 */
class UuidKeyGenerator(props: TypedProperties) extends SqlKeyGenerator(props) {

  override def getRecordKey(record: GenericRecord): String = UUID.randomUUID.toString

  override def getRecordKey(row: Row): String = UUID.randomUUID.toString
}
