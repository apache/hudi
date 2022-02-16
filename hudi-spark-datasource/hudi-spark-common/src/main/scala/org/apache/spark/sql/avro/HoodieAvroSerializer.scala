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
import org.apache.spark.sql.types.DataType

/**
 * As AvroSerializer cannot be access out of the spark.sql.avro package since spark 3.1, we define
 * this class to be accessed by other class.
 */
case class HoodieAvroSerializer(rootCatalystType: DataType, rootAvroType: Schema, nullable: Boolean)
  extends AvroSerializer(rootCatalystType, rootAvroType, nullable)
