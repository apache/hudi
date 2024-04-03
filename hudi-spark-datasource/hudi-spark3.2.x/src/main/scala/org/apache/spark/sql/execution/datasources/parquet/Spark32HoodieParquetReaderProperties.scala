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

package org.apache.spark.sql.execution.datasources.parquet

case class Spark32HoodieParquetReaderProperties(enableVectorizedReader: Boolean,
                                                datetimeRebaseModeInRead: String,
                                                int96RebaseModeInRead: String,
                                                enableParquetFilterPushDown: Boolean,
                                                pushDownDate: Boolean,
                                                pushDownTimestamp: Boolean,
                                                pushDownDecimal: Boolean,
                                                pushDownInFilterThreshold: Int,
                                                pushDownStringStartWith: Boolean,
                                                isCaseSensitive: Boolean,
                                                timestampConversion: Boolean,
                                                enableOffHeapColumnVector: Boolean,
                                                capacity: Int,
                                                returningBatch: Boolean,
                                                enableRecordFilter: Boolean,
                                                timeZoneId: Option[String])
  extends Spark3HoodieParquetReaderProperties(
    enableVectorizedReader = enableVectorizedReader,
    datetimeRebaseModeInRead = datetimeRebaseModeInRead,
    int96RebaseModeInRead = int96RebaseModeInRead,
    enableParquetFilterPushDown = enableParquetFilterPushDown,
    pushDownDate = pushDownDate,
    pushDownTimestamp = pushDownTimestamp,
    pushDownDecimal = pushDownDecimal,
    pushDownInFilterThreshold = pushDownInFilterThreshold,
    pushDownStringStartWith = pushDownStringStartWith,
    isCaseSensitive = isCaseSensitive,
    timestampConversion = timestampConversion,
    enableOffHeapColumnVector = enableOffHeapColumnVector,
    capacity = capacity,
    returningBatch = returningBatch,
    enableRecordFilter = enableRecordFilter,
    timeZoneId = timeZoneId)
