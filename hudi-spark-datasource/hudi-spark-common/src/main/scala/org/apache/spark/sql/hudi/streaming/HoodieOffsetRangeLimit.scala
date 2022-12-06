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

package org.apache.spark.sql.hudi.streaming

/**
 * Objects that describe the offset of the HoodieStreamSource start
 * Available types are earliest, latest, specified commit range.
 */
sealed trait HoodieOffsetRangeLimit

/**
 * Represent starting from the earliest commit in the hoodie timeline
 */
object HoodieEarliestOffsetRangeLimit extends HoodieOffsetRangeLimit

/**
 * Represent starting from the latest commit in the hoodie timeline
 */
object HoodieLatestOffsetRangeLimit extends HoodieOffsetRangeLimit

/**
 * Represent starting from the specified commit in the hoodie timeline
 */
case class HoodieSpecifiedOffsetRangeLimit(instantTime: String) extends HoodieOffsetRangeLimit


