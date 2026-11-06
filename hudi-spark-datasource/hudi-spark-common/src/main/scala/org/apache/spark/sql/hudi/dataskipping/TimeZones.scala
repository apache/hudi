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

package org.apache.spark.sql.hudi.dataskipping

import org.apache.spark.sql.catalyst.util.DateTimeUtils

import scala.util.Try

/**
 * Time zone checks shared by the rules whose ordering depends on a zone's offset: `TimeZoneShiftRule`, and
 * `DateTimeFormatRule` through `DateTimeOrdering`.
 */
object TimeZones {

  /**
   * Whether the zone never changes its offset, neither for DST nor historically. It is resolved the way Spark
   * evaluates it, so for ex +8:00 is +08:00 and the short ID PST is America/Los_Angeles; an unknown zone is not
   * fixed.
   */
  def isFixedOffsetZone(timeZoneId: String): Boolean =
    Try(DateTimeUtils.getZoneId(timeZoneId).getRules.isFixedOffset).getOrElse(false)
}
