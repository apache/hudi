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

package org.apache.spark.sql.hudi.v2;

import org.apache.spark.sql.connector.read.SupportsPushDownLimit;

/**
 * Extension of {@link SupportsPushDownLimit} that marks limit pushdown as partial.
 *
 * Spark 3.4+ added {@code isPartiallyPushed()} as a default method (returning false).
 * Spark 3.3 does not have this method. By providing the default here in a Java interface,
 * we avoid the Scala {@code override} keyword issue: Java default methods don't require
 * {@code @Override}, so this compiles against both Spark 3.3 (new method) and 3.4+ (override).
 *
 * When {@code isPartiallyPushed()} returns true, Spark adds a final LocalLimit on top of the
 * scan, which is necessary because Hudi's limit pushdown is best-effort (per-partition).
 */
public interface PartialLimitPushDown extends SupportsPushDownLimit {
  default boolean isPartiallyPushed() {
    return true;
  }
}
