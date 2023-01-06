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

package org.apache.hudi.spark3.internal;

import org.apache.hudi.HoodieSparkUtils;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.util.DateFormatter;

import scala.Option;
import scala.collection.Seq;
import scala.collection.immutable.Map;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.time.ZoneId;

public class ReflectUtil {

  public static InsertIntoStatement createInsertInto(LogicalPlan table, Map<String, Option<String>> partition, Seq<String> userSpecifiedCols,
                                                     LogicalPlan query, boolean overwrite, boolean ifPartitionNotExists) {
    try {
      if (HoodieSparkUtils.isSpark3_0()) {
        Constructor<InsertIntoStatement> constructor = InsertIntoStatement.class.getConstructor(
                LogicalPlan.class, Map.class, LogicalPlan.class, boolean.class, boolean.class);
        return constructor.newInstance(table, partition, query, overwrite, ifPartitionNotExists);
      } else {
        Constructor<InsertIntoStatement> constructor = InsertIntoStatement.class.getConstructor(
                LogicalPlan.class, Map.class, Seq.class, LogicalPlan.class, boolean.class, boolean.class);
        return constructor.newInstance(table, partition, userSpecifiedCols, query, overwrite, ifPartitionNotExists);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error in create InsertIntoStatement", e);
    }
  }

  public static DateFormatter getDateFormatter(ZoneId zoneId) {
    try {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      if (HoodieSparkUtils.gteqSpark3_2()) {
        Class<?> clazz = loader.loadClass(DateFormatter.class.getName());
        Method applyMethod = clazz.getDeclaredMethod("apply");
        applyMethod.setAccessible(true);
        return (DateFormatter)applyMethod.invoke(null);
      } else {
        Class<?> clazz = loader.loadClass(DateFormatter.class.getName());
        Method applyMethod = clazz.getDeclaredMethod("apply", ZoneId.class);
        applyMethod.setAccessible(true);
        return (DateFormatter)applyMethod.invoke(null, zoneId);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error in apply DateFormatter", e);
    }
  }
}
