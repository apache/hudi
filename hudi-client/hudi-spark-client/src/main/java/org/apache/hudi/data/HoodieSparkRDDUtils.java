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

package org.apache.hudi.data;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;

import java.util.HashSet;
import java.util.Set;

/**
 * Utility class for Spark RDD operations in Hudi.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HoodieSparkRDDUtils {

  /**
   * Unpersists an RDD and all its upstream dependencies recursively.
   * This method traverses the RDD lineage graph and unpersists any cached RDDs found.
   *
   * @param rdd the RDD to unpersist along with its dependencies
   */
  public static void unpersistRDDWithDependencies(RDD<?> rdd) {
    Set<Integer> visitedRddIds = new HashSet<>();
    unpersistRDDWithDependenciesInternal(rdd, visitedRddIds);
  }

  private static void unpersistRDDWithDependenciesInternal(RDD<?> rdd, Set<Integer> visitedRddIds) {
    if (rdd == null || visitedRddIds.contains(rdd.id())) {
      return;
    }

    visitedRddIds.add(rdd.id());

    // Unpersist if cached
    if (rdd.getStorageLevel() != StorageLevel.NONE()) {
      rdd.unpersist(false);
    }

    // Recursively unpersist dependencies
    scala.collection.Iterator<org.apache.spark.Dependency<?>> iter = rdd.dependencies().iterator();
    while (iter.hasNext()) {
      org.apache.spark.Dependency<?> dep = iter.next();
      unpersistRDDWithDependenciesInternal(dep.rdd(), visitedRddIds);
    }
  }
}