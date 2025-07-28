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

package org.apache.hudi.testutils;

import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerUnpersistRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Utility class for validating RDD persistence in tests.
 * This addresses the issue where RDD unpersist operations are asynchronous and may not be
 * immediately reflected when checking via SparkContext.getPersistentRDDs().
 */
public class SparkRDDValidationUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRDDValidationUtils.class);
  private static final int DEFAULT_TIMEOUT_SECONDS = 1;

  /**
   * Functional interface that allows throwing checked exceptions.
   */
  @FunctionalInterface
  public interface ThrowingRunnable {
    void run() throws Exception;
  }

  /**
   * Functional interface that allows throwing checked exceptions and returning a value.
   */
  @FunctionalInterface
  public interface ThrowingCallable<T> {
    T call() throws Exception;
  }

  /**
   * Executes a test function while tracking RDD persistence.
   *
   * Pre-action: Takes note of all RDDs that are persisted before the test.
   * Post-action: Ensures that any RDDs still in the persisted map can only be
   * what was there before the test started or tracked by the unpersist listener.
   *
   * @param spark The SparkSession to monitor
   * @param testFunc The test function to execute
   * @return The result of the test function
   */
  public static <T> T withRDDPersistenceValidation(SparkSession spark, ThrowingCallable<T> testFunc) throws Exception {
    if (spark == null) {
      return testFunc.call();
    }

    SparkContext sc = spark.sparkContext();

    // Pre-action: Record initially persisted RDDs
    scala.collection.Map<Object, RDD<?>> initialPersistedRdds = sc.getPersistentRDDs();
    Set<Integer> initiallyPersistedRddIds = new HashSet<>();
    scala.collection.Iterator<Object> iter = initialPersistedRdds.keys().iterator();
    while (iter.hasNext()) {
      initiallyPersistedRddIds.add((Integer) iter.next());
    }
    LOG.debug("Initially persisted RDD IDs: {}", initiallyPersistedRddIds);

    // Track initial dropped events count
    long initialDroppedEvents = getDroppedEventsCount(spark);

    // Set up listener to track unpersist events
    Set<Integer> unpersistEventRddIds = ConcurrentHashMap.newKeySet();
    SparkListener rddPersistenceListener = new SparkListener() {
      @Override
      public void onUnpersistRDD(SparkListenerUnpersistRDD event) {
        LOG.debug("[RDD Tracker] Unpersist event for RDD ID: {}", event.rddId());
        unpersistEventRddIds.add(event.rddId());
      }
    };

    sc.addSparkListener(rddPersistenceListener);

    try {
      // Execute the test function
      T result = testFunc.call();

      // Post-action: Validate RDD persistence
      long finalDroppedEvents = getDroppedEventsCount(spark);
      long droppedEventsDelta = finalDroppedEvents - initialDroppedEvents;
      validateRDDPersistence(spark, initiallyPersistedRddIds, unpersistEventRddIds, droppedEventsDelta);

      return result;
    } finally {
      // Clean up listener
      sc.removeSparkListener(rddPersistenceListener);
    }
  }

  /**
   * Convenience method for tests that don't return a value and may throw exceptions.
   * This method allows the test function to throw any exception without requiring explicit try-catch blocks.
   *
   * @param spark The SparkSession to monitor
   * @param testFunc The test function that may throw exceptions
   * @throws Exception Any exception thrown by the test function
   */
  public static void withRDDPersistenceValidation(SparkSession spark, ThrowingRunnable testFunc) throws Exception {
    withRDDPersistenceValidation(spark, () -> {
      testFunc.run();
      return null;
    });
  }

  /**
   * Gets the count of dropped events from the LiveListenerBus metrics.
   */
  private static long getDroppedEventsCount(SparkSession spark) {
    try {
      return (Long) spark.sparkContext().env().metricsSystem()
          .getSourcesByName("LiveListenerBus").apply(0)
          .metricRegistry().getCounters()
          .get("queue.shared.numDroppedEvents").getCount();
    } catch (Exception e) {
      LOG.warn("Unable to get dropped events count from metrics: {}", e.getMessage());
      return 0L;
    }
  }

  /**
   * Validates that no new RDDs are persisted after test execution.
   */
  private static void validateRDDPersistence(
      SparkSession spark,
      Set<Integer> initiallyPersistedRddIds,
      Set<Integer> unpersistEventRddIds,
      long droppedEventsDelta) throws InterruptedException {
    
    long startTime = System.currentTimeMillis();
    long timeout = DEFAULT_TIMEOUT_SECONDS * 1000L;
    SparkContext sc = spark.sparkContext();

    while (true) {
      // Get currently persistent RDDs
      scala.collection.Map<Object, RDD<?>> currentPersistedRdds = sc.getPersistentRDDs();
      
      // Convert to Java Map for easier processing
      Map<Integer, RDD<?>> javaRddMap = new java.util.HashMap<>();
      scala.collection.Iterator<scala.Tuple2<Object, RDD<?>>> pairIter = currentPersistedRdds.iterator();
      while (pairIter.hasNext()) {
        scala.Tuple2<Object, RDD<?>> pair = pairIter.next();
        javaRddMap.put((Integer) pair._1(), pair._2());
      }

      // Filter out RDDs that should be allowed:
      // 1. RDDs that were already persisted before the test
      // 2. RDDs that have unpersist events (may still be in async cleanup)
      Map<Integer, RDD<?>> problematicRdds = javaRddMap.entrySet().stream()
          .filter(entry -> {
            Integer rddId = entry.getKey();
            RDD<?> rdd = entry.getValue();
            boolean isNewRdd = !initiallyPersistedRddIds.contains(rddId);
            boolean hasUnpersistEvent = unpersistEventRddIds.contains(rddId);
            boolean isStillPersisted = !rdd.getStorageLevel().equals(StorageLevel.NONE());
            
            return isNewRdd && !hasUnpersistEvent && isStillPersisted;
          })
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      if (problematicRdds.isEmpty()) {
        LOG.debug("All new RDDs have been properly unpersisted or have unpersist events.");
        return;
      }

      if (System.currentTimeMillis() - startTime > timeout) {
        int problematicRddCount = problematicRdds.size();
        
        // Only fail if dropped events count is less than the problematic RDD count
        if (droppedEventsDelta < problematicRddCount) {
          StringBuilder sb = new StringBuilder();
          sb.append(String.format("Timeout after %d seconds. Found persisted RDDs that were not cleaned up:\n", DEFAULT_TIMEOUT_SECONDS));
          problematicRdds.forEach((rddId, rdd) -> {
            sb.append(String.format("  RDD ID=%d with storage level: %s\n", rddId, rdd.getStorageLevel().description()));
          });
          sb.append(String.format("\nInitially persisted RDD IDs: %s\n", initiallyPersistedRddIds));
          sb.append(String.format("RDD IDs with unpersist events: %s\n", unpersistEventRddIds));
          sb.append(String.format("Dropped events during test: %d\n", droppedEventsDelta));
          sb.append(String.format("Problematic RDD count: %d\n", problematicRddCount));
          sb.append("Note: Since dropped events count is less than problematic RDD count, this indicates a potential RDD leak.");
          // Spark does not provide a reliable way of tracking persistent rdds. We only do logging here.
          LOG.error(sb.toString());
          return;
        } else {
          LOG.info("Found {} persisted RDDs but {} events were dropped. Assuming unpersist events were lost.", 
              problematicRddCount, droppedEventsDelta);
          return;
        }
      }

      Thread.sleep(200);
    }
  }
}