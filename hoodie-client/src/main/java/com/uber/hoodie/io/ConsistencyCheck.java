/*
 * Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io;

import com.google.common.annotations.VisibleForTesting;
import com.uber.hoodie.common.SerializableConfiguration;
import com.uber.hoodie.common.util.FSUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Checks if all the written paths have their metadata consistent on storage and thus be listable to
 * queries. This is important for cloud, stores like AWS S3 which are eventually consistent with
 * their metadata. Without such checks, we may proceed to commit the written data, without the
 * written data being made available to queries. In cases like incremental pull this can lead to
 * downstream readers failing to ever see some data.
 */
public class ConsistencyCheck implements Serializable {

  private static final transient Logger log = LogManager.getLogger(ConsistencyCheck.class);

  private String basePath;

  private List<String> relPaths;

  private transient JavaSparkContext jsc;

  private SerializableConfiguration hadoopConf;

  private int parallelism;

  public ConsistencyCheck(String basePath, List<String> relPaths, JavaSparkContext jsc,
      int parallelism) {
    this.basePath = basePath;
    this.relPaths = relPaths;
    this.jsc = jsc;
    this.hadoopConf = new SerializableConfiguration(jsc.hadoopConfiguration());
    this.parallelism = parallelism;
  }

  @VisibleForTesting
  void sleepSafe(long waitMs) {
    try {
      Thread.sleep(waitMs);
    } catch (InterruptedException e) {
      // ignore & continue next attempt
    }
  }

  /**
   * Repeatedly lists the filesystem on the paths, with exponential backoff and marks paths found as
   * passing the check.
   *
   * @return list of (relative) paths failing the check
   */
  public List<String> check(int maxAttempts, long initalDelayMs) {
    long waitMs = initalDelayMs;
    int attempt = 0;

    List<String> remainingPaths = new ArrayList<>(relPaths);
    while (attempt++ < maxAttempts) {
      remainingPaths = jsc.parallelize(remainingPaths, parallelism)
          .groupBy(p -> new Path(basePath, p).getParent()) // list by partition
          .map(pair -> {
            FileSystem fs = FSUtils.getFs(basePath, hadoopConf.get());
            // list the partition path and obtain all file paths present
            Set<String> fileNames = Arrays.stream(fs.listStatus(pair._1()))
                .map(s -> s.getPath().getName())
                .collect(Collectors.toSet());

            // only return paths that can't be found
            return StreamSupport.stream(pair._2().spliterator(), false)
                .filter(p -> !fileNames.contains(new Path(basePath, p).getName()))
                .collect(Collectors.toList());
          })
          .flatMap(itr -> itr.iterator()).collect();
      if (remainingPaths.size() == 0) {
        break; // we are done.
      }

      log.info("Consistency check, waiting for " + waitMs + " ms , after attempt :" + attempt);
      sleepSafe(waitMs);
      waitMs = waitMs * 2; // double check interval every attempt
    }

    return remainingPaths;
  }
}
