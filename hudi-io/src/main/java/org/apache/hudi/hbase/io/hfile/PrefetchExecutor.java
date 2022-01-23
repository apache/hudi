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

package org.apache.hudi.hbase.io.hfile;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.hbase.HBaseConfiguration;
import org.apache.hudi.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class PrefetchExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(PrefetchExecutor.class);

  /** Futures for tracking block prefetch activity */
  private static final Map<Path,Future<?>> prefetchFutures = new ConcurrentSkipListMap<>();
  /** Executor pool shared among all HFiles for block prefetch */
  private static final ScheduledExecutorService prefetchExecutorPool;
  /** Delay before beginning prefetch */
  private static final int prefetchDelayMillis;
  /** Variation in prefetch delay times, to mitigate stampedes */
  private static final float prefetchDelayVariation;
  static {
    // Consider doing this on demand with a configuration passed in rather
    // than in a static initializer.
    Configuration conf = HBaseConfiguration.create();
    // 1s here for tests, consider 30s in hbase-default.xml
    // Set to 0 for no delay
    prefetchDelayMillis = conf.getInt("hbase.hfile.prefetch.delay", 1000);
    prefetchDelayVariation = conf.getFloat("hbase.hfile.prefetch.delay.variation", 0.2f);
    int prefetchThreads = conf.getInt("hbase.hfile.thread.prefetch", 4);
    prefetchExecutorPool = new ScheduledThreadPoolExecutor(prefetchThreads,
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            String name = "hfile-prefetch-" + System.currentTimeMillis();
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
          }
        });
  }

  private static final Random RNG = new Random();

  // TODO: We want HFile, which is where the blockcache lives, to handle
  // prefetching of file blocks but the Store level is where path convention
  // knowledge should be contained
  private static final Pattern prefetchPathExclude =
      Pattern.compile(
          "(" +
              Path.SEPARATOR_CHAR +
              HConstants.HBASE_TEMP_DIRECTORY.replace(".", "\\.") +
              Path.SEPARATOR_CHAR +
              ")|(" +
              Path.SEPARATOR_CHAR +
              HConstants.HREGION_COMPACTIONDIR_NAME.replace(".", "\\.") +
              Path.SEPARATOR_CHAR +
              ")");

  public static void request(Path path, Runnable runnable) {
    if (!prefetchPathExclude.matcher(path.toString()).find()) {
      long delay;
      if (prefetchDelayMillis > 0) {
        delay = (long)((prefetchDelayMillis * (1.0f - (prefetchDelayVariation/2))) +
            (prefetchDelayMillis * (prefetchDelayVariation/2) * RNG.nextFloat()));
      } else {
        delay = 0;
      }
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Prefetch requested for " + path + ", delay=" + delay + " ms");
        }
        prefetchFutures.put(path, prefetchExecutorPool.schedule(runnable, delay,
            TimeUnit.MILLISECONDS));
      } catch (RejectedExecutionException e) {
        prefetchFutures.remove(path);
        LOG.warn("Prefetch request rejected for " + path);
      }
    }
  }

  public static void complete(Path path) {
    prefetchFutures.remove(path);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Prefetch completed for " + path);
    }
  }

  public static void cancel(Path path) {
    Future<?> future = prefetchFutures.get(path);
    if (future != null) {
      // ok to race with other cancellation attempts
      future.cancel(true);
      prefetchFutures.remove(path);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Prefetch cancelled for " + path);
      }
    }
  }

  public static boolean isCompleted(Path path) {
    Future<?> future = prefetchFutures.get(path);
    if (future != null) {
      return future.isDone();
    }
    return true;
  }

  private PrefetchExecutor() {}
}
