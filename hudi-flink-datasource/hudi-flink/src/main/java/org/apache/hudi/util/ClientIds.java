/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.util;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.HoodieHeartbeatException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StoragePath;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableMetaClient.AUXILIARYFOLDER_NAME;

/**
 * This class creates a client id heartbeat for a new driver.
 * The heartbeat is used to ascertain whether a driver is still alive or not.
 * Currently, it is mainly used for two cases:
 *
 * <ol>
 *   <li>Legacy service cleaning, when a new driver starts up,
 *   it also checks whether there are some zombie clients whose heartbeat file can be removed;</li>
 *   <li>Auto generates a new client id, tries to reuse the existing ids from the zombie clients first or
 *   generates an inc id based on the largest alive client id.
 *   </li>
 * </ol>
 *
 * <p>NOTE: Due to CPU contention on the driver/client node, the heartbeats could be delayed, hence it's important to set
 * the value high enough to avoid that possibility.
 */
@Slf4j
public class ClientIds implements AutoCloseable, Serializable {
  private static final long serialVersionUID = 1L;

  private static final String HEARTBEAT_FOLDER_NAME = ".ids";
  private static final String HEARTBEAT_FILE_NAME_PREFIX = "_";
  public static final String INIT_CLIENT_ID = "";
  public static final long DEFAULT_HEARTBEAT_INTERVAL_IN_MS = 60 * 1000; // default 1 minute
  public static final int DEFAULT_NUM_TOLERABLE_HEARTBEAT_MISSES = 5;    // by default decide the service is stopped if it is inactive for 5 minutes

  /**
   * The filesystem.
   */
  private final transient FileSystem fs;

  /**
   * The heartbeat file path.
   */
  private final Path heartbeatFilePath;

  /**
   * The heartbeat interval in milliseconds.
   */
  private final long heartbeatIntervalInMs;

  /**
   * The threshold beyond which we can think the service is a zombie.
   */
  private final long heartbeatTimeoutThresholdInMs;

  private ScheduledExecutorService executor;

  private boolean started;

  private ClientIds(FileSystem fs, String basePath, String uniqueId, long heartbeatIntervalInMs, int numTolerableHeartbeatMisses) {
    this.fs = fs;
    this.heartbeatFilePath = getHeartbeatFilePath(basePath, uniqueId);
    this.heartbeatIntervalInMs = heartbeatIntervalInMs;
    this.heartbeatTimeoutThresholdInMs = numTolerableHeartbeatMisses * heartbeatIntervalInMs;
  }

  public void start() {
    if (started) {
      log.info("The service heartbeat client is already started, skips the action");
    }
    updateHeartbeat();
    this.executor = Executors.newScheduledThreadPool(1);
    this.executor.scheduleAtFixedRate(this::updateHeartbeat, this.heartbeatIntervalInMs, this.heartbeatIntervalInMs, TimeUnit.MILLISECONDS);
    this.started = true;
  }

  /**
   * Returns the builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void close() {
    if (this.executor != null) {
      this.executor.shutdownNow();
      this.executor = null;
    }
    this.started = false;
  }

  public static boolean isHeartbeatExpired(FileSystem fs, Path path, long timeoutThreshold) {
    try {
      if (fs.exists(path)) {
        long modifyTime = fs.getFileStatus(path).getModificationTime();
        long currentTime = System.currentTimeMillis();
        return currentTime - modifyTime > timeoutThreshold;
      }
    } catch (IOException e) {
      // if any exception happens, just return false.
      log.error("Check heartbeat file existence error: " + path);
    }
    return false;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  private String getHeartbeatFolderPath(String basePath) {
    return basePath + StoragePath.SEPARATOR + AUXILIARYFOLDER_NAME + StoragePath.SEPARATOR + HEARTBEAT_FOLDER_NAME;
  }

  private Path getHeartbeatFilePath(String basePath, String uniqueId) {
    String heartbeatFolderPath = getHeartbeatFolderPath(basePath);
    String fileName = StringUtils.isNullOrEmpty(uniqueId) ? HEARTBEAT_FILE_NAME_PREFIX : HEARTBEAT_FILE_NAME_PREFIX + uniqueId;
    return new Path(heartbeatFolderPath, fileName);
  }

  private void updateHeartbeat() throws HoodieHeartbeatException {
    updateHeartbeat(this.heartbeatFilePath);
  }

  private void updateHeartbeat(Path heartbeatFilePath) throws HoodieHeartbeatException {
    try (OutputStream outputStream = this.fs.create(heartbeatFilePath, true)) {
      // no operation
    } catch (IOException io) {
      throw new HoodieHeartbeatException("Unable to generate heartbeat for file path " + heartbeatFilePath, io);
    }
  }

  @VisibleForTesting
  public String nextId(Configuration conf) {
    String basePath = conf.get(FlinkOptions.PATH);
    String nextId = nextId(conf, basePath);
    // update the heartbeat immediately in case there are client preemption conflict for the same id.
    updateHeartbeat(getHeartbeatFilePath(basePath, nextId));
    return nextId;
  }

  private String nextId(Configuration conf, String basePath) {
    Path heartbeatFolderPath = new Path(getHeartbeatFolderPath(basePath));
    FileSystem fs = HadoopFSUtils.getFs(heartbeatFolderPath, HadoopConfigurations.getHadoopConf(conf));
    try {
      if (!fs.exists(heartbeatFolderPath)) {
        return INIT_CLIENT_ID;
      }
      List<Path> sortedPaths = Arrays.stream(fs.listStatus(heartbeatFolderPath))
          .map(FileStatus::getPath)
          .sorted(Comparator.comparing(Path::getName))
          .collect(Collectors.toList());
      if (sortedPaths.isEmpty()) {
        return INIT_CLIENT_ID;
      }
      List<Path> zombieHeartbeatPaths = sortedPaths.stream()
          .filter(path -> ClientIds.isHeartbeatExpired(fs, path, this.heartbeatTimeoutThresholdInMs))
          .collect(Collectors.toList());
      if (!zombieHeartbeatPaths.isEmpty()) {
        // 1. If there are any zombie client ids, reuse the smallest one
        for (Path path : zombieHeartbeatPaths) {
          fs.delete(path, true);
          log.warn("Deleting inactive checkpoint metadata path: {}", path);
        }
        return getClientId(zombieHeartbeatPaths.get(0));
      }
      // 2. else returns an auto inc id
      String largestClientId = getClientId(sortedPaths.get(sortedPaths.size() - 1));
      return INIT_CLIENT_ID.equals(largestClientId) ? "1" : (Integer.parseInt(largestClientId) + 1) + "";
    } catch (IOException e) {
      throw new RuntimeException("Generate next client id error", e);
    }
  }

  /**
   * Returns the client id from the heartbeat file path, the path name follows
   * the naming convention: _, _1, _2, ... _N.
   */
  private static String getClientId(Path path) {
    String[] splits = path.getName().split(HEARTBEAT_FILE_NAME_PREFIX);
    return splits.length > 1 ? splits[1] : INIT_CLIENT_ID;
  }

  // -------------------------------------------------------------------------
  //  Inner classes
  // -------------------------------------------------------------------------

  /**
   * Builder for {@link ClientIds}.
   */
  public static class Builder {
    private FileSystem fs;
    private String basePath;
    private String clientId = INIT_CLIENT_ID;
    private long heartbeatIntervalInMs = DEFAULT_HEARTBEAT_INTERVAL_IN_MS;
    private int numTolerableHeartbeatMisses = DEFAULT_NUM_TOLERABLE_HEARTBEAT_MISSES;

    public Builder fs(FileSystem fs) {
      this.fs = fs;
      return this;
    }

    public Builder basePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public Builder clientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder conf(Configuration conf) {
      this.basePath = conf.get(FlinkOptions.PATH);
      this.fs = HadoopFSUtils.getFs(this.basePath, HadoopConfigurations.getHadoopConf(conf));
      this.clientId = conf.get(FlinkOptions.WRITE_CLIENT_ID);
      return this;
    }

    public Builder heartbeatIntervalInMs(long interval) {
      this.heartbeatIntervalInMs = interval;
      return this;
    }

    public Builder numTolerableHeartbeatMisses(int numMisses) {
      this.numTolerableHeartbeatMisses = numMisses;
      return this;
    }

    public ClientIds build() {
      return new ClientIds(Objects.requireNonNull(this.fs), Objects.requireNonNull(this.basePath),
          this.clientId, this.heartbeatIntervalInMs, this.numTolerableHeartbeatMisses);
    }
  }
}
