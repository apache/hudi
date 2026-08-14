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

package org.apache.hudi.client.heartbeat;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieHeartbeatException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hudi.common.heartbeat.HoodieHeartbeatUtils.getLastHeartbeatTime;

/**
 * This class creates heartbeat for hudi client. This heartbeat is used to ascertain whether the running job is or not.
 * NOTE: Due to CPU contention on the driver/client node, the heartbeats could be delayed, hence it's important to set
 *       the value high enough to avoid that possibility.
 */
@NotThreadSafe
@Slf4j
public class HoodieHeartbeatClient implements AutoCloseable, Serializable {

  private final transient HoodieStorage storage;
  private final String basePath;
  // path to the heartbeat folder where all writers are updating their heartbeats
  @Getter
  private final String heartbeatFolderPath;
  // heartbeat interval in millis
  private final Long heartbeatIntervalInMs;
  private final Long maxAllowableHeartbeatIntervalInMs;
  // Maximum time the scheduler thread will wait for a single heartbeat file write to complete before
  // abandoning it and letting the next tick retry. Bounded to one interval so that a slow/hung
  // storage write cannot block the scheduler thread (and thus freeze all subsequent heartbeats).
  private final Long heartbeatWriteTimeoutMs;
  private final Map<String, Heartbeat> instantToHeartbeatMap;
  // Daemon executor used to perform the (potentially slow) storage write off the scheduler thread so the
  // write can be time-bounded. A cached pool is intentional: if one write hangs, that thread is left
  // parked while the next tick proceeds on a fresh thread. Lazily created and marked transient since
  // this client is Serializable with a transient storage handle.
  private transient ExecutorService heartbeatWriteExecutor;

  public HoodieHeartbeatClient(HoodieStorage storage, String basePath, Long heartbeatIntervalInMs,
                               Integer numTolerableHeartbeatMisses) {
    ValidationUtils.checkArgument(heartbeatIntervalInMs >= 1000, "Cannot set heartbeat lower than 1 second");
    this.storage = storage;
    this.basePath = basePath;
    this.heartbeatFolderPath = HoodieTableMetaClient.getHeartbeatFolderPath(basePath);
    this.heartbeatIntervalInMs = heartbeatIntervalInMs;
    this.maxAllowableHeartbeatIntervalInMs = this.heartbeatIntervalInMs * numTolerableHeartbeatMisses;
    this.heartbeatWriteTimeoutMs = this.heartbeatIntervalInMs;
    this.instantToHeartbeatMap = new ConcurrentHashMap<>();
  }

  private synchronized ExecutorService getHeartbeatWriteExecutor() {
    if (heartbeatWriteExecutor == null) {
      heartbeatWriteExecutor =
          Executors.newCachedThreadPool(new CustomizedThreadFactory("heartbeat_write", true));
    }
    return heartbeatWriteExecutor;
  }

  @Data
  static class Heartbeat {

    private String instantTime;
    private boolean isHeartbeatStarted = false;
    private boolean isHeartbeatStopped = false;
    private Long lastHeartbeatTime;
    private Integer numHeartbeats = 0;
    private ScheduledExecutorService heartbeatScheduler =
        Executors.newSingleThreadScheduledExecutor(new CustomizedThreadFactory("heartbeat_scheduler", true));
    private ScheduledFuture<?> scheduledFuture;
  }

  class HeartbeatTask implements Runnable {

    private final String instantTime;

    HeartbeatTask(String instantTime) {
      this.instantTime = instantTime;
    }

    @Override
    public void run() {
      try {
        updateHeartbeat(instantTime);
      } catch (Exception e) {
        log.error("Failed to update heartbeat for instant {}; will retry on next tick", instantTime, e);
      }
    }
  }

  /**
   * Start a new heartbeat for the specified instant. If there is already one running, this will be a NO_OP
   *
   * @param instantTime The instant time for the heartbeat.
   */
  public void start(String instantTime) {
    log.info("Received request to start heartbeat for instant time {}", instantTime);
    Heartbeat heartbeat = instantToHeartbeatMap.get(instantTime);
    ValidationUtils.checkArgument(heartbeat == null || !heartbeat.isHeartbeatStopped(), "Cannot restart a stopped heartbeat for " + instantTime);
    if (heartbeat != null && heartbeat.isHeartbeatStarted()) {
      // heartbeat already started, NO_OP
      return;
    }

    Heartbeat newHeartbeat = new Heartbeat();
    newHeartbeat.setHeartbeatStarted(true);
    instantToHeartbeatMap.put(instantTime, newHeartbeat);
    // Ensure heartbeat is generated for the first time with this blocking call.
    // Since scheduler submits the task to a thread, no guarantee when that thread will get CPU
    // cycles to generate the first heartbeat.
    updateHeartbeat(instantTime);
    newHeartbeat.setScheduledFuture(newHeartbeat.getHeartbeatScheduler().scheduleAtFixedRate(
        new HeartbeatTask(instantTime), this.heartbeatIntervalInMs, this.heartbeatIntervalInMs, TimeUnit.MILLISECONDS));
  }

  /**
   * Stops the heartbeat and deletes the heartbeat file for the specified instant.
   *
   * @param instantTime The instant time for the heartbeat.
   * @throws HoodieException
   */
  public Heartbeat stop(String instantTime) throws HoodieException {
    Heartbeat heartbeat = instantToHeartbeatMap.remove(instantTime);
    if (isHeartbeatStarted(heartbeat)) {
      stopHeartbeatScheduler(heartbeat);
      WriterHeartbeatUtils.deleteHeartbeatFile(storage, basePath, instantTime);
      log.info("Deleted heartbeat file for instant {}", instantTime);
    }
    return heartbeat;
  }

  /**
   * Stops all heartbeat schedulers started via this instance of the client.
   *
   * @throws HoodieException
   */
  public void stopHeartbeatTimers() throws HoodieException {
    instantToHeartbeatMap.values().stream().filter(this::isHeartbeatStarted).forEach(this::stopHeartbeatScheduler);
  }

  /**
   * Whether the given heartbeat is started.
   *
   * @param heartbeat The heartbeat to check whether is started.
   * @return Whether the heartbeat is started.
   * @throws IOException
   */
  private boolean isHeartbeatStarted(Heartbeat heartbeat) {
    return heartbeat != null && heartbeat.isHeartbeatStarted() && !heartbeat.isHeartbeatStopped();
  }

  /**
   * Stops the scheduler of the given heartbeat.
   *
   * @param heartbeat The heartbeat to stop.
   */
  private void stopHeartbeatScheduler(Heartbeat heartbeat) {
    log.info("Stopping heartbeat for instant {}", heartbeat.getInstantTime());
    shutdownHeartbeatScheduler(heartbeat);
    heartbeat.setHeartbeatStopped(true);
    log.info("Stopped heartbeat for instant {}", heartbeat.getInstantTime());
  }

  private void shutdownHeartbeatScheduler(Heartbeat heartbeat) {
    if (heartbeat.getScheduledFuture() != null) {
      heartbeat.getScheduledFuture().cancel(false);
    }
    heartbeat.getHeartbeatScheduler().shutdownNow();
  }

  public static Boolean heartbeatExists(HoodieStorage storage, String basePath, String instantTime) throws IOException {
    StoragePath heartbeatFilePath = new StoragePath(
        HoodieTableMetaClient.getHeartbeatFolderPath(basePath), instantTime);
    return storage.exists(heartbeatFilePath);
  }

  public boolean isHeartbeatExpired(String instantTime) throws IOException {
    Long currentTime = System.currentTimeMillis();
    Heartbeat lastHeartbeatForWriter = instantToHeartbeatMap.get(instantTime);
    Long lastHeartbeatTime = lastHeartbeatForWriter == null ? null : lastHeartbeatForWriter.getLastHeartbeatTime();
    // lastHeartbeatTime can be null when the heartbeat is not in the internal map, or when it is in the
    // map but no heartbeat has been generated yet (e.g. the first write timed out). In both cases fall
    // back to reading the last heartbeat time from DFS (returns 0 if no heartbeat file exists, which is
    // correctly treated as expired).
    if (lastHeartbeatTime == null) {
      log.info("Heartbeat time not available in internal map, falling back to reading from DFS");
      lastHeartbeatTime = getLastHeartbeatTime(this.storage, basePath, instantTime);
    }
    if (currentTime - lastHeartbeatTime > this.maxAllowableHeartbeatIntervalInMs) {
      log.warn("Heartbeat expired, currentTime = {}, last heartbeat = {}, heartbeat interval = {}", currentTime,
          lastHeartbeatTime, this.heartbeatIntervalInMs);
      return true;
    }
    return false;
  }

  private void updateHeartbeat(String instantTime) throws HoodieHeartbeatException {
    try {
      Long newHeartbeatTime = System.currentTimeMillis();
      writeHeartbeatFile(instantTime);
      Heartbeat heartbeat = instantToHeartbeatMap.get(instantTime);
      if (heartbeat.getLastHeartbeatTime() != null && isHeartbeatExpired(instantTime)) {
        // A previous refresh was delayed past the tolerable interval. Stop refreshing this heartbeat
        // (cancel the scheduler) and do NOT advance the last heartbeat time, so the heartbeat stays expired
        // and the writer aborts at commit time via WriterHeartbeatUtils.abortIfHeartbeatExpired(). We must not
        // keep refreshing here: a concurrent process (e.g. an async cleaner under LAZY failed-writes
        // policy) may already have started rolling back this instant once it observed the expiry, and
        // resurrecting the heartbeat could let this writer commit on top of rolled-back files.
        // The scheduler is cancelled cleanly rather than via Thread.interrupt(), which would permanently
        // kill the scheduler thread (turning a transient delay into a permanent blackout on the first miss).
        log.error("Missed generating heartbeat for instant {} within allowable interval {} ms; stopping heartbeat refresh",
            instantTime, this.maxAllowableHeartbeatIntervalInMs);
        shutdownHeartbeatScheduler(heartbeat);
        return;
      }
      heartbeat.setInstantTime(instantTime);
      heartbeat.setLastHeartbeatTime(newHeartbeatTime);
      heartbeat.setNumHeartbeats(heartbeat.getNumHeartbeats() + 1);
    } catch (TimeoutException te) {
      // The storage write did not complete within the bounded window. Do not advance the last heartbeat
      // time (the write is unconfirmed); the next scheduled tick will retry on a fresh executor thread.
      // Crucially, the scheduler thread is freed instead of being blocked by a hung storage call.
      log.warn("Heartbeat file write for instant {} did not complete within {} ms; will retry on next tick",
          instantTime, this.heartbeatWriteTimeoutMs);
    } catch (IOException io) {
      boolean isHeartbeatStopped = instantToHeartbeatMap.get(instantTime).isHeartbeatStopped();
      if (isHeartbeatStopped) {
        log.info("update heart beat failed, because the instant time {} was stopped", instantTime);
        return;
      }
      throw new HoodieHeartbeatException("Unable to generate heartbeat for instant " + instantTime, io);
    }
  }

  /**
   * Writes the heartbeat file for the given instant on a dedicated daemon executor, bounded by
   * {@link #heartbeatWriteTimeoutMs}. Performing the storage write off the scheduler thread (and with a
   * timeout) ensures that a slow or hung storage call cannot block the scheduler thread and freeze all
   * subsequent heartbeats for this instant.
   */
  private void writeHeartbeatFile(String instantTime) throws IOException, TimeoutException {
    Future<Void> future = getHeartbeatWriteExecutor().submit(() -> {
      try (OutputStream outputStream =
               this.storage.create(new StoragePath(heartbeatFolderPath, instantTime), true)) {
        // create + close confirms the heartbeat file write landed on storage.
      }
      return null;
    });
    try {
      future.get(heartbeatWriteTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      future.cancel(true);
      throw te;
    } catch (InterruptedException ie) {
      future.cancel(true);
      Thread.currentThread().interrupt();
      throw new HoodieHeartbeatException("Interrupted while writing heartbeat for instant " + instantTime, ie);
    } catch (ExecutionException ee) {
      Throwable cause = ee.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new HoodieHeartbeatException("Failed to write heartbeat for instant " + instantTime, cause);
    }
  }

  public Heartbeat getHeartbeat(String instantTime) {
    return this.instantToHeartbeatMap.get(instantTime);
  }

  @Override
  public synchronized void close() {
    this.stopHeartbeatTimers();
    this.instantToHeartbeatMap.clear();
    if (heartbeatWriteExecutor != null) {
      heartbeatWriteExecutor.shutdownNow();
      heartbeatWriteExecutor = null;
    }
  }
}
