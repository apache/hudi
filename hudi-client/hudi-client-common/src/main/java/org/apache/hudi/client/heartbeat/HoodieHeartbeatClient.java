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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieHeartbeatException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hudi.common.heartbeat.HoodieHeartbeatUtils.getLastHeartbeatTime;

/**
 * This class creates heartbeat for hudi client. This heartbeat is used to ascertain whether the running job is or not.
 * NOTE: Due to CPU contention on the driver/client node, the heartbeats could be delayed, hence it's important to set
 *       the value high enough to avoid that possibility.
 */
@NotThreadSafe
public class HoodieHeartbeatClient implements AutoCloseable, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieHeartbeatClient.class);

  private final transient HoodieStorage storage;
  private final String basePath;
  // path to the heartbeat folder where all writers are updating their heartbeats
  private final String heartbeatFolderPath;
  // heartbeat interval in millis
  private final Long heartbeatIntervalInMs;
  private final Long maxAllowableHeartbeatIntervalInMs;
  private final Map<String, Heartbeat> instantToHeartbeatMap;

  public HoodieHeartbeatClient(HoodieStorage storage, String basePath, Long heartbeatIntervalInMs,
                               Integer numTolerableHeartbeatMisses) {
    ValidationUtils.checkArgument(heartbeatIntervalInMs >= 1000, "Cannot set heartbeat lower than 1 second");
    this.storage = storage;
    this.basePath = basePath;
    this.heartbeatFolderPath = HoodieTableMetaClient.getHeartbeatFolderPath(basePath);
    this.heartbeatIntervalInMs = heartbeatIntervalInMs;
    this.maxAllowableHeartbeatIntervalInMs = this.heartbeatIntervalInMs * numTolerableHeartbeatMisses;
    this.instantToHeartbeatMap = new ConcurrentHashMap<>();
  }

  static class Heartbeat {

    private String instantTime;
    private Boolean isHeartbeatStarted = false;
    private Boolean isHeartbeatStopped = false;
    private Long lastHeartbeatTime;
    private Integer numHeartbeats = 0;
    private Timer timer = new Timer(true);

    public String getInstantTime() {
      return instantTime;
    }

    public void setInstantTime(String instantTime) {
      this.instantTime = instantTime;
    }

    public Boolean isHeartbeatStarted() {
      return isHeartbeatStarted;
    }

    public void setHeartbeatStarted(Boolean heartbeatStarted) {
      isHeartbeatStarted = heartbeatStarted;
    }

    public Boolean isHeartbeatStopped() {
      return isHeartbeatStopped;
    }

    public void setHeartbeatStopped(Boolean heartbeatStopped) {
      isHeartbeatStopped = heartbeatStopped;
    }

    public Long getLastHeartbeatTime() {
      return lastHeartbeatTime;
    }

    public void setLastHeartbeatTime(Long lastHeartbeatTime) {
      this.lastHeartbeatTime = lastHeartbeatTime;
    }

    public Integer getNumHeartbeats() {
      return numHeartbeats;
    }

    public void setNumHeartbeats(Integer numHeartbeats) {
      this.numHeartbeats = numHeartbeats;
    }

    public Timer getTimer() {
      return timer;
    }

    public void setTimer(Timer timer) {
      this.timer = timer;
    }

    @Override
    public String toString() {
      return "Heartbeat{"
              + "instantTime='" + instantTime + '\''
              + ", isHeartbeatStarted=" + isHeartbeatStarted
              + ", isHeartbeatStopped=" + isHeartbeatStopped
              + ", lastHeartbeatTime=" + lastHeartbeatTime
              + ", numHeartbeats=" + numHeartbeats
              + ", timer=" + timer
              + '}';
    }
  }

  class HeartbeatTask extends TimerTask {

    private final String instantTime;

    HeartbeatTask(String instantTime) {
      this.instantTime = instantTime;
    }

    @Override
    public void run() {
      updateHeartbeat(instantTime);
    }
  }

  /**
   * Start a new heartbeat for the specified instant. If there is already one running, this will be a NO_OP
   *
   * @param instantTime The instant time for the heartbeat.
   */
  public void start(String instantTime) {
    LOG.info("Received request to start heartbeat for instant time {}", instantTime);
    Heartbeat heartbeat = instantToHeartbeatMap.get(instantTime);
    ValidationUtils.checkArgument(heartbeat == null || !heartbeat.isHeartbeatStopped(), "Cannot restart a stopped heartbeat for " + instantTime);
    if (heartbeat != null && heartbeat.isHeartbeatStarted()) {
      // heartbeat already started, NO_OP
    } else {
      Heartbeat newHeartbeat = new Heartbeat();
      newHeartbeat.setHeartbeatStarted(true);
      instantToHeartbeatMap.put(instantTime, newHeartbeat);
      // Ensure heartbeat is generated for the first time with this blocking call.
      // Since timer submits the task to a thread, no guarantee when that thread will get CPU
      // cycles to generate the first heartbeat.
      updateHeartbeat(instantTime);
      newHeartbeat.getTimer().scheduleAtFixedRate(new HeartbeatTask(instantTime), this.heartbeatIntervalInMs,
          this.heartbeatIntervalInMs);
    }
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
      stopHeartbeatTimer(heartbeat);
      HeartbeatUtils.deleteHeartbeatFile(storage, basePath, instantTime);
      LOG.info("Deleted heartbeat file for instant {}", instantTime);
    }
    return heartbeat;
  }

  /**
   * Stops all timers of heartbeats started via this instance of the client.
   *
   * @throws HoodieException
   */
  public void stopHeartbeatTimers() throws HoodieException {
    instantToHeartbeatMap.values().stream().filter(this::isHeartbeatStarted).forEach(this::stopHeartbeatTimer);
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
   * Stops the timer of the given heartbeat.
   *
   * @param heartbeat The heartbeat to stop.
   */
  private void stopHeartbeatTimer(Heartbeat heartbeat) {
    LOG.info("Stopping heartbeat for instant {}", heartbeat.getInstantTime());
    heartbeat.getTimer().cancel();
    heartbeat.setHeartbeatStopped(true);
    LOG.info("Stopped heartbeat for instant {}", heartbeat.getInstantTime());
  }

  public static Boolean heartbeatExists(HoodieStorage storage, String basePath, String instantTime) throws IOException {
    StoragePath heartbeatFilePath = new StoragePath(
        HoodieTableMetaClient.getHeartbeatFolderPath(basePath), instantTime);
    return storage.exists(heartbeatFilePath);
  }

  public boolean isHeartbeatExpired(String instantTime) throws IOException {
    Long currentTime = System.currentTimeMillis();
    Heartbeat lastHeartbeatForWriter = instantToHeartbeatMap.get(instantTime);
    if (lastHeartbeatForWriter == null) {
      LOG.info("Heartbeat not found in internal map, falling back to reading from DFS");
      long lastHeartbeatForWriterTime = getLastHeartbeatTime(this.storage, basePath, instantTime);
      lastHeartbeatForWriter = new Heartbeat();
      lastHeartbeatForWriter.setLastHeartbeatTime(lastHeartbeatForWriterTime);
      lastHeartbeatForWriter.setInstantTime(instantTime);
      lastHeartbeatForWriter.getTimer().cancel();
    }
    if (currentTime - lastHeartbeatForWriter.getLastHeartbeatTime() > this.maxAllowableHeartbeatIntervalInMs) {
      LOG.warn("Heartbeat expired, currentTime = {}, last heartbeat = {}, heartbeat interval = {}", currentTime,
          lastHeartbeatForWriter, this.heartbeatIntervalInMs);
      return true;
    }
    return false;
  }

  private void updateHeartbeat(String instantTime) throws HoodieHeartbeatException {
    try {
      Long newHeartbeatTime = System.currentTimeMillis();
      OutputStream outputStream =
          this.storage.create(
              new StoragePath(heartbeatFolderPath, instantTime), true);
      outputStream.close();
      Heartbeat heartbeat = instantToHeartbeatMap.get(instantTime);
      if (heartbeat.getLastHeartbeatTime() != null && isHeartbeatExpired(instantTime)) {
        LOG.error("Aborting, missed generating heartbeat within allowable interval {} ms", this.maxAllowableHeartbeatIntervalInMs);
        // Since TimerTask allows only java.lang.Runnable, cannot throw an exception and bubble to the caller thread, hence
        // explicitly interrupting the timer thread.
        Thread.currentThread().interrupt();
      }
      heartbeat.setInstantTime(instantTime);
      heartbeat.setLastHeartbeatTime(newHeartbeatTime);
      heartbeat.setNumHeartbeats(heartbeat.getNumHeartbeats() + 1);
    } catch (IOException io) {
      Boolean isHeartbeatStopped = instantToHeartbeatMap.get(instantTime).isHeartbeatStopped;
      if (isHeartbeatStopped) {
        LOG.info("update heart beat failed, because the instant time {} was stopped", instantTime);
        return;
      }
      throw new HoodieHeartbeatException("Unable to generate heartbeat for instant " + instantTime, io);
    }
  }

  public String getHeartbeatFolderPath() {
    return heartbeatFolderPath;
  }

  public Heartbeat getHeartbeat(String instantTime) {
    return this.instantToHeartbeatMap.get(instantTime);
  }

  @Override
  public void close() {
    this.stopHeartbeatTimers();
    this.instantToHeartbeatMap.clear();
  }
}
