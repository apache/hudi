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

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieHeartbeatException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;

/**
 * <p>Base class for hoodie heartbeats.
 *
 * NOTE: Due to CPU contention on the driver/client node, the heartbeats could be delayed, hence it's important to set
 *       the value high enough to avoid that possibility.
 */
@NotThreadSafe
public abstract class BaseHoodieHeartbeat implements AutoCloseable, Serializable {

  private static final Logger LOG = LogManager.getLogger(BaseHoodieHeartbeat.class);

  private final transient FileSystem fs;
  protected final String basePath;
  // path to the heartbeat folder where all writers are updating their heartbeats
  private final String heartbeatFolderPath;
  // heartbeat interval in millis
  private final Long heartbeatIntervalInMs;
  private final Long maxAllowableHeartbeatIntervalInMs;
  protected final Map<String, Heartbeat> instantToHeartbeatMap;

  public BaseHoodieHeartbeat(FileSystem fs, String basePath, Long heartbeatIntervalInMs, Integer numTolerableHeartbeatMisses) {
    ValidationUtils.checkArgument(heartbeatIntervalInMs >= 1000, "Cannot set heartbeat lower than 1 second");
    this.fs = fs;
    this.basePath = basePath;
    this.heartbeatFolderPath = getHeartbeatFolderPath(basePath);
    this.heartbeatIntervalInMs = heartbeatIntervalInMs;
    this.maxAllowableHeartbeatIntervalInMs = this.heartbeatIntervalInMs * numTolerableHeartbeatMisses;
    this.instantToHeartbeatMap = new HashMap<>();
  }

  /**
   * Returns the heartbeat folder path.
   */
  protected abstract String getHeartbeatFolderPath(String basePath);

  /**
   * Deletes the heartbeat file.
   */
  protected abstract void deleteHeartbeatFile(FileSystem fs, String basePath, String instantTime);

  class Heartbeat {

    private String instantTime;
    private Boolean isHeartbeatStarted = false;
    private Boolean isHeartbeatStopped = false;
    private Long lastHeartbeatTime;
    private Integer numHeartbeats = 0;
    private Timer timer = new Timer();

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
   * @param instantTime
   */
  public void start(String instantTime) {
    LOG.info("Received request to start heartbeat for instant time " + instantTime);
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
   * Stops the heartbeat for the specified instant.
   * @param instantTime
   * @throws HoodieException
   */
  public void stop(String instantTime) throws HoodieException {
    Heartbeat heartbeat = instantToHeartbeatMap.get(instantTime);
    if (heartbeat != null && heartbeat.isHeartbeatStarted() && !heartbeat.isHeartbeatStopped()) {
      LOG.info("Stopping heartbeat for instant " + instantTime);
      heartbeat.getTimer().cancel();
      heartbeat.setHeartbeatStopped(true);
      LOG.info("Stopped heartbeat for instant " + instantTime);
      deleteHeartbeatFile(fs, basePath, instantTime);
      LOG.info("Deleted heartbeat file for instant " + instantTime);
    }
  }

  /**
   * Stops all heartbeats started via this instance of the client.
   * @throws HoodieException
   */
  public void stop() throws HoodieException {
    instantToHeartbeatMap.values().forEach(heartbeat -> stop(heartbeat.getInstantTime()));
  }

  public Long getLastHeartbeatTime(FileSystem fs, String instantTime) throws IOException {
    Path heartbeatFilePath = new Path(getHeartbeatFolderPath(basePath) + Path.SEPARATOR + instantTime);
    if (fs.exists(heartbeatFilePath)) {
      return fs.getFileStatus(heartbeatFilePath).getModificationTime();
    } else {
      // NOTE : This can happen when a writer is upgraded to use lazy cleaning and the last write had failed
      return 0L;
    }
  }

  public boolean isHeartbeatExpired(String instantTime) throws IOException {
    Long currentTime = System.currentTimeMillis();
    Heartbeat lastHeartbeat = instantToHeartbeatMap.get(instantTime);
    if (lastHeartbeat == null) {
      LOG.info("Heartbeat not found in internal map, falling back to reading from DFS");
      long lastHeartbeatTime = getLastHeartbeatTime(this.fs, instantTime);
      lastHeartbeat = new Heartbeat();
      lastHeartbeat.setLastHeartbeatTime(lastHeartbeatTime);
      lastHeartbeat.setInstantTime(instantTime);
    }
    if (currentTime - lastHeartbeat.getLastHeartbeatTime() > this.maxAllowableHeartbeatIntervalInMs) {
      LOG.warn("Heartbeat expired, currentTime = " + currentTime + ", last heartbeat = " + lastHeartbeat
          + ", heartbeat interval = " + this.heartbeatIntervalInMs);
      return true;
    }
    return false;
  }

  public List<String> getAllExistingHeartbeatInstants() throws IOException {
    Path heartbeatFolder = new Path(heartbeatFolderPath);
    if (this.fs.exists(heartbeatFolder)) {
      FileStatus[] fileStatus = this.fs.listStatus(new Path(heartbeatFolderPath));
      return Arrays.stream(fileStatus).map(fs -> fs.getPath().getName()).collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  private void updateHeartbeat(String instantTime) throws HoodieHeartbeatException {
    try {
      Long newHeartbeatTime = System.currentTimeMillis();
      OutputStream outputStream =
          this.fs.create(new Path(heartbeatFolderPath + Path.SEPARATOR + instantTime), true);
      outputStream.close();
      Heartbeat heartbeat = instantToHeartbeatMap.get(instantTime);
      if (heartbeat.getLastHeartbeatTime() != null && isHeartbeatExpired(instantTime)) {
        LOG.error("Aborting, missed generating heartbeat within allowable interval " + this.maxAllowableHeartbeatIntervalInMs);
        // Since TimerTask allows only java.lang.Runnable, cannot throw an exception and bubble to the caller thread, hence
        // explicitly interrupting the timer thread.
        Thread.currentThread().interrupt();
      }
      heartbeat.setInstantTime(instantTime);
      heartbeat.setLastHeartbeatTime(newHeartbeatTime);
      heartbeat.setNumHeartbeats(heartbeat.getNumHeartbeats() + 1);
    } catch (IOException io) {
      throw new HoodieHeartbeatException("Unable to generate heartbeat ", io);
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
    this.stop();
    this.instantToHeartbeatMap.clear();
  }
}
