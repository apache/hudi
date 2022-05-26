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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class creates heartbeat for hudi reader.
 *
 * <p>This heartbeat is used to ascertain whether an instant is currently consumed by reader.
 * An instant can be seen as a consuming offset, the instant data files should not be cleaned if a reader
 * is consuming from the instant.
 */
public class ReaderHeartbeat extends BaseHoodieHeartbeat {
  private static final Logger LOG = LoggerFactory.getLogger(ReaderHeartbeat.class);

  private static final String OFFSET_EARLIEST = "earliest";

  private ReaderHeartbeat(FileSystem fs, String basePath,
                         Long heartbeatIntervalInMs, Integer numTolerableHeartbeatMisses) {
    super(fs, basePath, heartbeatIntervalInMs, numTolerableHeartbeatMisses);
  }

  public static ReaderHeartbeat create(FileSystem fs, HoodieWriteConfig writeConfig) {
    return new ReaderHeartbeat(fs, writeConfig.getBasePath(), writeConfig.getHoodieReaderHeartbeatIntervalInMs(),
        writeConfig.getHoodieReaderHeartbeatTolerableMisses());
  }

  // visible for testing
  public static ReaderHeartbeat create(FileSystem fs, String basePath,
                                       Long heartbeatIntervalInMs, Integer numTolerableHeartbeatMisses) {
    return new ReaderHeartbeat(fs, basePath, heartbeatIntervalInMs, numTolerableHeartbeatMisses);
  }

  @Override
  protected String getHeartbeatFolderPath(String basePath) {
    return HoodieTableMetaClient.getReaderHeartbeatFolderPath(basePath);
  }

  @Override
  protected void deleteHeartbeatFile(FileSystem fs, String basePath, String instantTime) {
    HeartbeatUtils.deleteReaderHeartbeatFile(fs, basePath, instantTime);
  }

  @Override
  public void stop(String instantTime) throws HoodieException {
    super.stop(instantTime);
    this.instantToHeartbeatMap.remove(instantTime);
  }

  public static Boolean heartbeatExists(FileSystem fs, String basePath, String instantTime) throws IOException {
    Path heartbeatFilePath = new Path(HoodieTableMetaClient.getReaderHeartbeatFolderPath(basePath) + Path.SEPARATOR + instantTime);
    return fs.exists(heartbeatFilePath);
  }

  public Heartbeats getValidReaderHeartbeats() {
    List<String> allHeartbeatInstants = Collections.emptyList();
    try {
      allHeartbeatInstants = getAllExistingHeartbeatInstants();
    } catch (IOException e) {
      LOG.warn("Exception while fetching the existing heartbeat instants, ignore the heartbeats");
    }
    List<String> validHeartbeatInstants = allHeartbeatInstants.stream().filter(instant -> {
      try {
        return !isHeartbeatExpired(instant);
      } catch (IOException e) {
        e.printStackTrace();
        return false;
      }
    }).collect(Collectors.toList());
    return Heartbeats.create(validHeartbeatInstants);
  }

  // -------------------------------------------------------------------------
  //  Inner Classes
  // -------------------------------------------------------------------------
  public static class Heartbeats {
    private final List<String> instants;
    private final boolean consumingFromEarliest;

    private Heartbeats(List<String> instants) {
      this.instants = new ArrayList<>(instants);
      this.consumingFromEarliest = this.instants.contains(OFFSET_EARLIEST);
      if (consumingFromEarliest) {
        this.instants.remove(OFFSET_EARLIEST);
      }
      this.instants.sort(Comparator.naturalOrder());
    }

    public static Heartbeats create(List<String> instants) {
      return new Heartbeats(instants);
    }

    public boolean isConsumingFromEarliest() {
      return consumingFromEarliest;
    }

    public Option<String> getEarliestConsumingInstant() {
      return instants.isEmpty() ? Option.empty() : Option.of(instants.get(0));
    }
  }
}
