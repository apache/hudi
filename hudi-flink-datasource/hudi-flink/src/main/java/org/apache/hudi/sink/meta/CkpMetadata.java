/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.meta;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The checkpoint metadata for bookkeeping the checkpoint messages.
 *
 * <p>Each time the driver starts a new instant, it writes a commit message into the metadata, the write tasks
 * then consume the message and unblock the data flushing.
 *
 * <p>Why we use the DFS based message queue instead of sending
 * the {@link org.apache.flink.runtime.operators.coordination.OperatorEvent} ?
 * The write task handles the operator event using the main mailbox executor which has the lowest priority for mails,
 * it is also used to process the inputs. When the write task blocks and waits for the operator event to ack the valid instant to write,
 * it actually blocks all the subsequent events in the mailbox, the operator event would never be consumed then it causes deadlock.
 *
 * <p>The checkpoint metadata is also more lightweight than the active timeline.
 *
 * <p>NOTE: should be removed in the future if we have good manner to handle the async notifications from driver.
 */
public class CkpMetadata implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(CkpMetadata.class);

  protected static final int MAX_RETAIN_CKP_NUM = 3;

  // the ckp metadata directory
  private static final String CKP_META = "ckp_meta";

  private final FileSystem fs;
  protected final Path path;

  private List<CkpMessage> messages;
  private List<String> instantCache;

  private CkpMetadata(Configuration config) {
    this(FSUtils.getFs(config.getString(FlinkOptions.PATH), HadoopConfigurations.getHadoopConf(config)), config.getString(FlinkOptions.PATH));
  }

  private CkpMetadata(FileSystem fs, String basePath) {
    this.fs = fs;
    this.path = new Path(ckpMetaPath(basePath));
  }

  public void close() {
    this.instantCache = null;
  }

  // -------------------------------------------------------------------------
  //  WRITE METHODS
  // -------------------------------------------------------------------------

  /**
   * Initialize the message bus, would clean all the messages and publish the last pending instant.
   *
   * <p>This expects to be called by the driver.
   */
  public void bootstrap(HoodieTableMetaClient metaClient) throws IOException {
    fs.delete(path, true);
    fs.mkdirs(path);
  }

  public void startInstant(String instant) {
    Path path = fullPath(CkpMessage.getFileName(instant, CkpMessage.State.INFLIGHT));
    try {
      fs.createNewFile(path);
    } catch (IOException e) {
      throw new HoodieException("Exception while adding checkpoint start metadata for instant: " + instant);
    }
    // cleaning
    clean(instant);
  }

  private void clean(String newInstant) {
    if (this.instantCache == null) {
      this.instantCache = new ArrayList<>();
    }
    this.instantCache.add(newInstant);
    if (instantCache.size() > MAX_RETAIN_CKP_NUM) {
      final String instant = instantCache.get(0);
      boolean[] error = new boolean[1];
      CkpMessage.getAllFileNames(instant).stream().map(this::fullPath).forEach(path -> {
        try {
          fs.delete(path, false);
        } catch (IOException e) {
          error[0] = true;
          LOG.warn("Exception while cleaning the checkpoint meta file: " + path);
        }
      });
      if (!error[0]) {
        instantCache.remove(0);
      }
    }
  }

  /**
   * Add a checkpoint commit message.
   *
   * @param instant The committed instant
   */
  public void commitInstant(String instant) {
    Path path = fullPath(CkpMessage.getFileName(instant, CkpMessage.State.COMPLETED));
    try {
      fs.createNewFile(path);
    } catch (IOException e) {
      throw new HoodieException("Exception while adding checkpoint commit metadata for instant: " + instant);
    }
  }

  /**
   * Add an aborted checkpoint message.
   */
  public void abortInstant(String instant) {
    Path path = fullPath(CkpMessage.getFileName(instant, CkpMessage.State.ABORTED));
    try {
      if (fs.exists(fullPath(CkpMessage.getFileName(instant, CkpMessage.State.INFLIGHT)))) {
        fs.createNewFile(path);
      }
    } catch (IOException e) {
      throw new HoodieException("Exception while adding checkpoint abort metadata for instant: " + instant);
    }
  }

  // -------------------------------------------------------------------------
  //  READ METHODS
  // -------------------------------------------------------------------------

  private void load() {
    try {
      this.messages = scanCkpMetadata(this.path);
    } catch (IOException e) {
      throw new HoodieException("Exception while scanning the checkpoint meta files under path: " + this.path);
    }
  }

  @Nullable
  public String lastPendingInstant() {
    load();
    for (int i = this.messages.size() - 1; i >= 0; i--) {
      CkpMessage ckpMsg = this.messages.get(i);
      // consider 'aborted' as pending too to reuse the instant
      if (!ckpMsg.isComplete()) {
        return ckpMsg.getInstant();
      }
    }
    return null;
  }

  public List<CkpMessage> getMessages() {
    load();
    return messages;
  }

  public boolean isAborted(String instant) {
    ValidationUtils.checkState(this.messages != null, "The checkpoint metadata should #load first");
    return this.messages.stream().anyMatch(ckpMsg -> instant.equals(ckpMsg.getInstant()) && ckpMsg.isAborted());
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  public static CkpMetadata getInstance(Configuration config) {
    return new CkpMetadata(config);
  }

  public static CkpMetadata getInstance(FileSystem fs, String basePath) {
    return new CkpMetadata(fs, basePath);
  }

  protected static String ckpMetaPath(String basePath) {
    return basePath + Path.SEPARATOR + HoodieTableMetaClient.AUXILIARYFOLDER_NAME + Path.SEPARATOR + CKP_META;
  }

  private Path fullPath(String fileName) {
    return new Path(path, fileName);
  }

  private List<CkpMessage> scanCkpMetadata(Path ckpMetaPath) throws IOException {
    return Arrays.stream(this.fs.listStatus(ckpMetaPath)).map(CkpMessage::new)
        .collect(Collectors.groupingBy(CkpMessage::getInstant)).values().stream()
        .map(messages -> messages.stream().reduce((x, y) -> {
          // Pick the one with the highest state
          if (x.getState().compareTo(y.getState()) >= 0) {
            return x;
          }
          return y;
        }).get())
        .sorted().collect(Collectors.toList());
  }
}
