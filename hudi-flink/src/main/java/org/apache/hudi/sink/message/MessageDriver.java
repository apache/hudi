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

package org.apache.hudi.sink.message;

import org.apache.hudi.exception.HoodieException;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * A driver that produces messages to the {@link MessageBus}.
 */
public class MessageDriver extends MessageBus {
  private final TreeMap<Long, Boolean> ckpIdCache; // checkpoint id -> isCommitted mapping

  public MessageDriver(FileSystem fs, String basePath) throws IOException {
    super(fs, basePath);
    this.ckpIdCache = new TreeMap<>();
    initialize();
  }

  /**
   * Returns the message bus instance.
   *
   * <p>This expects to be called by the driver.
   *
   * @param fs       The filesystem
   * @param basePath The table base path
   * @return The instance of message bus
   */
  public static MessageDriver getInstance(FileSystem fs, String basePath) {
    try {
      return new MessageDriver(fs, basePath);
    } catch (IOException e) {
      throw new HoodieException("Initialize checkpoint message bus error", e);
    }
  }

  /**
   * Initialize the message bus, would clean all the messages.
   *
   * <p>This expects to be called by the driver.
   */
  private void initialize() throws IOException {
    Path path = new Path(messageBusPath(basePath));
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
    fs.mkdirs(path);
  }

  /**
   * Add a checkpoint commit message.
   *
   * @param checkpointId    The checkpoint id
   * @param commitInstant   The committed instant
   * @param inflightInstant The new inflight instant
   */
  public void commitCkp(long checkpointId, String commitInstant, String inflightInstant) {
    Path path = fullFilePath(getCommitFileName(checkpointId));

    try (FSDataOutputStream outputStream = fs.create(path, true)) {
      byte[] bytes = CkpMessage.toBytes(commitInstant, inflightInstant);
      outputStream.write(bytes);
      outputStream.close();
      this.ckpIdCache.put(checkpointId, true);
      clean();
    } catch (Throwable e) {
      throw new HoodieException("Adding committed message error for checkpoint: " + checkpointId, e);
    }
  }

  /**
   * Add an aborted checkpoint message.
   *
   * @param checkpointId    The checkpoint id
   */
  public void abortCkp(long checkpointId) {
    Path path = fullFilePath(getAbortedFileName(checkpointId));
    try {
      fs.createNewFile(path);
      this.ckpIdCache.put(checkpointId, false);
      clean();
    } catch (Throwable e) {
      throw new HoodieException("Adding aborted message error for checkpoint: " + checkpointId, e);
    }
  }

  private void clean() throws IOException {
    int numToClean = this.ckpIdCache.size() - MESSAGE_QUEUE_LENGTH;
    if (numToClean >= 10) {
      for (int i = 0; i < numToClean; i++) {
        Map.Entry<Long, Boolean> entry = this.ckpIdCache.pollFirstEntry();
        final String fileName = entry.getValue() ? getCommitFileName(entry.getKey()) : getAbortedFileName(entry.getKey());
        final Path filePath = fullFilePath(fileName);
        fs.delete(filePath, false);
      }
    }
  }

  @VisibleForTesting
  public TreeMap<Long, Boolean> getCkpIdCache() {
    return ckpIdCache;
  }

  @Override
  public void close() throws Exception {
    this.ckpIdCache.clear();
  }
}
