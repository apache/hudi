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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * A client that consumes messages from the {@link MessageBus}.
 */
public class MessageClient extends MessageBus {
  private static final Logger LOG = LoggerFactory.getLogger(MessageClient.class);

  private static final Map<String, MessageClient> CLIENTS = new HashMap<>();

  private final TreeMap<Long, CkpMessage> ckpCache; // checkpoint id -> CkpMessage mapping

  private MessageClient(FileSystem fs, String basePath) throws IOException {
    super(fs, basePath);
    this.ckpCache = new TreeMap<>();
  }

  /**
   * Returns the message bus instance.
   *
   * <p>This expects to be called by the client.
   *
   * @param fs       The filesystem
   * @param basePath The table base path
   * @return The instance of message bus
   */
  private static MessageClient getInstance(FileSystem fs, String basePath) {
    try {
      return new MessageClient(fs, basePath);
    } catch (IOException e) {
      throw new HoodieException("Initialize checkpoint message bus error", e);
    }
  }

  /**
   * Returns the singleton message bus instance.
   *
   * <p>This expects to be called by the client.
   *
   * @param fs       The filesystem
   * @param basePath The table base path
   * @return The instance of message bus
   */
  public static synchronized MessageClient getSingleton(FileSystem fs, String basePath) {
    return CLIENTS.computeIfAbsent(basePath,
        k -> getInstance(fs, basePath));
  }

  public synchronized Option<CkpMessage> getCkpMessage(long checkpointId) {
    if (this.ckpCache.size() >= CLIENT_MESSAGE_CACHE_SIZE) {
      this.ckpCache.pollFirstEntry();
    }
    if (this.ckpCache.containsKey(checkpointId)) {
      return Option.of(this.ckpCache.get(checkpointId));
    }
    final Path commitFilePath = fullFilePath(getCommitFileName(checkpointId));
    try {
      if (fs.exists(commitFilePath)) {
        CkpMessage ckpMessage = CkpMessage.fromPath(fs, commitFilePath);
        this.ckpCache.put(checkpointId, ckpMessage);
        return Option.of(ckpMessage);
      }
    } catch (Throwable e) {
      // ignored
      LOG.warn("Read committed checkpoint message error: " + checkpointId, e);
      return Option.empty();
    }
    final Path abortedFilePath = fullFilePath(getAbortedFileName(checkpointId));
    try {
      if (fs.exists(abortedFilePath)) {
        CkpMessage ckpMessage = CkpMessage.fromPath(fs, abortedFilePath);
        this.ckpCache.put(checkpointId, ckpMessage);
        return Option.of(ckpMessage);
      }
    } catch (Throwable e) {
      // ignored
      LOG.warn("Read aborted checkpoint message error: " + checkpointId, e);
      return Option.empty();
    }
    return Option.empty();
  }

  @VisibleForTesting
  public TreeMap<Long, CkpMessage> getCkpCache() {
    return ckpCache;
  }

  @Override
  public void close() {
    synchronized (this) {
      this.ckpCache.clear();
    }
  }
}
