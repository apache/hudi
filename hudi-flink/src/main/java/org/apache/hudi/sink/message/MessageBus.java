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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.util.StreamerUtil;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * A message bus for transferring the checkpoint messages.
 *
 * <p>Each time the driver starts a new instant, it writes a commit message into the bus, the write tasks
 * then consume the message and unblocking the data flush.
 *
 * <p>Why we use the DFS based message queue instead of sending
 * the {@link org.apache.flink.runtime.operators.coordination.OperatorEvent} ?
 * The write task handles the operator event using the main mailbox executor which has the lowest priority for mails,
 * it is also used to process the inputs. When the write task blocks and waits for the operator event to ack the valid instant to write,
 * it actually blocks all the following events in the mailbox, the operator event can never be consumed then it causes deadlock.
 *
 * <p>The message bus is also more lightweight than the active timeline.
 */
public abstract class MessageBus implements AutoCloseable {

  public static final long INITIAL_CKP_ID = 0L;

  public static final String ABORTED_CKP_INSTANT = "aborted";

  protected static final int MESSAGE_QUEUE_LENGTH = 20;

  protected static final int CLIENT_MESSAGE_CACHE_SIZE = 10;

  private static final String MESSAGE_BUS = "message_bus";

  private static final String COMMIT = "commit";

  private static final String COMMIT_EXTENSION = "." + COMMIT;
  private static final String ABORTED_EXTENSION = ".aborted";

  protected final FileSystem fs;
  protected final String basePath;
  protected final String messageBusPath;

  protected MessageBus(FileSystem fs, String basePath) {
    this.fs = fs;
    this.basePath = basePath;
    this.messageBusPath = messageBusPath(basePath);
  }

  public static MessageDriver getDriver(FileSystem fs, String basePath) {
    return MessageDriver.getInstance(fs, basePath);
  }

  public static MessageClient getClient(FileSystem fs, String basePath) {
    return MessageClient.getSingleton(fs, basePath);
  }

  public static MessageClient getClient(String basePath) {
    FileSystem fs = FSUtils.getFs(basePath, StreamerUtil.getHadoopConf());
    return MessageClient.getSingleton(fs, basePath);
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  public static boolean canAbort(String instant, long checkpointId) {
    return ABORTED_CKP_INSTANT.equals(instant) && MessageBus.notInitialCkp(checkpointId);
  }

  public static boolean notInitialCkp(long checkpointId) {
    return checkpointId != INITIAL_CKP_ID;
  }

  protected Path fullFilePath(String fileName) {
    return new Path(messageBusPath, fileName);
  }

  protected static String messageBusPath(String basePath) {
    return basePath + Path.SEPARATOR + HoodieTableMetaClient.AUXILIARYFOLDER_NAME + Path.SEPARATOR + MESSAGE_BUS;
  }

  protected static String getCommitFileName(long checkpointId) {
    return checkpointId + COMMIT_EXTENSION;
  }

  protected static String getAbortedFileName(long checkpointId) {
    return checkpointId + ABORTED_EXTENSION;
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * A checkpoint message.
   */
  public static class CkpMessage {
    private static final String SEPARATOR = ",";

    public final boolean committed; // whether the checkpoint is committed

    public final long checkpointId;
    public final String commitInstant;
    public final String inflightInstant;

    private CkpMessage(long checkpointId, String commitInstant, String inflightInstant) {
      this.committed = true;
      this.checkpointId = checkpointId;
      this.commitInstant = commitInstant;
      this.inflightInstant = inflightInstant;
    }

    private CkpMessage(long checkpointId) {
      this.committed = false;
      this.checkpointId = checkpointId;
      this.commitInstant = ABORTED_CKP_INSTANT;
      this.inflightInstant = ABORTED_CKP_INSTANT;
    }

    /**
     * Encodes the instants as 'commitInstant,inflightInstant'.
     */
    public static byte[] toBytes(String commitInstant, String inflightInstant) {
      return (commitInstant + SEPARATOR + inflightInstant).getBytes(StandardCharsets.UTF_8);
    }

    public static CkpMessage fromBytes(long checkpointId, byte[] bytes) {
      String content = new String(bytes, StandardCharsets.UTF_8);
      String[] splits = content.split(SEPARATOR);
      return new CkpMessage(checkpointId, splits[0], splits[1]);
    }

    public static CkpMessage fromPath(FileSystem fs, Path path) throws IOException {
      final String[] splits = path.getName().split("\\.");
      ValidationUtils.checkState(splits.length == 2, "Invalid checkpoint message file name: " + path.getName());
      final long checkpointId = Long.parseLong(splits[0]);
      final String suffix = splits[1];
      if (suffix.equals(COMMIT)) {
        try (FSDataInputStream is = fs.open(path)) {
          byte[] bytes = FileIOUtils.readAsByteArray(is);
          return CkpMessage.fromBytes(checkpointId, bytes);
        }
      } else {
        return new CkpMessage(checkpointId);
      }
    }
  }
}
