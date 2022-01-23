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

package org.apache.hudi.hbase.io.hfile.bucket;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hudi.hbase.util.Bytes;
import org.apache.hadoop.util.Shell;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class implementing PersistentIOEngine interface supports file integrity verification
 * for {@link BucketCache} which use persistent IOEngine
 */
@InterfaceAudience.Private
public abstract class PersistentIOEngine implements IOEngine {
  private static final Logger LOG = LoggerFactory.getLogger(PersistentIOEngine.class);
  private static final DuFileCommand DU = new DuFileCommand(new String[] {"du", ""});
  protected final String[] filePaths;

  public PersistentIOEngine(String... filePaths) {
    this.filePaths = filePaths;
  }

  /**
   * Verify cache files's integrity
   * @param algorithm the backingMap persistence path
   */
  protected void verifyFileIntegrity(byte[] persistentChecksum, String algorithm)
      throws IOException {
    byte[] calculateChecksum = calculateChecksum(algorithm);
    if (!Bytes.equals(persistentChecksum, calculateChecksum)) {
      throw new IOException("Mismatch of checksum! The persistent checksum is " +
          Bytes.toString(persistentChecksum) + ", but the calculate checksum is " +
          Bytes.toString(calculateChecksum));
    }
  }

  /**
   * Using an encryption algorithm to calculate a checksum, the default encryption algorithm is MD5
   * @return the checksum which is convert to HexString
   * @throws IOException something happened like file not exists
   * @throws NoSuchAlgorithmException no such algorithm
   */
  protected byte[] calculateChecksum(String algorithm) {
    try {
      StringBuilder sb = new StringBuilder();
      for (String filePath : filePaths){
        File file = new File(filePath);
        sb.append(filePath);
        sb.append(getFileSize(filePath));
        sb.append(file.lastModified());
      }
      MessageDigest messageDigest = MessageDigest.getInstance(algorithm);
      messageDigest.update(Bytes.toBytes(sb.toString()));
      return messageDigest.digest();
    } catch (IOException ioex) {
      LOG.error("Calculating checksum failed, because of ", ioex);
      return new byte[0];
    } catch (NoSuchAlgorithmException e) {
      LOG.error("No such algorithm : " + algorithm + "!");
      return new byte[0];
    }
  }

  /**
   * Using Linux command du to get file's real size
   * @param filePath the file
   * @return file's real size
   * @throws IOException something happened like file not exists
   */
  private static long getFileSize(String filePath) throws IOException {
    DU.setExecCommand(filePath);
    DU.execute();
    return Long.parseLong(DU.getOutput().split("\t")[0]);
  }

  private static class DuFileCommand extends Shell.ShellCommandExecutor {
    private String[] execCommand;

    DuFileCommand(String[] execString) {
      super(execString);
      execCommand = execString;
    }

    void setExecCommand(String filePath) {
      this.execCommand[1] = filePath;
    }

    @Override
    public String[] getExecString() {
      return this.execCommand;
    }
  }
}
