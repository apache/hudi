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

package org.apache.hudi.secondary.index.lucene.hadoop;

import org.apache.hudi.common.util.FileIOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.LockReleaseFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

public class HdfsLockFactory extends LockFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final HdfsLockFactory INSTANCE = new HdfsLockFactory();

  private HdfsLockFactory() {
  }

  @Override
  public Lock obtainLock(Directory dir, String lockName) throws IOException {
    if (!(dir instanceof HdfsDirectory)) {
      throw new UnsupportedOperationException(
          "HdfsLockFactory can only be used with HdfsDirectory subclasses, got: " + dir);
    }
    final HdfsDirectory hdfsDir = (HdfsDirectory) dir;
    final Configuration conf = hdfsDir.getConfiguration();
    final Path lockPath = hdfsDir.getHdfsDirPath();
    final Path lockFile = new Path(lockPath, lockName);

    FSDataOutputStream file = null;
    final FileSystem fs = FileSystem.get(lockPath.toUri(), conf);
    while (true) {
      try {
        if (!fs.exists(lockPath)) {
          boolean success = fs.mkdirs(lockPath);
          if (!success) {
            throw new RuntimeException("Could not create directory: " + lockPath);
          }
        } else {
          // just to check for safe mode
          fs.mkdirs(lockPath);
        }

        file = fs.create(lockFile, false);
        break;
      } catch (RemoteException e) {
        if (e.getClassName().equals("org.apache.hadoop.hdfs.server.namenode.SafeModeException")) {
          LOG.warn("The NameNode is in SafeMode - Solr will wait 5 seconds and try again.");
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e1) {
            Thread.interrupted();
          }
          continue;
        }
        throw new LockObtainFailedException("Cannot obtain lock file: " + lockFile, e);
      } catch (IOException e) {
        throw new LockObtainFailedException("Cannot obtain lock file: " + lockFile, e);
      } finally {
        FileIOUtils.closeQuietly(file);
      }
    }

    return new HdfsLock(conf, lockFile);
  }

  private static final class HdfsLock extends Lock {
    private final Configuration conf;
    private final Path lockFile;
    private volatile boolean closed;

    HdfsLock(Configuration conf, Path lockFile) {
      this.conf = conf;
      this.lockFile = lockFile;
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      final FileSystem fs = FileSystem.get(lockFile.toUri(), conf);
      try {
        if (fs.exists(lockFile) && !fs.delete(lockFile, false)) {
          throw new LockReleaseFailedException("failed to delete: " + lockFile);
        }
      } finally {
        FileIOUtils.closeQuietly(fs);
      }
    }

    @Override
    public void ensureValid() throws IOException {
      // no idea how to implement this on HDFS
    }

    @Override
    public String toString() {
      return "HdfsLock(lockFile=" + lockFile + ")";
    }
  }
}
