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

package org.apache.hudi.testutils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.TaskContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A local file system that records the paths that Spark tasks open and list.
 *
 * <p>The records are static because Hadoop creates a new instance per {@code FileSystem.get} call
 * once caching is disabled, which {@link #register} does.
 */
public class TaskFileAccessRecordingFileSystem extends LocalFileSystem {

  private static final Queue<Access> TASK_OPENS = new ConcurrentLinkedQueue<>();
  private static final Queue<Access> TASK_LISTINGS = new ConcurrentLinkedQueue<>();

  /**
   * One access to a path by a Spark task attempt.
   */
  public static final class Access {
    private final long taskAttemptId;
    private final String path;

    private Access(long taskAttemptId, String path) {
      this.taskAttemptId = taskAttemptId;
      this.path = path;
    }

    public long getTaskAttemptId() {
      return taskAttemptId;
    }

    public String getPath() {
      return path;
    }

    @Override
    public String toString() {
      return path + " [task " + taskAttemptId + "]";
    }
  }

  /**
   * Makes the {@code file} scheme resolve to this file system on the given configuration.
   */
  public static void register(Configuration conf) {
    conf.set("fs.file.impl", TaskFileAccessRecordingFileSystem.class.getName());
    conf.set("fs.file.impl.disable.cache", "true");
  }

  public static void reset() {
    TASK_OPENS.clear();
    TASK_LISTINGS.clear();
  }

  /**
   * Returns the files opened by Spark tasks, once per open.
   */
  public static List<Access> taskOpens() {
    return new ArrayList<>(TASK_OPENS);
  }

  /**
   * Returns the directories listed by Spark tasks, once per listing.
   */
  public static List<Access> taskListings() {
    return new ArrayList<>(TASK_LISTINGS);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    record(TASK_OPENS, path);
    return super.open(path, bufferSize);
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    record(TASK_LISTINGS, path);
    return super.listStatus(path);
  }

  private static void record(Queue<Access> accesses, Path path) {
    TaskContext taskContext = TaskContext.get();
    if (taskContext != null) {
      accesses.add(new Access(taskContext.taskAttemptId(), path.toUri().getPath()));
    }
  }
}
