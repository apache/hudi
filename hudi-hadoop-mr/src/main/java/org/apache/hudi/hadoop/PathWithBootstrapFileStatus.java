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

package org.apache.hudi.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Hacky Workaround !!!
 * With the base input format implementations in Hadoop/Hive,
 * we need to encode additional information in Path to track matching external file.
 * Hence, this weird looking class which tracks an external file status
 * in Path.
 */
public class PathWithBootstrapFileStatus extends Path {

  private final FileStatus bootstrapFileStatus;

  public PathWithBootstrapFileStatus(Path path, FileStatus bootstrapFileStatus) {
    super(path.getParent(), path.getName());
    this.bootstrapFileStatus = bootstrapFileStatus;
  }

  public FileStatus getBootstrapFileStatus() {
    return bootstrapFileStatus;
  }
}