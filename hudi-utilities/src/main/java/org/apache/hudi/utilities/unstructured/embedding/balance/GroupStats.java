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

package org.apache.hudi.utilities.unstructured.embedding.balance;

import java.io.Serializable;
import java.util.Objects;

/**
 * The per-group aggregate of one prep batch that size/type-aware prep balancing brings to the driver: the group's
 * file count and byte total (row weights), and how many of its files weigh at least the group's budget ({@code soloFiles},
 * each taking a task of its own) with their byte total.
 */
public final class GroupStats implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String group;
  private final long files;
  private final long bytes;
  private final long soloFiles;
  private final long soloBytes;

  public GroupStats(String group, long files, long bytes, long soloFiles, long soloBytes) {
    this.group = Objects.requireNonNull(group, "group");
    this.files = files;
    this.bytes = bytes;
    this.soloFiles = soloFiles;
    this.soloBytes = soloBytes;
  }

  public String group() {
    return group;
  }

  public long files() {
    return files;
  }

  public long bytes() {
    return bytes;
  }

  public long soloFiles() {
    return soloFiles;
  }

  public long soloBytes() {
    return soloBytes;
  }

  @Override
  public String toString() {
    return group + "{files=" + files + ", bytes=" + bytes + ", soloFiles=" + soloFiles + ", soloBytes=" + soloBytes + "}";
  }
}
