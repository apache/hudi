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

import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * NOTE: This class is thread-safe
 */
public class FileNameCachingPath extends Path {

  // NOTE: volatile keyword is redundant here and put mostly for reader notice, since all
  //       reads/writes to references are always atomic (including 64-bit JVMs)
  //       https://docs.oracle.com/javase/specs/jls/se8/html/jls-17.html#jls-17.7
  private volatile String fileName;

  public FileNameCachingPath(URI aUri) {
    super(aUri);
  }

  @Override
  public String getName() {
    // This value could be overwritten concurrently and that's okay, since
    // {@code Path} is immutable
    if (fileName == null) {
      fileName = super.getName();
    }
    return fileName;
  }
}
