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

package org.apache.hudi.common.fs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class LeakTrackingFSDataInputStream extends FSDataInputStream {

  private static final Logger LOG = Logger.getLogger(LeakTrackingFSDataInputStream.class.getName());

  private final StackTraceElement[] callSite;

  private boolean closed = false;

  public LeakTrackingFSDataInputStream(FSDataInputStream in) {
    super(in);
    this.callSite = Thread.currentThread().getStackTrace();
  }

  @Override
  public void close() throws IOException {
    super.close();
    closed = true;
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    if (!closed) {
      close();
      String trace = Arrays.stream(callSite)
          .map(StackTraceElement::toString)
          .collect(Collectors.joining("\n\t"));
      LOG.error(String.format("Input stream have not been closed! Created at:\n\t%s", trace));
    }

    super.finalize();
  }

}
