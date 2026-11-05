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

package org.apache.hudi.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * The local filesystem exposed under a scheme other than {@code file}, so a test can tell apart
 * storage resolved from a path (correct) and storage resolved from a default {@code file:///} URI
 * (wrong), the way s3a and gs do in production without needing a remote object store.
 *
 * <p>{@link RawLocalFileSystem#pathToFile} keeps only the path component of a URI, so a
 * {@code <scheme>://<bucket>/tmp/x} path reads and writes the local file {@code /tmp/x}. What this
 * subclass changes is only the identity the filesystem reports, which is what
 * {@link FileSystem#checkPath} validates every path against.
 *
 * <p>Register it per scheme on the test's configuration:
 *
 * <pre>
 *   conf.setClass("fs.s3a.impl", NonLocalSchemeLocalFileSystem.class, FileSystem.class);
 * </pre>
 *
 * A scheme is only safe to borrow when no real implementation claims it on the module's test
 * classpath.
 */
public class NonLocalSchemeLocalFileSystem extends RawLocalFileSystem {

  /**
   * Answer for {@link #getUri()} until {@link #initialize} supplies the real one. The superclass
   * constructor calls {@code getUri()}, which runs before any instance field of this subclass is
   * assigned, so the fallback has to be a static. The scheme is a placeholder: it is replaced on
   * the first {@code initialize} call, and no path is validated against it before then.
   */
  private static final URI UNINITIALIZED_URI = URI.create("uninitialized:///");

  private URI uri;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    this.uri = URI.create(name.getScheme() + "://"
        + (name.getAuthority() == null ? "" : name.getAuthority()));
    setWorkingDirectory(new Path(this.uri.toString() + Path.SEPARATOR));
  }

  @Override
  public URI getUri() {
    return uri == null ? UNINITIALIZED_URI : uri;
  }

  @Override
  public String getScheme() {
    return getUri().getScheme();
  }

  /**
   * The superclass qualifies the process working directory against {@link #getUri()} from its own
   * constructor. Skip that: this filesystem does not know its real URI yet, and every path a test
   * gives it is absolute, so the working directory is never consulted.
   */
  @Override
  protected Path getInitialWorkingDirectory() {
    return new Path(System.getProperty("user.dir"));
  }
}
