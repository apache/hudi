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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

/**
 * TEST-ONLY file system that stands in for S3 on the local disk: {@code bucketfs://<bucket>/<key>} is the file
 * {@code <root>/<bucket>/<key>}, with {@code <root>} from {@value #ROOT}. With
 * {@code hoodie.streamer.source.s3incr.fs.prefix=bucketfs}, the upstream object URL {@code <prefix>://<bucket>/<key>} of an
 * S3 events row then opens a local file, so the blob source runs unchanged over local objects in several buckets.
 */
public class BucketFolderFileSystem extends RawLocalFileSystem {

  public static final String SCHEME = "bucketfs";
  public static final String ROOT = "fs.bucketfs.root";

  private URI uri;
  private File root;

  /** Registers the scheme in {@code conf}, its buckets as folders under {@code root}. */
  public static void install(Configuration conf, java.nio.file.Path root) {
    conf.set("fs." + SCHEME + ".impl", BucketFolderFileSystem.class.getName());
    // one instance per bucket: the cache keys by scheme and authority, and is off so a changed root is seen
    conf.setBoolean("fs." + SCHEME + ".impl.disable.cache", true);
    conf.set(ROOT, root.toAbsolutePath().toString());
    try {
      // a bucket is created as a folder of the root on its first write, which needs the root itself
      java.nio.file.Files.createDirectories(root);
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  /** The local file of {@code bucketfs://bucket/key} under {@code root}. */
  public static java.nio.file.Path file(java.nio.file.Path root, String bucket, String key) {
    return root.resolve(bucket).resolve(key);
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    this.uri = URI.create(scheme() + "://" + (name.getAuthority() == null ? "" : name.getAuthority()) + "/");
    this.root = new File(conf.get("fs." + scheme() + ".root"));
  }

  /** The superclass constructor asks for the URI before {@link #initialize} has set it. */
  @Override
  public URI getUri() {
    return uri == null ? URI.create(scheme() + ":///") : uri;
  }

  @Override
  public String getScheme() {
    return scheme();
  }

  /** The scheme this file system serves; a subclass serves another one over the same layout. */
  protected String scheme() {
    return SCHEME;
  }

  @Override
  public File pathToFile(Path path) {
    URI pathUri = path.toUri();
    if (!scheme().equals(pathUri.getScheme())) {
      return super.pathToFile(path);
    }
    return new File(new File(root, pathUri.getAuthority()), pathUri.getPath());
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    File file = pathToFile(f);
    if (!file.exists()) {
      throw new FileNotFoundException("File " + f + " does not exist");
    }
    // a test may fail an object's status (SourceOpenHooks.failWith)
    SourceOpenHooks.checkStatus(file.getAbsolutePath());
    // the object's version tag as the blob source reads it: the local file's modification time (ms) and length
    return new FileStatus(file.isDirectory() ? 0 : file.length(), file.isDirectory(), 1, getDefaultBlockSize(f),
        file.lastModified(), makeQualified(f));
  }

  /**
   * Lists each entry under its {@code bucketfs://} URL, as S3A lists under {@code s3a://}. The inherited listing would name a
   * file by its local path, which does not exist under this scheme, and a Spark read of a listed object would find no file.
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    File file = pathToFile(f);
    if (!file.isDirectory()) {
      return new FileStatus[] {getFileStatus(f)};
    }
    String[] names = file.list();
    if (names == null) {
      throw new IOException("Cannot list " + f);
    }
    FileStatus[] statuses = new FileStatus[names.length];
    for (int i = 0; i < names.length; i++) {
      statuses[i] = getFileStatus(new Path(f, names[i]));
    }
    return statuses;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    SourceOpenHooks.opening(pathToFile(f).getAbsolutePath());
    FSDataInputStream in = super.open(f, bufferSize);
    SourceOpenHooks.opened(pathToFile(f).getAbsolutePath());
    return in;
  }
}
