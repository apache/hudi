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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;

/**
 * TEST-ONLY file system that stands in for GCS on the local disk: {@code gs://<bucket>/<key>} is the file
 * {@code <root>/<bucket>/<key>}, as {@link BucketFolderFileSystem} lays it out. The GCS events table's object URL is always
 * {@code gs://<bucket>/<name>}, so registering this file system for {@code gs} makes the unstructured data source on
 * {@code cloud=gcs} read local objects.
 */
public class GcsBucketFolderFileSystem extends BucketFolderFileSystem {

  public static final String GCS_SCHEME = "gs";

  /** Registers the {@code gs} scheme in {@code conf}, its buckets as folders under {@code root}. */
  public static void installGcs(Configuration conf, java.nio.file.Path root) {
    conf.set("fs." + GCS_SCHEME + ".impl", GcsBucketFolderFileSystem.class.getName());
    conf.setBoolean("fs." + GCS_SCHEME + ".impl.disable.cache", true);
    conf.set("fs." + GCS_SCHEME + ".root", root.toAbsolutePath().toString());
    try {
      Files.createDirectories(root);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Removes the registration of {@link #installGcs}. */
  public static void uninstallGcs(Configuration conf) {
    conf.unset("fs." + GCS_SCHEME + ".impl");
    conf.unset("fs." + GCS_SCHEME + ".impl.disable.cache");
    conf.unset("fs." + GCS_SCHEME + ".root");
  }

  @Override
  protected String scheme() {
    return GCS_SCHEME;
  }
}
