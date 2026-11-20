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

package org.apache.hudi.utilities.unstructured.embedding.source.fixtures;

import org.apache.hudi.utilities.unstructured.embedding.source.CountingObjectFileSystem;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/** Public ETag API with public and non-public implementations, as returned by Hadoop connectors. */
public class EtagStatusFileSystem extends CountingObjectFileSystem {
  public static String etag = "initial";
  public static boolean hidden;
  public static String versionId;
  public static boolean unavailableEtag;

  public interface EtagProvider {
    String getEtag();

    String getVersionId();
  }

  public static class PublicStatus extends FileStatus implements EtagProvider {
    private final String value;
    private final String sourceVersion = versionId;
    private final boolean missingEtag = unavailableEtag;

    public PublicStatus(FileStatus status, String value) {
      super(status.getLen(), status.isDirectory(), status.getReplication(), status.getBlockSize(), status.getModificationTime(), status.getPath());
      this.value = value;
    }

    @Override
    public String getEtag() {
      if (missingEtag) {
        throw new UnsupportedOperationException("ETag unavailable");
      }
      return value;
    }

    @Override
    public String getVersionId() {
      return sourceVersion;
    }
  }

  // Declares its own getter: inheriting a public implementation would hide the connector's access boundary.
  private static final class HiddenStatus extends FileStatus implements EtagProvider {
    private final String value;
    private final String sourceVersion = versionId;
    private final boolean missingEtag = unavailableEtag;

    private HiddenStatus(FileStatus status, String value) {
      super(status.getLen(), status.isDirectory(), status.getReplication(), status.getBlockSize(), status.getModificationTime(), status.getPath());
      this.value = value;
    }

    @Override
    public String getEtag() {
      if (missingEtag) {
        throw new UnsupportedOperationException("ETag unavailable");
      }
      return value;
    }

    @Override
    public String getVersionId() {
      return sourceVersion;
    }
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    FileStatus status = super.getFileStatus(path);
    return hidden ? new HiddenStatus(status, etag) : new PublicStatus(status, etag);
  }
}
