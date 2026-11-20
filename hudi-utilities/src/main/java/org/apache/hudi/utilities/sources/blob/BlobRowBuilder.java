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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;
import org.apache.hudi.utilities.unstructured.storage.StorageReferenceRules;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/**
 * Builds blob metadata rows and bounded inline values from the current object size. Storage read failures propagate as {@link HoodieException}.
 */
public class BlobRowBuilder implements Serializable {

  private static final long serialVersionUID = 1L;

  private final StorageConfiguration<?> storageConf;
  private final long inlineMaxBytes;

  public BlobRowBuilder(TypedProperties props, StorageConfiguration<?> storageConf) {
    this.storageConf = storageConf;
    this.inlineMaxBytes = BlobTableConfig.inlineMaxBytes(props);
  }

  /** Lazily maps one Spark partition of selected objects to rows of {@link BlobTableSchema#schema()}. */
  public Iterator<Row> build(Iterator<CloudObjectMetadata> files) {
    return new Iterator<Row>() {
      @Override
      public boolean hasNext() {
        return files.hasNext();
      }

      @Override
      public Row next() {
        return buildRow(files.next());
      }
    };
  }

  /**
   * Builds one row of one version of the object; reads the bytes of an inline-size object, touches nothing under the table. The
   * version tag (modification time, size) is the object's as its status reports it (a storage that reports no modification time
   * gives 0, and its versions then differ by size alone). Inline reads refuse an observed change of that tag after closing the
   * stream; a rewrite preserving both fields is indistinguishable to this version identity. Any failure of a status call, including an object that is gone,
   * and any failure to read an inline-size object fail the round.
   */
  Row buildRow(CloudObjectMetadata file) {
    // the object's own path, not normalized
    String sourcePath = file.getPath();
    String fileName = BlobFileNames.fileName(sourcePath);
    String extension = BlobFileNames.extension(fileName);
    String sourcePathSha = BlobFileNames.sourcePathSha(sourcePath);
    String contentType = BlobFileNames.contentType(extension);
    StoragePath path = checkedPath(sourcePath);
    long currentSize;
    long modificationTime;
    try {
      // The observed length and modification time (S3: LastModified, ms) are the row's version tag.
      StoragePathInfo info = HoodieStorageUtils.getStorage(path, storageConf).getPathInfo(path);
      currentSize = info.getLength();
      modificationTime = info.getModificationTime();
    } catch (IOException e) {
      // any status failure, a missing object included, fails the round
      throw new HoodieIOException("Failed to stat source file " + sourcePath, e);
    }
    Row blob;
    if (currentSize <= inlineMaxBytes) {
      byte[] bytes = readInline(path, currentSize, modificationTime);
      blob = RowFactory.create(HoodieSchema.Blob.INLINE, bytes, null);
    } else {
      // An unmanaged whole-file reference: the source remains where it is.
      StorageReferenceRules.validateWriteOwnership(sourcePath, false);
      blob = RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null,
          RowFactory.create(sourcePath, null, null, false));
    }
    return RowFactory.create(sourcePathSha, modificationTime, sourcePath, fileName, extension, currentSize, contentType, blob);
  }

  /** Refuse decoded cloud keys that the storage path would address as a different object. */
  private static StoragePath checkedPath(String sourcePath) {
    StoragePath path = new StoragePath(sourcePath);
    int start = path.toUri().getScheme() == null ? 0 : sourcePath.indexOf(':') + 1;
    if (sourcePath.startsWith("//", start)) {
      int slash = sourcePath.indexOf('/', start + 2);
      start = slash < 0 ? sourcePath.length() : slash;
    }
    if (!sourcePath.substring(start).equals(HadoopFSUtils.convertToHadoopPath(path).toUri().getPath())) {
      throw new IllegalArgumentException("Source path would be normalized to a different object: " + sourcePath);
    }
    return path;
  }

  private byte[] readInline(StoragePath path, long size, long modificationTime) {
    byte[] bytes = new byte[(int) size];
    try {
      try (DataInputStream in = new DataInputStream(HoodieStorageUtils.getStorage(path, storageConf).open(path))) {
        in.readFully(bytes);
        if (in.read() != -1) {
          throw new IOException("Source file " + path + " holds more than the " + size + " bytes its status call reported: "
              + "it changed after the status call");
        }
      }
      StoragePathInfo after = HoodieStorageUtils.getStorage(path, storageConf).getPathInfo(path);
      if (after.getLength() != size || after.getModificationTime() != modificationTime) {
        throw new IOException("Source file " + path + " changed after the status call");
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read source file " + path, e);
    }
    return bytes;
  }
}
