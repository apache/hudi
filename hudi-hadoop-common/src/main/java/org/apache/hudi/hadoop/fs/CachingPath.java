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

import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.fs.Path;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * This is an extension of the {@code Path} class allowing to avoid repetitive
 * computations (like {@code getFileName}, {@code toString}) which are secured
 * by its immutability
 *
 * NOTE: This class is thread-safe
 */
@ThreadSafe
public class CachingPath extends Path {

  // NOTE: `volatile` keyword is redundant here and put mostly for reader notice, since all
  //       reads/writes to references are always atomic (including 64-bit JVMs)
  //       https://docs.oracle.com/javase/specs/jls/se8/html/jls-17.html#jls-17.7
  private volatile Path parent;
  private volatile String fileName;
  private volatile String fullPathStr;

  public CachingPath(String parent, String child) {
    super(parent, child);
  }

  public CachingPath(Path parent, String child) {
    super(parent, child);
  }

  public CachingPath(String parent, Path child) {
    super(parent, child);
  }

  public CachingPath(Path parent, Path child) {
    super(parent, child);
  }

  public CachingPath(String pathString) throws IllegalArgumentException {
    super(pathString);
  }

  public CachingPath(URI aUri) {
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

  @Override
  public Path getParent() {
    // This value could be overwritten concurrently and that's okay, since
    // {@code Path} is immutable
    if (parent == null) {
      parent = super.getParent();
    }

    return parent;
  }

  @Override
  public String toString() {
    // This value could be overwritten concurrently and that's okay, since
    // {@code Path} is immutable
    if (fullPathStr == null) {
      fullPathStr = super.toString();
    }
    return fullPathStr;
  }

  public CachingPath subPath(String relativePath) {
    return new CachingPath(this, createRelativePathUnsafe(relativePath));
  }

  public static CachingPath wrap(Path path) {
    if (path instanceof CachingPath) {
      return (CachingPath) path;
    }

    return new CachingPath(path.toUri());
  }

  /**
   * Creates path based on the provided *relative* path
   *
   * NOTE: This is an unsafe version that is relying on the fact that the caller is aware
   *       what they are doing this is not going to work with paths having scheme (which require
   *       parsing) and is only meant to work w/ relative paths in a few specific cases.
   */
  public static CachingPath createRelativePathUnsafe(String relativePath) {
    try {
      // NOTE: {@code normalize} is going to be invoked by {@code Path} ctor, so there's no
      //       point in invoking it here
      URI uri = new URI(null, null, relativePath, null, null);
      return new CachingPath(uri);
    } catch (URISyntaxException e) {
      throw new HoodieException("Failed to instantiate relative path", e);
    }
  }

  // TODO java-doc
  public static CachingPath concatPathUnsafe(Path basePath, Path relativePath) {
    checkState(!relativePath.toUri().isAbsolute());
    return concatPathUnsafe(basePath, relativePath.toUri().getPath());
  }

  // TODO java-doc
  public static CachingPath concatPathUnsafe(Path basePath, String relativePath) {
    try {
      URI baseURI = basePath.toUri();
      // NOTE: {@code normalize} is going to be invoked by {@code Path} ctor, so there's no
      //       point in invoking it here
      String resolvedPath = resolveRelativePath(baseURI.getPath(), relativePath);
      URI resolvedURI = new URI(baseURI.getScheme(), baseURI.getAuthority(), resolvedPath,
          baseURI.getQuery(), baseURI.getFragment());

      return new CachingPath(resolvedURI);
    } catch (URISyntaxException e) {
      throw new HoodieException("Failed to instantiate relative path", e);
    }
  }

  // NOTE: This method does NOT perform any normalization, assuming that the incoming paths
  //       are already normalized
  private static String resolveRelativePath(String basePath, String relativePath) {
    StringBuffer sb = new StringBuffer(basePath);
    if (basePath.endsWith("/")) {
      if (relativePath.startsWith("/")) {
        sb.append(relativePath.substring(1));
      } else {
        sb.append(relativePath);
      }
    } else if (relativePath.startsWith("/")) {
      sb.append(relativePath);
    } else {
      sb.append('/');
      sb.append(relativePath);
    }

    return sb.toString();
  }

  /**
   * This is {@link Path#getPathWithoutSchemeAndAuthority(Path)} counterpart, instantiating
   * {@link CachingPath}
   */
  public static Path getPathWithoutSchemeAndAuthority(Path path) {
    // This code depends on Path.toString() to remove the leading slash before
    // the drive specification on Windows.
    return path.isUriPathAbsolute()
        ? createRelativePathUnsafe(path.toUri().getPath())
        : path;
  }
}
