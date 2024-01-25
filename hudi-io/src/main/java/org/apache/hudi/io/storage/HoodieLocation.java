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

package org.apache.hudi.io.storage;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

public class HoodieLocation implements Comparable<HoodieLocation>, Serializable {
  public static final char SEPARATOR_CHAR = '/';
  public static final String SEPARATOR = "" + SEPARATOR_CHAR;
  private final URI uri;
  private transient volatile HoodieLocation cachedParent;
  private transient volatile String cachedName;
  private transient volatile String uriString;

  public HoodieLocation(URI uri) {
    this.uri = uri;
  }

  public HoodieLocation(String path) {
    try {
      this.uri = new URI(normalize(path, true));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public HoodieLocation(String parent, String child) {
    this(new HoodieLocation(parent), child);
  }

  public HoodieLocation(HoodieLocation parent, String child) {
    URI parentUri = parent.toUri();
    String normalizedChild = normalize(child, false);

    if (normalizedChild.isEmpty()) {
      this.uri = parentUri;
      return;
    }

    if (!child.contains(SEPARATOR)) {
      this.cachedParent = parent;
    }
    String parentPathWithSeparator = parentUri.getPath();
    if (!parentPathWithSeparator.endsWith(SEPARATOR)) {
      parentPathWithSeparator = parentPathWithSeparator + SEPARATOR;
    }
    try {
      this.uri = new URI(
          parentUri.getScheme(),
          parentUri.getAuthority(),
          parentPathWithSeparator + child,
          null,
          parentUri.getFragment());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public boolean isAbsolute() {
    return true;
  }

  public HoodieLocation getParent() {
    // This value could be overwritten concurrently and that's okay, since
    // {@code HoodieLocation} is immutable
    if (cachedParent == null) {
      String path = uri.getPath();
      int lastSlash = path.lastIndexOf(SEPARATOR_CHAR);
      if (path.isEmpty() || path.equals(SEPARATOR)) {
        throw new IllegalStateException("Cannot get parent location of a root location");
      }
      String parentPath = lastSlash == -1
          ? "" : path.substring(0, lastSlash == 0 ? 1 : lastSlash);
      try {
        cachedParent = new HoodieLocation(new URI(
            uri.getScheme(), uri.getAuthority(), parentPath, null, uri.getFragment()));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
    }
    return cachedParent;
  }

  public String getName() {
    // This value could be overwritten concurrently and that's okay, since
    // {@code HoodieLocation} is immutable
    if (cachedName == null) {
      String path = uri.getPath();
      int slash = path.lastIndexOf(SEPARATOR);
      cachedName = path.substring(slash + 1);
    }
    return cachedName;
  }

  public HoodieLocation getLocationWithoutSchemeAndAuthority() {
    try {
      return new HoodieLocation(
          new URI(null, null, uri.getPath(), uri.getQuery(), uri.getFragment()));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public int depth() {
    String path = uri.getPath();
    int depth = 0;
    int slash = path.length() == 1 && path.charAt(0) == SEPARATOR_CHAR ? -1 : 0;
    while (slash != -1) {
      depth++;
      slash = path.indexOf(SEPARATOR_CHAR, slash + 1);
    }
    return depth;
  }

  public URI toUri() {
    return uri;
  }

  @Override
  public String toString() {
    // This value could be overwritten concurrently and that's okay, since
    // {@code HoodieLocation} is immutable
    if (uriString == null) {
      uriString = uri.toString();
    }
    return uriString;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof HoodieLocation)) {
      return false;
    }
    return this.uri.equals(((HoodieLocation) o).toUri());
  }

  @Override
  public int hashCode() {
    return uri.hashCode();
  }

  @Override
  public int compareTo(HoodieLocation o) {
    return this.uri.compareTo(o.uri);
  }

  private static String normalize(String path, boolean keepSingleSlash) {
    int minLength = keepSingleSlash ? 1 : 0;
    if (path.length() > minLength && path.endsWith(SEPARATOR)) {
      return path.substring(0, path.length() - 1);
    }
    return path;
  }
}
