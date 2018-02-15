/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.table.log;

/**
 * A set of feature flags associated with a log block format.
 * Versions are changed when the log block format changes.
 * TODO(na) - Implement policies around major/minor versions
 */
abstract class LogBlockVersion {
  private final int version;

  LogBlockVersion(int version) {
    this.version = version;
  }

  public int getVersion() {
    return version;
  }

  public abstract boolean hasMagicHeader();

  public abstract boolean hasContent();

  public abstract boolean hasContentLength();

  public abstract boolean hasOrdinal();

  public abstract boolean hasHeader();

  public abstract boolean hasFooter();

  public abstract boolean hasLogBlockLength();
}

/**
 * Implements logic to determine behavior for feature flags for {@link LogBlockVersion}
 */
final class HoodieLogBlockVersion extends LogBlockVersion {

  public final static int DEFAULT_VERSION = 0;

  HoodieLogBlockVersion(int version) {
    super(version);
  }
  @Override
  public boolean hasMagicHeader() {
    switch (super.getVersion()) {
      case DEFAULT_VERSION:
        return true;
      default:
        return true;
    }
  }

  @Override
  public boolean hasContent() {
    switch (super.getVersion()) {
      case DEFAULT_VERSION:
        return true;
      default:
        return true;
    }
  }

  @Override
  public boolean hasContentLength() {
    switch (super.getVersion()) {
      case DEFAULT_VERSION:
        return true;
      default:
        return true;
    }
  }

  @Override
  public boolean hasOrdinal() {
    switch (super.getVersion()) {
      case DEFAULT_VERSION:
        return true;
      default:
        return true;
    }
  }

  @Override
  public boolean hasHeader() {
    switch (super.getVersion()) {
      case DEFAULT_VERSION:
        return false;
      default:
        return true;
    }
  }

  @Override
  public boolean hasFooter() {
    switch (super.getVersion()) {
      case DEFAULT_VERSION:
        return false;
      case 1:
        return true;
    }
    return false;
  }

  @Override
  public boolean hasLogBlockLength() {
    switch (super.getVersion()) {
      case DEFAULT_VERSION:
        return false;
      case 1:
        return true;
    }
    return false;
  }
}