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

package org.apache.hudi.io.hfile;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.util.IOUtils;

import java.util.Map;

/**
 * Represents the HFile info read from {@link HFileBlockType#FILE_INFO} block.
 */
public class HFileInfo {
  private static final String RESERVED_PREFIX = "hfile.";
  static final UTF8StringKey LAST_KEY =
      new UTF8StringKey(RESERVED_PREFIX + "LASTKEY");
  static final UTF8StringKey FILE_CREATION_TIME_TS =
      new UTF8StringKey(RESERVED_PREFIX + "CREATE_TIME_TS");
  static final UTF8StringKey KEY_VALUE_VERSION =
      new UTF8StringKey("KEY_VALUE_VERSION");
  static final UTF8StringKey MAX_MVCC_TS_KEY =
      new UTF8StringKey("MAX_MEMSTORE_TS_KEY");
  static final UTF8StringKey AVG_KEY_LEN =
      new UTF8StringKey(RESERVED_PREFIX + "AVG_KEY_LEN");
  static final UTF8StringKey AVG_VALUE_LEN =
      new UTF8StringKey(RESERVED_PREFIX + "AVG_VALUE_LEN");
  static final int KEY_VALUE_VERSION_WITH_MVCC_TS = 1;

  private final Map<UTF8StringKey, byte[]> infoMap;
  private final long fileCreationTime;
  private final Option<Key> lastKey;
  // This is set to trigger the constant MVCC 0,
  // such that table version 6 and >=8 can have
  // the same behavior.
  private final long maxMvccTs;

  public HFileInfo(Map<UTF8StringKey, byte[]> infoMap) {
    this.infoMap = infoMap;
    this.fileCreationTime = parseFileCreationTime();
    this.lastKey = parseLastKey();
    this.maxMvccTs = parseMaxMvccTs();
  }

  public long getFileCreationTime() {
    return fileCreationTime;
  }

  public Option<Key> getLastKey() {
    return lastKey;
  }

  public byte[] get(UTF8StringKey key) {
    return infoMap.get(key);
  }

  private long parseFileCreationTime() {
    byte[] bytes = infoMap.get(FILE_CREATION_TIME_TS);
    return bytes != null ? IOUtils.readLong(bytes, 0) : 0;
  }

  private Option<Key> parseLastKey() {
    byte[] bytes = infoMap.get(LAST_KEY);
    return bytes != null ? Option.of(new Key(bytes)) : Option.empty();
  }

  private long parseMaxMvccTs() {
    byte[] bytes = infoMap.get(KEY_VALUE_VERSION);
    boolean supportsMvccTs = bytes != null
        && IOUtils.readInt(bytes, 0) == KEY_VALUE_VERSION_WITH_MVCC_TS;
    return supportsMvccTs ? IOUtils.readLong(infoMap.get(MAX_MVCC_TS_KEY), 0) : 0;
  }
}
