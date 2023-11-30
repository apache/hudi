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

package org.apache.hudi.storage;

import org.apache.hudi.common.table.timeline.dto.FileStatusDTO;
import org.apache.hudi.common.util.Option;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface HoodieStorage {

  String getScheme();

  default OutputStream create(HoodieLocation loc) throws IOException {
    return create(loc, true);
  }

  OutputStream create(HoodieLocation loc, boolean overwrite) throws IOException;

  default boolean createNewFile(HoodieLocation loc) throws IOException {
    if (exists(loc)) {
      return false;
    } else {
      create(loc, false).close();
      return true;
    }
  }

  void createImmutableFileInPath(HoodieLocation fullPath, Option<byte[]> content);

  InputStream open(HoodieLocation loc) throws IOException;

  boolean exists(HoodieLocation loc) throws IOException;

  // should throw FileNotFoundException if not found
  HoodieFileInfo getFileInfo(HoodieLocation loc) throws FileNotFoundException;

  void mkdirs(HoodieLocation loc) throws IOException;

  List<HoodieFileInfo> listFiles(HoodieLocation loc) throws IOException;

  List<HoodieFileInfo> listFiles(List<HoodieLocation> locList) throws IOException;

  List<HoodieFileInfo> listFiles(HoodieLocation loc, HoodieLocationFilter filter) throws IOException;

  default List<HoodieFileInfo> globFiles(HoodieLocation loc) throws IOException {
    return globFiles(loc, e -> true);
  }

  List<HoodieFileInfo> globFiles(HoodieLocation loc, HoodieLocationFilter filter) throws IOException;

  boolean rename(HoodieLocation oldLoc, HoodieLocation newLoc);

  default boolean delete(HoodieLocation loc) throws IOException {
    return delete(loc, true);
  }

  boolean delete(HoodieLocation loc, boolean recursive) throws IOException;

  long getDefaultBlockSize(HoodieLocation loc);

  HoodieLocation makeQualified(HoodieLocation loc);

  Object getFileSystem();

  HoodieFileInfo toHoodieFileInfo(FileStatusDTO fileStatusDTO);
}
