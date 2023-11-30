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

package org.apache.hudi.storage.hadoop;

import org.apache.hudi.common.table.timeline.dto.FileStatusDTO;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieFileInfo;
import org.apache.hudi.storage.HoodieLocation;
import org.apache.hudi.storage.HoodieLocationFilter;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HoodieHadoopBasedStorage implements HoodieStorage {
  private final FileSystem fs;

  public HoodieHadoopBasedStorage(FileSystem fs) {
    this.fs = fs;
  }

  @Override
  public String getScheme() {
    return fs.getScheme();
  }

  @Override
  public OutputStream create(HoodieLocation loc, boolean overwrite) throws IOException {
    return fs.create(convertHoodieLocationToPath(loc), overwrite);
  }

  @Override
  public void createImmutableFileInPath(HoodieLocation fullPath, Option<byte[]> content) {
    //return fs.createImmutableFileInPath();
  }

  @Override
  public InputStream open(HoodieLocation loc) throws IOException {
    return fs.open(convertHoodieLocationToPath(loc));
  }

  @Override
  public boolean exists(HoodieLocation loc) throws IOException {
    return fs.exists(convertHoodieLocationToPath(loc));
  }

  @Override
  public HoodieFileInfo getFileInfo(HoodieLocation loc) throws FileNotFoundException {
    return null; // fs.getFileStatus(convertHoodieLocationToPath(loc));
  }

  @Override
  public void mkdirs(HoodieLocation loc) throws IOException {
    fs.mkdirs(convertHoodieLocationToPath(loc));
  }

  @Override
  public List<HoodieFileInfo> listFiles(HoodieLocation loc) throws IOException {
    return Arrays.stream(fs.listStatus(convertHoodieLocationToPath(loc)))
        .map(HoodieFileInfoWithFileStatus::new)
        .collect(Collectors.toList());
  }

  @Override
  public List<HoodieFileInfo> listFiles(List<HoodieLocation> locList) throws IOException {
    return Arrays.stream(fs.listStatus(locList.stream()
            .map(loc -> convertHoodieLocationToPath(loc))
            .toArray(Path[]::new)))
        .map(HoodieFileInfoWithFileStatus::new)
        .collect(Collectors.toList());
  }

  @Override
  public List<HoodieFileInfo> listFiles(HoodieLocation loc, HoodieLocationFilter filter) throws IOException {
    return Arrays.stream(fs.listStatus(
            convertHoodieLocationToPath(loc), path -> filter.accept(new HoodieLocation(path.toString()))))
        .map(HoodieFileInfoWithFileStatus::new)
        .collect(Collectors.toList());
  }

  @Override
  public List<HoodieFileInfo> globFiles(HoodieLocation loc, HoodieLocationFilter filter) throws IOException {
    return null;
  }

  @Override
  public boolean rename(HoodieLocation oldLoc, HoodieLocation newLoc) {
    return false;
  }

  @Override
  public boolean delete(HoodieLocation loc, boolean recursive) throws IOException {
    return false;
  }

  @Override
  public long getDefaultBlockSize(HoodieLocation loc) {
    return 0;
  }

  @Override
  public HoodieLocation makeQualified(HoodieLocation loc) {
    return null;
  }

  @Override
  public Object getFileSystem() {
    return null;
  }

  @Override
  public HoodieFileInfo toHoodieFileInfo(FileStatusDTO fileStatusDTO) {
    return null;
  }

  private Path convertHoodieLocationToPath(HoodieLocation loc) {
    return new Path(loc.toString());
  }
}
