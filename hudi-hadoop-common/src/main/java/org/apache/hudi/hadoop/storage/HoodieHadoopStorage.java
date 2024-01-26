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

package org.apache.hudi.hadoop.storage;

import org.apache.hudi.io.storage.HoodieFileStatus;
import org.apache.hudi.io.storage.HoodieLocation;
import org.apache.hudi.io.storage.HoodieLocationFilter;
import org.apache.hudi.io.storage.HoodieStorage;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of {@link HoodieStorage} using Hadoop's {@link FileSystem}
 */
public class HoodieHadoopStorage extends HoodieStorage {
  private final FileSystem fs;

  public HoodieHadoopStorage(FileSystem fs) {
    this.fs = fs;
  }

  @Override
  public String getScheme() {
    return fs.getScheme();
  }

  @Override
  public OutputStream create(HoodieLocation location, boolean overwrite) throws IOException {
    return fs.create(convertHoodieLocationToPath(location), overwrite);
  }

  @Override
  public InputStream open(HoodieLocation location) throws IOException {
    return fs.open(convertHoodieLocationToPath(location));
  }

  @Override
  public OutputStream append(HoodieLocation location) throws IOException {
    return fs.append(convertHoodieLocationToPath(location));
  }

  @Override
  public boolean exists(HoodieLocation location) throws IOException {
    return fs.exists(convertHoodieLocationToPath(location));
  }

  @Override
  public HoodieFileStatus getFileStatus(HoodieLocation location) throws IOException {
    return convertToHoodieFileStatus(fs.getFileStatus(convertHoodieLocationToPath(location)));
  }

  @Override
  public boolean createDirectory(HoodieLocation location) throws IOException {
    return fs.mkdirs(convertHoodieLocationToPath(location));
  }

  @Override
  public List<HoodieFileStatus> listDirectEntries(HoodieLocation location) throws IOException {
    return Arrays.stream(fs.listStatus(convertHoodieLocationToPath(location)))
        .map(this::convertToHoodieFileStatus)
        .collect(Collectors.toList());
  }

  @Override
  public List<HoodieFileStatus> listFiles(HoodieLocation location) throws IOException {
    List<HoodieFileStatus> result = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(convertHoodieLocationToPath(location), true);
    while (iterator.hasNext()) {
      result.add(convertToHoodieFileStatus(iterator.next()));
    }
    return result;
  }

  @Override
  public List<HoodieFileStatus> listDirectEntries(List<HoodieLocation> locationList) throws IOException {
    return Arrays.stream(fs.listStatus(locationList.stream()
            .map(this::convertHoodieLocationToPath)
            .toArray(Path[]::new)))
        .map(this::convertToHoodieFileStatus)
        .collect(Collectors.toList());
  }

  @Override
  public List<HoodieFileStatus> listDirectEntries(HoodieLocation location,
                                                  HoodieLocationFilter filter)
      throws IOException {
    return Arrays.stream(fs.listStatus(
            convertHoodieLocationToPath(location), path ->
                filter.accept(convertPathToHoodieLocation(path))))
        .map(this::convertToHoodieFileStatus)
        .collect(Collectors.toList());
  }

  @Override
  public List<HoodieFileStatus> globEntries(HoodieLocation locationPattern)
      throws IOException {
    return Arrays.stream(fs.globStatus(convertHoodieLocationToPath(locationPattern)))
        .map(this::convertToHoodieFileStatus)
        .collect(Collectors.toList());
  }

  @Override
  public List<HoodieFileStatus> globEntries(HoodieLocation locationPattern, HoodieLocationFilter filter)
      throws IOException {
    return Arrays.stream(fs.globStatus(convertHoodieLocationToPath(locationPattern), path ->
            filter.accept(convertPathToHoodieLocation(path))))
        .map(this::convertToHoodieFileStatus)
        .collect(Collectors.toList());
  }

  @Override
  public boolean rename(HoodieLocation oldLocation, HoodieLocation newLocation) throws IOException {
    return fs.rename(convertHoodieLocationToPath(oldLocation), convertHoodieLocationToPath(newLocation));
  }

  @Override
  public boolean deleteDirectory(HoodieLocation location) throws IOException {
    return fs.delete(convertHoodieLocationToPath(location), true);
  }

  @Override
  public boolean deleteFile(HoodieLocation location) throws IOException {
    return fs.delete(convertHoodieLocationToPath(location), false);
  }

  @Override
  public HoodieLocation makeQualified(HoodieLocation location) {
    return convertPathToHoodieLocation(
        fs.makeQualified(convertHoodieLocationToPath(location)));
  }

  @Override
  public Object getFileSystem() {
    return fs;
  }

  @Override
  public Object getConf() {
    return fs.getConf();
  }

  @Override
  public OutputStream create(HoodieLocation location) throws IOException {
    return fs.create(convertHoodieLocationToPath(location));
  }

  @Override
  public boolean createNewFile(HoodieLocation location) throws IOException {
    return fs.createNewFile(convertHoodieLocationToPath(location));
  }

  private Path convertHoodieLocationToPath(HoodieLocation loc) {
    return new Path(loc.toUri());
  }

  private HoodieLocation convertPathToHoodieLocation(Path path) {
    return new HoodieLocation(path.toUri());
  }

  private HoodieFileStatus convertToHoodieFileStatus(FileStatus fileStatus) {
    return new HoodieFileStatus(
        convertPathToHoodieLocation(fileStatus.getPath()),
        fileStatus.getLen(),
        fileStatus.isDirectory(),
        fileStatus.getModificationTime());
  }

  @Override
  public void close() throws IOException {
    fs.close();
  }
}
