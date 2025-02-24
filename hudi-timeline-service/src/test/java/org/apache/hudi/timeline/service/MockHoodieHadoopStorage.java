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

package org.apache.hudi.timeline.service;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * A mock implementation of {@link HoodieHadoopStorage} that validates the storage scheme and authority
 * before delegating operations to the superclass.
 * <p>
 * This class is primarily used for testing and ensures that all storage operations conform
 * to the expected scheme and authority.
 * </p>
 */
public class MockHoodieHadoopStorage extends HoodieHadoopStorage {

  private final String scheme;
  private final String authority;

  public MockHoodieHadoopStorage(StoragePath path, StorageConfiguration<?> conf) {
    super(convertToHadoopPath(path), conf);
    this.scheme = path.toUri().getScheme();
    this.authority = path.toUri().getAuthority();
  }

  private static StoragePath convertToHadoopPath(StoragePath storagePath) {
    return new StoragePath(storagePath.toUri().getPath());
  }

  @Override
  public String getScheme() {
    return super.getScheme();
  }

  @Override
  public boolean exists(StoragePath path) throws IOException {
    return super.exists(validateAndConvertPath(path));
  }

  @Override
  public boolean createDirectory(StoragePath path) throws IOException {
    return super.createDirectory(validateAndConvertPath(path));
  }

  @Override
  public OutputStream create(StoragePath path, boolean overwrite) throws IOException {
    return super.create(validateAndConvertPath(path), overwrite);
  }

  @Override
  public OutputStream create(StoragePath path) throws IOException {
    return super.create(validateAndConvertPath(path));
  }

  @Override
  public List<StoragePathInfo> listDirectEntries(StoragePath path) throws IOException {
    return super.listDirectEntries(validateAndConvertPath(path));
  }

  private StoragePath validateAndConvertPath(StoragePath path) {
    ValidationUtils.checkArgument(scheme == null || scheme.equals(path.toUri().getScheme()),
        String.format("Invalid scheme, expected %s, found %s", scheme, path.toUri().getScheme()));
    ValidationUtils.checkArgument(authority == null || authority.equals(path.toUri().getAuthority()),
        String.format("Invalid authority, expected %s, found %s", authority, path.toUri().getAuthority()));

    return convertToHadoopPath(path);
  }
}
